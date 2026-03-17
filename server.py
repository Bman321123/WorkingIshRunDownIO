import asyncio
import json
import time
import threading
from typing import Any
from datetime import datetime, timezone

from aiohttp import web

import therundown

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
ENABLE_AUTO_SCAN: bool = False
DELTA_POLL_INTERVAL: int = 10  # seconds between delta polls
SNAPSHOT_MAX_AGE: int = 300    # 5 min — re-bootstrap snapshot if older


def _now_ms() -> int:
    return int(time.time() * 1000)


@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        try:
            resp = await handler(request)
        except web.HTTPException as ex:
            resp = ex
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def _serialize(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    return obj


# ──────────────────────────────────────────────
# IN-MEMORY EVENT STORE
# Maintains the latest state of all events per sport,
# updated by both snapshot and delta calls.
# ──────────────────────────────────────────────
class EventStore:
    def __init__(self):
        self._lock = threading.Lock()
        # sport_id -> { event_id -> event_dict }
        self.events: dict[int, dict[str, dict]] = {}
        # sport_id -> delta cursor string
        self.cursors: dict[int, str] = {}
        # sport_id -> timestamp of last snapshot
        self.snapshot_ts: dict[int, float] = {}
        # Timestamp of the most recent successfully fetched data
        self.last_update_ts: float = 0.0
        # Datapoint budget tracking
        self.dp_remaining: str = "?"

    def set_snapshot(self, sport_id: int, events: list[dict], cursor: str | None):
        with self._lock:
            by_id = {}
            for e in events:
                eid = e.get("event_id")
                if eid:
                    by_id[eid] = e
            self.events[sport_id] = by_id
            if cursor:
                self.cursors[sport_id] = cursor
            self.snapshot_ts[sport_id] = time.time()
            self.last_update_ts = time.time()

    def merge_delta_events(self, sport_id: int, delta_events: list[dict], new_cursor: str | None):
        """Merge event-level delta updates."""
        with self._lock:
            store = self.events.setdefault(sport_id, {})
            for de in delta_events:
                eid = de.get("event_id")
                if eid:
                    store[eid] = de
            if new_cursor:
                self.cursors[sport_id] = new_cursor
            self.last_update_ts = time.time()

    def get_events(self, sport_id: int) -> list[dict]:
        with self._lock:
            return list(self.events.get(sport_id, {}).values())

    def needs_bootstrap(self, sport_id: int) -> bool:
        ts = self.snapshot_ts.get(sport_id, 0)
        return (time.time() - ts) > SNAPSHOT_MAX_AGE

    def freshest_update_age(self) -> float:
        """Seconds since last successful data update."""
        if self.last_update_ts == 0:
            return float("inf")
        return time.time() - self.last_update_ts


_STORE = EventStore()


# ──────────────────────────────────────────────
# SCAN: SNAPSHOT + ANALYZE
# ──────────────────────────────────────────────
def scan_arbs_once(sport_ids: list[int]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Fetch fresh events (snapshot + delta merge), then run arbitrage analysis.
    Returns (arbs, raw_lines).
    """
    # Ensure affiliate names include the fallback
    fresh = therundown.fetch_affiliates()
    if fresh:
        merged = dict(therundown._KNOWN_BOOKS_FALLBACK)
        merged.update(fresh)
        therundown.KNOWN_BOOKS = merged

    client = therundown.RundownClient(therundown.API_KEY, therundown.USE_RAPIDAPI)
    today = therundown.datetime.date.today().strftime("%Y-%m-%d")

    for sport_id in sport_ids:
        # Only fetch a fresh snapshot if the store is stale or empty
        if _STORE.needs_bootstrap(sport_id):
            try:
                data = client.get_events(sport_id, today)
                events = (data or {}).get("events") or []
                cursor = (data or {}).get("meta", {}).get("delta_last_id")
                _STORE.set_snapshot(sport_id, events, cursor)
                _STORE.dp_remaining = client.last_headers.get("X-Datapoints-Remaining", "?")
                print(f"  SNAPSHOT :: sport {sport_id} loaded {len(events)} events (cursor={str(cursor)[:12]}…)")
                time.sleep(1.2)
            except Exception as e:
                print(f"  SNAPSHOT :: sport {sport_id} failed: {e}")
                time.sleep(1.2)
        else:
            age = int(time.time() - _STORE.snapshot_ts.get(sport_id, 0))
            print(f"  SNAPSHOT :: sport {sport_id} using cached data ({age}s old, max {SNAPSHOT_MAX_AGE}s)")

        # Try a delta poll to get any recent changes
        cursor = _STORE.cursors.get(sport_id)
        if cursor:
            try:
                delta_data = client.get_markets_delta(sport_id, cursor)
                deltas = (delta_data or {}).get("deltas") or []
                new_cursor = (delta_data or {}).get("meta", {}).get("delta_last_id", cursor)
                _STORE.cursors[sport_id] = new_cursor
                _STORE.dp_remaining = client.last_headers.get("X-Datapoints-Remaining", "?")

                if deltas:
                    print(f"  DELTA :: sport {sport_id} got {len(deltas)} price changes (cursor → {str(new_cursor)[:12]}…)")
                    # Markets delta returns individual price changes, not full events.
                    # We need to re-fetch the full snapshot to get the merged state.
                    # But we can update the cursor so the next snapshot is fresher.
                else:
                    print(f"  DELTA :: sport {sport_id} no changes (cursor → {str(new_cursor)[:12]}…)")
                time.sleep(1.2)
            except Exception as e:
                print(f"  DELTA :: sport {sport_id} failed ({e}), cursor may be stale — will re-bootstrap next scan")
                _STORE.snapshot_ts[sport_id] = 0  # Force re-bootstrap
                time.sleep(1.2)

    # Analyze all events in the store
    all_results: list[dict] = []
    all_raw_lines: list[dict] = []
    all_best_lines: list[dict] = []

    for sport_id in sport_ids:
        events = _STORE.get_events(sport_id)
        sport_name = therundown.ALL_SPORTS.get(sport_id, str(sport_id))

        # Filter out finished games AND games that have already started
        now_utc = datetime.now(timezone.utc)
        active_events = []
        for evt in events:
            # Skip completed games
            status = (evt.get("score") or {}).get("event_status", "")
            if isinstance(status, str) and status.lower() in {"final", "complete", "closed", "in_progress", "in progress"}:
                continue

            # Skip games that started more than 15 min ago (odds are unreliable)
            start_raw = evt.get("event_date") or evt.get("event_date_start")
            if start_raw:
                try:
                    start_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                    mins_since_start = (now_utc - start_dt).total_seconds() / 60.0
                    if mins_since_start > 15:
                        continue  # Game already underway, odds are stale
                except Exception:
                    pass

            active_events.append(evt)

        if len(events) != len(active_events):
            print(f"  FILTER :: {sport_name}: {len(events)} total → {len(active_events)} active (dropped {len(events) - len(active_events)} started/finished)")

        books_in_batch: set[str] = set()
        for evt in active_events:
            new_arbs, new_lines = therundown.analyze_event(evt, sport_name)
            best_lines_for_event = therundown.compute_best_lines_for_event(evt, sport_name)

            for line in new_lines:
                books_in_batch.add(line["book"].lower())
                print(
                    f"     RAW LINE :: [{line['sport']}] {line['game']} | {line['book']} | "
                    f"{line['market_kind'].upper()} {line.get('line_label','')} {line['side']} ({line['odds_am']})"
                )
            all_results.extend(new_arbs)
            all_raw_lines.extend(new_lines)
            all_best_lines.extend(best_lines_for_event)

        if active_events and "betmgm" not in books_in_batch:
            print(f"  ⚠ BETMGM NOT FOUND in {sport_name} ({len(active_events)} events)")

    all_results.sort(key=lambda r: float(r.get("profit", 0.0)), reverse=True)

    data_age = _STORE.freshest_update_age()
    print(
        "  ── Scan complete: "
        f"{len(all_results)} arbs, {len(all_raw_lines)} lines, {len(all_best_lines)} best-lines, "
        f"data age: {int(data_age)}s, dp_remaining: {_STORE.dp_remaining}"
    )

    return all_results, all_raw_lines, all_best_lines


# ──────────────────────────────────────────────
# STATE + HANDLERS
# ──────────────────────────────────────────────
class ArbState:
    def __init__(self):
        self.arbs: list[dict] = []
        self.lines: list[dict] = []
        self.best_lines: list[dict] = []
        self.last_scan_ms: int | None = None
        self.last_error: str | None = None
        self.last_scan_sports: list[str] = []


async def handle_health(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    return web.json_response({
        "ok": True,
        "lastScanMs": state.last_scan_ms,
        "sports": state.last_scan_sports,
        "count": len(state.arbs),
        "error": state.last_error,
        "dataAge": int(_STORE.freshest_update_age()),
        "dpRemaining": _STORE.dp_remaining,
    })


async def handle_arbs(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    return web.json_response({
        "arbs": state.arbs,
        "lines": state.lines,
        "bestLines": state.best_lines,
        "dataAge": int(_STORE.freshest_update_age()),
        "dpRemaining": _STORE.dp_remaining,
    }, dumps=lambda x: json.dumps(x, default=_serialize))


def _sport_name_by_id() -> dict[int, str]:
    return {int(k): str(v) for k, v in (therundown.ALL_SPORTS or {}).items()}


def _sport_id_by_name() -> dict[str, int]:
    out: dict[str, int] = {}
    for sid, name in _sport_name_by_id().items():
        out[name.lower()] = sid
    return out


async def handle_scan_now(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    sports = payload.get("sports") if isinstance(payload, dict) else None
    if not isinstance(sports, list):
        return web.json_response({"ok": False, "error": "Expected JSON body: {\"sports\": [\"NBA\", ...]}"},
                                 status=400)

    name_to_id = _sport_id_by_name()
    selected_ids: list[int] = []
    selected_names: list[str] = []
    for s in sports:
        if not isinstance(s, str):
            continue
        sid = name_to_id.get(s.lower())
        if sid is None:
            continue
        selected_ids.append(sid)
        selected_names.append(_sport_name_by_id().get(sid, s))

    if not selected_ids:
        return web.json_response(
            {"ok": False, "error": "No valid sports selected", "supported": list(_sport_id_by_name().keys())},
            status=400,
        )

    try:
        state.last_error = None
        arbs, lines, best_lines = await asyncio.to_thread(scan_arbs_once, selected_ids)
        state.arbs = arbs
        state.lines = lines
        state.best_lines = best_lines
        state.last_scan_ms = _now_ms()
        state.last_scan_sports = selected_names
        resp = web.json_response(
            {
                "ok": True,
                "sports": selected_names,
                "lastScanMs": state.last_scan_ms,
                "count": len(state.arbs),
                "arbs": state.arbs,
                "lines": state.lines,
                "bestLines": state.best_lines,
                "dataAge": int(_STORE.freshest_update_age()),
                "dpRemaining": _STORE.dp_remaining,
            },
            dumps=lambda x: json.dumps(x, default=_serialize),
        )
    except Exception as e:
        state.last_error = str(e)
        resp = web.json_response({"ok": False, "error": state.last_error}, status=500)

    return resp


async def scan_loop(app: web.Application):
    state: ArbState = app["state"]
    sport_ids = app["sport_ids"]
    interval_s = app["interval_s"]

    while True:
        try:
            state.last_error = None
            arbs, lines, best_lines = scan_arbs_once(sport_ids)
            state.arbs = arbs
            state.lines = lines
            state.best_lines = best_lines
            state.last_scan_ms = _now_ms()
            state.last_scan_sports = [_sport_name_by_id().get(sid, str(sid)) for sid in sport_ids]
        except Exception as e:
            state.last_error = str(e)
        await asyncio.sleep(interval_s)


def create_app(sport_ids: list[int], interval_s: float) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["state"] = ArbState()
    app["sport_ids"] = sport_ids
    app["interval_s"] = interval_s

    app.router.add_get("/health", handle_health)
    app.router.add_get("/arbs", handle_arbs)
    app.router.add_post("/scan-now", handle_scan_now)

    async def on_startup(app: web.Application):
        if ENABLE_AUTO_SCAN:
            app["scan_task"] = asyncio.create_task(scan_loop(app))

    async def on_cleanup(app: web.Application):
        task = app.get("scan_task")
        if task:
            task.cancel()
            try:
                await task
            except Exception:
                pass

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    import sys
    import traceback
    print("Initialize Server: Checking port binding on 0.0.0.0:3030...")
    try:
        app = create_app(sport_ids=[4], interval_s=5.0)
        web.run_app(app, host="0.0.0.0", port=3030)
    except Exception as e:
        print(f"FATAL STARTUP ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
