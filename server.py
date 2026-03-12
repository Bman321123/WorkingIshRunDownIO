import asyncio
import json
import time
from typing import Any

from aiohttp import web

import therundown

ENABLE_AUTO_SCAN: bool = False


def _now_ms() -> int:
    return int(time.time() * 1000)


def _serialize(obj: Any) -> Any:
    # Convert datetimes (fresh_ts/stale_ts) to ISO strings for JSON.
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    return obj


def scan_arbs_once(sport_ids: list[int]) -> list[dict]:
    """
    Minimal wrapper around existing logic.
    Does NOT change therundown's extraction/arb math; it just runs it and returns results.
    """
    # Ensure affiliate names are populated (best-effort).
    fresh = therundown.fetch_affiliates()
    if fresh:
        therundown.KNOWN_BOOKS = fresh

    client = therundown.RundownClient(therundown.API_KEY, therundown.USE_RAPIDAPI)
    dates = [
        (therundown.datetime.date.today() + therundown.datetime.timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(therundown.DAYS_AHEAD + 1)
    ]

    all_results: list[dict] = []

    def fetch_events(date_str: str, sport_id: int) -> list[dict]:
        data = client.get_events(sport_id, date_str)
        return (data or {}).get("events") or []

    def filter_live(events: list[dict]) -> list[dict]:
        if not therundown.LIVE_ONLY:
            return events
        now_utc = therundown.datetime.datetime.now(therundown.datetime.timezone.utc)
        kept = []
        for event in events:
            status = (event.get("score") or {}).get("event_status")
            if isinstance(status, str) and status.lower() in {"final", "complete", "closed"}:
                continue
            start_raw = event.get("event_date") or event.get("event_date_start")
            try:
                if not start_raw:
                    continue
                start_dt = therundown.datetime.datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=therundown.datetime.timezone.utc)
                delta_min = (now_utc - start_dt).total_seconds() / 60.0
                if delta_min <= therundown.PRE_GAME_MINUTES:
                    kept.append(event)
            except Exception:
                continue
        return kept

    for sport_id in sport_ids:
        # Try today first, fallback to tomorrow if no arbs
        raw_today = fetch_events(dates[0], sport_id)
        filtered_today = filter_live(raw_today)
        arbs_today: list[dict] = []
        for evt in filtered_today:
            arbs_today.extend(therundown.analyze_event(evt, therundown.ALL_SPORTS.get(sport_id, str(sport_id))))
        if arbs_today:
            all_results.extend(arbs_today)
            continue

        if len(dates) > 1:
            raw_tmrw = fetch_events(dates[1], sport_id)
            filtered_tmrw = filter_live(raw_tmrw)
            for evt in filtered_tmrw:
                all_results.extend(
                    therundown.analyze_event(evt, therundown.ALL_SPORTS.get(sport_id, str(sport_id)))
                )

    # Only true arbs (profit > 0) should already be enforced by ARB_THRESHOLD=0.0.
    # Keep profit desc ordering here so frontend gets already-ranked data.
    all_results.sort(key=lambda r: float(r.get("profit", 0.0)), reverse=True)
    return all_results


class ArbState:
    def __init__(self):
        self.arbs: list[dict] = []
        self.last_scan_ms: int | None = None
        self.last_error: str | None = None
        self.last_scan_sports: list[str] = []


async def handle_health(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    return web.json_response(
        {
            "ok": True,
            "lastScanMs": state.last_scan_ms,
            "sports": state.last_scan_sports,
            "count": len(state.arbs),
            "error": state.last_error,
        }
    )


async def handle_arbs(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    # Add CORS header for local dev.
    resp = web.json_response(state.arbs, dumps=lambda x: json.dumps(x, default=_serialize))
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def _sport_name_by_id() -> dict[int, str]:
    return {int(k): str(v) for k, v in (therundown.ALL_SPORTS or {}).items()}


def _sport_id_by_name() -> dict[str, int]:
    # Accept exact labels from therundown.ALL_SPORTS (e.g. "NBA") case-insensitive.
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

    # One-time scan (blocking scan runs in thread to avoid blocking event loop).
    try:
        state.last_error = None
        arbs = await asyncio.to_thread(scan_arbs_once, selected_ids)
        state.arbs = arbs
        state.last_scan_ms = _now_ms()
        state.last_scan_sports = selected_names
        resp = web.json_response(
            {
                "ok": True,
                "sports": selected_names,
                "lastScanMs": state.last_scan_ms,
                "count": len(state.arbs),
                "arbs": state.arbs,
            },
            dumps=lambda x: json.dumps(x, default=_serialize),
        )
    except Exception as e:
        state.last_error = str(e)
        resp = web.json_response({"ok": False, "error": state.last_error}, status=500)

    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


async def scan_loop(app: web.Application):
    state: ArbState = app["state"]
    sport_ids = app["sport_ids"]
    interval_s = app["interval_s"]

    while True:
        try:
            state.last_error = None
            state.arbs = scan_arbs_once(sport_ids)
            state.last_scan_ms = _now_ms()
            state.last_scan_sports = [_sport_name_by_id().get(sid, str(sid)) for sid in sport_ids]
        except Exception as e:
            state.last_error = str(e)
        await asyncio.sleep(interval_s)


def create_app(sport_ids: list[int], interval_s: float) -> web.Application:
    app = web.Application()
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
    # Default: NBA only, scan every 5 seconds.
    app = create_app(sport_ids=[4], interval_s=5.0)
    web.run_app(app, host="0.0.0.0", port=8080)

