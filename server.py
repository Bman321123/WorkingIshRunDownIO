import asyncio
import json
import time
import threading
import logging
import re
from typing import Any
from datetime import datetime, timezone

from aiohttp import web
from rapidfuzz import fuzz

import bovada_scraper
import therundown

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
ENABLE_AUTO_SCAN: bool = False
DELTA_POLL_INTERVAL: int = 10  # seconds between delta polls
SNAPSHOT_MAX_AGE: int = 300    # 5 min — re-bootstrap snapshot if older

logger = logging.getLogger(__name__)

# Sports shared across TheRundown and Bovada feeds.
_RUNDOWN_TO_BOVADA_SPORT = {
    "NFL": "nfl",
    "NBA": "nba",
    "NCAAB": "ncaab",
    "MLB": "mlb",
    "NHL": "nhl",
}

_BOVADA_TO_DISPLAY_SPORT = {
    "nfl": "NFL",
    "nba": "NBA",
    "ncaab": "NCAAB",
    "mlb": "MLB",
    "nhl": "NHL",
}

_NON_ALNUM = re.compile(r"[^a-z0-9\s]")
_MULTISPACE = re.compile(r"\s+")
_FUZZY_MATCH_THRESHOLD = 80.0

# Tier-2 alias map for common shorthand/abbreviations.
_TEAM_ALIAS_MAP: dict[str, str] = {
    "mia": "miami heat",
    "mia heat": "miami heat",
    "nyk": "new york knicks",
    "la lakers": "los angeles lakers",
    "lal": "los angeles lakers",
    "la clippers": "los angeles clippers",
    "lac": "los angeles clippers",
    "okc": "oklahoma city thunder",
    "gsw": "golden state warriors",
    "tb": "tampa bay buccaneers",
    "ne": "new england patriots",
    "sf": "san francisco 49ers",
    "kc": "kansas city chiefs",
    "wsh": "washington capitals",
    "vgk": "vegas golden knights",
}


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
# MODULE-LEVEL CLIENT (one Session for process lifetime)
# ──────────────────────────────────────────────
# Shared across all scan_arbs_once() calls. scan_arbs_once() is called
# serially (asyncio.to_thread, one at a time), so no locking is needed.
_CLIENT = therundown.RundownClient(therundown.API_KEY, therundown.USE_RAPIDAPI)

# Affiliate cache — /affiliates data changes at most monthly; refresh daily.
_AFFILIATES_TTL: int = 86400
_affiliates_last_fetched: float = 0.0


def _refresh_affiliates_if_stale() -> None:
    """Fetch affiliate names at most once every _AFFILIATES_TTL seconds."""
    global _affiliates_last_fetched
    if time.time() - _affiliates_last_fetched < _AFFILIATES_TTL:
        return
    fresh = therundown.fetch_affiliates()
    if fresh:
        merged = dict(therundown._KNOWN_BOOKS_FALLBACK)
        merged.update(fresh)
        therundown.KNOWN_BOOKS = merged
        _affiliates_last_fetched = time.time()
        print(f"  AFFILIATES :: refreshed ({len(fresh)} books cached for {_AFFILIATES_TTL}s)")
    else:
        # Keep existing KNOWN_BOOKS and retry on next scan.
        print("  AFFILIATES :: refresh failed, retaining existing book names")


# ──────────────────────────────────────────────
# SCAN: SNAPSHOT + ANALYZE
# ──────────────────────────────────────────────
def scan_arbs_once(sport_ids: list[int]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Fetch fresh events (snapshot + delta merge), then run arbitrage analysis.
    Returns (arbs, raw_lines).
    """
    _refresh_affiliates_if_stale()

    client = _CLIENT
    today_dt = therundown.datetime.date.today()
    today = today_dt.strftime("%Y-%m-%d")
    tomorrow = (today_dt + therundown.datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    def _filter_active_events(events: list[dict]) -> list[dict]:
        """Keep only events that are not completed and not too far in-progress."""
        now_utc = datetime.now(timezone.utc)
        active_events: list[dict] = []
        for evt in events:
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
                        continue
                except Exception:
                    pass

            active_events.append(evt)
        return active_events

    def _unwrap_event_payload(event_payload: dict) -> dict:
        """
        V2 single-event responses can be either:
          - { "events": [ {...} ] }
          - { ...event fields... }
        """
        if not isinstance(event_payload, dict):
            return {}
        events_list = event_payload.get("events")
        if isinstance(events_list, list) and events_list and isinstance(events_list[0], dict):
            return events_list[0]
        return event_payload

    def _extract_prop_markets_from_event_payload(event_payload: dict) -> list[dict]:
        event_obj = _unwrap_event_payload(event_payload)
        markets = event_obj.get("markets") or []
        if not isinstance(markets, list):
            return []
        prop_markets: list[dict] = []
        for market in markets:
            if not isinstance(market, dict):
                continue
            if market.get("market_id") in therundown.PROP_MARKET_IDS:
                prop_markets.append(market)
        return prop_markets

    def _market_signature(market: dict) -> tuple:
        """
        Build a stable-ish signature so prop-market dedupe survives same market_id
        with multiple players/line variants.
        """
        participants = market.get("participants") or []
        participant_names: list[str] = []
        line_values: list[str] = []
        if isinstance(participants, list):
            for p in participants:
                if not isinstance(p, dict):
                    continue
                participant_names.append(str(p.get("name") or ""))
                for ln in (p.get("lines") or []):
                    if not isinstance(ln, dict):
                        continue
                    line_values.append(str(ln.get("value")))
        return (
            market.get("market_id"),
            market.get("period_id"),
            market.get("name"),
            tuple(sorted(participant_names)[:6]),
            tuple(sorted(line_values)[:10]),
        )

    def _discover_sport_prop_market_ids(sport_id: int, date_str: str) -> set[int]:
        """
        Discover which prop markets are active for a sport/date and intersect
        with our configured prop allowlist.
        """
        try:
            payload = client.get_available_markets_by_date(date_str, offset="300") or {}
            sport_markets = payload.get(str(sport_id)) or payload.get(sport_id) or []
            out: set[int] = set()
            for m in sport_markets:
                if not isinstance(m, dict):
                    continue
                try:
                    mid = int(m.get("id"))
                except (TypeError, ValueError):
                    continue
                # Keep only configured prop IDs that are currently available.
                if mid in therundown.PROP_MARKET_IDS and bool(m.get("proposition")):
                    out.add(mid)
            return out
        except Exception as e:
            print(f"  PROP DISCOVERY :: sport {sport_id} date={date_str} failed: {e}")
            return set()

    def _discover_event_prop_market_ids(event_refs: list[str]) -> set[int]:
        """
        Discover prop market IDs available for one event.
        """
        for ref in event_refs:
            try:
                catalog_payload = client.get_event_markets(ref) or []
                rows = (
                    catalog_payload
                    if isinstance(catalog_payload, list)
                    else (catalog_payload.get("markets") or catalog_payload.get("data") or [])
                )
                discovered: set[int] = set()
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    rid = row.get("id")
                    try:
                        rid_i = int(rid)
                    except (TypeError, ValueError):
                        continue
                    if rid_i in therundown.PROP_MARKET_IDS:
                        discovered.add(rid_i)
                if discovered:
                    return discovered
            except Exception:
                continue
        return set()

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
                time.sleep(therundown.get_safe_delay())
            except Exception as e:
                print(f"  SNAPSHOT :: sport {sport_id} failed: {e}")
                time.sleep(therundown.get_safe_delay())
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
                time.sleep(therundown.get_safe_delay())
            except Exception as e:
                print(f"  DELTA :: sport {sport_id} failed ({e}), cursor may be stale — will re-bootstrap next scan")
                _STORE.snapshot_ts[sport_id] = 0  # Force re-bootstrap
                time.sleep(therundown.get_safe_delay())

    # Analyze all events in the store
    all_results: list[dict] = []
    all_raw_lines: list[dict] = []
    all_best_lines: list[dict] = []

    for sport_id in sport_ids:
        events = _STORE.get_events(sport_id)
        sport_name = therundown.ALL_SPORTS.get(sport_id, str(sport_id))
        active_events = _filter_active_events(events)
        analysis_date = today

        # If today's slate is fully started/closed, pull tomorrow and merge.
        if not active_events:
            try:
                print(f"  FALLBACK :: {sport_name} had 0 active events for {today}, fetching {tomorrow}")
                data_tmr = client.get_events(sport_id, tomorrow)
                ev_tmr = (data_tmr or {}).get("events") or []
                cur_tmr = (data_tmr or {}).get("meta", {}).get("delta_last_id")
                merged = {e.get("event_id"): e for e in events if e.get("event_id")}
                for e in ev_tmr:
                    eid = e.get("event_id")
                    if eid:
                        merged[eid] = e
                merged_events = list(merged.values())
                _STORE.set_snapshot(sport_id, merged_events, cur_tmr or _STORE.cursors.get(sport_id))
                _STORE.dp_remaining = client.last_headers.get("X-Datapoints-Remaining", "?")
                active_events = _filter_active_events(merged_events)
                analysis_date = tomorrow
                time.sleep(therundown.get_safe_delay())
            except Exception as e:
                print(f"  FALLBACK :: {sport_name} tomorrow fetch failed: {e}")

        if len(events) != len(active_events):
            print(f"  FILTER :: {sport_name}: {len(events)} total → {len(active_events)} active (dropped {len(events) - len(active_events)} started/finished)")

        # ── V2-compliant prop enrichment ────────────────────────────────────
        # 1) Discover available prop market IDs for this sport/date
        # 2) Discover event-specific prop market IDs
        # 3) Fetch props in <=12-ID chunks with two affiliate passes
        # 4) Merge back into evt["markets"]
        sport_prop_ids = _discover_sport_prop_market_ids(sport_id, analysis_date)
        if sport_prop_ids:
            print(
                f"  PROP DISCOVERY :: {sport_name} date={analysis_date} "
                f"available_prop_ids={sorted(sport_prop_ids)}"
            )
        else:
            print(
                f"  PROP DISCOVERY :: {sport_name} date={analysis_date} no prop IDs "
                f"from /sports/markets"
            )

        enrich_events_with_props = 0
        enrich_market_total = 0
        for evt in active_events:
            event_refs: list[str] = []
            if evt.get("event_uuid"):
                event_refs.append(str(evt.get("event_uuid")))
            if evt.get("event_id"):
                event_refs.append(str(evt.get("event_id")))
            if not event_refs:
                continue

            event_prop_ids = _discover_event_prop_market_ids(event_refs)
            target_prop_ids = sorted(event_prop_ids.intersection(sport_prop_ids) if sport_prop_ids else event_prop_ids)
            if not target_prop_ids:
                continue

            chunks = therundown.chunk_market_ids(target_prop_ids, chunk_size=12)
            evt_markets = evt.get("markets")
            if not isinstance(evt_markets, list):
                evt_markets = []
            seen_signatures = {
                _market_signature(m)
                for m in evt_markets
                if isinstance(m, dict)
            }
            merged_count_for_event = 0

            for chunk in chunks:
                fetched_chunk_markets: list[dict] = []

                # Pass A: priority books only
                for ref in event_refs:
                    try:
                        payload = client.get_event_with_markets(
                            ref,
                            market_ids=chunk,
                            affiliate_ids="22,19,23",
                            main_line="false",  # keep alt lines
                        ) or {}
                        fetched_chunk_markets.extend(_extract_prop_markets_from_event_payload(payload))
                        if fetched_chunk_markets:
                            break
                    except Exception:
                        continue

                # Pass B fallback: all available books
                if not fetched_chunk_markets:
                    for ref in event_refs:
                        try:
                            payload = client.get_event_with_markets(
                                ref,
                                market_ids=chunk,
                                affiliate_ids=None,
                                main_line="false",  # keep alt lines
                            ) or {}
                            fetched_chunk_markets.extend(_extract_prop_markets_from_event_payload(payload))
                            if fetched_chunk_markets:
                                break
                        except Exception:
                            continue

                for m in fetched_chunk_markets:
                    sig = _market_signature(m)
                    if sig in seen_signatures:
                        continue
                    evt_markets.append(m)
                    seen_signatures.add(sig)
                    merged_count_for_event += 1

            evt["markets"] = evt_markets
            if merged_count_for_event > 0:
                enrich_events_with_props += 1
                enrich_market_total += merged_count_for_event
                if therundown.PROP_DIAGNOSTICS:
                    print(
                        f"  PROP EVENT :: {sport_name} eid={evt.get('event_id')} "
                        f"event_prop_ids={sorted(event_prop_ids)} requested_chunks={chunks} "
                        f"merged_markets={merged_count_for_event}"
                    )

        print(
            f"  PROP ENRICH :: {sport_name} events_enriched={enrich_events_with_props}/{len(active_events)} "
            f"markets_merged={enrich_market_total}"
        )

        books_in_batch: set[str] = set()
        prop_raw_count = 0
        prop_best_count = 0
        for evt in active_events:
            new_arbs, new_lines = therundown.analyze_event(evt, sport_name)
            best_lines_for_event = therundown.compute_best_lines_for_event(evt, sport_name)
            prop_raw, prop_best = therundown.parse_player_props(evt, sport_name)
            new_lines.extend(prop_raw)
            best_lines_for_event.extend(prop_best)
            prop_raw_count += len(prop_raw)
            prop_best_count += len(prop_best)

            for line in new_lines:
                books_in_batch.add(line["book"].lower())
                print(
                    f"     RAW LINE :: [{line['sport']}] {line['game']} | {line['book']} | "
                    f"{line['market_kind'].upper()} {line.get('line_label','')} {line['side']} ({line['odds_am']})"
                )
            all_results.extend(new_arbs)
            all_raw_lines.extend(new_lines)
            all_best_lines.extend(best_lines_for_event)

        if active_events:
            print(
                f"  PROP SUMMARY :: {sport_name} events={len(active_events)} "
                f"raw_props={prop_raw_count} best_props={prop_best_count}"
            )
            if prop_raw_count == 0:
                print(
                    f"  ⚠ PROP SUMMARY :: {sport_name} has 0 parsed prop lines after enrichment "
                    f"(date={analysis_date})."
                )

        if active_events and "betmgm" not in books_in_batch:
            print(f"  ⚠ BETMGM NOT FOUND in {sport_name} ({len(active_events)} events)")

    all_results.sort(key=lambda r: float(r.get("profit", 0.0)), reverse=True)

    data_age = _STORE.freshest_update_age()
    data_age_display = "inf" if data_age == float("inf") else str(int(data_age))
    print(
        "  ── Scan complete: "
        f"{len(all_results)} arbs, {len(all_raw_lines)} lines, {len(all_best_lines)} best-lines, "
        f"data age: {data_age_display}s, dp_remaining: {_STORE.dp_remaining}"
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
    age = _STORE.freshest_update_age()
    return web.json_response({
        "ok": True,
        "lastScanMs": state.last_scan_ms,
        "sports": state.last_scan_sports,
        "count": len(state.arbs),
        "error": state.last_error,
        "dataAge": None if age == float("inf") else int(age),
        "dpRemaining": _STORE.dp_remaining,
    })


async def handle_arbs(request: web.Request) -> web.Response:
    state: ArbState = request.app["state"]
    age = _STORE.freshest_update_age()
    return web.json_response({
        "arbs": state.arbs,
        "lines": state.lines,
        "bestLines": state.best_lines,
        "dataAge": None if age == float("inf") else int(age),
        "dpRemaining": _STORE.dp_remaining,
    }, dumps=lambda x: json.dumps(x, default=_serialize))


def _sport_name_by_id() -> dict[int, str]:
    return {int(k): str(v) for k, v in (therundown.ALL_SPORTS or {}).items()}


def _sport_id_by_name() -> dict[str, int]:
    out: dict[str, int] = {}
    for sid, name in _sport_name_by_id().items():
        out[name.lower()] = sid
    return out


def _normalize_team_name(team_name: str) -> str:
    """
    Tier-1 normalization + tier-2 alias mapping.
    """
    normalized = (team_name or "").strip().lower()
    normalized = normalized.replace(".", " ")
    normalized = _NON_ALNUM.sub(" ", normalized)
    normalized = _MULTISPACE.sub(" ", normalized).strip()
    return _TEAM_ALIAS_MAP.get(normalized, normalized)


def _team_match_score(a: str, b: str) -> float:
    """
    Tier-3 fuzzy fallback after normalization/alias checks.
    """
    left = _normalize_team_name(a)
    right = _normalize_team_name(b)
    if not left or not right:
        return 0.0
    if left == right:
        return 100.0
    return float(fuzz.token_set_ratio(left, right))


def _normalize_rundown_events_for_matching(sport_ids: list[int]) -> list[dict[str, Any]]:
    """
    Build a matching-friendly view of the latest TheRundown events in memory.
    Shape aligns with bovada_scraper output:
      sport, home_team, away_team, start_time, event_url, markets
    """
    normalized: list[dict[str, Any]] = []
    for sport_id in sport_ids:
        sport_name = _sport_name_by_id().get(sport_id, str(sport_id))
        for event in _STORE.get_events(sport_id):
            try:
                _game, home_team, away_team = therundown._resolve_teams(event)
            except Exception:
                continue
            if not home_team or not away_team:
                continue
            start_time = event.get("event_date") or event.get("event_date_start") or ""
            normalized.append(
                {
                    "source": "therundown",
                    "sport": str(sport_name).lower(),
                    "home_team": home_team,
                    "away_team": away_team,
                    "start_time": start_time,
                    "event_url": "",
                    "markets": therundown.compute_best_lines_for_event(event, sport_name),
                }
            )
    return normalized


async def _fetch_bovada_events_for_sports(selected_names: list[str]) -> list[dict[str, Any]]:
    bovada_sports = [
        _RUNDOWN_TO_BOVADA_SPORT[name]
        for name in selected_names
        if name in _RUNDOWN_TO_BOVADA_SPORT
    ]
    if not bovada_sports:
        return []

    tasks = [bovada_scraper.fetch_bovada(sport) for sport in bovada_sports]
    nested = await asyncio.gather(*tasks, return_exceptions=True)
    events: list[dict[str, Any]] = []
    for entry in nested:
        if isinstance(entry, Exception):
            logger.warning("Bovada sport fetch failed: %s", entry)
            continue
        events.extend(entry)
    return events


def _match_intersection_events(
    bovada_events: list[dict[str, Any]],
    rundown_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Match events across books using:
      Tier1 normalization -> Tier2 alias map -> Tier3 fuzzy (>=80).
    Only intersection games are returned.
    """
    matched_games: list[dict[str, Any]] = []
    used_rundown_indices: set[int] = set()

    for bovada_event in bovada_events:
        b_sport = str(bovada_event.get("sport", "")).lower()
        b_home = str(bovada_event.get("home_team", ""))
        b_away = str(bovada_event.get("away_team", ""))
        if not b_home or not b_away:
            continue

        best_idx: int | None = None
        best_score = -1.0
        best_swapped = False

        for idx, rundown_event in enumerate(rundown_events):
            if idx in used_rundown_indices:
                continue
            if str(rundown_event.get("sport", "")).lower() != b_sport:
                continue

            r_home = str(rundown_event.get("home_team", ""))
            r_away = str(rundown_event.get("away_team", ""))
            if not r_home or not r_away:
                continue

            direct_home = _team_match_score(b_home, r_home)
            direct_away = _team_match_score(b_away, r_away)
            direct_score = min(direct_home, direct_away)

            swapped_home = _team_match_score(b_home, r_away)
            swapped_away = _team_match_score(b_away, r_home)
            swapped_score = min(swapped_home, swapped_away)

            if direct_score >= swapped_score:
                candidate_score = direct_score
                candidate_swapped = False
            else:
                candidate_score = swapped_score
                candidate_swapped = True

            if candidate_score >= _FUZZY_MATCH_THRESHOLD and candidate_score > best_score:
                best_score = candidate_score
                best_idx = idx
                best_swapped = candidate_swapped

        if best_idx is None:
            continue

        used_rundown_indices.add(best_idx)
        matched_rundown = rundown_events[best_idx]

        if best_swapped:
            home_team = str(bovada_event.get("away_team", ""))
            away_team = str(bovada_event.get("home_team", ""))
        else:
            home_team = str(bovada_event.get("home_team", ""))
            away_team = str(bovada_event.get("away_team", ""))

        consolidated = {
            "sport": b_sport,
            "start_time": bovada_event.get("start_time") or matched_rundown.get("start_time"),
            "home_team": home_team,
            "away_team": away_team,
            "matchScore": round(best_score, 2),
            "books": {
                "bovada": bovada_event.get("markets", []),
                "therundown": matched_rundown.get("markets", []),
            },
        }

        # INSERT ARBITRAGE AND BEST LINE MATH LOGIC HERE
        matched_games.append(consolidated)

    return matched_games


def _best_american_option(options: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [o for o in options if isinstance(o.get("american_odds"), int)]
    if not valid:
        return None
    positives = [o for o in valid if int(o["american_odds"]) > 0]
    negatives = [o for o in valid if int(o["american_odds"]) < 0]
    if positives:
        return max(positives, key=lambda o: int(o["american_odds"]))
    if negatives:
        return max(negatives, key=lambda o: int(o["american_odds"]))
    return None


def _bovada_events_to_raw_lines(bovada_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    market_kind_map = {"moneyline": "ml", "spread": "spread", "total": "total"}
    side_map = {"home": "Home", "away": "Away", "over": "Over", "under": "Under"}

    for event in bovada_events:
        sport = _BOVADA_TO_DISPLAY_SPORT.get(str(event.get("sport", "")).lower(), str(event.get("sport", "")).upper())
        home = str(event.get("home_team", ""))
        away = str(event.get("away_team", ""))
        game = f"{away} @ {home}".strip()

        for market in event.get("markets", []):
            market_type = str(market.get("market_type", "")).lower()
            market_kind = market_kind_map.get(market_type)
            if market_kind is None:
                continue

            line_value = market.get("line_value")
            if market_kind == "ml":
                line_label = "ML"
            elif line_value is None:
                line_label = ""
            else:
                line_label = f"{float(line_value):g}"

            odds_am = market.get("american_odds")
            if not isinstance(odds_am, int):
                continue

            lines.append(
                {
                    "sport": sport,
                    "game": game,
                    "market_kind": market_kind,
                    "line_label": line_label,
                    "side": side_map.get(str(market.get("selection", "")).lower(), str(market.get("selection", "")).title()),
                    "book": "bovada",
                    "odds_am": odds_am,
                    "updated_at": None,
                }
            )

    return lines


def _bovada_events_to_best_lines(bovada_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_lines: list[dict[str, Any]] = []
    for event in bovada_events:
        sport = _BOVADA_TO_DISPLAY_SPORT.get(str(event.get("sport", "")).lower(), str(event.get("sport", "")).upper())
        home_team = str(event.get("home_team", ""))
        away_team = str(event.get("away_team", ""))
        game = f"{away_team} @ {home_team}".strip()
        markets: list[dict[str, Any]] = [m for m in event.get("markets", []) if isinstance(m, dict)]
        if not markets:
            continue

        moneyline_home = _best_american_option(
            [m for m in markets if m.get("market_type") == "moneyline" and str(m.get("selection")).lower() == "home"]
        )
        moneyline_away = _best_american_option(
            [m for m in markets if m.get("market_type") == "moneyline" and str(m.get("selection")).lower() == "away"]
        )
        if moneyline_home and moneyline_away:
            best_lines.append(
                {
                    "type": "moneyline",
                    "sport": sport,
                    "game": game,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home": {"book": "bovada", "odds_am": int(moneyline_home["american_odds"])},
                    "away": {"book": "bovada", "odds_am": int(moneyline_away["american_odds"])},
                }
            )

        # Spread: one record per signed side and line.
        for spread in [m for m in markets if m.get("market_type") == "spread"]:
            odds_am = spread.get("american_odds")
            if not isinstance(odds_am, int):
                continue
            line_value = spread.get("line_value")
            side = str(spread.get("selection", "")).lower()
            if side not in {"home", "away"}:
                continue
            team = home_team if side == "home" else away_team
            best_lines.append(
                {
                    "type": "spread",
                    "line": line_value,
                    "side": side,
                    "team": team,
                    "sport": sport,
                    "game": game,
                    "pick": {"book": "bovada", "odds_am": odds_am},
                }
            )

        # Totals: pair over/under by line value.
        totals_by_line: dict[float, dict[str, list[dict[str, Any]]]] = {}
        for total in [m for m in markets if m.get("market_type") == "total"]:
            line_value = total.get("line_value")
            if line_value is None:
                continue
            try:
                key = float(line_value)
            except (TypeError, ValueError):
                continue
            bucket = totals_by_line.setdefault(key, {"over": [], "under": []})
            sel = str(total.get("selection", "")).lower()
            if sel in bucket:
                bucket[sel].append(total)

        for line_value, buckets in totals_by_line.items():
            best_over = _best_american_option(buckets["over"])
            best_under = _best_american_option(buckets["under"])
            if not best_over or not best_under:
                continue
            best_lines.append(
                {
                    "type": "total",
                    "line": line_value,
                    "sport": sport,
                    "game": game,
                    "over": {"book": "bovada", "odds_am": int(best_over["american_odds"])},
                    "under": {"book": "bovada", "odds_am": int(best_under["american_odds"])},
                }
            )

    return best_lines


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
        rundown_task = asyncio.to_thread(scan_arbs_once, selected_ids)
        bovada_task = asyncio.wait_for(
            _fetch_bovada_events_for_sports(selected_names),
            timeout=10.0,
        )
        rundown_result, bovada_result = await asyncio.gather(
            rundown_task,
            bovada_task,
            return_exceptions=True,
        )

        if isinstance(rundown_result, Exception):
            raise rundown_result

        arbs, lines, best_lines = rundown_result
        bovada_error: str | None = None
        bovada_events: list[dict[str, Any]] = []
        if isinstance(bovada_result, Exception):
            bovada_error = str(bovada_result)
            logger.warning("Bovada scan failed or timed out: %s", bovada_error)
        else:
            bovada_events = bovada_result

        rundown_events = _normalize_rundown_events_for_matching(selected_ids)
        matched_games = _match_intersection_events(bovada_events, rundown_events)
        bovada_lines = _bovada_events_to_raw_lines(bovada_events)
        bovada_best_lines = _bovada_events_to_best_lines(bovada_events)

        state.arbs = arbs
        state.lines = lines + bovada_lines
        state.best_lines = best_lines + bovada_best_lines
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
                "matchedGames": matched_games,
                "bovadaError": bovada_error,
                "dataAge": None if _STORE.freshest_update_age() == float("inf") else int(_STORE.freshest_update_age()),
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
