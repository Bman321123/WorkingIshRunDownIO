"""
TheRundown API - Sports Arbitrage Finder (Full Line Coverage)
=============================================================
python therundown.py              # all sports
python therundown.py 4            # NBA only
python therundown.py 4 6 5        # NBA + NHL + NCAAB
"""

import os
import sys
import time
import datetime
import json
import requests
from prettytable import PrettyTable

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
API_KEY: str = os.getenv(
    "THERUNDOWN_API_KEY",
    "b05e571893ea8db8950f9b6497d480ad7a5547ddcbc5064131a8e3bfe713599d"
)
USE_RAPIDAPI:   bool  = False
TOTAL_STAKE:    float = 100.0
POLL_INTERVAL:  int   = 60
REQUEST_DELAY:  float = 1.2
DAYS_AHEAD:     int   = 1

# Live-game / near-arb configuration
LIVE_ONLY:        bool = True
# only consider games that have not started yet (with tolerance)
PRE_GAME_MINUTES: int  = 10     # include games starting within this many minutes of now
SHOW_NEAR_ARBS:   bool = False  # when False, hide near-arb section in output

# Only keep true, positive-profit arbs (arb < 1.0)
ARB_THRESHOLD: float = 0.0

# Treat anything above this profit% as suspicious and don't report it as a real arb
MAX_PROFIT_CAP: float = 10.0

# When True, prints extra per-opportunity detail for manual verification
VALIDATION_MODE: bool = False

ALL_SPORTS = {
    1: "NCAAF", 2: "NFL",    3: "MLB",
    4: "NBA",   5: "NCAAB",  6: "NHL",
    8: "NCAAWB", 9: "MMA",
}

SPORT_IDS = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else list(ALL_SPORTS.keys())

# Fallback book IDs if affiliates fetch fails
_KNOWN_BOOKS_FALLBACK = {
    2: "Bovada", 3: "Pinnacle", 4: "Sportsbetting", 6: "BetOnline",
    11: "Lowvig", 12: "Bodog", 14: "Intertops", 16: "Matchbook",
    18: "YouWager", 19: "Draftkings", 21: "Unibet", 22: "BetMGM",
    23: "Fanduel", 25: "Kalshi", 26: "Polymarket",
}

KNOWN_BOOKS: dict[int, str] = dict(_KNOWN_BOOKS_FALLBACK)


def fetch_affiliates() -> dict[int, str] | None:
    """Fetch affiliate_id -> name from /api/v2/affiliates (no auth). Returns None on failure."""
    try:
        resp = requests.get(
            "https://therundown.io/api/v2/affiliates",
            headers={"Accept": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        affiliates = data.get("affiliates") or []
        return {int(a["affiliate_id"]): a.get("affiliate_name", f"Book{a['affiliate_id']}") for a in affiliates if isinstance(a, dict) and "affiliate_id" in a}
    except Exception:
        return None

LINE_TYPES = [
    ("Moneyline", "moneyline_home", "moneyline_away"),
    ("Spread",    "spread_home",    "spread_away"),
    ("Total",     "total_over",     "total_under"),
]

# ──────────────────────────────────────────────
# API CLIENT
# ──────────────────────────────────────────────
class RundownClient:
    DIRECT_BASE = "https://therundown.io/api/v2"
    RAPID_BASE  = "https://therundown-proxy.p.rapidapi.com"
    RAPID_HOST  = "therundown-proxy.p.rapidapi.com"

    def __init__(self, api_key, use_rapidapi=False):
        self.base_url = self.RAPID_BASE if use_rapidapi else self.DIRECT_BASE
        self.last_headers = {}
        self.session  = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if use_rapidapi:
            self.session.headers.update({
                "X-RapidAPI-Key": api_key,
                "X-RapidAPI-Host": self.RAPID_HOST,
            })
        else:
            self.session.headers.update({"X-TheRundown-Key": api_key})

    def _get(self, path, params=None):
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params or {}, timeout=15)
        self.last_headers = dict(resp.headers)
        resp.raise_for_status()
        return resp.json()

    def get_events(self, sport_id, date_str):
        return self._get(f"/sports/{sport_id}/events/{date_str}")

    def get_delta(self, sport_id, last_id):
        return self._get(f"/sports/{sport_id}/events/delta", params={"last_id": last_id})


# ──────────────────────────────────────────────
# RAW STRUCTURE INSPECTOR
# Runs once on first event to map the actual API layout
# ──────────────────────────────────────────────
def inspect_raw_structure(event: dict, sport_name: str):
    """Print the actual odds structure so we know what keys the API uses."""
    print(f"  ⚑ Inspecting raw structure for [{sport_name}] event")
    print(f"     Top-level keys: {list(event.keys())}")

    markets = event.get("markets") or []
    if not markets:
        print("     ⚠ No 'markets' key or empty list on event.")
        return

    first_market = markets[0]
    print(f"     First market keys: {list(first_market.keys())}")
    print(f"     market_id={first_market.get('market_id')}  period_id={first_market.get('period_id')}  name={first_market.get('name')!r}")

    participants = first_market.get("participants") or []
    print(f"     participants ({len(participants)}): {[p.get('name') for p in participants if isinstance(p, dict)]}")
    if not participants:
        return

    first_participant = participants[0]
    print(f"     First participant keys: {list(first_participant.keys())}")

    lines = first_participant.get("lines") or []
    print(f"     lines count: {len(lines)}")
    if lines:
        first_line = lines[0]
        print(f"     First line keys: {list(first_line.keys())}")
        prices = first_line.get("prices") or {}
        print(f"     prices keys (book ids): {list(prices.keys())}")


# ──────────────────────────────────────────────
# ODDS CONVERSION
# ──────────────────────────────────────────────
def american_to_decimal(val: float) -> float:
    val = float(val)
    if val >= 100:
        return round((val / 100) + 1, 6)
    elif val <= -100:
        return round((100 / abs(val)) + 1, 6)
    return 0.0

def to_decimal(val) -> float:
    try:
        val = float(val)
    except (TypeError, ValueError):
        return 0.0
    if val == 0:
        return 0.0
    if abs(val) >= 100:
        return american_to_decimal(val)
    if val > 1.0:
        return round(val, 6)  # already decimal
    return 0.0


# ──────────────────────────────────────────────
# LINE EXTRACTION — markets-based, line-aware
# ──────────────────────────────────────────────
def build_market_index(event: dict) -> tuple[dict, dict]:
    """
    Build a per-event index of available prices grouped by (market kind, line value).

    Returns:
        {
            ("ml", None): {
                "home": [ { "book": str, "price_am": int, "price_dec": float }, ... ],
                "away": [ ... ],
            },
            ("spread", 4.5): {
                "home": [...],
                "away": [...],
            },
            ("total", 226.5): {
                "over": [...],
                "under": [...],
            },
            ...
        }
    """
    markets = event.get("markets") or []
    if not isinstance(markets, list):
        return {}, {}

    index: dict[tuple[str, float | None], dict[str, list[dict]]] = {}
    # For spreads we also maintain complementary buckets by absolute line value:
    # abs_line -> {"home_minus": [...], "away_plus": [...]}
    spread_pairs: dict[float, dict[str, list[dict]]] = {}

    for m in markets:
        if not isinstance(m, dict):
            continue
        # Only use full-game markets
        if m.get("period_id") != 0:
            continue

        market_id = m.get("market_id")
        m_name = (m.get("name") or "").lower()
        participants = m.get("participants") or []
        if not isinstance(participants, list) or len(participants) == 0:
            continue

        # Determine market kind
        is_totals = market_id == 3 or "total" in m_name
        is_spread = market_id == 2 or "spread" in m_name
        is_ml     = market_id == 1 or "moneyline" in m_name or "money line" in m_name or "winner" in m_name

        if not (is_totals or is_spread or is_ml):
            continue

        for idx, participant in enumerate(participants):
            if not isinstance(participant, dict):
                continue

            pname = (participant.get("name") or "").lower()

            # Map participant to abstract side label
            side_label: str | None = None
            if is_totals:
                if "over" in pname:
                    side_label = "over"
                elif "under" in pname:
                    side_label = "under"
            elif is_spread:
                # Assume ordering: first = away, second = home
                side_label = "away" if idx == 0 else "home"
            elif is_ml:
                side_label = "away" if idx == 0 else "home"

            if not side_label:
                continue

            lines = participant.get("lines") or []
            if not isinstance(lines, list):
                continue

            for line in lines:
                if not isinstance(line, dict):
                    continue

                # Line value (spread or total points); moneyline has no line value
                line_value: float | None = None
                if is_totals or is_spread:
                    val = line.get("value")
                    try:
                        line_value = float(val)
                    except (TypeError, ValueError):
                        continue

                prices = line.get("prices") or {}
                if not isinstance(prices, dict):
                    continue

                for raw_book_id, price_obj in prices.items():
                    try:
                        book_id = int(raw_book_id)
                    except (ValueError, TypeError):
                        continue
                    if not isinstance(price_obj, dict):
                        continue

                    price_am = price_obj.get("price")
                    if price_am in (None, 0, "0"):
                        continue
                    dec = to_decimal(price_am)
                    if dec <= 1.0:
                        continue

                    book_name = KNOWN_BOOKS.get(book_id, f"Book{book_id}")
                    updated_at = price_obj.get("updated_at")

                    kind = "ml" if is_ml else ("spread" if is_spread else "total")
                    key = (kind, line_value if kind != "ml" else None)
                    per_line = index.setdefault(key, {"home": [], "away": [], "over": [], "under": []})
                    per_line[side_label].append(
                        {
                            "book": book_name,
                            "price_am": int(price_am),
                            "price_dec": dec,
                            "updated_at": updated_at,
                        }
                    )

                    # Build complementary spread index: home -X vs away +X for same |X|
                    if kind == "spread" and line_value is not None:
                        abs_lv = abs(line_value)
                        buckets = spread_pairs.setdefault(abs_lv, {"home_minus": [], "away_plus": []})
                        if side_label == "home" and line_value < 0:
                            buckets["home_minus"].append(
                                {
                                    "book": book_name,
                                    "price_am": int(price_am),
                                    "price_dec": dec,
                                    "updated_at": updated_at,
                                }
                            )
                        elif side_label == "away" and line_value > 0:
                            buckets["away_plus"].append(
                                {
                                    "book": book_name,
                                    "price_am": int(price_am),
                                    "price_dec": dec,
                                    "updated_at": updated_at,
                                }
                            )

    return index, spread_pairs


# ──────────────────────────────────────────────
# ARBITRAGE CORE
# ──────────────────────────────────────────────
def analyze_event(event: dict, sport_name: str) -> list:
    """
    Find all near-arb and arb situations for an event.
    Returns list of comparison dicts sorted by profit descending.
    """
    # Prefer explicit home/away flags from event["teams"], fall back to teams_normalized.
    teams = event.get("teams") or []
    if teams:
        home_team = next((t for t in teams if t.get("is_home")), teams[-1])
        away_team = next((t for t in teams if t.get("is_away")), teams[0])
        home = home_team.get("name", "Home")
        away = away_team.get("name", "Away")
    else:
        tn = event.get("teams_normalized", [])
        home = tn[0].get("name", "Home") if len(tn) > 0 else "Home"
        away = tn[1].get("name", "Away") if len(tn) > 1 else "Away"
    game  = f"{away} @ {home}"

    market_index, spread_pairs = build_market_index(event)
    if not market_index:
        return []

    results: list[dict] = []

    # Helper to parse updated_at into UTC datetime
    def _parse_ts(val):
        try:
            if not val:
                return None
            if isinstance(val, (int, float)):
                ts = float(val)
                if ts > 1e10:
                    ts /= 1000  # assume milliseconds
                return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
            return datetime.datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception:
            return None

    # Handle moneyline and totals using the regular index
    for (kind, line_value), sides in market_index.items():
        if kind == "ml":
            label = "Moneyline"
            side_a_key, side_b_key = "home", "away"
            market_line_label = "ML"
        elif kind == "total":
            label = "Total"
            side_a_key, side_b_key = "over", "under"
            market_line_label = f"{line_value:g}" if line_value is not None else ""
        else:
            continue

        side_a_list = sides.get(side_a_key) or []
        side_b_list = sides.get(side_b_key) or []
        if not side_a_list or not side_b_list:
            continue

        # Best prices per side (highest decimal odds) across all books at this exact line
        best_a = max(side_a_list, key=lambda e: e["price_dec"])
        best_b = max(side_b_list, key=lambda e: e["price_dec"])

        dec_a = best_a["price_dec"]
        dec_b = best_b["price_dec"]
        arb   = (1 / dec_a) + (1 / dec_b)
        profit = round((1 - arb) * 100, 4)

        # Filter out non-interesting / unrealistic results
        if profit < ARB_THRESHOLD:
            continue
        if profit > MAX_PROFIT_CAP:
            # Extremely high implied profit is almost certainly an artifact; skip reporting.
            continue

        stake_a = round(TOTAL_STAKE * (1 / dec_a) / arb, 2)
        stake_b = round(TOTAL_STAKE * (1 / dec_b) / arb, 2)

        # Freshness metrics: how recent are the two sides?
        ts_a = _parse_ts(best_a.get("updated_at"))
        ts_b = _parse_ts(best_b.get("updated_at"))
        fresh_ts = stale_ts = None
        fresh_age_s = stale_age_s = None
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        ts_list = [t for t in (ts_a, ts_b) if t is not None]
        if ts_list:
            fresh_ts = max(ts_list)
            stale_ts = min(ts_list)
            fresh_age_s = max((now_utc - t).total_seconds() for t in ts_list)
            stale_age_s = min((now_utc - t).total_seconds() for t in ts_list)

        results.append(
            {
                "sport":          sport_name,
                "game":           game,
                "market_kind":    kind,
                "market_label":   label,
                "line_value":     line_value,
                "line_label":     market_line_label,
                "side_a":         side_a_key.capitalize(),
                "book_a":         best_a["book"],
                "odds_a_am":      best_a["price_am"],
                "odds_a_dec":     dec_a,
                "updated_at_a":   best_a.get("updated_at"),
                "side_b":         side_b_key.capitalize(),
                "book_b":         best_b["book"],
                "odds_b_am":      best_b["price_am"],
                "odds_b_dec":     dec_b,
                "updated_at_b":   best_b.get("updated_at"),
                "fresh_ts":       fresh_ts,
                "stale_ts":       stale_ts,
                "fresh_age_s":    fresh_age_s,
                "stale_age_s":    stale_age_s,
                "arb_pct":        round(arb, 6),
                "profit":         profit,
                "stake_a":        stake_a,
                "stake_b":        stake_b,
                "is_arb":         profit > 0,
            }
        )

    # Handle spreads using complementary home -X vs away +X pairs
    for abs_lv, buckets in spread_pairs.items():
        home_minus = buckets.get("home_minus") or []
        away_plus  = buckets.get("away_plus") or []
        if not home_minus or not away_plus:
            continue

        best_home = max(home_minus, key=lambda e: e["price_dec"])
        best_away = max(away_plus,  key=lambda e: e["price_dec"])

        dec_home = best_home["price_dec"]
        dec_away = best_away["price_dec"]
        arb      = (1 / dec_home) + (1 / dec_away)
        profit   = round((1 - arb) * 100, 4)

        if profit < ARB_THRESHOLD:
            continue
        if profit > MAX_PROFIT_CAP:
            continue

        stake_home = round(TOTAL_STAKE * (1 / dec_home) / arb, 2)
        stake_away = round(TOTAL_STAKE * (1 / dec_away) / arb, 2)

        ts_home = _parse_ts(best_home.get("updated_at"))
        ts_away = _parse_ts(best_away.get("updated_at"))
        fresh_ts = stale_ts = None
        fresh_age_s = stale_age_s = None
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        ts_list = [t for t in (ts_home, ts_away) if t is not None]
        if ts_list:
            fresh_ts = max(ts_list)
            stale_ts = min(ts_list)
            fresh_age_s = max((now_utc - t).total_seconds() for t in ts_list)
            stale_age_s = min((now_utc - t).total_seconds() for t in ts_list)

        same_book = best_home["book"] == best_away["book"]

        results.append(
            {
                "sport":          sport_name,
                "game":           game,
                "market_kind":    "spread",
                "market_label":   "Spread",
                "line_value":     abs_lv,
                "line_label":     f"{abs_lv:g}",
                "side_a":         "Home",
                "book_a":         best_home["book"],
                "odds_a_am":      best_home["price_am"],
                "odds_a_dec":     dec_home,
                "updated_at_a":   best_home.get("updated_at"),
                "side_b":         "Away",
                "book_b":         best_away["book"],
                "odds_b_am":      best_away["price_am"],
                "odds_b_dec":     dec_away,
                "updated_at_b":   best_away.get("updated_at"),
                "fresh_ts":       fresh_ts,
                "stale_ts":       stale_ts,
                "fresh_age_s":    fresh_age_s,
                "stale_age_s":    stale_age_s,
                "same_book":      same_book,
                "arb_pct":        round(arb, 6),
                "profit":         profit,
                "stake_a":        stake_home,
                "stake_b":        stake_away,
                "is_arb":         profit > 0,
            }
        )

    return results


# ──────────────────────────────────────────────
# DISPLAY
# ──────────────────────────────────────────────
def _format_staleness(updated_at) -> str:
    """Return human-readable age like '2m ago' or '' if not available."""
    if updated_at is None:
        return ""
    try:
        if isinstance(updated_at, (int, float)):
            ts = float(updated_at)
            if ts > 1e10:
                ts /= 1000  # assume milliseconds
        else:
            dt = datetime.datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            ts = dt.timestamp()
        age_sec = time.time() - ts
        if age_sec < 60:
            return "just now"
        if age_sec < 3600:
            return f"{int(age_sec // 60)}m ago"
        if age_sec < 86400:
            return f"{int(age_sec // 3600)}h ago"
        return f"{int(age_sec // 86400)}d ago"
    except Exception:
        return ""


def display_results(all_results: list, books_seen: set, events_checked: int):
    print(f"\n{'═'*80}")
    threshold_label = "TRUE ARB ONLY" if ARB_THRESHOLD == 0 else f"within {abs(ARB_THRESHOLD):.0f}% of arb"
    print(f"  SCAN COMPLETE  |  {events_checked} events  |  "
          f"Books: {', '.join(sorted(books_seen)) or 'NONE FOUND'}  |  Filter: {threshold_label}")
    print(f"{'═'*80}")

    if not all_results:
        print(f"\n  Nothing found. Check 'Books: NONE FOUND' above — if books are empty,")
        print(f"  the line extraction logic needs adjusting for your API's response format.\n")
        return

    # Ordering: highest profit first, then by freshness
    def _profit_sort_key(r):
        ts = r.get("fresh_ts")
        ts_val = ts.timestamp() if isinstance(ts, datetime.datetime) else 0.0
        return (r.get("profit", 0.0), ts_val)

    ordered = sorted(all_results, key=_profit_sort_key, reverse=True)

    # Split true arbs from near-arbs, preserving profit-first order
    true_arbs  = [r for r in ordered if r["is_arb"]]
    near_arbs  = [r for r in ordered if not r["is_arb"]]

    def _format_age_seconds(age_s: float | None) -> str:
        if age_s is None:
            return ""
        try:
            if age_s < 60:
                return "just now"
            if age_s < 3600:
                return f"{int(age_s // 60)}m ago"
            if age_s < 86400:
                return f"{int(age_s // 3600)}h ago"
            return f"{int(age_s // 86400)}d ago"
        except Exception:
            return ""

    def make_table(rows, title):
        if not rows:
            return
        print(f"\n  ── {title} ({len(rows)}) ──")
        for i, r in enumerate(rows, 1):
            if r["market_kind"] == "ml":
                market_str = "Moneyline"
            elif r["market_kind"] == "spread":
                market_str = f"Spread {r['line_label']}"
            else:
                market_str = f"Total {r['line_label']}"

            staleness_a = _format_staleness(r.get("updated_at_a"))
            staleness_b = _format_staleness(r.get("updated_at_b"))
            suffix_a = f", {staleness_a}" if staleness_a else ""
            suffix_b = f", {staleness_b}" if staleness_b else ""

            profit_str = f"{'+' if r['profit']>0 else ''}{r['profit']:.3f}%"
            same_book_flag = " [same book]" if r.get("same_book") else ""
            print(f"  [{i}] {r['sport']} – {r['game']} – {market_str}{same_book_flag}")
            print(f"      Side A: {r['side_a']} @ {r['book_a']} ({r['odds_a_am']:+d}{suffix_a})")
            print(f"      Side B: {r['side_b']} @ {r['book_b']} ({r['odds_b_am']:+d}{suffix_b})")
            print(f"      Profit: {profit_str}  |  Stake A: ${r['stake_a']:.2f}  |  Stake B: ${r['stake_b']:.2f}")
            # Overall freshness summary line
            fresh_age = _format_age_seconds(r.get("fresh_age_s"))
            stale_age = _format_age_seconds(r.get("stale_age_s"))
            if fresh_age:
                if stale_age and stale_age != fresh_age:
                    print(f"      Freshness: newest price {fresh_age} (other side {stale_age})")
                else:
                    print(f"      Freshness: newest price {fresh_age}")
            print()

    make_table(true_arbs, "✅ TRUE ARBITRAGE OPPORTUNITIES")
    if SHOW_NEAR_ARBS:
        make_table(near_arbs, f"📊 NEAR-ARB (within {abs(ARB_THRESHOLD):.0f}% — validation data)")

    print(f"\n  Total comparisons shown: {len(all_results)}")
    print(f"  True arbs: {len(true_arbs)}  |  Near-arb: {len(near_arbs) if SHOW_NEAR_ARBS else 0}\n")


def display_coverage(sport_event_counts: dict):
    t = PrettyTable(["Sport", "ID", "Events", "Lines Checked"])
    t.align = "l"
    total_e, total_l = 0, 0
    for sid, (sname, n) in sport_event_counts.items():
        nl = n * len(LINE_TYPES)
        t.add_row([sname, sid, n, nl])
        total_e += n
        total_l += nl
    t.add_row(["TOTAL", "—", total_e, total_l])
    print("\n── COVERAGE ──")
    print(t)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    global KNOWN_BOOKS
    fresh = fetch_affiliates()
    if fresh:
        KNOWN_BOOKS = fresh
    else:
        KNOWN_BOOKS = dict(_KNOWN_BOOKS_FALLBACK)

    client = RundownClient(API_KEY, USE_RAPIDAPI)
    dates  = [
        (datetime.date.today() + datetime.timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(DAYS_AHEAD + 1)
    ]

    print("=" * 70)
    print("  TheRundown Arbitrage Finder  —  Full Line Coverage")
    sport_names = [ALL_SPORTS.get(s, f"Sport{s}") for s in SPORT_IDS]
    print(f"  Sports: {sport_names}")
    print(f"  Threshold: profit >= {ARB_THRESHOLD}%  (0 = real arb only)")
    print(f"  Mode: {'RapidAPI' if USE_RAPIDAPI else 'Direct'}")
    book_names = sorted(KNOWN_BOOKS.values())
    print(f"  Books available: {', '.join(book_names)}")
    print(f"  Data delay: 5 min (free tier)")
    print("=" * 70)

    all_results      = []
    books_seen       = set()
    total_events     = 0
    sport_counts     = {}
    delta_cursors    = {}
    structure_dumped = False
    daily_exhausted  = False

    def _fetch_events_for_date(client, sport_id, sname, date_str):
        """Fetch events for one sport+date. Returns (events_list, got_quota_error)."""
        print(f"  [{sname}] {date_str}…", end=" ", flush=True)
        time.sleep(REQUEST_DELAY)
        try:
            data   = client.get_events(sport_id, date_str)
            events = (data or {}).get("events") or []
            cursor = (data or {}).get("meta", {}).get("delta_last_id")
            if cursor:
                delta_cursors[sport_id] = cursor
            dp_rem = client.last_headers.get("x-datapoints-remaining", "?")
            dp_lim = client.last_headers.get("x-datapoints-limit", "?")
            print(f"{len(events)} events  (dp: {dp_rem}/{dp_lim} remaining)")
            return events, False
        except requests.HTTPError as e:
            code = e.response.status_code
            hdrs = e.response.headers
            if code == 429:
                remaining = hdrs.get("x-datapoints-remaining", "?")
                retry_s   = int(hdrs.get("retry-after", "60"))
                if remaining == "0":
                    reset_time = (datetime.datetime.now() + datetime.timedelta(seconds=retry_s)).strftime("%H:%M")
                    used  = hdrs.get("x-datapoints-used", "?")
                    limit = hdrs.get("x-datapoints-limit", "?")
                    print(f"DAILY QUOTA EXHAUSTED ({used}/{limit} datapoints)")
                    print(f"  ✗ Resets at ~{reset_time} (in {retry_s // 60}m). Cannot continue until then.")
                    return [], True
                else:
                    print(f"rate limited — waiting 5s…")
                    time.sleep(5)
                    return [], False
            print(f"HTTP {code} — {'not on plan' if code in (403,404) else 'error'}")
            if code == 401:
                print("  ✗ Invalid API key.")
            return [], False
        except Exception as e:
            print(f"skipped ({e})")
            return [], False

    def _filter_live(events):
        """Apply LIVE_ONLY / PRE_GAME_MINUTES filter. Returns filtered list."""
        if not LIVE_ONLY:
            return list(events)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        kept = []
        for event in events:
            status = (event.get("score") or {}).get("event_status")
            if isinstance(status, str) and status.lower() in {"final", "complete", "closed"}:
                continue
            start_raw = event.get("event_date") or event.get("event_date_start")
            try:
                if not start_raw:
                    continue
                start_dt = datetime.datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=datetime.timezone.utc)
                delta_min = (now_utc - start_dt).total_seconds() / 60.0
                if delta_min <= PRE_GAME_MINUTES:
                    kept.append(event)
            except Exception:
                continue
        return kept

    def _analyze_events(filtered, sname):
        """Run analyze_event on a list of filtered events. Returns (arbs, books)."""
        arbs = []
        bks  = set()
        for event in filtered:
            if VALIDATION_MODE and not structure_dumped:
                inspect_raw_structure(event, sname)
            results = analyze_event(event, sname)
            for r in results:
                bks.add(r["book_a"])
                bks.add(r["book_b"])
            arbs.extend(results)
        return arbs, bks

    for sport_id in SPORT_IDS:
        if daily_exhausted:
            break
        sname = ALL_SPORTS.get(sport_id, f"Sport{sport_id}")

        # -- Try today first --
        raw_today, quota_hit = _fetch_events_for_date(client, sport_id, sname, dates[0])
        if quota_hit:
            daily_exhausted = True
            break

        filtered_today = _filter_live(raw_today)
        arbs_today, books_today = _analyze_events(filtered_today, sname)

        if arbs_today:
            sport_counts[sport_id] = (sname, len(filtered_today))
            total_events += len(filtered_today)
            books_seen.update(books_today)
            all_results.extend(arbs_today)
        else:
            # -- Fallback to tomorrow if no arbs today and DAYS_AHEAD >= 1 --
            if len(dates) > 1:
                print(f"  [{sname}] no arbs today, checking {dates[1]}…")
                raw_tmrw, quota_hit = _fetch_events_for_date(client, sport_id, sname, dates[1])
                if quota_hit:
                    daily_exhausted = True
                    break
                filtered_tmrw = _filter_live(raw_tmrw)
                arbs_tmrw, books_tmrw = _analyze_events(filtered_tmrw, sname)
                sport_counts[sport_id] = (sname, len(filtered_tmrw))
                total_events += len(filtered_tmrw)
                books_seen.update(books_tmrw)
                all_results.extend(arbs_tmrw)
            else:
                sport_counts[sport_id] = (sname, len(filtered_today))
                total_events += len(filtered_today)

    display_coverage(sport_counts)
    display_results(all_results, books_seen, total_events)

    # ── Delta polling ──────────────────────────
    if not delta_cursors:
        print("  ⚠  No delta cursors — one-shot scan only.\n")
        return

    print(f"  Delta polling every {POLL_INTERVAL}s. Ctrl-C to stop.\n")
    poll_n = 0
    while True:
        try:
            time.sleep(POLL_INTERVAL)
            poll_n += 1
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] Poll #{poll_n}")
            round_results = []
            round_events = 0

            for sport_id, last_id in list(delta_cursors.items()):
                sname = ALL_SPORTS.get(sport_id, f"Sport{sport_id}")
                time.sleep(REQUEST_DELAY)
                try:
                    raw    = client.get_delta(sport_id, last_id)
                    evts   = (raw or {}).get("events") or []
                    cursor = (raw or {}).get("meta", {}).get("delta_last_id", last_id)
                    delta_cursors[sport_id] = cursor
                    print(f"  [{sname}] {len(evts)} updated")
                    round_events += len(evts)
                    for event in evts:
                        for raw_key in event.get("lines", {}).keys():
                            try:
                                bid = int(raw_key)
                                if bid in KNOWN_BOOKS:
                                    books_seen.add(KNOWN_BOOKS[bid])
                            except (ValueError, TypeError):
                                pass
                        round_results.extend(analyze_event(event, sname))
                except requests.HTTPError as e:
                    if e.response.status_code == 429:
                        print(f"  ⚠ Rate limited, waiting 15s…")
                        time.sleep(15)
                    else:
                        print(f"  ✗ [{sname}] HTTP {e.response.status_code}")

            if round_results:
                display_results(round_results, books_seen, round_events)
            else:
                print(f"  No comparisons in threshold range this poll.")

        except KeyboardInterrupt:
            print("\n  Stopped. Goodbye!")
            break
        except Exception as e:
            print(f"  ✗ Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
