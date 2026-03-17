"""
Run this ONCE to dump the raw API response structure for a given sport/date.

Usage:
    THERUNDOWN_API_KEY=your_key .venv/bin/python debug_structure.py
    THERUNDOWN_API_KEY=your_key .venv/bin/python debug_structure.py 4 2026-04-01
"""

import datetime
import json
import os
import sys

import requests


API_KEY = os.getenv("THERUNDOWN_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit(
        "ERROR: set the THERUNDOWN_API_KEY environment variable before running this script.\n"
        "  export THERUNDOWN_API_KEY=your_key_here"
    )

SPORT_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 4
DATE_STR = sys.argv[2] if len(sys.argv) > 2 else datetime.date.today().strftime("%Y-%m-%d")


def main() -> None:
    url = f"https://therundown.io/api/v2/sports/{SPORT_ID}/events/{DATE_STR}"
    print(f"Fetching: {url}\n")
    resp = requests.get(
        url,
        headers={"X-TheRundown-Key": API_KEY, "Accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    events = data.get("events", [])
    print(f"Total events: {len(events)}\n")

    if not events:
        return

    event = events[0]
    print("=== TOP-LEVEL KEYS ===")
    print(list(event.keys()))

    print("\n=== LINES KEYS (book IDs present) ===")
    lines = event.get("lines", {})
    print(list(lines.keys()))

    if lines:
        first_key = list(lines.keys())[0]
        print(f"\n=== FIRST BOOK KEY: '{first_key}' ===")
        print(json.dumps(lines[first_key], indent=2)[:2000])
    else:
        print("\n⚠ 'lines' is empty — checking other keys for odds...")
        for k, v in event.items():
            if isinstance(v, (dict, list)) and k != "teams_normalized":
                print(f"\n--- '{k}' ---")
                print(json.dumps(v, indent=2)[:500])


if __name__ == "__main__":
    main()

