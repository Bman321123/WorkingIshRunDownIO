"""
Run this ONCE to dump the raw API response structure.

Usage:
    .venv/bin/python debug_structure.py
"""

import json

import requests


API_KEY = "10c79a340413a26b827a2d34b0c32e86e7bf5c964eb4d4c3b8e8cadc995d1c58"


def main() -> None:
    resp = requests.get(
        "https://therundown.io/api/v2/sports/4/events/2026-03-04",
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

