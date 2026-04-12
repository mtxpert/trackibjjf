#!/usr/bin/env python3
"""
Manual roster cache builder for trackIBJJF.
Run this to pre-build the athlete roster for one or all active tournaments.

Usage:
  python build_cache.py                          # build all active tournaments
  python build_cache.py --tournament 3106        # specific tournament ID
  python build_cache.py --url http://localhost:5000  # local dev server
"""
import argparse
import sys
import time
import requests

DEFAULT_URL = "https://trackibjjf.onrender.com"


def build_tournament(base_url, tournament_id, tournament_name=""):
    label = tournament_name or tournament_id
    print(f"\nBuilding cache for {label}...")
    try:
        r = requests.post(f"{base_url}/api/cache/{tournament_id}", timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  Failed to start build: {e}")
        return False

    while True:
        time.sleep(3)
        try:
            r = requests.get(f"{base_url}/api/cache/{tournament_id}/status", timeout=10)
            job = r.json()
        except Exception:
            print("  (polling error — retrying)")
            continue

        status   = job.get("status", "?")
        progress = job.get("progress", 0)
        total    = job.get("total", 0)
        cat      = job.get("current_cat", "")

        if total > 0:
            pct = round(progress / total * 100)
            print(f"  {progress}/{total} ({pct}%) — {cat[:60]}", end="\r")

        if status == "done":
            count = job.get("athlete_count", "?")
            print(f"\n  Done — {count} athletes cached")
            return True
        elif status == "error":
            print(f"\n  Error: {job.get('error', 'unknown')}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Pre-build IBJJF roster cache")
    parser.add_argument("--url", default=DEFAULT_URL, help="App base URL")
    parser.add_argument("--tournament", help="Specific tournament ID (omit for all)")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    if args.tournament:
        ok = build_tournament(base_url, args.tournament)
        sys.exit(0 if ok else 1)

    print(f"Fetching tournament list from {base_url}...")
    try:
        r = requests.get(f"{base_url}/api/tournaments", timeout=15)
        tournaments = r.json()
    except Exception as e:
        print(f"Failed to fetch tournaments: {e}")
        sys.exit(1)

    print(f"Found {len(tournaments)} tournaments")
    failures = []
    for t in tournaments:
        ok = build_tournament(base_url, t["id"], t.get("name", ""))
        if not ok:
            failures.append(t["id"])

    print(f"\nAll done. {len(tournaments) - len(failures)}/{len(tournaments)} succeeded.")
    if failures:
        print(f"Failed: {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
