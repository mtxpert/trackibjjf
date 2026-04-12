#!/usr/bin/env python3
"""
Build roster cache locally (uses local Playwright) and push to Render.

Usage:
  python push_cache.py --key YOUR_UPLOAD_KEY
  python push_cache.py --key YOUR_UPLOAD_KEY --tournament 3106
  python push_cache.py --key YOUR_UPLOAD_KEY --url http://localhost:5000  # local test

Set UPLOAD_KEY env var on Render dashboard so the server accepts uploads.
"""
import argparse
import asyncio
import json
import sys
import time
import requests
from pathlib import Path

DEFAULT_URL = "https://trackibjjf.onrender.com"


def get_tournaments(base_url):
    r = requests.get(f"{base_url}/api/tournaments", timeout=15)
    r.raise_for_status()
    return r.json()


async def build_locally(tournament_id, tournament_name=""):
    """Run the scraper locally and return the cache dict."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from scraper import get_category_ids, parse_all_athletes, _build_roster
    from datetime import datetime
    from playwright.async_api import async_playwright

    print(f"  Fetching category list...")
    cats = get_category_ids(tournament_id)
    print(f"  {len(cats)} categories found")

    all_athletes = []
    CONCURRENCY = 6

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        sem = asyncio.Semaphore(CONCURRENCY)

        async def process(cat):
            async with sem:
                page = await context.new_page()
                try:
                    from scraper import BASE, parse_all_athletes
                    url = f"{BASE}/tournaments/{tournament_id}/categories/{cat['id']}"
                    await page.goto(url, wait_until="networkidle", timeout=20000)
                    text = await page.inner_text("body")
                    return parse_all_athletes(text, cat["name"], cat["id"])
                except Exception as e:
                    return []
                finally:
                    await page.close()

        tasks = [asyncio.create_task(process(cat)) for cat in cats]
        done = 0
        for task in asyncio.as_completed(tasks):
            result = await task
            all_athletes.extend(result)
            done += 1
            print(f"  {done}/{len(cats)} categories scraped ({len(all_athletes)} athletes so far)", end="\r")

        await browser.close()

    print()

    # Dedupe by (name, category_id)
    seen, deduped = set(), []
    for a in all_athletes:
        key = (a["name"].lower(), a.get("category_id", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(a)

    return {
        "tournament_id": tournament_id,
        "built_at":      __import__("datetime").datetime.now().isoformat(),
        "total_cats":    len(cats),
        "athletes":      deduped,
    }


def push_to_server(base_url, upload_key, tournament_id, cache):
    print(f"  Pushing {len(cache['athletes'])} athletes to {base_url}...")
    r = requests.put(
        f"{base_url}/api/roster/{tournament_id}",
        json=cache,
        headers={"X-Upload-Key": upload_key, "Content-Type": "application/json"},
        timeout=60,
    )
    if r.status_code == 200:
        print(f"  Uploaded successfully")
        return True
    else:
        print(f"  Upload failed: {r.status_code} {r.text[:200]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Build roster locally and push to Render")
    parser.add_argument("--key",        required=True, help="UPLOAD_KEY set on Render service")
    parser.add_argument("--url",        default=DEFAULT_URL, help="App base URL")
    parser.add_argument("--tournament", help="Specific tournament ID (omit for all active)")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    if args.tournament:
        ids = [{"id": args.tournament, "name": args.tournament}]
    else:
        print(f"Fetching tournament list from {base_url}...")
        ids = get_tournaments(base_url)
        print(f"Found {len(ids)} tournaments")

    failures = []
    for t in ids:
        tid  = t["id"]
        name = t.get("name", tid)
        print(f"\nBuilding {name} ({tid})...")
        try:
            cache = asyncio.run(build_locally(tid, name))
            ok = push_to_server(base_url, args.key, tid, cache)
            if not ok:
                failures.append(tid)
        except Exception as e:
            print(f"  Error: {e}")
            failures.append(tid)

    print(f"\nDone. {len(ids) - len(failures)}/{len(ids)} succeeded.")
    if failures:
        print(f"Failed: {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
