"""
scrape_uaejjf_historical.py — Scrape all UAEJJF event results from events.uaejjf.org.

API (same pattern as Smoothcomp):
  GET  /en/event/{id}/results          → get CSRF token
  POST /en/event/{id}/results/getResults → JSON: {eventResults: [...]}

Each result: group.name (division), top3+after3 (placements with athlete+club+country)

Event IDs discovered via Playwright from /en/events/past (6 pages, 218 events).

Usage:
    python scrape_uaejjf_historical.py                        # full run
    python scrape_uaejjf_historical.py --dry-run              # no saves
    python scrape_uaejjf_historical.py --event-id 183         # single event test
    python scrape_uaejjf_historical.py --save-local out.json  # custom output
    python scrape_uaejjf_historical.py --search "Gordon Ryan" # search after
    python scrape_uaejjf_historical.py --resume               # skip already-saved
    python scrape_uaejjf_historical.py --upload               # upload to Supabase
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("uaejjf_historical.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("uaejjf_hist")

BASE = "https://events.uaejjf.org"
SOURCE = "uaejjf"
DEFAULT_OUT = "uaejjf_all_results.json"
DELAY = 1.2

# All event IDs discovered from /en/events/past (218 events, scraped 2026-04-15)
ALL_EVENT_IDS = [
    13, 16, 22, 23, 34, 35, 36, 37, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
    86, 87, 88, 89, 119, 156, 157, 158, 160, 161, 162, 163, 164, 165, 166, 167,
    168, 169, 170, 171, 175, 181, 182, 183, 187, 190, 191, 192, 193, 196, 197,
    198, 199, 201, 202, 203, 204, 205, 206, 209, 210, 211, 212, 213, 214, 215,
    216, 218, 219, 221, 222, 223, 224, 225, 227, 228, 229, 230, 231, 233, 234,
    235, 236, 238, 239, 240, 242, 243, 244, 245, 256, 258, 259, 260, 261, 262,
    263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 275, 276, 277, 280, 281,
    282, 283, 284, 285, 289, 290, 291, 292, 295, 296, 297, 298, 299, 300, 301,
    302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316,
    317, 318, 319, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332,
    333, 334, 336, 337, 338, 339, 347, 348, 349, 350, 354, 355, 357, 358, 359,
    360, 361, 362, 363, 364, 365, 369, 370, 371, 372, 373, 375, 377, 378, 379,
    381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 395, 396, 397,
    398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 409, 412, 413, 414, 419,
    420, 427, 428,
]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    return s


SESSION = make_session()


def _get_csrf(event_id: int) -> tuple[str, str]:
    """Fetch event results page. Returns (csrf_token, page_title)."""
    r = SESSION.get(f"{BASE}/en/event/{event_id}/results", timeout=20)
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', r.text)
    csrf = m.group(1) if m else ""
    title_m = re.search(r'"title"\s*:\s*"([^"]+)"', r.text)
    title = title_m.group(1) if title_m else ""
    # Also try <title> tag
    if not title:
        tm = re.search(r'<title>(.*?)</title>', r.text)
        if tm:
            title = tm.group(1).replace(" - UAE Jiu Jitsu Federation", "").strip()
            # Strip "Results - " prefix
            title = re.sub(r'^Results\s*-\s*', '', title).strip()
    return csrf, title


def fetch_event_results(event_id: int) -> list[dict]:
    """POST getResults for one event. Returns list of result rows."""
    try:
        # Fetch results page — get CSRF, title, and event_start date in one shot
        page_r = SESSION.get(f"{BASE}/en/event/{event_id}/results", timeout=20)
        csrf_m = re.search(r'<meta name="csrf-token" content="([^"]+)"', page_r.text)
        if not csrf_m:
            log.warning("Event %d: no CSRF token", event_id)
            return []
        csrf = csrf_m.group(1)

        # Title from window.sc JSON blob
        title_m = re.search(r'"title"\s*:\s*"([^"]+)"', page_r.text)
        event_title = title_m.group(1) if title_m else ""
        if not event_title:
            tm = re.search(r'<title>(.*?)</title>', page_r.text)
            if tm:
                event_title = re.sub(r'^Results\s*[-–]\s*', '', tm.group(1)).replace(" - UAE Jiu Jitsu Federation", "").strip()

        # Date from window.sc
        date_m = re.search(r'"event_start"\s*:\s*"(\d{4}-\d{2}-\d{2})', page_r.text)
        event_date = date_m.group(1) if date_m else ""

        r = SESSION.post(
            f"{BASE}/en/event/{event_id}/results/getResults",
            data={"_token": csrf},
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-TOKEN": csrf,
                "Referer": f"{BASE}/en/event/{event_id}/results",
            },
            timeout=20,
        )
        if r.status_code != 200:
            log.warning("Event %d: HTTP %d", event_id, r.status_code)
            return []

        data = r.json()
        categories = data.get("eventResults", [])

        if not categories:
            log.info("Event %d (%s): no results published", event_id, event_title[:50])
            return []

        rows = []
        for cat in categories:
            division = (cat.get("group") or {}).get("name", "")
            if not division:
                continue

            placements = (cat.get("top3") or []) + (cat.get("after3") or [])
            for p in placements:
                place = p.get("placement")
                target = p.get("target") or {}
                club = p.get("club") or {}
                affiliation = p.get("affiliation") or {}

                name = (target.get("fullname") or "").strip()
                if not name:
                    name = f"{target.get('firstname','')} {target.get('lastname','')}".strip()
                if not name:
                    continue

                rows.append({
                    "source": SOURCE,
                    "event_id": event_id,
                    "event_title": event_title,
                    "event_date": event_date,
                    "division": division,
                    "placement": place,
                    "athlete_name": name,
                    "athlete_id": target.get("user_id"),
                    "club": club.get("name", ""),
                    "affiliation": affiliation.get("name", ""),
                    "country": target.get("country_human", ""),
                    "country_code": target.get("country", ""),
                })

        log.info("Event %d (%s): %d rows, %d divisions",
                 event_id, event_title[:45], len(rows), len(categories))
        return rows

    except Exception as e:
        log.error("Event %d: unexpected error: %s", event_id, e)
        import traceback; traceback.print_exc()
        return []


# ── Upload ─────────────────────────────────────────────────────────────────────

def upload_to_supabase(rows: list[dict]) -> None:
    supabase_url = os.environ.get("SUPABASE_URL", "")
    service_key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not service_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY not set")
        return
    try:
        from supabase import create_client
        client = create_client(supabase_url, service_key)
    except Exception as e:
        log.error("Supabase client init failed: %s", e)
        return
    BATCH = 500
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            client.table("tournament_results").upsert(
                batch, on_conflict="event_id,division,placement,athlete_name"
            ).execute()
            total += len(batch)
            log.info("Uploaded %d / %d rows", total, len(rows))
        except Exception as e:
            log.error("Upload batch %d failed: %s", i // BATCH, e)
    log.info("Upload complete: %d rows", total)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Scrape UAEJJF results from events.uaejjf.org")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--event-id", type=int, default=None)
    ap.add_argument("--save-local", default=DEFAULT_OUT)
    ap.add_argument("--search", default=None)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--delay", type=float, default=DELAY)
    args = ap.parse_args()

    out_path = Path(args.save_local)

    # Load existing for resume
    existing_rows: list[dict] = []
    existing_ids: set[int] = set()
    if args.resume and out_path.exists():
        try:
            existing_rows = json.loads(out_path.read_text(encoding="utf-8"))
            existing_ids = {r["event_id"] for r in existing_rows}
            log.info("Resume: %d existing rows, %d event IDs", len(existing_rows), len(existing_ids))
        except Exception as e:
            log.warning("Could not load existing: %s", e)

    # Determine which events to scrape
    if args.event_id:
        event_ids = [args.event_id]
    else:
        event_ids = ALL_EVENT_IDS

    all_rows = list(existing_rows)
    total_events = len(event_ids)

    for i, eid in enumerate(event_ids, 1):
        if eid in existing_ids:
            log.info("[%d/%d] Skipping (resume): event %d", i, total_events, eid)
            continue

        log.info("[%d/%d] Scraping event %d", i, total_events, eid)
        rows = fetch_event_results(eid)
        all_rows.extend(rows)

        if not args.dry_run and rows:
            out_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        time.sleep(args.delay)

    log.info("Done. Total rows: %d", len(all_rows))

    if args.search:
        q = args.search.lower()
        matches = [r for r in all_rows if q in r.get("athlete_name", "").lower()]
        print(f"\nSearch '{args.search}': {len(matches)} matches")
        for r in matches:
            print(f"  [{r['placement']}] {r['athlete_name']} | {r['division']} | {r['event_title']} ({r['event_date']})")

    if args.upload and not args.dry_run:
        upload_to_supabase(all_rows)


if __name__ == "__main__":
    main()
