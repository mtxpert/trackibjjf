"""
scrape_sjjif_historical.py — Scrape SJJIF event results from sjjif.com/public/results/{id}

Table format: Division header row, then Place | Name | Team | Country rows.

Usage:
    python scrape_sjjif_historical.py                        # full run
    python scrape_sjjif_historical.py --dry-run
    python scrape_sjjif_historical.py --save-local out.json
    python scrape_sjjif_historical.py --search "Name"
    python scrape_sjjif_historical.py --upload
    python scrape_sjjif_historical.py --resume
"""

import argparse, json, logging, os, re, sys, time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("sjjif_historical.log", encoding="utf-8")])
log = logging.getLogger("sjjif_hist")

BASE = "https://sjjif.com"
SOURCE = "sjjif"
DEFAULT_OUT = "sjjif_all_results.json"

# All known event IDs with results (scanned 2026-04-15)
ALL_EVENT_IDS = [
    310,320,330,351,510,520,535,540,910,920,956,980,990,
    1100,1110,1140,1150,1160,1171,1180,1400,1410,1420,1430,
    1440,1446,1460,1477,1650,1655,1670,1675,1680,1685,
    1697,1698,1700,1705,
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})


def fetch_event_results(event_id: int) -> list[dict]:
    try:
        r = SESSION.get(f"{BASE}/public/results/{event_id}", timeout=20)
        if r.status_code != 200:
            log.warning("Event %d: HTTP %d", event_id, r.status_code)
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            log.info("Event %d: no table found", event_id)
            return []

        # Get event title from h1/h2/h3
        title_el = soup.find("h1") or soup.find("h2") or soup.find("h3")
        event_title = title_el.get_text(strip=True) if title_el else f"SJJIF Event {event_id}"

        # Get date from page text (look for year)
        year_m = re.search(r"(20\d\d)", event_title)
        event_date = year_m.group(1) if year_m else ""

        rows = []
        current_division = ""
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            if not cells:
                continue
            # Division header: single cell spanning columns (or first cell with no place number)
            if len(cells) == 1:
                current_division = cells[0]
                continue
            # Check if it looks like a result row: first cell is place (1st/2nd/3rd or number)
            place_str = cells[0].strip()
            place_m = re.match(r"(\d+)(?:st|nd|rd|th)?$", place_str, re.I)
            if not place_m and place_str.lower() not in ("1st","2nd","3rd","4th","5th"):
                # Might be a division header spanning multiple columns
                if len(cells) <= 2:
                    current_division = " | ".join(cells)
                continue

            if place_m:
                place = int(place_m.group(1))
            else:
                place = {"1st":1,"2nd":2,"3rd":3,"4th":4,"5th":5}.get(place_str.lower(), 0)

            if len(cells) < 2 or not current_division:
                continue

            name = cells[1] if len(cells) > 1 else ""
            team = cells[2] if len(cells) > 2 else ""
            country = cells[3] if len(cells) > 3 else ""

            if not name:
                continue

            rows.append({
                "source": SOURCE,
                "event_id": event_id,
                "event_title": event_title,
                "event_date": event_date,
                "division": current_division,
                "placement": place,
                "athlete_name": name,
                "team": team,
                "country": country,
            })

        log.info("Event %d (%s): %d rows", event_id, event_title[:50], len(rows))
        return rows
    except Exception as e:
        log.error("Event %d: %s", event_id, e)
        return []


def upload_to_supabase(rows):
    url = os.environ.get("SUPABASE_URL",""); key = os.environ.get("SUPABASE_SERVICE_KEY","")
    if not url or not key: log.error("Supabase env not set"); return
    from supabase import create_client
    client = create_client(url, key)
    for i in range(0, len(rows), 500):
        try:
            client.table("tournament_results").upsert(rows[i:i+500],
                on_conflict="source,event_id,division,placement,athlete_name").execute()
            log.info("Uploaded %d rows", min(i+500, len(rows)))
        except Exception as e:
            log.error("Upload batch %d: %s", i//500, e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--save-local", default=DEFAULT_OUT)
    ap.add_argument("--search", default=None)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()

    out_path = Path(args.save_local)
    existing, existing_ids = [], set()
    if args.resume and out_path.exists():
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        existing_ids = {r["event_id"] for r in existing}
        log.info("Resume: %d rows, %d events", len(existing), len(existing_ids))

    all_rows = list(existing)
    for i, eid in enumerate(ALL_EVENT_IDS, 1):
        if eid in existing_ids:
            continue
        log.info("[%d/%d] Event %d", i, len(ALL_EVENT_IDS), eid)
        rows = fetch_event_results(eid)
        all_rows.extend(rows)
        if not args.dry_run and rows:
            out_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        time.sleep(args.delay)

    log.info("Done. Total: %d rows", len(all_rows))

    if args.search:
        q = args.search.lower()
        hits = [r for r in all_rows if q in r.get("athlete_name","").lower()]
        print(f"\nSearch '{args.search}': {len(hits)} matches")
        for r in hits:
            print(f"  [{r['placement']}] {r['athlete_name']} | {r['division']} | {r['event_title']}")

    if args.upload and not args.dry_run:
        upload_to_supabase(all_rows)


if __name__ == "__main__":
    main()
