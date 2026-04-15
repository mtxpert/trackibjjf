"""
scrape_adcc_historical.py — Scrape all ADCC event results from adcombat.com.

Site structure:
  GET /adcc-events/results/          → paginated list of events (page=N)
  GET /adcc-events/{event-slug}/     → event page with results HTML

Results HTML:
  <div class="rw-event-results rw-basic-results">
    <ul>
      <li><strong>DIVISION NAME</strong>
        <ol>
          <li>Name — (Academy) — Country</li>
          ...
        </ol>
      </li>
    </ul>
  </div>

Usage:
    python scrape_adcc_historical.py                        # full run
    python scrape_adcc_historical.py --dry-run              # no saves
    python scrape_adcc_historical.py --pages 1-5            # page range (event list pages)
    python scrape_adcc_historical.py --save-local out.json  # custom output file
    python scrape_adcc_historical.py --search "Mike Bambic" # search after scraping
    python scrape_adcc_historical.py --upload               # upload saved JSON to Supabase
    python scrape_adcc_historical.py --resume               # skip already-saved events
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
        logging.FileHandler("adcc_historical.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("adcc_hist")

BASE = "https://adcombat.com"
SOURCE = "adcc"
DEFAULT_OUT = "adcc_all_results.json"
DELAY = 1.0  # seconds between requests


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    return s


SESSION = make_session()


# ── Index pages ────────────────────────────────────────────────────────────────

def get_total_pages() -> int:
    r = SESSION.get(f"{BASE}/adcc-events/results/", timeout=20)
    soup = BeautifulSoup(r.text, "html.parser")
    pages = []
    # Pagination: /adcc-events/results/page/N/
    for a in soup.select("a[href*='/adcc-events/results/page/']"):
        m = re.search(r"/page/(\d+)/", a["href"])
        if m:
            pages.append(int(m.group(1)))
    return max(pages) if pages else 1


def fetch_event_list_page(page: int) -> list[dict]:
    """Fetch one page of the event listing. Returns list of {title, url}."""
    if page == 1:
        url = f"{BASE}/adcc-events/results/"
    else:
        url = f"{BASE}/adcc-events/results/page/{page}/"

    r = SESSION.get(url, timeout=20)
    if r.status_code != 200:
        log.warning("Event list page %d: HTTP %d", page, r.status_code)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    events = []
    seen_urls: set[str] = set()

    # Events are in <h2 class="entry-title"><a href="...">TITLE</a></h2>
    for a in soup.select("h2.entry-title a[href]"):
        href = a.get("href", "")
        if not href or href in seen_urls:
            continue
        slug = href.rstrip("/").split("/adcc-events/")[-1]
        if not slug or slug in ("results",):
            continue
        seen_urls.add(href)
        title = a.get_text(strip=True)
        if not title:
            continue
        events.append({
            "title": title,
            "url": href if href.startswith("http") else BASE + href,
            "date": "",  # fetched from event page
        })

    log.info("Event list page %d: found %d events", page, len(events))
    return events


# ── Event results parser ───────────────────────────────────────────────────────

def _parse_athlete_line(line: str) -> tuple[str, str, str]:
    """
    Parse 'Name — (Academy) — Country' or 'Name - (Academy) - Country'.
    Returns (name, academy, country). Fields may be empty.
    """
    # Normalize em-dash variants
    line = line.replace("–", "—").replace("-", "—")
    parts = [p.strip() for p in re.split(r"—", line)]

    name = parts[0] if len(parts) > 0 else line.strip()
    academy = ""
    country = ""

    for p in parts[1:]:
        p = p.strip()
        if p.startswith("(") and p.endswith(")"):
            academy = p[1:-1].strip()
        elif p and not academy:
            # Could be academy without parens
            academy = p
        elif p:
            country = p

    return name.strip(), academy.strip(), country.strip()


def fetch_event_results(event: dict) -> list[dict]:
    """
    Fetch and parse results from a single event page.
    Returns list of result rows.
    """
    url = event["url"]
    try:
        r = SESSION.get(url, timeout=20)
        if r.status_code != 200:
            log.warning("Event %s: HTTP %d", url, r.status_code)
            return []
    except Exception as e:
        log.warning("Event %s: fetch error: %s", url, e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # Date: .rw-event-start-date e.g. "Mar282026"
    event_date = event.get("date", "")
    if not event_date:
        el = soup.select_one(".rw-event-start-date")
        if el:
            event_date = el.get_text(strip=True)
        if not event_date:
            m = re.search(r"(20\d\d)", event.get("title", ""))
            if m:
                event_date = m.group(1)

    # Location
    location = ""
    el = soup.select_one(".rw-event-location, p.rw-event-location")
    if el:
        location = el.get_text(separator=" ", strip=True)[:200]

    rows = []

    # Primary: <div class="rw-event-results ..."><ul><li><strong>DIVISION</strong><ol>...
    results_divs = soup.select("div.rw-event-results, div.rw-basic-results, #container.rw-event-results")
    if not results_divs:
        # Try generic results container
        results_divs = soup.select(".rw-event-results, .event-results")
    if not results_divs:
        # Last resort: any <ul> that contains <strong> followed by <ol>
        results_divs = [soup]

    for container in results_divs:
        for division_li in container.select("ul > li"):
            # Division name is in <strong> tag
            strong = division_li.find("strong")
            if not strong:
                # Some pages use direct text before <ol>
                texts = [t.strip() for t in division_li.find_all(string=True, recursive=False)]
                division_name = " ".join(t for t in texts if t)
            else:
                division_name = strong.get_text(strip=True)

            if not division_name:
                continue

            # Athletes are in <ol><li>...</li></ol>
            ol = division_li.find("ol")
            if not ol:
                continue

            for place_idx, athlete_li in enumerate(ol.find_all("li", recursive=False), 1):
                line = athlete_li.get_text(separator=" ", strip=True)
                if not line:
                    continue

                name, academy, country = _parse_athlete_line(line)
                if not name:
                    continue

                rows.append({
                    "source": SOURCE,
                    "event_title": event["title"],
                    "event_date": event_date,
                    "event_url": url,
                    "location": location,
                    "division": division_name,
                    "placement": place_idx,
                    "athlete_name": name,
                    "academy": academy,
                    "country": country,
                })

    log.info("Event '%s': %d result rows, %d divisions",
             event["title"], len(rows),
             len({r["division"] for r in rows}))
    return rows


# ── Upload ─────────────────────────────────────────────────────────────────────

def upload_to_supabase(rows: list[dict]) -> None:
    supabase_url = os.environ.get("SUPABASE_URL", "")
    service_key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not service_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — skipping upload")
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
            client.table("tournament_results").upsert(batch, on_conflict="event_url,division,placement,athlete_name").execute()
            total += len(batch)
            log.info("Uploaded %d / %d rows", total, len(rows))
        except Exception as e:
            log.error("Upload batch %d failed: %s", i // BATCH, e)

    log.info("Upload complete: %d rows", total)


# ── Search ─────────────────────────────────────────────────────────────────────

def search_results(rows: list[dict], query: str) -> list[dict]:
    q = query.lower()
    return [r for r in rows if q in r.get("athlete_name", "").lower()
                               or q in r.get("academy", "").lower()]


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_page_range(s: str) -> tuple[int, int]:
    if "-" in s:
        a, b = s.split("-", 1)
        return int(a), int(b)
    n = int(s)
    return n, n


def main():
    ap = argparse.ArgumentParser(description="Scrape ADCC results from adcombat.com")
    ap.add_argument("--dry-run", action="store_true", help="Don't write any files")
    ap.add_argument("--pages", default=None, help="Event list page range, e.g. 1-5")
    ap.add_argument("--save-local", default=DEFAULT_OUT, help="Output JSON file")
    ap.add_argument("--search", default=None, help="Search athlete name in results")
    ap.add_argument("--upload", action="store_true", help="Upload to Supabase after scraping")
    ap.add_argument("--resume", action="store_true", help="Skip events already in output file")
    ap.add_argument("--delay", type=float, default=DELAY, help="Delay between requests (seconds)")
    args = ap.parse_args()

    delay = args.delay
    out_path = Path(args.save_local)

    # Load existing results for resume
    existing_rows: list[dict] = []
    existing_urls: set[str] = set()
    if args.resume and out_path.exists():
        try:
            existing_rows = json.loads(out_path.read_text(encoding="utf-8"))
            existing_urls = {r["event_url"] for r in existing_rows}
            log.info("Resume: loaded %d existing rows, %d event URLs", len(existing_rows), len(existing_urls))
        except Exception as e:
            log.warning("Could not load existing file for resume: %s", e)

    # Determine page range
    total_pages = get_total_pages()
    log.info("Total event list pages: %d", total_pages)

    if args.pages:
        page_start, page_end = parse_page_range(args.pages)
    else:
        page_start, page_end = 1, total_pages

    # Collect all event URLs
    all_events: list[dict] = []
    for pg in range(page_start, page_end + 1):
        events = fetch_event_list_page(pg)
        all_events.extend(events)
        time.sleep(delay)

    # Deduplicate by URL
    seen = set()
    unique_events = []
    for e in all_events:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique_events.append(e)

    log.info("Total unique events to scrape: %d", len(unique_events))

    # Scrape each event
    all_rows: list[dict] = list(existing_rows)
    for i, event in enumerate(unique_events, 1):
        if event["url"] in existing_urls:
            log.info("[%d/%d] Skipping (resume): %s", i, len(unique_events), event["title"])
            continue

        log.info("[%d/%d] Scraping: %s", i, len(unique_events), event["title"])
        rows = fetch_event_results(event)
        all_rows.extend(rows)

        if not args.dry_run:
            out_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        time.sleep(delay)

    log.info("Done. Total rows: %d", len(all_rows))

    if args.search:
        matches = search_results(all_rows, args.search)
        print(f"\nSearch results for '{args.search}': {len(matches)} matches")
        for r in matches:
            print(f"  [{r['placement']}] {r['athlete_name']} | {r['division']} | {r['event_title']} ({r['event_date']})")

    if args.upload and not args.dry_run:
        upload_to_supabase(all_rows)


if __name__ == "__main__":
    main()
