"""
scrape_ibjjf_registrations.py — Scrape upcoming IBJJF event registrations.

Designed for cron. Idempotent: deletes stale 'registered' rows for past IBJJF
events, then delete+reinserts per event so the list stays fresh.

Flow:
  1. Purge 'registered' rows for events whose date has passed
  2. Fetch upcoming IBJJF tournament list from mattrack API
     (fallback to ibjjf.com/api/v1/events/upcomings.json if mattrack is down)
  3. For each championship: scrape ibjjfdb.com/ChampionshipResults/{id}/PublicRegistrations
     and parse the embedded `const model = [...]` JSON blob
  4. Upsert into tournament_results (Supabase) with status='registered'

Usage:
    python scrape_ibjjf_registrations.py                # full nightly refresh
    python scrape_ibjjf_registrations.py --id 3133      # single event
    python scrape_ibjjf_registrations.py --days 3       # only events within N days
    python scrape_ibjjf_registrations.py --dry-run      # print only, no DB writes
    python scrape_ibjjf_registrations.py --no-purge     # skip purge step
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, timedelta

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ibjjf_reg")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kzqvfuqxtbrhlgphyntb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

SOURCE = "ibjjf"

MATTRACK_API = "https://www.mattrack.net/api/tournaments"
UPCOMING_API = "https://ibjjf.com/api/v1/events/upcomings.json"
REG_URL      = "https://www.ibjjfdb.com/ChampionshipResults/{id}/PublicRegistrations"
LOGO_ID_RE   = re.compile(r"/Championship/Logo/(\d+)")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://ibjjf.com/",
}

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ── Date parsing (fallback) ────────────────────────────────────────────────────

def parse_date(text: str, year: int) -> str:
    """'May 2 - May 3' → '2026-05-02'  (returns start date)"""
    text = (text or "").replace("*", "").strip()
    if not text:
        return ""
    first = text.split(" - ")[0].strip()
    parts = first.split()
    if len(parts) >= 2:
        m = MONTH_MAP.get(parts[0].lower()[:3])
        if m:
            try:
                return date(year, m, int(parts[1])).isoformat()
            except ValueError:
                pass
    return ""


# ── Fetch upcoming events ──────────────────────────────────────────────────────

def get_upcoming_events_from_mattrack() -> list[dict]:
    """Primary source: mattrack.net tournament API (reliable, has parsed dates)."""
    try:
        r = requests.get(MATTRACK_API, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("mattrack API fetch failed: %s", e)
        return []

    events = []
    for t in data:
        if t.get("source") != SOURCE:
            continue
        if t.get("is_past"):
            continue
        events.append({
            "id": str(t.get("id")),
            "name": t.get("name", ""),
            "start": t.get("start") or "",
            "end": t.get("end") or "",
        })
    events.sort(key=lambda e: e["start"] or "9999-99-99")
    log.info("mattrack API: %d upcoming IBJJF events", len(events))
    return events


def get_upcoming_events_from_ibjjf() -> list[dict]:
    """Fallback source: IBJJF upcomings.json."""
    try:
        resp = requests.get(UPCOMING_API, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning("IBJJF upcomings fetch failed: %s", e)
        return []

    today = date.today()
    events = []
    for ev in data.get("championships", []):
        logo_url = ev.get("urlLogo", "")
        m = LOGO_ID_RE.search(logo_url)
        if not m:
            continue
        champ_id = m.group(1)
        name     = ev.get("name", "")
        interval = ev.get("eventIntervalDays", "")
        month    = ev.get("eventMonth", "")
        year     = today.year if today.month <= 6 else today.year + 1
        if month:
            mn = MONTH_MAP.get(month.lower()[:3], 0)
            if mn and mn < today.month - 1:
                year = today.year + 1
            elif mn and mn >= today.month:
                year = today.year
        start = parse_date(interval, year)
        events.append({"id": champ_id, "name": name, "start": start, "end": ""})

    log.info("IBJJF API: %d upcoming championships", len(events))
    return events


def get_upcoming_events() -> list[dict]:
    """Try mattrack first, fall back to IBJJF API."""
    events = get_upcoming_events_from_mattrack()
    if events:
        return events
    return get_upcoming_events_from_ibjjf()


# ── Scrape one event registration page ────────────────────────────────────────

def _parse_model(html: str) -> list[dict]:
    """Extract the `const model = [...]` JSON embedded in the page."""
    idx = html.find("const model = [")
    if idx == -1:
        return []
    chunk = html[idx + len("const model = "):]
    depth = 0
    for j, c in enumerate(chunk):
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(chunk[: j + 1])
                except json.JSONDecodeError:
                    return []
    return []


def scrape_registrations(champ_id: str, event_name: str, event_date: str) -> list[dict]:
    """Scrape all registrations for one championship from the embedded JS model."""
    url = REG_URL.format(id=champ_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning("  %s fetch error: %s", champ_id, e)
        return []

    model = _parse_model(resp.text)
    if not model:
        log.warning("  %s: no model data found (%d bytes)", champ_id, len(resp.text))
        return []

    rows = []
    for cat in model:
        friendly = cat.get("FriendlyName", "")  # e.g. "BLACK / Master 6 / Male / Ultra-Heavy"
        for reg in cat.get("RegistrationCategories", []):
            name = (reg.get("AthleteName") or "").strip()
            team = (reg.get("AcademyTeamName") or "").strip()
            if not name:
                continue
            rows.append({
                "event_id":        str(champ_id),
                "event_title":     event_name,
                "event_date":      event_date or None,
                "source":          SOURCE,
                "division":        friendly,
                "athlete_name":    name.lower(),
                "athlete_display": name,
                "team":            team,
                "placement":       None,
                "status":          "registered",
                "athlete_id":      None,
                "country":         None,
                "country_code":    None,
            })

    return rows


# ── Supabase upsert ───────────────────────────────────────────────────────────

def purge_past_registrations() -> int:
    """Delete 'registered' rows for IBJJF events whose date has passed."""
    if not SUPABASE_KEY:
        return 0
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    try:
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/tournament_results",
            params={
                "source": f"eq.{SOURCE}",
                "status": "eq.registered",
                "event_date": f"lt.{yesterday}",
            },
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "return=representation",
            },
            timeout=30,
        )
    except Exception as e:
        log.warning("Purge failed: %s", e)
        return 0

    count = 0
    if resp.status_code in (200, 204):
        try:
            count = len(resp.json())
        except Exception:
            pass
    if count:
        log.info("Purged %d stale registrations for past IBJJF events", count)
    return count


def supabase_upsert(rows: list[dict], event_id: str) -> int:
    """Delete old 'registered' rows for this event, then insert fresh batch."""
    if not rows or not SUPABASE_KEY:
        return 0

    # Delete existing registered rows for this event
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/tournament_results",
            params={
                "source": f"eq.{SOURCE}",
                "event_id": f"eq.{event_id}",
                "status": "eq.registered",
            },
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            timeout=30,
        )
    except Exception as e:
        log.warning("  Delete step failed for %s: %s", event_id, e)

    insert_url = f"{SUPABASE_URL}/rest/v1/tournament_results"
    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i : i + 500]
        try:
            resp = requests.post(
                insert_url,
                json=batch,
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates",
                },
                timeout=60,
            )
        except Exception as e:
            log.warning("  Insert batch failed for %s: %s", event_id, e)
            continue

        if resp.status_code in (200, 201):
            total += len(batch)
        else:
            log.warning("  Supabase insert error %d: %s", resp.status_code, resp.text[:300])

    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Scrape IBJJF registrations (cron-safe)")
    ap.add_argument("--id", help="Scrape single championship ID")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--days", type=int, default=0,
                    help="Only scrape events within N days from today (0 = all upcoming)")
    ap.add_argument("--no-purge", action="store_true",
                    help="Skip purging stale registrations for past events")
    args = ap.parse_args()

    if not SUPABASE_KEY and not args.dry_run:
        log.error("SUPABASE_SERVICE_KEY not set. Use --dry-run or set env var.")
        sys.exit(1)

    if not args.dry_run and not args.no_purge:
        purge_past_registrations()

    if args.id:
        # Try to get name+date from upcoming list; fall back to bare id
        all_ev = get_upcoming_events()
        match = next((e for e in all_ev if str(e["id"]) == str(args.id)), None)
        events = [match] if match else [{"id": str(args.id), "name": f"Championship {args.id}", "start": ""}]
    else:
        events = get_upcoming_events()

    if args.days and not args.id:
        cutoff = date.today() + timedelta(days=args.days)
        events = [
            e for e in events
            if e["start"] and date.today() <= date.fromisoformat(e["start"]) <= cutoff
        ]
        log.info("Filtered to %d events within %d days", len(events), args.days)

    if not events:
        log.info("No upcoming events to scrape.")
        return

    total_rows = 0
    total_events = 0
    per_event_counts = []

    for ev in events:
        log.info("Scraping %s — %s (%s)", ev["id"], ev["name"][:70], ev["start"])
        rows = scrape_registrations(ev["id"], ev["name"], ev["start"])

        if args.dry_run:
            for r in rows[:3]:
                print(f"  {r['division']} | {r['athlete_display']} | {r['team']}")
            if len(rows) > 3:
                print(f"  ... {len(rows)} total")
            per_event_counts.append((ev["id"], ev["name"], len(rows)))
        else:
            n = supabase_upsert(rows, ev["id"])
            total_rows += n
            per_event_counts.append((ev["id"], ev["name"], n))
            log.info("  Saved %d rows", n)

        total_events += 1
        time.sleep(0.4)

    log.info("Done. Total: %d registrations across %d events", total_rows, total_events)

    # Summary
    print()
    print("=== Per-event registration counts ===")
    for eid, name, count in per_event_counts:
        print(f"  {eid:>5} | {count:>5} | {name[:70]}")


if __name__ == "__main__":
    main()
