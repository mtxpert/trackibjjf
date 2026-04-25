"""
scrape_sc_registrations.py — Scrape upcoming Smoothcomp event registrations.

Designed for nightly cron. Idempotent: deletes stale registered rows for
past events, then delete+reinserts per event so the list stays fresh.

Flow:
  1. Purge 'registered' rows for events whose date has passed
  2. For each Smoothcomp subdomain/federation: fetch upcoming events
  3. For each event: fetch paginated participant registrations
  4. Upsert into tournament_results with status='registered'

Usage:
    python scrape_sc_registrations.py                  # full nightly refresh
    python scrape_sc_registrations.py --sub naga       # single subdomain
    python scrape_sc_registrations.py --days 3         # only events in next 3 days
    python scrape_sc_registrations.py --dry-run        # print only, no DB writes
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import date, timedelta

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sc_reg")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kzqvfuqxtbrhlgphyntb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

SUBDOMAINS = {
    "adcc":                  {"fed_id": 176, "org": "adcc"},
    "naga":                  {"fed_id": 32,  "org": "naga"},
    "compnet":               {"fed_id": 30,  "org": "compnet"},
    "grapplingindustries":   {"fed_id": 23,  "org": "gi"},
    "fujibjj":               {"fed_id": 201, "org": "fuji"},
    "goodfight":             {"fed_id": 333, "org": "goodfight"},
    "newbreedbjj":           {"fed_id": 65,  "org": "newbreed"},
    "pbjjf":                 {"fed_id": 124, "org": "pbjjf"},
    "united":                {"fed_id": 272, "org": "united"},
    "submissionchallenge":   {"fed_id": 45,  "org": "subchallenge"},
    "grapplingx":            {"fed_id": 27,  "org": "grapplingx"},
    "rollalot":              {"fed_id": 220, "org": "rollalot"},
}


def _parse_events_js(html: str) -> list:
    idx = html.find("var events")
    if idx == -1:
        return []
    chunk = html[idx:]
    try:
        start = chunk.index("[")
        depth = 0
        for j, c in enumerate(chunk[start:], start):
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    return json.loads(chunk[start : j + 1])
    except Exception:
        pass
    return []


def get_upcoming_events(subdomain: str, fed_id: int) -> list[dict]:
    url = f"https://{subdomain}.smoothcomp.com/en/federation/{fed_id}/events/upcoming"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log.warning("  %s event fetch failed: %s", subdomain, e)
        return []

    raw = _parse_events_js(r.text)
    events = []
    for e in raw:
        start = (e.get("startdate") or "")[:10]
        events.append({
            "id": str(e["id"]),
            "title": e.get("title", ""),
            "start": start,
            "end": (e.get("enddate") or "")[:10],
            "city": e.get("location_city", ""),
            "country": e.get("location_country_human", ""),
            "url": e.get("url", ""),
        })
    return events


def scrape_registrations(subdomain: str, org: str, event: dict) -> list[dict]:
    """Scrape participants for an SC event.

    Uses the POST /en/event/<id>/participants endpoint (with CSRF) which
    returns ALL registrations including unapproved ones. The older
    GET /participants-new only returned approved entries — students at
    clubs that require coach approval (e.g., Gracie Barra Lawrenceville
    kids) were invisible to us until the coach manually approved them in
    Smoothcomp.
    """
    event_id = event["id"]
    # Bare smoothcomp.com is required for the POST /participants endpoint —
    # www.smoothcomp.com 302-redirects POST to bare and the body is dropped,
    # so federation-affiliated events use a real subdomain (ajpbjj, naga,
    # etc.) and 'misc' events use bare.
    base = "https://smoothcomp.com" if subdomain in ("www", "", None) else f"https://{subdomain}.smoothcomp.com"
    log.info("  Scraping event %s: %s", event_id, event["title"][:50])

    sess = requests.Session()
    sess.headers.update(HEADERS)

    # Load participants page (not the event landing) — its session cookies
    # and CSRF token are what the POST endpoint validates against.
    try:
        r = sess.get(f"{base}/en/event/{event_id}/participants", timeout=(10, 20))
        r.raise_for_status()
    except Exception as e:
        log.warning("  Participants page %s failed: %s", event_id, e)
        return []

    csrf = ""
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', r.text)
    if m:
        csrf = m.group(1)

    try:
        r2 = sess.post(
            f"{base}/en/event/{event_id}/participants",
            data={"_token": csrf, "show_unapproved": "1"},
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-TOKEN": csrf,
                "Accept": "application/json",
            },
            timeout=(10, 30),
        )
        r2.raise_for_status()
        data = r2.json()
    except requests.exceptions.Timeout:
        log.warning("  Event %s timed out", event_id)
        return []
    except Exception as e:
        log.warning("  Event %s fetch failed: %s", event_id, e)
        return []

    # Build category-value-id → name map from the categories tree so we can
    # synthesize a readable division string per registration.
    cat_value_names = {}
    cats = data.get("categories") or {}
    if isinstance(cats, dict):
        for entry in cats.values():
            entry_cats = (entry or {}).get("categories") or []
            for cat in entry_cats:
                for v in cat.get("values") or []:
                    if v.get("id") is not None:
                        cat_value_names[v["id"]] = v.get("name") or v.get("title") or ""

    rows = []
    for group in data.get("participants", []):
        if not isinstance(group, dict):
            continue
        group_division = group.get("name") or ""
        for reg in (group.get("registrations") or []):
            if not isinstance(reg, dict):
                continue
            first = (reg.get("firstname") or "").strip()
            middle = (reg.get("middle_name") or "").strip()
            last = (reg.get("lastname") or "").strip()
            name = " ".join(p for p in (first, middle, last) if p).strip()
            if not name:
                continue

            club_name = reg.get("clubName") or ""
            affil = reg.get("affiliationName") or ""
            team_field = reg.get("teamName") or ""
            # Prefer affiliation > teamName > club, matching how SC shows it
            team = affil or team_field or club_name

            # Build a division string from the registration's category values
            # if the group name doesn't already encode them.
            division = group_division
            if not division:
                parts = []
                for c in (reg.get("categories") or []):
                    nm = cat_value_names.get(c.get("category_value_id"))
                    if nm:
                        parts.append(nm)
                if parts:
                    division = " / ".join(parts)

            rows.append({
                "event_id": event_id,
                "event_title": event["title"],
                "event_date": event["start"],
                "source": org,
                "division": division,
                "athlete_name": name.lower(),
                "athlete_display": name,
                "team": team,
                "placement": None,
                "status": "registered",
                "athlete_id": None,
                "country": reg.get("cn") or "",
                "country_code": reg.get("cn") or "",
            })

    log.info("  Event %s (%s): %d registrations (incl. unapproved)",
             event_id, event["title"][:50], len(rows))
    return rows


SMOOTHCOMP_ORGS = list({cfg["org"] for cfg in SUBDOMAINS.values()})


def purge_past_registrations() -> int:
    """Delete 'registered' rows for Smoothcomp events whose date has passed."""
    if not SUPABASE_KEY:
        return 0
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    total_deleted = 0
    for org in SMOOTHCOMP_ORGS:
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/tournament_results",
            params={
                "source": f"eq.{org}",
                "status": "eq.registered",
                "event_date": f"lt.{yesterday}",
            },
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "return=representation",
            },
            timeout=15,
        )
        if resp.status_code in (200, 204):
            try:
                total_deleted += len(resp.json())
            except Exception:
                pass
    if total_deleted:
        log.info("Purged %d stale registrations for past events", total_deleted)
    return total_deleted


def supabase_upsert(rows: list[dict], org: str, event_id: str) -> int:
    if not rows or not SUPABASE_KEY:
        return 0

    # Delete existing registered rows for this org+event
    del_url = f"{SUPABASE_URL}/rest/v1/tournament_results"
    del_params = {"source": f"eq.{org}", "event_id": f"eq.{event_id}", "status": "eq.registered"}
    requests.delete(
        del_url,
        params=del_params,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
        timeout=15,
    )

    # Insert in batches of 500
    insert_url = f"{SUPABASE_URL}/rest/v1/tournament_results"
    total = 0
    for i in range(0, len(rows), 500):
        batch = []
        for r in rows[i : i + 500]:
            batch.append({
                "source": r["source"],
                "event_id": r["event_id"],
                "event_title": r["event_title"],
                "event_date": r["event_date"] or None,
                "division": r["division"],
                "athlete_name": r["athlete_name"],
                "athlete_display": r["athlete_display"],
                "team": r["team"],
                "placement": None,
                "status": "registered",
                "athlete_id": None,
                "country": r.get("country"),
                "country_code": r.get("country_code"),
            })

        resp = requests.post(
            insert_url,
            json=batch,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            timeout=30,
        )
        if resp.status_code in (200, 201):
            total += len(batch)
        else:
            log.warning("  Supabase insert error %d: %s", resp.status_code, resp.text[:200])

    return total


def main():
    ap = argparse.ArgumentParser(description="Scrape Smoothcomp registrations (cron-safe)")
    ap.add_argument("--sub", help="Single subdomain to scrape")
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

    subs = {args.sub: SUBDOMAINS[args.sub]} if args.sub else SUBDOMAINS
    total_events = 0
    total_regs = 0

    for subdomain, cfg in subs.items():
        fed_id = cfg["fed_id"]
        org = cfg["org"]
        log.info("=== %s (fed %d, org=%s) ===", subdomain, fed_id, org)

        events = get_upcoming_events(subdomain, fed_id)
        log.info("  Found %d upcoming events", len(events))

        if args.days:
            cutoff = date.today() + timedelta(days=args.days)
            events = [e for e in events if e["start"] and
                      date.today() <= date.fromisoformat(e["start"]) <= cutoff]
            log.info("  Filtered to %d events within %d days", len(events), args.days)

        for ev in events:
            # 90-second overall timeout per event to avoid hangs
            pool = ThreadPoolExecutor(max_workers=1)
            fut = pool.submit(scrape_registrations, subdomain, org, ev)
            try:
                rows = fut.result(timeout=90)
            except TimeoutError:
                log.warning("  Event %s timed out after 90s, skipping", ev["id"])
                rows = []
            except Exception as e:
                log.warning("  Event %s error: %s", ev["id"], e)
                rows = []
            pool.shutdown(wait=False)
            total_events += 1

            if args.dry_run:
                for r in rows[:3]:
                    print(f"  {r['division']} | {r['athlete_display']} | {r['team']}")
                if len(rows) > 3:
                    print(f"  ... {len(rows)} total")
                total_regs += len(rows)
            else:
                n = supabase_upsert(rows, org, ev["id"])
                total_regs += n
                log.info("  Saved %d rows for event %s", n, ev["id"])

            time.sleep(0.5)

        time.sleep(1)

    # ── Misc / one-off SC events (hosted on bare smoothcomp.com, no fed
    # subdomain) — pulled from tournament_events where source='misc' and
    # start_date is within the lookback window. Otherwise these events
    # land in the DB metadata but their rosters never get scraped, and
    # mattrack.net shows 0 athletes for charity/local opens like Tap
    # Cancer Out the day they happen.
    if not args.sub:  # only on full runs
        misc_lookback_days = args.days or 7
        misc_events = _fetch_misc_upcoming(misc_lookback_days)
        log.info("=== misc (bare smoothcomp.com, %d upcoming events) ===", len(misc_events))
        for ev in misc_events:
            pool = ThreadPoolExecutor(max_workers=1)
            fut = pool.submit(scrape_registrations, "www", "misc", ev)
            try:
                rows = fut.result(timeout=120)
            except TimeoutError:
                log.warning("  Misc event %s timed out, skipping", ev["id"])
                rows = []
            except Exception as e:
                log.warning("  Misc event %s error: %s", ev["id"], e)
                rows = []
            pool.shutdown(wait=False)
            total_events += 1
            if args.dry_run:
                total_regs += len(rows)
            else:
                n = supabase_upsert(rows, "misc", ev["id"])
                total_regs += n
                log.info("  Saved %d rows for misc event %s", n, ev["id"])
            time.sleep(0.5)

    log.info("Done. Total: %d registrations across %d events", total_regs, total_events)


def _fetch_misc_upcoming(within_days: int) -> list[dict]:
    """Return upcoming events from tournament_events where source='misc' and
    start_date is in [today, today + within_days]."""
    if not SUPABASE_KEY:
        return []
    today = date.today()
    cutoff = (today + timedelta(days=within_days)).isoformat()
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }
    url = (f"{SUPABASE_URL}/rest/v1/tournament_events"
           f"?select=event_id,name,start_date,end_date,location"
           f"&source=eq.misc"
           f"&start_date=gte.{today.isoformat()}"
           f"&start_date=lte.{cutoff}"
           f"&order=start_date.asc"
           f"&limit=200")
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
    except Exception as e:
        log.warning("misc event fetch failed: %s", e)
        return []
    return [{
        "id": str(row["event_id"]),
        "title": row.get("name") or f"Event {row['event_id']}",
        "start": row.get("start_date") or "",
        "end": row.get("end_date") or "",
        "location": row.get("location") or "",
    } for row in rows]


if __name__ == "__main__":
    main()
