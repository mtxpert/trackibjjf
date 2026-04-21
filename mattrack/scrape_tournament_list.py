"""
scrape_tournament_list.py — Populate public.tournament_events with all
upcoming/recent tournaments across every source (IBJJF, NAGA, CompNet,
ADCC, GI, Fuji, GoodFight, NewBreed, PBJJF, United, SubChallenge,
GrapplingX, RollAlot). Designed for a nightly cron.

The client UI reads ONLY from this table — no more live scrapers on the
request path.

Usage:
    python scrape_tournament_list.py                 # full refresh
    python scrape_tournament_list.py --source naga   # one source only
    python scrape_tournament_list.py --dry-run       # print, don't write
"""

import argparse
import calendar
import json
import logging
import os
import re
import sys
import time
from datetime import date, timedelta

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tlist")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kzqvfuqxtbrhlgphyntb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

TIMEOUT = (5, 15)

# ── Smoothcomp subdomains → org key ─────────────────────────────────────────
SMOOTHCOMP_SUBS = {
    "adcc":                {"fed_id": 176, "org": "adcc"},
    "naga":                {"fed_id": 32,  "org": "naga"},
    "compnet":             {"fed_id": 30,  "org": "compnet"},
    "grapplingindustries": {"fed_id": 23,  "org": "gi"},
    "fujibjj":             {"fed_id": 201, "org": "fuji"},
    "goodfight":           {"fed_id": 333, "org": "goodfight"},
    "newbreedbjj":         {"fed_id": 65,  "org": "newbreed"},
    "pbjjf":               {"fed_id": 124, "org": "pbjjf"},
    "united":              {"fed_id": 272, "org": "united"},
    "submissionchallenge": {"fed_id": 45,  "org": "subchallenge"},
    "grapplingx":          {"fed_id": 27,  "org": "grapplingx"},
    "rollalot":            {"fed_id": 220, "org": "rollalot"},
}

# Static city lat/lng for IBJJF events (the schedule API doesn't return coords)
_CITY_COORDS = {
    "long beach, ca":     (33.770, -118.193),
    "college park, ga":   (33.653, -84.449),
    "chicago, il":        (41.878, -87.630),
    "denver, co":         (39.739, -104.984),
    "columbus, oh":       (39.961, -82.999),
    "boston, ma":         (42.360, -71.059),
    "san diego, ca":      (32.716, -117.161),
    "santa cruz, ca":     (36.974, -122.030),
    "san antonio, tx":    (29.424, -98.494),
    "las vegas, nv":      (36.175, -115.136),
    "kissimmee, fl":      (28.292, -81.408),
    "miami, fl":          (25.775, -80.208),
    "orlando, fl":        (28.538, -81.379),
    "houston, tx":        (29.760, -95.370),
    "dallas, tx":         (32.776, -96.797),
    "los angeles, ca":    (34.052, -118.244),
    "new york, ny":       (40.713, -74.006),
    "philadelphia, pa":   (39.953, -75.165),
    "charlotte, nc":      (35.227, -80.843),
    "seattle, wa":        (47.606, -122.332),
    "phoenix, az":        (33.448, -112.074),
    "salt lake city, ut": (40.761, -111.891),
    "sacramento, ca":     (38.581, -121.494),
    "reno, nv":           (39.530, -119.813),
    "richmond, va":       (37.541, -77.434),
    "washington":         (38.907, -77.037),
    "lisbon":             (38.717, -9.139),
    "dublin":             (53.349, -6.260),
    "barcelona":          (41.389,  2.159),
    "madrid":             (40.417, -3.703),
    "london":             (51.507, -0.128),
    "paris":              (48.857,  2.353),
    "milan":              (45.464,  9.190),
    "rome":               (41.902, 12.496),
    "amsterdam":          (52.370,  4.895),
    "berlin":             (52.520, 13.405),
    "abu dhabi":          (24.453, 54.377),
    "barueri":            (-23.505, -46.876),
    "tokyo":              (35.689, 139.692),
    "sydney":             (-33.868, 151.209),
    "dubai":              (25.204, 55.270),
    "petit-lancy":        (46.184,  6.112),
}


def _geocode(city: str, state: str = "") -> tuple[float, float] | None:
    key = f"{city.lower()}, {state.lower()}".strip(", ") if state else city.lower()
    if key in _CITY_COORDS:
        return _CITY_COORDS[key]
    city_only = city.lower().split(",")[0].strip()
    if city_only in _CITY_COORDS:
        return _CITY_COORDS[city_only]
    return None


# ── Smoothcomp parsing ──────────────────────────────────────────────────────
def _parse_events_js(html: str) -> list:
    """Extract the `var events = [...]` array from a Smoothcomp events page."""
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


def scrape_smoothcomp(subdomain: str, fed_id: int, org: str) -> list[dict]:
    """Return tournament_events rows for one Smoothcomp federation."""
    url = f"https://{subdomain}.smoothcomp.com/en/federation/{fed_id}/events/upcoming"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.warning("  [%s] fetch failed: %s", org, e)
        return []

    raw = _parse_events_js(r.text)
    today = date.today()
    rows = []
    for e in raw:
        try:
            event_id = str(e.get("id") or "").strip()
            if not event_id:
                continue
            start = (e.get("startdate") or "")[:10] or None
            end   = (e.get("enddate")   or "")[:10] or None
            city  = (e.get("location_city") or "").strip()
            country = (e.get("location_country_human") or "").strip()
            country_code = (e.get("location_country") or "").strip()

            try:
                lat = float(e.get("location_lat") or 0) or None
                lng = float(e.get("location_long") or 0) or None
            except (ValueError, TypeError):
                lat = lng = None

            location_bits = [b for b in (city, country) if b]
            location = ", ".join(location_bits)

            is_past = bool(end and date.fromisoformat(end) < today)

            rows.append({
                "source": org,
                "event_id": event_id,
                "name": (e.get("title") or "").strip(),
                "start_date": start,
                "end_date": end,
                "location": location,
                "city": city,
                "country": country,
                "country_code": country_code,
                "lat": lat,
                "lng": lng,
                "url": e.get("url") or "",
                "cover_image": e.get("cover_image") or "",
                "has_brackets": False,
                "is_past": is_past,
                "registered_count": int(e.get("registrations") or 0) if isinstance(e.get("registrations"), (int, str)) and str(e.get("registrations")).isdigit() else 0,
            })
        except Exception as ex:
            log.warning("  [%s] skip event %s: %s", org, e.get("id"), ex)
    return rows


# ── IBJJF ───────────────────────────────────────────────────────────────────
_IBJJF_API = "https://ibjjf.com/api/v1/events/upcomings.json"
_IBJJF_HEADERS = {
    **HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://ibjjf.com/events/championships",
    "X-Requested-With": "XMLHttpRequest",
}
_LOGO_ID_RE = re.compile(r"/Championship/Logo/(\d+)")
_MONTH_MAP  = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}

_BCS_BASE = "https://www.bjjcompsystem.com"
_BCS_BLOCK_RE = re.compile(r"id=['\"]tournament-display-(\d+)['\"]", re.DOTALL)
_BCS_IMG_ALT  = re.compile(r'alt=["\']([^"\']+)["\']')
_BCS_TDAYS_DATE = re.compile(
    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*(\d{2}/\d{2})'
)


def _parse_ibjjf_date(text: str, year: int) -> tuple[str | None, str | None]:
    """Parse 'May 28* - May 31' → ('2026-05-28', '2026-05-31')."""
    text = text.replace("*", "").strip()
    if not text:
        return None, None
    parts = [p.strip() for p in text.split(" - ")]

    def single(s, fallback_month=None):
        tokens = s.split()
        if len(tokens) == 2:
            m = _MONTH_MAP.get(tokens[0].lower())
            if m:
                return m, int(tokens[1])
        elif len(tokens) == 1 and tokens[0].isdigit():
            return fallback_month, int(tokens[0])
        return None, None

    sm, sd = single(parts[0])
    if sm is None:
        return None, None
    try:
        start_iso = date(year, sm, sd).isoformat()
        if len(parts) == 2:
            em, ed = single(parts[1], fallback_month=sm)
            end_iso = date(year, em or sm, ed or sd).isoformat() if ed else start_iso
        else:
            end_iso = start_iso
        return start_iso, end_iso
    except (ValueError, TypeError):
        return None, None


def _infer_bcs_dates(tournament_id: str) -> tuple[str | None, str | None]:
    """Infer (start, end) from bjjcompsystem tournament_days page."""
    today = date.today()
    try:
        r = requests.get(
            f"{_BCS_BASE}/tournaments/{tournament_id}/tournament_days",
            headers=HEADERS, timeout=(3, 8),
        )
        if not r.ok:
            return None, None
        mmdd = sorted(set(_BCS_TDAYS_DATE.findall(r.text)))
        if not mmdd:
            return None, None
        year = today.year
        parsed = []
        for d in mmdd:
            mm, dd = d.split("/")
            cand = date(year, int(mm), int(dd))
            if (cand - today).days > 180:
                cand = cand.replace(year=year - 1)
            parsed.append(cand)
        parsed.sort()
        return parsed[0].isoformat(), parsed[-1].isoformat()
    except Exception:
        return None, None


def scrape_ibjjf_schedule() -> list[dict]:
    """IBJJF championships from https://ibjjf.com/api/v1/events/upcomings.json."""
    today = date.today()
    try:
        r = requests.get(_IBJJF_API, headers=_IBJJF_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("  [ibjjf schedule] fetch failed: %s", e)
        return []

    rows = []
    for ev in data.get("championships", []) or []:
        try:
            slug = ev.get("slug", "")
            name = ev.get("name") or slug
            date_text = ev.get("eventIntervalDays", "") or ""
            city = (ev.get("city") or "").strip()
            state = (ev.get("state") or "").strip()
            country = (ev.get("country") or "").strip()
            logo_url = ev.get("urlLogo", "") or ""

            m = _LOGO_ID_RE.search(logo_url)
            champ_id = m.group(1) if m else slug
            if not champ_id:
                continue

            year_m = re.search(r"(\d{4})", slug or "")
            year = int(year_m.group(1)) if year_m else today.year

            start_iso, end_iso = _parse_ibjjf_date(date_text, year)
            is_past = bool(end_iso and date.fromisoformat(end_iso) < today)
            coords = _geocode(city, state)
            location = f"{city}, {state}" if state else city

            rows.append({
                "source": "ibjjf",
                "event_id": str(champ_id),
                "name": name,
                "start_date": start_iso,
                "end_date": end_iso,
                "location": location,
                "city": city,
                "country": country,
                "country_code": "US" if state and len(state) == 2 else "",
                "lat": coords[0] if coords else None,
                "lng": coords[1] if coords else None,
                "url": f"https://ibjjf.com/events/{slug}" if slug else "",
                "cover_image": f"https://www.ibjjfdb.com/Championship/Logo/{champ_id}"
                               if str(champ_id).isdigit() else "",
                "has_brackets": False,
                "is_past": is_past,
                "registered_count": 0,
            })
        except Exception as ex:
            log.warning("  [ibjjf schedule] skip %s: %s", ev.get("slug"), ex)
    return rows


def scrape_ibjjf_bcs() -> list[dict]:
    """Active bjjcompsystem tournaments (IBJJF events with live brackets)."""
    today = date.today()
    try:
        r = requests.get(f"{_BCS_BASE}/tournaments", headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.warning("  [ibjjf bcs] fetch failed: %s", e)
        return []

    html = r.text
    rows = []
    seen = set()
    positions = [(m.start(), m.group(1)) for m in _BCS_BLOCK_RE.finditer(html)]
    for i, (pos, tid) in enumerate(positions):
        if tid in seen:
            continue
        seen.add(tid)
        end_pos = positions[i + 1][0] if i + 1 < len(positions) else pos + 2000
        block = html[pos:end_pos]
        img_m = _BCS_IMG_ALT.search(block)
        name = img_m.group(1) if img_m else f"Tournament {tid}"
        start_iso, end_iso = _infer_bcs_dates(tid)
        is_past = bool(start_iso and date.fromisoformat(start_iso) < today)
        rows.append({
            "source": "ibjjf",
            "event_id": tid,
            "name": name,
            "start_date": start_iso,
            "end_date": end_iso,
            "location": "",
            "city": "",
            "country": "",
            "country_code": "",
            "lat": None,
            "lng": None,
            "url": f"{_BCS_BASE}/tournaments/{tid}",
            "cover_image": "",
            "has_brackets": True,
            "is_past": is_past,
            "registered_count": 0,
        })
    return rows


def scrape_ibjjf() -> list[dict]:
    """Merged IBJJF rows: schedule + bjjcompsystem. BCS wins on has_brackets."""
    schedule = scrape_ibjjf_schedule()
    bcs = scrape_ibjjf_bcs()
    log.info("  [ibjjf] schedule=%d  bcs=%d", len(schedule), len(bcs))

    by_key = {(r["source"], r["event_id"]): r for r in schedule}
    # BCS IDs are numeric tournament IDs; schedule IDs are championship IDs from
    # the logo URL. They're distinct — just union them. BCS is the source of
    # truth for has_brackets.
    for r in bcs:
        key = (r["source"], r["event_id"])
        if key in by_key:
            by_key[key].update({k: v for k, v in r.items()
                                if v not in (None, "", 0, False)})
            by_key[key]["has_brackets"] = True
        else:
            by_key[key] = r
    return list(by_key.values())


# ── Supabase upsert ─────────────────────────────────────────────────────────
def supabase_upsert(rows: list[dict]) -> int:
    if not rows:
        return 0
    if not SUPABASE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        return 0

    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i : i + 500]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/tournament_events?on_conflict=source,event_id",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=batch,
                timeout=30,
            )
        except Exception as e:
            log.warning("  upsert network error: %s", e)
            continue

        if resp.status_code in (200, 201, 204):
            total += len(batch)
        else:
            log.warning("  upsert HTTP %d: %s", resp.status_code, resp.text[:300])
    return total


# ── Main ────────────────────────────────────────────────────────────────────
# ── Misc: generic Smoothcomp BJJ/grappling events not in a known federation ──
_MISC_BJJ_KEYWORDS = re.compile(
    r"\b(bjj|jiu[\s-]?jitsu|jujitsu|grappl|submission|no[\s-]?gi|nogi|"
    r"adcc|ebi|quintet|sub\s*only|brew\s*jitsu|mma\s*grappl)\b",
    re.IGNORECASE,
)
_KNOWN_SUBDOMAINS = set(SMOOTHCOMP_SUBS.keys())


def _extract_subdomain(url: str) -> str:
    """Return subdomain of a smoothcomp URL. '' if no subdomain (bare smoothcomp.com)."""
    m = re.match(r"https?://([a-z0-9-]+)\.smoothcomp\.com", url or "", re.I)
    return m.group(1).lower() if m else ""


def scrape_smoothcomp_misc() -> list[dict]:
    """Generic smoothcomp.com events not already captured by known federations.
    Filters to BJJ/grappling-keyword events to avoid judo/karate/etc."""
    url = "https://smoothcomp.com/en/events/upcoming"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.warning("  [misc] fetch failed: %s", e)
        return []

    raw = _parse_events_js(r.text)
    today = date.today()
    rows = []
    for e in raw:
        try:
            event_url = e.get("url") or ""
            sub = _extract_subdomain(event_url)
            # Skip if event is on a subdomain we already cover
            if sub in _KNOWN_SUBDOMAINS:
                continue
            title = (e.get("title") or "").strip()
            if not _MISC_BJJ_KEYWORDS.search(title):
                continue

            event_id = str(e.get("id") or "").strip()
            if not event_id:
                continue
            start = (e.get("startdate") or "")[:10] or None
            end = (e.get("enddate") or "")[:10] or None
            city = (e.get("location_city") or "").strip()
            country = (e.get("location_country_human") or "").strip()
            country_code = (e.get("location_country") or "").strip()
            try:
                lat = float(e.get("location_lat") or 0) or None
                lng = float(e.get("location_long") or 0) or None
            except (ValueError, TypeError):
                lat = lng = None
            is_past = bool(end and date.fromisoformat(end) < today)

            rows.append({
                "source": "misc",
                "event_id": event_id,
                "name": title,
                "start_date": start,
                "end_date": end,
                "location": ", ".join([b for b in (city, country) if b]),
                "city": city,
                "country": country,
                "country_code": country_code,
                "lat": lat,
                "lng": lng,
                "url": event_url,  # used by registrations scraper to know subdomain
                "cover_image": e.get("cover_image") or "",
                "has_brackets": False,
                "is_past": is_past,
                "registered_count": 0,
            })
        except Exception as ex:
            log.warning("  [misc] skip event %s: %s", e.get("id"), ex)
    return rows


def _all_sources():
    return ["ibjjf", "misc"] + [cfg["org"] for cfg in SMOOTHCOMP_SUBS.values()]


def main():
    ap = argparse.ArgumentParser(description="Populate tournament_events.")
    ap.add_argument("--source", help="Only run one source (e.g. naga, ibjjf, adcc)")
    ap.add_argument("--dry-run", action="store_true", help="Print rows, don't write")
    args = ap.parse_args()

    if not SUPABASE_KEY and not args.dry_run:
        log.error("SUPABASE_SERVICE_KEY not set. Use --dry-run or export key.")
        sys.exit(1)

    grand_scraped = 0
    grand_upserted = 0
    per_source_report = []

    want = args.source

    # IBJJF (schedule + bjjcompsystem merged under source='ibjjf')
    if not want or want == "ibjjf":
        log.info("=== ibjjf ===")
        rows = scrape_ibjjf()
        log.info("  [ibjjf] scraped %d events", len(rows))
        if args.dry_run:
            for r in rows[:5]:
                print(f"  {r['event_id']:>10}  {r['start_date']}  {r['name'][:60]}")
            if len(rows) > 5:
                print(f"  ...and {len(rows) - 5} more")
            per_source_report.append(("ibjjf", len(rows), 0))
        else:
            n = supabase_upsert(rows)
            per_source_report.append(("ibjjf", len(rows), n))
            grand_upserted += n
        grand_scraped += len(rows)

    # Misc: generic Smoothcomp events on subdomains we don't explicitly cover
    if not want or want == "misc":
        log.info("=== misc  (generic smoothcomp.com) ===")
        rows = scrape_smoothcomp_misc()
        log.info("  [misc] scraped %d events", len(rows))
        if args.dry_run:
            for r in rows[:5]:
                print(f"  {r['event_id']:>10}  {r['start_date']}  {r['name'][:60]}")
            if len(rows) > 5:
                print(f"  ...and {len(rows) - 5} more")
            per_source_report.append(("misc", len(rows), 0))
        else:
            n = supabase_upsert(rows)
            per_source_report.append(("misc", len(rows), n))
            grand_upserted += n
        grand_scraped += len(rows)

    # Smoothcomp: one request per subdomain
    for subdomain, cfg in SMOOTHCOMP_SUBS.items():
        org = cfg["org"]
        if want and want != org:
            continue
        log.info("=== %s  (sub=%s, fed=%d) ===", org, subdomain, cfg["fed_id"])
        rows = scrape_smoothcomp(subdomain, cfg["fed_id"], org)
        log.info("  [%s] scraped %d events", org, len(rows))
        if args.dry_run:
            for r in rows[:5]:
                print(f"  {r['event_id']:>10}  {r['start_date']}  {r['name'][:60]}")
            if len(rows) > 5:
                print(f"  ...and {len(rows) - 5} more")
            per_source_report.append((org, len(rows), 0))
        else:
            n = supabase_upsert(rows)
            per_source_report.append((org, len(rows), n))
            grand_upserted += n
        grand_scraped += len(rows)
        time.sleep(0.2)

    log.info("────────────────────────────────────────")
    for src, scraped, upserted in per_source_report:
        log.info("  %-14s scraped=%4d  upserted=%4d", src, scraped, upserted)
    log.info("  TOTAL          scraped=%4d  upserted=%4d", grand_scraped, grand_upserted)


if __name__ == "__main__":
    main()
