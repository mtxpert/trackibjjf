"""
MatTrack Scraper
All fetching uses requests + BeautifulSoup. No Playwright/browser needed.
Rosters are built by fetching all bracket pages concurrently (~2-5s per tournament).
"""

import re
import json
import calendar
from pathlib import Path
from datetime import datetime
import requests
from bs4 import BeautifulSoup

_instance = Path(__file__).parent / "instance"
if _instance.exists():
    ROSTER_DIR = _instance / "bracket_states"
else:
    # Render: use /tmp which is always writable
    ROSTER_DIR = Path("/tmp/bracket_states")
ROSTER_DIR.mkdir(parents=True, exist_ok=True)
SEED_DIR = Path(__file__).parent / "seed_cache"
_TOURNEY_CACHE_FILE = ROSTER_DIR.parent / "tournaments.json"

def _seed_rosters():
    """Copy seed_cache files into runtime dirs on first boot (won't overwrite existing)."""
    if not SEED_DIR.exists():
        return
    for src in SEED_DIR.glob("*_roster.json"):
        dst = ROSTER_DIR / src.name
        if not dst.exists():
            dst.write_bytes(src.read_bytes())
    # Seed tournament list cache
    t_src = SEED_DIR / "tournaments.json"
    if t_src.exists() and not _TOURNEY_CACHE_FILE.exists():
        try:
            _TOURNEY_CACHE_FILE.write_bytes(t_src.read_bytes())
        except Exception:
            pass

_seed_rosters()

BASE    = "https://www.bjjcompsystem.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


_BLOCK_RE   = re.compile(r"id=['\"]tournament-display-(\d+)['\"]", re.DOTALL)
_IMG_ALT    = re.compile(r'alt=["\']([^"\']+)["\']')
_CAT_HREF   = re.compile(r'/tournaments/(\d+)/categories/(\d+)["\']')
_TDAYS_HREF = re.compile(r'/tournaments/(\d+)/tournament_days/(\d+)["\']')
_TDAYS_DATE = re.compile(r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*(\d{2}/\d{2})')

# Process-level cache: a tournament's dates never change, only need to fetch once.
_TDAYS_CACHE: dict[str, tuple[str, str]] = {}


def _infer_ibjjf_dates(tournament_id: str) -> tuple[str, str]:
    # Fast path — already fetched this process
    if tournament_id in _TDAYS_CACHE:
        return _TDAYS_CACHE[tournament_id]
    result = _infer_ibjjf_dates_uncached(tournament_id)
    _TDAYS_CACHE[tournament_id] = result
    return result


def _infer_ibjjf_dates_uncached(tournament_id: str) -> tuple[str, str]:
    """Return (start_iso, end_iso) for an IBJJF tournament.

    Tries roster cache first (fight_time: 'Sat 04/11 at 03:40 PM').
    Falls back to fetching the tournament_days page.
    Returns ('', '') if nothing found.
    """
    from datetime import date as _date
    today = _date.today()

    # ── 1. try roster cache ────────────────────────────────────────────────
    for roster_dir in [ROSTER_DIR, SEED_DIR]:
        f = roster_dir / f"{tournament_id}_roster.json"
        if f.exists():
            try:
                athletes = json.loads(f.read_text()).get("athletes", [])
                dates = set()
                for a in athletes:
                    ft = a.get("fight_time", "")
                    m = re.search(r'(\d{2}/\d{2})', ft)
                    if m:
                        dates.add(m.group(1))
                if dates:
                    year = today.year
                    parsed = []
                    for d in dates:
                        mm, dd = d.split("/")
                        candidate = _date(year, int(mm), int(dd))
                        # if it looks >6 months in the future, it's probably last year
                        if (candidate - today).days > 180:
                            candidate = candidate.replace(year=year - 1)
                        parsed.append(candidate)
                    parsed.sort()
                    return parsed[0].isoformat(), parsed[-1].isoformat()
            except Exception:
                pass

    # ── 2. fall back to fetching tournament_days page ──────────────────────
    # (only reached if no roster cache — avoids extra HTTP on normal path)
    try:
        resp = requests.get(f"{BASE}/tournaments/{tournament_id}/tournament_days",
                            headers=HEADERS, timeout=(3, 8))
        if resp.ok:
            dates = sorted(set(_TDAYS_DATE.findall(resp.text)))
            if dates:
                year = today.year
                parsed = []
                for d in dates:
                    mm, dd = d.split("/")
                    candidate = _date(year, int(mm), int(dd))
                    if (candidate - today).days > 180:
                        candidate = candidate.replace(year=year - 1)
                    parsed.append(candidate)
                parsed.sort()
                return parsed[0].isoformat(), parsed[-1].isoformat()
    except Exception:
        pass

    return "", ""


def get_tournaments(use_cache_on_fail=True):
    """Fetch all currently listed tournaments from bjjcompsystem.com (regex, no BS4).

    Returns dicts with: id, name, start (YYYY-MM-DD), end (YYYY-MM-DD), is_past.
    """
    from datetime import date as _date
    today = _date.today()

    try:
        resp = requests.get(f"{BASE}/tournaments", headers=HEADERS, timeout=(5, 12))
        resp.raise_for_status()
    except Exception:
        if use_cache_on_fail and _TOURNEY_CACHE_FILE.exists():
            raw = json.loads(_TOURNEY_CACHE_FILE.read_text())
            # Back-fill dates if cache is old format (no start field)
            enriched = []
            for t in raw:
                if not t.get("start"):
                    s, e = _infer_ibjjf_dates(t["id"])
                    t = dict(t, start=s, end=e,
                             is_past=bool(s and _date.fromisoformat(s) < today))
                enriched.append(t)
            return enriched
        raise

    html   = resp.text
    result = []
    seen   = set()
    positions = [(m.start(), m.group(1)) for m in _BLOCK_RE.finditer(html)]
    for i, (pos, tid) in enumerate(positions):
        if tid in seen:
            continue
        seen.add(tid)
        end   = positions[i + 1][0] if i + 1 < len(positions) else pos + 2000
        block = html[pos:end]
        img_m = _IMG_ALT.search(block)
        name  = img_m.group(1) if img_m else f"Tournament {tid}"
        start, end_date = _infer_ibjjf_dates(tid)
        is_past = bool(start and _date.fromisoformat(start) < today)
        result.append({"id": tid, "name": name, "start": start, "end": end_date,
                       "is_past": is_past})

    # Also include any roster-cached tournaments not currently on bjjcompsystem.com
    live_ids = {t["id"] for t in result}
    for roster_file in sorted(SEED_DIR.glob("*_roster.json")):
        tid = roster_file.stem.replace("_roster", "")
        if tid in live_ids:
            continue
        start, end_date = _infer_ibjjf_dates(tid)
        if not start:
            continue  # can't determine date, skip
        is_past = _date.fromisoformat(start) < today
        # Try to get name from seed tournaments.json
        name = f"Tournament {tid}"
        t_seed = SEED_DIR / "tournaments.json"
        if t_seed.exists():
            try:
                for t in json.loads(t_seed.read_text()):
                    if str(t["id"]) == tid:
                        name = t["name"]
                        break
            except Exception:
                pass
        result.append({"id": tid, "name": name, "start": start, "end": end_date,
                       "is_past": is_past})

    if result:
        try:
            _TOURNEY_CACHE_FILE.write_text(json.dumps(result))
        except Exception:
            pass
    return result


# ── IBJJF city geocoding ──────────────────────────────────────────────────────
# Static lat/lng for cities that appear frequently in the IBJJF schedule.
# Key: lowercase "city, state" or "city" for international.
_CITY_COORDS: dict[str, tuple[float, float]] = {
    # USA
    "long beach, ca":       (33.770, -118.193),
    "college park, ga":     (33.653, -84.449),
    "chicago, il":          (41.878, -87.630),
    "denver, co":           (39.739, -104.984),
    "columbus, oh":         (39.961, -82.999),
    "boston, ma":           (42.360, -71.059),
    "san diego, ca":        (32.716, -117.161),
    "santa cruz, ca":       (36.974, -122.030),
    "san antonio, tx":      (29.424, -98.494),
    "las vegas, nv":        (36.175, -115.136),
    "kissimmee, fl":        (28.292, -81.408),
    "miami, fl":            (25.775, -80.208),
    "orlando, fl":          (28.538, -81.379),
    "houston, tx":          (29.760, -95.370),
    "dallas, tx":           (32.776, -96.797),
    "los angeles, ca":      (34.052, -118.244),
    "new york, ny":         (40.713, -74.006),
    "philadelphia, pa":     (39.953, -75.165),
    "charlotte, nc":        (35.227, -80.843),
    "seattle, wa":          (47.606, -122.332),
    "phoenix, az":          (33.448, -112.074),
    "salt lake city, ut":   (40.761, -111.891),
    "sacramento, ca":       (38.581, -121.494),
    "reno, nv":             (39.530, -119.813),
    "richmond, va":         (37.541, -77.434),
    "washington":           (38.907, -77.037),
    # International
    "lisbon":               (38.717, -9.139),
    "dublin":               (53.349, -6.260),
    "barcelona":            (41.389,  2.159),
    "madrid":               (40.417, -3.703),
    "london":               (51.507, -0.128),
    "paris":                (48.857,  2.353),
    "milan":                (45.464,  9.190),
    "rome":                 (41.902, 12.496),
    "amsterdam":            (52.370,  4.895),
    "berlin":               (52.520, 13.405),
    "abu dhabi":            (24.453, 54.377),
    "barueri":              (-23.505, -46.876),
    "tokyo":                (35.689, 139.692),
    "sydney":               (-33.868, 151.209),
    "dubai":                (25.204, 55.270),
    "petit-lancy":          (46.184,  6.112),
}

def _geocode(city: str, state: str = "") -> tuple[float, float] | None:
    """Return (lat, lng) for a city from the static lookup only — no external calls."""
    key = f"{city.lower()}, {state.lower()}".strip(", ") if state else city.lower()
    if key in _CITY_COORDS:
        return _CITY_COORDS[key]
    city_only = city.lower().split(",")[0].strip()
    if city_only in _CITY_COORDS:
        return _CITY_COORDS[city_only]
    return None


IBJJF_UPCOMING_API = "https://ibjjf.com/api/v1/events/upcomings.json"
_LOGO_ID_RE  = re.compile(r'/Championship/Logo/(\d+)')
_MONTH_MAP   = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
_IBJJF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://ibjjf.com/events/championships",
    "X-Requested-With": "XMLHttpRequest",
}


def _parse_ibjjf_date(text: str, year: int):
    """Parse 'May 28* - May 31' → ('2026-05-28', '2026-05-31'). Returns ('','') on failure."""
    from datetime import date as _date
    text = text.replace('*', '').strip()
    parts = [p.strip() for p in text.split(' - ')]

    def _single(s, fallback_month=None):
        tokens = s.split()
        if len(tokens) == 2:
            m = _MONTH_MAP.get(tokens[0].lower())
            if m:
                return m, int(tokens[1])
        elif len(tokens) == 1 and tokens[0].isdigit():
            return fallback_month, int(tokens[0])
        return None, None

    sm, sd = _single(parts[0])
    if sm is None:
        return '', ''
    try:
        start_iso = _date(year, sm, sd).isoformat()
        if len(parts) == 2:
            em, ed = _single(parts[1], fallback_month=sm)
            end_iso = _date(year, em or sm, ed or sd).isoformat() if ed else start_iso
        else:
            end_iso = start_iso
        return start_iso, end_iso
    except (ValueError, TypeError):
        return '', ''


def get_ibjjf_schedule():
    """Fetch all upcoming IBJJF championship events from the ibjjf.com JSON API.

    Returns dicts with: id (championship_id), name, start, end, location,
    source='ibjjf', is_past, has_brackets=False.
    api_tournaments() merges this with get_tournaments() to set has_brackets=True
    and replace ids with bjjcompsystem ids where brackets exist.
    """
    from datetime import date as _date
    today = _date.today()

    resp = requests.get(IBJJF_UPCOMING_API, headers=_IBJJF_HEADERS, timeout=(5, 20))
    resp.raise_for_status()
    data = resp.json()
    championships = data.get("championships", [])

    result = []
    for ev in championships:
        try:
            slug      = ev.get("slug", "")
            name      = ev.get("name", slug)
            logo_url  = ev.get("urlLogo", "")
            date_text = ev.get("eventIntervalDays", "")
            city      = ev.get("city", "").strip()
            state     = ev.get("state", "").strip()

            # Championship ID from logo URL
            m = _LOGO_ID_RE.search(logo_url)
            champ_id  = m.group(1) if m else slug

            # Year from slug
            year_m = re.search(r'(\d{4})', slug)
            year   = int(year_m.group(1)) if year_m else today.year

            start_iso, end_iso = _parse_ibjjf_date(date_text, year)
            location = f"{city}, {state}" if state else city
            is_past  = bool(end_iso and _date.fromisoformat(end_iso) < today)
            coords   = _geocode(city, state)

            result.append({
                "id":           champ_id,
                "name":         name,
                "start":        start_iso,
                "end":          end_iso,
                "location":     location,
                "city":         f"{city}, {state}" if state else city,
                "country":      ev.get("country", ""),
                "country_code": "US" if state and len(state) == 2 else "",
                "lat":          coords[0] if coords else None,
                "lng":          coords[1] if coords else None,
                "cover_image":  f"https://www.ibjjfdb.com/Championship/Logo/{champ_id}" if champ_id and champ_id.isdigit() else "",
                "url":          f"https://ibjjf.com/events/{slug}",
                "source":       "ibjjf",
                "is_past":      is_past,
                "has_brackets": False,
            })
        except Exception:
            continue

    return result


def get_category_ids(tournament_id):
    """Return list of {id, name} for all bracket categories (regex, no BS4)."""
    resp = requests.get(f"{BASE}/tournaments/{tournament_id}/categories",
                        headers=HEADERS, timeout=(5, 12))
    resp.raise_for_status()
    seen, cats = set(), []
    for m in _CAT_HREF.finditer(resp.text):
        if m.group(1) != str(tournament_id):
            continue
        cid = m.group(2)
        if cid in seen:
            continue
        seen.add(cid)
        # extract name from surrounding markup via BS4 — small targeted parse
        start = max(0, m.start() - 20)
        end   = min(len(resp.text), m.end() + 400)
        chunk = resp.text[start:end]
        soup  = BeautifulSoup(chunk, "html.parser")
        a_tag = soup.find("a")
        name  = a_tag.get_text(" ", strip=True) if a_tag else cid
        cats.append({"id": cid, "name": name})
    return cats


def build_roster(tournament_id, job):
    """
    Build roster cache using requests+BS4 via watcher.fetch_brackets_batch.
    Fetches ALL bracket pages concurrently (~2-5s for a full tournament).
    Returns dict of category_id -> bracket_state so caller can register watchers.
    """
    from watcher import fetch_brackets_batch

    cats = get_category_ids(tournament_id)
    job["total"]       = len(cats)
    job["current_cat"] = "Fetching brackets…"

    items           = [(tournament_id, cat["id"], cat["name"]) for cat in cats]
    bracket_results = fetch_brackets_batch(items, concurrency=5)

    job["progress"] = len(cats)

    all_athletes = []
    seen         = set()

    for cat in cats:
        cid   = cat["id"]
        state = bracket_results.get(cid, {})
        if "error" in state:
            continue

        division     = state.get("division", cat["name"])
        athlete_map  = {}   # name_lower -> record

        for fight in state.get("fights", []):
            for comp in fight.get("competitors", []):
                name = comp.get("name", "")
                if not name or name.lower() == "bye":
                    continue
                team = comp.get("team", "")
                key  = name.lower()
                if key not in athlete_map:
                    athlete_map[key] = {
                        "name":        name,
                        "team":        team,
                        "division":    division,
                        "category_id": cid,
                        "mat":         fight.get("mat", ""),
                        "fight_num":   fight.get("fight_num", ""),
                        "fight_time":  fight.get("time", ""),
                    }

        for a in athlete_map.values():
            dedup_key = (a["name"].lower(), cid)
            if dedup_key not in seen:
                seen.add(dedup_key)
                all_athletes.append(a)

    cache = {
        "tournament_id": tournament_id,
        "built_at":      datetime.now().isoformat(),
        "total_cats":    len(cats),
        "athletes":      all_athletes,
    }
    save_roster_cache(tournament_id, cache)
    job["status"]       = "done"
    job["athlete_count"] = len(all_athletes)
    return bracket_results


def _safe_roster_path(tournament_id):
    """Return a validated path, or None if tournament_id looks malicious."""
    if not re.match(r'^[a-zA-Z0-9_-]+$', str(tournament_id)):
        return None
    path = ROSTER_DIR / f"{tournament_id}_roster.json"
    try:
        path.resolve().relative_to(ROSTER_DIR.resolve())
    except ValueError:
        return None
    return path


def load_roster_cache(tournament_id):
    path = _safe_roster_path(tournament_id)
    if path and path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def save_roster_cache(tournament_id, data):
    path = _safe_roster_path(tournament_id)
    if path:
        path.write_text(json.dumps(data))


def filter_roster(cache, school_name):
    """Filter cached roster by school name (case-insensitive substring match)."""
    sl   = school_name.lower().strip()
    seen = {}
    for a in cache.get("athletes", []):
        if sl in a.get("team", "").lower() or sl in a.get("name", "").lower():
            key = a["name"].lower()
            if key not in seen:
                seen[key] = dict(a)
    return list(seen.values())
