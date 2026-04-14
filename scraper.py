"""
MatTrack Scraper
All fetching uses requests + BeautifulSoup. No Playwright/browser needed.
Rosters are built by fetching all bracket pages concurrently (~2-5s per tournament).
"""

import re
import json
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


def _infer_ibjjf_dates(tournament_id: str) -> tuple[str, str]:
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
