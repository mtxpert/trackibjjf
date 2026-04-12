"""
IBJJF Scraper
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

def _seed_rosters():
    """Copy seed_cache/*.json into ROSTER_DIR on first boot (won't overwrite existing)."""
    if not SEED_DIR.exists():
        return
    for src in SEED_DIR.glob("*_roster.json"):
        dst = ROSTER_DIR / src.name
        if not dst.exists():
            dst.write_bytes(src.read_bytes())

_seed_rosters()

BASE    = "https://www.bjjcompsystem.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


_BLOCK_RE = re.compile(r"id=['\"]tournament-display-(\d+)['\"]", re.DOTALL)
_IMG_ALT  = re.compile(r'alt=["\']([^"\']+)["\']')
_CAT_HREF = re.compile(r'/tournaments/(\d+)/categories/(\d+)["\']')

def get_tournaments():
    """Fetch all currently listed tournaments from bjjcompsystem.com (regex, no BS4)."""
    resp = requests.get(f"{BASE}/tournaments", headers=HEADERS, timeout=12)
    resp.raise_for_status()
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
        result.append({"id": tid, "name": name})
    return result


def get_category_ids(tournament_id):
    """Return list of {id, name} for all bracket categories (regex, no BS4)."""
    resp = requests.get(f"{BASE}/tournaments/{tournament_id}/categories",
                        headers=HEADERS, timeout=12)
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


def load_roster_cache(tournament_id):
    path = ROSTER_DIR / f"{tournament_id}_roster.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def save_roster_cache(tournament_id, data):
    path = ROSTER_DIR / f"{tournament_id}_roster.json"
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
