"""
MatTrack — NAGA / Smoothcomp scraper (local development only).

All data comes from public JSON APIs — no Playwright needed.

Key endpoints (all unauthenticated):
  brackets.json        → all divisions with mat + estimated_start
  matchlist?club={id}  → all matches for a club (one page, server-rendered HTML)
  schedule/new/bracket.json/{bracket_id}  → per-match times + live state
  bracket/{bracket_id}/getRenderData      → same, different shape
  bracket/{bracket_id}/getPlacementTableData → final placements
"""

import re
import json
import time
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, */*",
}

# Base for any subdomain (naga, compnet, etc.)
def _base(subdomain="naga"):
    return f"https://{subdomain}.smoothcomp.com"

NAGA_BASE = _base("naga")

# ─── Event discovery ──────────────────────────────────────────────────────────

def get_naga_events(subdomain="naga", **_kwargs):
    """
    Return list of upcoming NAGA events scraped from nagafighter.com/tournaments-by-city/
    Each event has: id, name, start (YYYY-MM-DD), location, url, source.
    """
    url = "https://www.nagafighter.com/tournaments-by-city/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log.warning("get_naga_events fetch failed: %s", e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        log.warning("get_naga_events: no table found on nagafighter.com")
        return []

    seen = {}   # event_id → event dict (dedup across city rows)
    today = datetime.now(timezone.utc).date()

    for row in table.find_all("tr")[1:]:   # skip header
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        city_text = cells[0].get_text(strip=True)
        tourney_cell = cells[1]

        # Find the smoothcomp "Register Now" link → event ID
        reg_link = tourney_cell.find("a", href=re.compile(r"naga\.smoothcomp\.com/en/event/(\d+)"))
        if not reg_link:
            continue
        m = re.search(r"/event/(\d+)", reg_link["href"])
        if not m:
            continue
        event_id = m.group(1)

        if event_id in seen:
            continue   # already added from another city row

        # Extract name + date from cell text
        raw = tourney_cell.get_text(" ", strip=True)

        # Date pattern: "Month D" or "Month D-D" near end of name
        date_m = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\s+(\d{1,2})(?:-\d{1,2})?',
            raw
        )
        start_iso = ""
        if date_m:
            month_str = date_m.group(1)
            day_str   = date_m.group(2)
            year      = today.year
            try:
                from datetime import date as _date
                d = datetime.strptime(f"{month_str} {day_str} {year}", "%B %d %Y").date()
                # If date is >6 months in the past, assume next year
                if (today - d).days > 180:
                    d = d.replace(year=year + 1)
                start_iso = d.isoformat()
            except Exception:
                pass

        # Skip events more than 90 days in the past
        if start_iso:
            try:
                from datetime import date as _date
                d = _date.fromisoformat(start_iso)
                if (today - d).days > 90:
                    continue
            except Exception:
                pass

        # Event name = everything before the date
        name = raw
        if date_m:
            name = raw[:raw.find(date_m.group(0))].strip(" –-")
        # Strip trailing "Register Now" / "Tournament Details" if present
        name = re.sub(r'\s*(Register Now|Tournament Details).*$', '', name, flags=re.IGNORECASE).strip()
        if not name:
            name = f"NAGA Event {event_id}"

        # Location: prefer the city/state in the event name, fallback to city column
        loc_m = re.search(r'–\s*(.+?)\s*(?:,\s*[A-Z]{2})?$', name)
        if loc_m:
            location = loc_m.group(0).lstrip("– ").strip()
        else:
            location = city_text

        is_past = False
        if start_iso:
            try:
                from datetime import date as _date
                is_past = _date.fromisoformat(start_iso) < today
            except Exception:
                pass

        seen[event_id] = {
            "id":        event_id,
            "name":      name,
            "start":     start_iso,
            "end":       start_iso,
            "location":  location,
            "url":       f"https://naga.smoothcomp.com/en/event/{event_id}",
            "source":    "naga",
            "subdomain": subdomain,
            "is_past":   is_past,
        }

    events = sorted(seen.values(), key=lambda e: e.get("start") or "")
    log.info("get_naga_events: found %d events", len(events))
    return events


# ─── Club lookup ─────────────────────────────────────────────────────────────

def get_naga_clubs(event_id, subdomain="naga"):
    """Return sorted list of club name strings for a NAGA event."""
    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/matchlist"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        m = re.search(r'"clubs"\s*:\s*(\[.+?\])\s*[,}]', r.text, re.DOTALL)
        if not m:
            m = re.search(r'window\.clubs\s*=\s*(\[.+?\]);', r.text, re.DOTALL)
        if m:
            clubs = json.loads(m.group(1))
            return sorted(c["name"] for c in clubs if c.get("name"))
    except Exception as e:
        log.warning("get_naga_clubs failed: %s", e)
    return []


def find_club_id(event_id, school_name, subdomain="naga"):
    """
    Search embedded clubs list in matchlist page for a school name.
    Returns (club_id, canonical_name) or (None, None).
    """
    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/matchlist"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        # Clubs are embedded as JSON in the page
        m = re.search(r'"clubs"\s*:\s*(\[.+?\])\s*[,}]', r.text, re.DOTALL)
        if not m:
            m = re.search(r'window\.clubs\s*=\s*(\[.+?\]);', r.text, re.DOTALL)
        if m:
            clubs = json.loads(m.group(1))
            query = school_name.lower().strip()
            # Exact then partial match
            for c in clubs:
                if c["name"].lower() == query:
                    return c["id"], c["name"]
            for c in clubs:
                if query in c["name"].lower():
                    return c["id"], c["name"]
    except Exception as e:
        log.warning("find_club_id failed: %s", e)
    return None, None


# ─── Roster from matchlist ────────────────────────────────────────────────────

def build_naga_roster(event_id, school_name, subdomain="naga"):
    """
    Build athlete roster for a school at a NAGA event.
    Returns list of athlete dicts with division, bracket_id, mat, estimated_start.
    Each athlete can appear multiple times (one entry per division/bracket).
    """
    club_id, canonical = find_club_id(event_id, school_name, subdomain)
    if not club_id:
        log.warning("Club not found for '%s' in event %s", school_name, event_id)
        return []

    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/matchlist?club={club_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log.warning("build_naga_roster matchlist fetch failed: %s", e)
        return []

    # Get brackets lookup (mat + estimated_start per bracket_id)
    bracket_meta = _get_brackets_meta(event_id, subdomain)

    soup = BeautifulSoup(r.text, "html.parser")
    athletes = {}  # name_lower → {name, club, divisions: []}

    for match in soup.find_all("div", class_="match-row"):
        # Get bracket_id from embedded link
        bl = match.find("a", href=re.compile(r"/bracket/"))
        bracket_id = None
        if bl:
            bm = re.search(r"/bracket/(\d+)", bl["href"])
            if bm:
                bracket_id = int(bm.group(1))

        meta = bracket_meta.get(bracket_id, {})

        for participant in match.find_all("span", class_="participant"):
            club_el = participant.find("span", class_="club")
            if not club_el:
                continue
            club_txt = club_el.get_text(strip=True)
            if canonical.lower() not in club_txt.lower() and school_name.lower() not in club_txt.lower():
                continue

            # Extract name — first NavigableString child (before any sub-tags)
            name = ""
            for node in participant.children:
                if hasattr(node, "tag") or (hasattr(node, "name") and node.name):
                    break
                t = str(node).strip()
                if t:
                    name = (name + " " + t).strip()
            if not name:
                continue

            win_el = participant.find("span", class_="text-success")
            won = bool(win_el)
            result_txt = win_el.get_text(strip=True) if win_el else None

            score_el = match.find("div", class_="number")
            score = score_el.get_text(strip=True) if score_el else ""

            key = name.lower()
            if key not in athletes:
                athletes[key] = {
                    "name":       name,
                    "team":       canonical,
                    "divisions":  [],
                }

            div_entry = {
                "division":        meta.get("name", "Unknown"),
                "category_id":     str(bracket_id) if bracket_id else "",
                "bracket_id":      bracket_id,
                "mat_name":        meta.get("mats", ""),
                "estimated_start": meta.get("estimated_start", ""),
                "won":             won,
                "result":          result_txt,
                "score":           score,
                "source":          "naga",
            }

            # Avoid duplicate division entries
            existing_divs = [d["bracket_id"] for d in athletes[key]["divisions"]]
            if bracket_id not in existing_divs:
                athletes[key]["divisions"].append(div_entry)

    # Flatten: one athlete entry per unique (name, bracket_id) for compatibility
    # with MatTrack's allAthletes format
    result = []
    for a in athletes.values():
        for d in a["divisions"]:
            result.append({
                "name":            a["name"],
                "team":            a["team"],
                "division":        d["division"],
                "category_id":     d["category_id"],
                "mat_name":        d["mat_name"],
                "estimated_start": d["estimated_start"],
                "source":          "naga",
            })
    return result


def _get_brackets_meta(event_id, subdomain="naga"):
    """Return dict of bracket_id → {name, mats, estimated_start}."""
    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/brackets.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        return {
            b["bracket_id"]: {
                "name":            b["name"],
                "mats":            b.get("mats", ""),
                "estimated_start": b.get("estimated_start", ""),
                "bundle_id":       b.get("bracket_bundle_id"),
            }
            for b in data.get("brackets", [])
        }
    except Exception as e:
        log.warning("_get_brackets_meta failed: %s", e)
    return {}


# ─── Live bracket state ───────────────────────────────────────────────────────

def fetch_naga_bracket(event_id, bracket_id, subdomain="naga"):
    """
    Fetch per-match state for a single bracket.
    Returns MatTrack-compatible state dict with fights, ranking, results_final.
    """
    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/new/bracket.json/{bracket_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("fetch_naga_bracket %s failed: %s", bracket_id, e)
        return {"error": str(e)}

    matches = data.get("matches", [])
    if not matches:
        return {"error": "no matches"}

    division = matches[0].get("group", "")
    fights = []
    all_finished = True

    for m in matches:
        seats = m.get("seats", [])
        competitors = []
        for seat in seats:
            competitors.append({
                "name":   seat.get("name", ""),
                "team":   seat.get("club", ""),
                "winner": seat.get("name", "") if seat.get("isWinner") else "",
                "loser":  seat.get("name", "") if not seat.get("isWinner") and m.get("state") == "finished" else "",
            })

        state = m.get("state", "pending")
        completed = state == "finished"
        if not completed:
            all_finished = False

        # Parse estimated_start → fight_time string
        est = m.get("estimated_start", "")
        fight_time = ""
        fight_time_utc = ""
        if est:
            try:
                dt = datetime.fromisoformat(est)
                fight_time = dt.strftime("%a %m/%d at %I:%M %p")
                fight_time_utc = dt.astimezone(timezone.utc).isoformat()
            except Exception:
                fight_time = est

        fight = {
            "fight_num":   str(m.get("match_nr", "")),
            "mat":         m.get("mat_name", ""),
            "time":        fight_time,
            "time_utc":    fight_time_utc,
            "mat_match_nr": m.get("mat_match_nr", ""),
            "completed":   completed,
            "won_by":      m.get("wonBy", ""),
            "phase":       _round_to_phase(m.get("round", 1), len(matches)),
            "competitors": competitors,
            "state":       state,
        }
        fights.append(fight)

    # Build ranking from placements endpoint
    ranking = _get_naga_placements(event_id, bracket_id, subdomain)
    results_final = all_finished and bool(ranking)

    return {
        "category_id":     str(bracket_id),
        "division":        division,
        "fights":          fights,
        "ranking":         ranking,
        "results_final":   results_final,
        "fetched_at":      datetime.now().isoformat(),
        "total_fights":    len(fights),
        "completed_fights": sum(1 for f in fights if f["completed"]),
        "source":          "naga",
    }


def _round_to_phase(round_nr, total_matches):
    if total_matches <= 1:
        return "FINAL"
    if round_nr == total_matches:
        return "FINAL"
    if round_nr == total_matches - 1:
        return "SEMI"
    return f"R{round_nr}"


def _get_naga_placements(event_id, bracket_id, subdomain="naga"):
    """Return ranking list [{pos, name}] from placement endpoint."""
    url = f"{_base(subdomain)}/en/event/{event_id}/bracket/{bracket_id}/getPlacementTableData"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        placements = data.get("placementTableState", {}).get("placements", [])
        return [
            {"pos": str(p["placement"]), "name": p["name"].lower()}
            for p in placements
            if p.get("placement") in (1, 2, 3)
        ]
    except Exception:
        return []


# ─── Batch bracket fetch (mirrors watcher.fetch_brackets_batch) ───────────────

def fetch_naga_brackets_batch(items, concurrency=10):
    """
    items: list of (event_id, bracket_id, subdomain)
    Returns dict of bracket_id → state.
    """
    results = {}
    lock = threading.Lock()

    def _fetch(event_id, bracket_id, subdomain):
        state = fetch_naga_bracket(event_id, bracket_id, subdomain)
        with lock:
            results[str(bracket_id)] = state

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_fetch, eid, bid, sub): bid for eid, bid, sub in items}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log.warning("batch fetch error: %s", e)

    return results
