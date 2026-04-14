"""
MatTrack — CompNet scraper.

CompNet (Gracie Barra federation) runs on Smoothcomp with subdomain "compnet".
All bracket/club/roster functions delegate to scraper_naga with subdomain="compnet".

Event discovery scrapes federation 30 event pages:
  https://compnet.smoothcomp.com/en/federation/30/events/upcoming
  https://compnet.smoothcomp.com/en/federation/30/events/past?page=N
"""

import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, date as _date

import requests

log = logging.getLogger(__name__)

SUBDOMAIN     = "compnet"
FEDERATION_ID = 30
SOURCE        = "compnet"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,*/*",
}

_SEED_DIR           = Path(__file__).parent / "seed_cache"
_COMPNET_SEED_FILE  = _SEED_DIR / "compnet_events.json"
_COMPNET_CACHE_FILE = Path("/tmp/compnet_events_cache.json")


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _load_compnet_cache() -> dict:
    """Return {str(event_id): event_dict} merged from seed + runtime cache."""
    merged = {}
    for f in [_COMPNET_SEED_FILE, _COMPNET_CACHE_FILE]:
        if f.exists():
            try:
                for ev in json.loads(f.read_text()):
                    merged[str(ev["id"])] = ev
            except Exception:
                pass
    return merged


def _save_compnet_cache(events: list) -> None:
    try:
        _COMPNET_CACHE_FILE.write_text(json.dumps(events, ensure_ascii=False))
    except Exception:
        pass


# ── Event discovery ────────────────────────────────────────────────────────────

def _parse_events_js(html: str) -> list:
    """
    Extract the `var events = [...]` JS variable from a federation page.
    Uses bracket counting because the array has no clean `];` terminator.
    """
    idx = html.find('var events')
    if idx == -1:
        return []
    chunk = html[idx:]
    try:
        start = chunk.index('[')
        depth = 0
        for j, c in enumerate(chunk[start:], start):
            if c == '[':
                depth += 1
            elif c == ']':
                depth -= 1
                if depth == 0:
                    return json.loads(chunk[start:j + 1])
    except Exception:
        pass
    return []


def _scrape_page(page_type: str, page: int = 1) -> list:
    """Fetch one upcoming or past federation events page."""
    if page_type == "upcoming":
        url = f"https://compnet.smoothcomp.com/en/federation/{FEDERATION_ID}/events/upcoming"
    else:
        url = f"https://compnet.smoothcomp.com/en/federation/{FEDERATION_ID}/events/past?page={page}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return _parse_events_js(r.text)
    except Exception as e:
        log.warning("_scrape_page %s p%d: %s", page_type, page, e)
        return []


def _normalize(ev: dict) -> dict:
    """Convert raw Smoothcomp event dict to MatTrack event format."""
    today = datetime.now(timezone.utc).date()
    eid   = str(ev["id"])
    start = (ev.get("startdate") or "")[:10]
    end   = (ev.get("enddate")   or start)[:10]

    is_past = False
    if end:
        try:
            is_past = _date.fromisoformat(end) < today
        except Exception:
            pass

    city    = ev.get("location_city", "")
    country = ev.get("location_country_human", "")
    parts   = [p for p in [city, country] if p]
    location = ", ".join(parts)

    return {
        "id":        eid,
        "name":      ev.get("title", f"CompNet {eid}"),
        "start":     start,
        "end":       end,
        "location":  location,
        "url":       ev.get("url", f"https://compnet.smoothcomp.com/en/event/{eid}"),
        "source":    SOURCE,
        "subdomain": SUBDOMAIN,
        "is_past":   is_past,
    }


def get_compnet_events(**_kwargs) -> list:
    """
    Return CompNet events with same shape as get_naga_events().
    Scrapes federation 30 upcoming + past pages, merges with seed/runtime cache.
    Past events within 90 days are preserved in /tmp cache across restarts.
    """
    today  = datetime.now(timezone.utc).date()
    cached = _load_compnet_cache()
    live   = {}

    # Upcoming (single page)
    for ev in _scrape_page("upcoming"):
        norm = _normalize(ev)
        live[norm["id"]] = norm

    # Past (paginate until empty or all older than 90 days)
    for page in range(1, 20):
        page_evs = _scrape_page("past", page)
        if not page_evs:
            break
        had_recent = False
        for ev in page_evs:
            norm = _normalize(ev)
            if norm.get("start"):
                try:
                    if (_date.fromisoformat(norm["start"]) - today).days < -90:
                        continue
                    had_recent = True
                except Exception:
                    pass
            live[norm["id"]] = norm
        if not had_recent:
            break

    # Fold in cache for past events no longer on live pages
    for eid, ev in cached.items():
        if eid not in live and ev.get("start"):
            try:
                if (today - _date.fromisoformat(ev["start"])).days <= 90:
                    live[eid] = ev
            except Exception:
                pass

    events = sorted(live.values(), key=lambda e: e.get("start") or "")

    # Persist past events so they survive future restarts
    new_past = [e for e in events if e.get("is_past")]
    if new_past:
        _save_compnet_cache(new_past)

    log.info("get_compnet_events: %d events", len(events))
    return events


# ── Club / roster / bracket — thin wrappers around scraper_naga ────────────────

from scraper_naga import (
    get_naga_clubs            as _get_clubs,
    find_club_id              as _find_club_id,
    build_naga_roster         as _build_roster,
    fetch_naga_bracket        as _fetch_bracket,
    fetch_naga_brackets_batch as _fetch_brackets_batch,
    _get_brackets_meta,
)


def get_compnet_clubs(event_id: str) -> list:
    return _get_clubs(event_id, subdomain=SUBDOMAIN)


def find_compnet_club_id(event_id: str, school_name: str):
    return _find_club_id(event_id, school_name, subdomain=SUBDOMAIN)


def build_compnet_roster(event_id: str, school_name: str) -> list:
    roster = _build_roster(event_id, school_name, subdomain=SUBDOMAIN)
    for a in roster:
        a["source"] = SOURCE
    return roster


def fetch_compnet_bracket(event_id, bracket_id) -> dict:
    state = _fetch_bracket(event_id, bracket_id, subdomain=SUBDOMAIN)
    if isinstance(state, dict):
        state["source"] = SOURCE
    return state


def fetch_compnet_brackets_batch(items, concurrency=10) -> dict:
    """items: list of (event_id, bracket_id) — subdomain is always compnet."""
    tagged = [(eid, bid, SUBDOMAIN) for eid, bid in items]
    results = _fetch_brackets_batch(tagged, concurrency=concurrency)
    for state in results.values():
        if isinstance(state, dict):
            state["source"] = SOURCE
    return results


def get_compnet_brackets_meta(event_id: str) -> dict:
    return _get_brackets_meta(event_id, subdomain=SUBDOMAIN)
