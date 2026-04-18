"""
scrape_sc_brackets.py — Scrape Smoothcomp brackets (fights + placements) per event.

Cron-ready. Pulls bracket skeletons AND live state so we capture brackets even
before an event starts (pre-event rosters) and again after finals.

Endpoints (all unauthenticated):
  GET /en/event/{event_id}/schedule/brackets.json
      → {brackets: [{id, name, bracket_id, registrations_count, estimated_start, mats, bracket_bundle_id}]}
  GET /en/event/{event_id}/bracket/{bracket_id}/getRenderData
      → {state: {matches: [...]}, bracketInfo: {...}}
  GET /en/event/{event_id}/bracket/{bracket_id}/getPlacementTableData
      → {placementTableState: {placements: [...]}}

Storage:
  Upserts to Supabase bracket_finals keyed on category_id = str(bracket_id).
  state_json payload matches the IBJJF shape consumed by the app:
    { category_id, division, fights: [...], ranking: [...], results_final,
      fetched_at, total_fights, completed_fights, source, tournament_id,
      tournament_name, event_id }

Usage:
    python scrape_sc_brackets.py                       # today + tomorrow (weekend mode)
    python scrape_sc_brackets.py --event-id 28183 --source adcc
    python scrape_sc_brackets.py --source adcc         # all adcc SC events within --since-days
    python scrape_sc_brackets.py --since-days 3
    python scrape_sc_brackets.py --worklist WORKLIST.json  # batch from JSON list
    python scrape_sc_brackets.py --dry-run
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests

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
        logging.FileHandler("scrape_sc_brackets.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("sc_brackets")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kzqvfuqxtbrhlgphyntb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
}

# subdomain → {fed_id, org_key}. Same map used by scrape_sc_registrations.py
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

ORG_TO_SUBDOMAIN = {cfg["org"]: sub for sub, cfg in SUBDOMAINS.items()}


# ── HTTP helpers ────────────────────────────────────────────────────────────

def _base(subdomain: str) -> str:
    return f"https://{subdomain}.smoothcomp.com" if subdomain else "https://smoothcomp.com"


def _parse_events_js(html: str) -> list:
    """Extract the `var events = [...]` JSON block from a federation events page."""
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
    url = f"{_base(subdomain)}/en/federation/{fed_id}/events/upcoming"
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
            "subdomain": subdomain,
        })
    return events


# ── Bracket fetch ───────────────────────────────────────────────────────────

def list_brackets(event_id: str, subdomain: str) -> list[dict]:
    """
    Return the top-level list from /schedule/brackets.json. 403 with body
    'Brackets is not published' is a common pre-event state — return [].
    """
    url = f"{_base(subdomain)}/en/event/{event_id}/schedule/brackets.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 403:
            body = (r.text or "")[:120].lower()
            if "not published" in body or "not public" in body:
                log.info("  brackets not yet published for event %s", event_id)
                return []
        r.raise_for_status()
        return r.json().get("brackets", []) or []
    except Exception as e:
        log.warning("  list_brackets event=%s failed: %s", event_id, e)
        return []


def fetch_render_data(event_id: str, bracket_id: int, subdomain: str) -> dict | None:
    url = f"{_base(subdomain)}/en/event/{event_id}/bracket/{bracket_id}/getRenderData"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("  fetch_render_data event=%s bracket=%s failed: %s", event_id, bracket_id, e)
        return None


def fetch_placements(event_id: str, bracket_id: int, subdomain: str) -> list[dict]:
    """Return ranking list [{pos, name}] for pos in (1,2,3). Empty pre-event."""
    url = f"{_base(subdomain)}/en/event/{event_id}/bracket/{bracket_id}/getPlacementTableData"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json() or {}
        placements = (data.get("placementTableState") or {}).get("placements") or []
        return [
            {"pos": str(p["placement"]), "name": (p.get("name") or "").lower()}
            for p in placements
            if p.get("placement") in (1, 2, 3) and p.get("name")
        ]
    except Exception:
        return []


def _round_to_phase(round_nr: int, total_rounds: int) -> str:
    if total_rounds <= 1:
        return "FINAL"
    if round_nr >= total_rounds:
        return "FINAL"
    if round_nr == total_rounds - 1:
        return "SEMI"
    return f"R{round_nr}"


def _parse_seat(seat: dict | None) -> dict:
    if not seat:
        return {"name": "", "team": "", "loser": "", "winner": ""}
    player = seat.get("player") or {}
    name = seat.get("name") or player.get("name") or ""
    club = seat.get("club") or player.get("club") or ""
    is_bye = (seat.get("type") == "bye") or name.upper() == "BYE"
    is_winner = bool(seat.get("isWinner"))
    return {
        "name": "BYE" if is_bye else name,
        "team": "" if is_bye else club,
        "winner": name if is_winner and not is_bye else "",
        "loser": name if (seat.get("result") == "lost" and not is_bye) else "",
        "country": player.get("country") or seat.get("country") or "",
    }


def build_state(event_id: str, bracket: dict, subdomain: str, source: str,
                event_title: str, event_date: str) -> dict | None:
    """
    Pull getRenderData + getPlacementTableData and transform to a state dict
    matching the IBJJF format expected by the app / bracket_finals reader.

    Returns None on 404 (deleted bracket), a skeleton state otherwise — even
    pre-event the skeleton is worth persisting so the app can render it.
    """
    bracket_id = bracket.get("bracket_id") or bracket.get("id")
    if not bracket_id:
        return None

    render = fetch_render_data(event_id, bracket_id, subdomain)
    if render is None:
        return None

    state_obj = render.get("state") or {}
    raw_matches = state_obj.get("matches")
    matches: list = []
    # Variant A: list of match dicts
    if isinstance(raw_matches, list):
        matches = [m for m in raw_matches if isinstance(m, dict)]
    # Variant B: dict keyed by string index {"0": {...}, "1": {...}}
    elif isinstance(raw_matches, dict):
        for k in sorted(raw_matches.keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
            v = raw_matches[k]
            if isinstance(v, dict):
                matches.append(v)
    # Variant C: round-robin / group-stage brackets nest matches under state.rounds
    if not matches and isinstance(state_obj.get("rounds"), dict):
        for rn in sorted(state_obj["rounds"].keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
            for m in state_obj["rounds"][rn] or []:
                if isinstance(m, dict):
                    matches.append(m)
    total_rounds = max((m.get("round") or 1) for m in matches) if matches else 1

    fights = []
    all_finished = bool(matches)
    for m in matches:
        seats = m.get("seats") or {}
        left = _parse_seat(seats.get("left"))
        right = _parse_seat(seats.get("right"))
        state_str = (m.get("state") or "pending").lower()
        completed = state_str == "finished"
        if not completed:
            all_finished = False

        est = m.get("estimated_starttime") or ""
        fight_time = ""
        fight_time_utc = ""
        if est and not est.startswith("xxxx"):
            try:
                dt = datetime.fromisoformat(est.replace("Z", "+00:00"))
                fight_time = dt.strftime("%a %m/%d at %I:%M %p")
                fight_time_utc = dt.astimezone(timezone.utc).isoformat()
            except Exception:
                fight_time = est

        winner_name = ""
        for s in (left, right):
            if s["winner"]:
                winner_name = s["winner"]
                break

        fights.append({
            "fight_num":    str(m.get("match_nr", "") or ""),
            "mat":          m.get("mat_name", "") or "",
            "mat_match_nr": m.get("mat_match_nr", "") or "",
            "time":         fight_time,
            "time_utc":     fight_time_utc,
            "completed":    completed,
            "state":        state_str,
            "won_by":       m.get("wonBy", "") or "",
            "winner":       winner_name,
            "phase":        _round_to_phase(m.get("round", 1) or 1, total_rounds),
            "round":        m.get("round", 1) or 1,
            "competitors":  [left, right],
            "is_bye":       bool(m.get("isBye")),
        })

    ranking = fetch_placements(event_id, bracket_id, subdomain)
    results_final = all_finished and bool(ranking) and len(matches) > 0

    return {
        "category_id":      str(bracket_id),
        "bracket_id":       bracket_id,
        "event_id":         event_id,
        "tournament_id":    event_id,
        "tournament_name":  event_title,
        "division":         bracket.get("name", ""),
        "mat":              bracket.get("mats", ""),
        "estimated_start":  bracket.get("estimated_start", ""),
        "registrations_count": bracket.get("registrations_count", 0),
        "fights":           fights,
        "ranking":          ranking,
        "results_final":    results_final,
        "total_fights":     len(fights),
        "completed_fights": sum(1 for f in fights if f["completed"]),
        "source":           source,
        "event_date":       event_date,
        "fetched_at":       datetime.now(timezone.utc).isoformat(),
    }


# ── Supabase upsert ─────────────────────────────────────────────────────────

def upsert_bracket(state: dict, dry_run: bool = False) -> bool:
    if not state:
        return False
    row = {
        "category_id":     state["category_id"],
        "tournament_id":   state["tournament_id"],
        "tournament_name": state["tournament_name"],
        "division":        state["division"],
        "source":          state["source"],
        "ranking":         state.get("ranking", []),
        "state_json":      state,
        "event_date":      state.get("event_date") or None,
    }
    if dry_run:
        return True
    if not SUPABASE_KEY:
        log.warning("SUPABASE_SERVICE_KEY not set — skipping write")
        return False

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/bracket_finals?on_conflict=category_id",
        params={"on_conflict": "category_id"},
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=[row],
        timeout=30,
    )
    if resp.status_code in (200, 201, 204):
        return True
    log.warning("  Upsert failed %s for bracket %s: %s",
                resp.status_code, state["category_id"], resp.text[:200])
    return False


# ── Per-event driver ────────────────────────────────────────────────────────

def scrape_event(event_id: str, source: str, subdomain: str | None = None,
                 event_title: str = "", event_date: str = "",
                 concurrency: int = 12, dry_run: bool = False,
                 per_bracket_sleep: float = 0.3) -> dict:
    if subdomain is None:
        subdomain = ORG_TO_SUBDOMAIN.get(source, source)

    log.info("=== event %s [%s/%s] %s ===", event_id, source, subdomain, event_title or "")

    brackets = list_brackets(event_id, subdomain)
    log.info("  %d brackets listed", len(brackets))
    if not brackets:
        return {"event_id": event_id, "source": source, "brackets": 0, "saved": 0, "errors": 0}

    saved = 0
    errors = 0
    lock = threading.Lock()

    def _work(bracket):
        try:
            st = build_state(event_id, bracket, subdomain, source, event_title, event_date)
            if st is None:
                log.warning("  bracket %s returned None (404/missing)", bracket.get("bracket_id"))
                return "missing"
            ok = upsert_bracket(st, dry_run=dry_run)
            return "saved" if ok else "error"
        except Exception as e:
            log.warning("  bracket %s error: %s", bracket.get("bracket_id"), e)
            return "error"

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_work, b): b for b in brackets}
        done = 0
        for f in as_completed(futures):
            done += 1
            result = f.result()
            with lock:
                if result == "saved":
                    saved += 1
                elif result == "error":
                    errors += 1
            # Gentle pacing across the pool
            if per_bracket_sleep:
                time.sleep(per_bracket_sleep / concurrency)

    log.info("  event %s: %d saved / %d brackets / %d errors",
             event_id, saved, len(brackets), errors)
    return {
        "event_id": event_id,
        "source":   source,
        "title":    event_title,
        "brackets": len(brackets),
        "saved":    saved,
        "errors":   errors,
    }


# ── Event discovery for cron default ────────────────────────────────────────

def events_in_window(since_days: int = 0) -> list[dict]:
    """
    Return [{id, title, start, subdomain, org}] for all SC events occurring
    today (or later if since_days>0).
    Today + tomorrow when since_days==0 (i.e. weekend default).
    """
    today = date.today()
    if since_days <= 0:
        cutoff = today + timedelta(days=1)
    else:
        cutoff = today + timedelta(days=since_days)

    results: list[dict] = []
    for subdomain, cfg in SUBDOMAINS.items():
        log.info("discover: %s", subdomain)
        for ev in get_upcoming_events(subdomain, cfg["fed_id"]):
            if not ev.get("start"):
                continue
            try:
                d = date.fromisoformat(ev["start"])
            except Exception:
                continue
            if today <= d <= cutoff:
                ev["org"] = cfg["org"]
                results.append(ev)
        time.sleep(0.2)
    return results


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Scrape Smoothcomp brackets (cron-safe)")
    ap.add_argument("--event-id", help="Single event ID to scrape")
    ap.add_argument("--source", help="Org key (adcc, compnet, fuji, gi, naga, ...)")
    ap.add_argument("--subdomain", help="Override subdomain (if non-standard)")
    ap.add_argument("--event-title", default="", help="Event title for the row")
    ap.add_argument("--event-date", default="", help="YYYY-MM-DD event date")
    ap.add_argument("--since-days", type=int, default=0,
                    help="Scrape all SC events starting within N days from today "
                         "(0 = today + tomorrow, i.e. 'this weekend')")
    ap.add_argument("--worklist", help="JSON file with [{event_id, source, ...}] entries")
    ap.add_argument("--concurrency", type=int, default=12, help="Bracket fetch concurrency (10-15 recommended)")
    ap.add_argument("--sleep", type=float, default=0.3, help="Per-bracket sleep budget (seconds)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not SUPABASE_KEY and not args.dry_run:
        log.error("SUPABASE_SERVICE_KEY not set. Use --dry-run or set env var.")
        sys.exit(1)

    # Build the list of events to process
    worklist: list[dict] = []

    if args.worklist:
        with open(args.worklist) as f:
            worklist = json.load(f)
    elif args.event_id:
        if not args.source:
            log.error("--source required with --event-id")
            sys.exit(1)
        worklist = [{
            "event_id": args.event_id,
            "source":   args.source,
            "subdomain": args.subdomain or ORG_TO_SUBDOMAIN.get(args.source, args.source),
            "title":    args.event_title,
            "date":     args.event_date,
        }]
    elif args.source:
        cfg = next((c for s, c in SUBDOMAINS.items() if c["org"] == args.source), None)
        if not cfg:
            log.error("Unknown source %s", args.source)
            sys.exit(1)
        subdomain = args.subdomain or ORG_TO_SUBDOMAIN[args.source]
        for ev in get_upcoming_events(subdomain, cfg["fed_id"]):
            if args.since_days > 0:
                cutoff = date.today() + timedelta(days=args.since_days)
                try:
                    d = date.fromisoformat(ev["start"])
                except Exception:
                    continue
                if not (date.today() <= d <= cutoff):
                    continue
            worklist.append({
                "event_id":  ev["id"],
                "source":    args.source,
                "subdomain": subdomain,
                "title":     ev["title"],
                "date":      ev["start"],
            })
    else:
        discovered = events_in_window(args.since_days)
        for ev in discovered:
            worklist.append({
                "event_id":  ev["id"],
                "source":    ev["org"],
                "subdomain": ev["subdomain"],
                "title":     ev["title"],
                "date":      ev["start"],
            })

    log.info("Processing %d events", len(worklist))
    summary = []
    for ev in worklist:
        out = scrape_event(
            event_id=str(ev["event_id"]),
            source=ev["source"],
            subdomain=ev.get("subdomain"),
            event_title=ev.get("title", ""),
            event_date=ev.get("date", ""),
            concurrency=args.concurrency,
            dry_run=args.dry_run,
            per_bracket_sleep=args.sleep,
        )
        summary.append(out)
        time.sleep(0.5)

    # Final summary
    total_brackets = sum(s["brackets"] for s in summary)
    total_saved    = sum(s["saved"]    for s in summary)
    total_errors   = sum(s["errors"]   for s in summary)
    log.info("=" * 60)
    log.info("DONE: %d events, %d brackets listed, %d saved, %d errors",
             len(summary), total_brackets, total_saved, total_errors)
    for s in summary:
        log.info("  %-5s %-8s  %4d saved / %4d  %s",
                 s["source"], s["event_id"], s["saved"], s["brackets"], s.get("title", ""))


if __name__ == "__main__":
    main()
