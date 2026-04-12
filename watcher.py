"""
IBJJF Bracket Watcher
Uses requests + BeautifulSoup — no Playwright needed.
~2s to fetch all brackets from a live tournament (20 concurrent workers).

Phase detection uses DOM column classes:
  tournament-category__sf / sf--right  → SEMI-FINAL
  non-SF card with fight# + 2 comps    → FINAL (identified by SF winner presence)
  everything else                       → earlier round

Loser detection uses: match-competitor--loser CSS class (server-rendered HTML).
"""

import json
import argparse
import time
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from datetime import datetime, date
from pathlib import Path

_FIGHT_DATE_RE = re.compile(r'(\d{2})/(\d{2})')
_FIGHT_TIME_RE = re.compile(r'((?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s+\d{2}/\d{2}\s+at\s+\d{1,2}:\d{2}\s*[AP]M)', re.IGNORECASE)
_FIGHT_NUM_RE  = re.compile(r'Fight\s+(\d+)', re.IGNORECASE)
_MAT_RE        = re.compile(r'Mat\s+(\d+)', re.IGNORECASE)

BASE = "https://www.bjjcompsystem.com"
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_instance = Path(__file__).parent / "instance"
if _instance.exists():
    STATE_DIR = _instance / "bracket_states"
else:
    STATE_DIR = Path("/tmp/bracket_states")
STATE_DIR.mkdir(parents=True, exist_ok=True)


# ── HTML parser ───────────────────────────────────────────────────────────────

def parse_bracket_html(html, category_id, category_name=""):
    """
    Parse IBJJF bracket HTML into structured state.
    Returns: {category_id, division, fights, ranking, results_final, ...}
    """
    soup = BeautifulSoup(html, "lxml")

    # Division name from page title area
    division = category_name
    title_el = soup.select_one(".tournament-category__title, h1, h2")
    if title_el:
        t = title_el.get_text(" ", strip=True)
        if any(b in t.upper() for b in ["WHITE","BLUE","PURPLE","BROWN","BLACK","NO-GI","NOGI","MASTER","ADULT"]):
            division = t
    if not division:
        # fallback: find first text with belt color
        for el in soup.select("h1,h2,h3,.breadcrumb"):
            t = el.get_text(" ", strip=True)
            if any(b in t.upper() for b in ["WHITE","BLUE","PURPLE","BROWN","BLACK"]):
                division = t
                break

    # ── Parse fight cards ─────────────────────────────────────────────────────
    fights = []
    sf_winners = set()   # names of SF winners (to identify the FINAL fight)

    for match_container in soup.select(".tournament-category__match"):
        # Determine phase from parent column classes
        phase = _get_phase(match_container)

        # Fight metadata from container text
        txt = match_container.get_text(" ", strip=True)
        fn_m  = _FIGHT_NUM_RE.search(txt)
        tm_m  = _FIGHT_TIME_RE.search(txt)
        mat_m = _MAT_RE.search(txt)

        fight_num  = fn_m.group(1)  if fn_m  else ""
        fight_time = tm_m.group(1)  if tm_m  else ""
        mat        = mat_m.group(1) if mat_m else ""

        # Competitors from match card
        competitors = []
        card = match_container.select_one(".match-card")
        if not card:
            continue

        for desc in card.select(".match-card__competitor-description"):
            # Skip BYE slots
            if desc.select_one(".match-card__bye"):
                continue
            name_el = desc.select_one(".match-card__competitor-name")
            club_el = desc.select_one(".match-card__club-name")
            if not name_el:
                continue
            name  = name_el.get_text(strip=True)
            team  = club_el.get_text(strip=True) if club_el else ""
            loser = "match-competitor--loser" in desc.get("class", [])
            competitors.append({"name": name, "team": team, "loser": loser})

        if not competitors:
            continue

        completed = any(c["loser"] for c in competitors)
        winner = ""
        if completed:
            non_losers = [c for c in competitors if not c["loser"]]
            if len(non_losers) == 1:
                winner = non_losers[0]["name"].lower()

        # Track SF winners so we can later identify the FINAL fight
        if phase == "SEMI-FINAL":
            for c in competitors:
                if not c["loser"]:
                    sf_winners.add(c["name"].lower())

        fights.append({
            "fight_num":   fight_num,
            "mat":         mat,
            "time":        fight_time,
            "phase":       phase,
            "completed":   completed,
            "winner":      winner,
            "competitors": [{"name": c["name"], "team": c["team"], "loser": c["loser"]} for c in competitors],
        })

    # ── Promote non-SF fights to FINAL only when ALL competitors are SF winners ─
    # A fight where only one competitor is an SF winner is an earlier round.
    # The true FINAL has exactly 2 competitors who are both SF winners.
    if sf_winners:
        for f in fights:
            if f["phase"] in ("SEMI-FINAL", "FINAL"):
                continue
            if not f["fight_num"]:
                continue
            real_comps = [c for c in f["competitors"] if c["name"].lower() != "bye"]
            comp_names = {c["name"].lower() for c in real_comps}
            if len(comp_names) == 2 and comp_names.issubset(sf_winners):
                f["phase"] = "FINAL"

    # ── Small bracket fallback: no SF fights means 2 or 3 person bracket ───────
    # When sf_winners is empty, the only real fight IS the final.
    # Also covers 3-person brackets where only 1 fight reaches the final column.
    if not sf_winners:
        non_sf_fights = [
            f for f in fights
            if f["fight_num"] and f["phase"] not in ("SEMI-FINAL", "FINAL")
        ]
        # If exactly 1 fight with 2 real competitors → that fight is the FINAL
        real_fights = [
            f for f in non_sf_fights
            if len([c for c in f["competitors"] if c["name"].lower() != "bye"]) == 2
        ]
        if len(real_fights) == 1:
            real_fights[0]["phase"] = "FINAL"

    # ── Derive live placements from FINAL/SF results ──────────────────────────
    # Only assign placement when the fight is completed (has a loser).
    ranking = []
    results_final = False

    for f in fights:
        if not f["completed"]:
            continue
        if f["phase"] == "FINAL":
            # Loser of FINAL = Silver, Winner = Gold
            for c in f["competitors"]:
                pos = "2" if c["loser"] else "1"
                if not any(e["name"].lower() == c["name"].lower() for e in ranking):
                    ranking.append({"pos": pos, "name": c["name"].lower()})
        elif f["phase"] == "SEMI-FINAL":
            # Loser of SF = Bronze
            for c in f["competitors"]:
                if c["loser"]:
                    if not any(e["name"].lower() == c["name"].lower() for e in ranking):
                        ranking.append({"pos": "3", "name": c["name"].lower()})

    # results_final = True when Gold has been awarded (FINAL completed)
    final_fights = [f for f in fights if f["phase"] == "FINAL"]
    if final_fights and all(f["completed"] for f in final_fights):
        results_final = True

    # ── Walkover: sole competitor in bracket = default Gold ───────────────────
    if not ranking:
        real_fighters = {
            c["name"].lower()
            for f in fights
            for c in f["competitors"]
            if c["name"].lower() not in ("bye", "")
        }
        if len(real_fighters) == 1:
            ranking      = [{"pos": "1", "name": next(iter(real_fighters))}]
            results_final = True

    # ── Fallback: official placement block (medalists section) ───────────────
    # If IBJJF has published the official podium, use that instead.
    medalists = soup.select_one(".tournament-category__medalists, .tournament-category__podium")
    if medalists and not results_final:
        official = _parse_medalists(medalists)
        if official:
            ranking = official
            results_final = True

    has_upcoming = any(_fight_is_upcoming(f) for f in fights)
    if has_upcoming:
        results_final = False   # still fights to go

    return {
        "category_id":      category_id,
        "division":         division,
        "fetched_at":       datetime.now().isoformat(),
        "fights":           fights,
        "ranking":          ranking,
        "results_final":    results_final,
        "total_fights":     len([f for f in fights if f["fight_num"]]),
        "completed_fights": sum(1 for f in fights if f["completed"]),
    }


def _get_phase(match_container):
    """Determine bracket phase from parent column CSS classes."""
    el = match_container.parent
    while el and el.name != "body":
        cls = " ".join(el.get("class", []))
        if "tournament-category__sf" in cls:
            return "SEMI-FINAL"
        if "col-qf" in cls:
            return "QUARTER-FINAL"
        el = el.parent
    return "ROUND"


def _fight_is_upcoming(fight):
    """True if fight is not completed AND scheduled today or later."""
    if fight.get("completed"):
        return False
    m = _FIGHT_DATE_RE.search(fight.get("time") or "")
    if not m:
        return False
    today = date.today()
    fight_date = date(today.year, int(m.group(1)), int(m.group(2)))
    return fight_date >= today


def _parse_medalists(container):
    """Parse official podium block into [{pos, name}] list."""
    results = []
    # Look for numbered entries: 1/2/3 + name pairs
    entries = container.select("li, .medalist, [class*=medalist], [class*=podium-item]")
    for entry in entries:
        txt = entry.get_text(" ", strip=True)
        m = re.match(r'^(\d)\s+(.+)$', txt)
        if m and m.group(1) in ("1","2","3"):
            results.append({"pos": m.group(1), "name": m.group(2).lower()})
    return results


# ── Fetch functions ───────────────────────────────────────────────────────────

def fetch_bracket(tournament_id, category_id, category_name=""):
    """Fetch and parse a single bracket. Synchronous, ~0.5s."""
    url = f"{BASE}/tournaments/{tournament_id}/categories/{category_id}"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=(5, 15))
        r.raise_for_status()
        return parse_bracket_html(r.text, category_id, category_name)
    except Exception as e:
        return {"category_id": category_id, "error": str(e)}


def fetch_brackets_batch(items, concurrency=20):
    """
    Fetch multiple brackets concurrently using a thread pool.
    items: list of (tournament_id, category_id, category_name) tuples
    Returns: dict of category_id -> state
    ~2s for 166 brackets at concurrency=20.
    """
    if not items:
        return {}

    results = {}

    def fetch_one(args):
        tid, cid, name = args
        return cid, fetch_bracket(tid, cid, name)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(fetch_one, item): item for item in items}
        for future in as_completed(futures):
            try:
                cid, state = future.result()
                results[cid] = state
            except Exception as e:
                item = futures[future]
                results[item[1]] = {"category_id": item[1], "error": str(e)}

    return results


# ── Change detector ───────────────────────────────────────────────────────────

def diff_states(old, new):
    """Compare two bracket states, return list of human-readable change strings."""
    changes = []
    old_fights = {f["fight_num"]: f for f in old.get("fights", []) if f.get("fight_num")}
    new_fights = {f["fight_num"]: f for f in new.get("fights", []) if f.get("fight_num")}

    for num, nf in new_fights.items():
        of = old_fights.get(num)
        if of is None:
            changes.append(f"New fight {num} — Mat {nf['mat']} {nf['time']} [{nf['phase']}]")
            continue
        if nf["time"] != of["time"] and nf["time"]:
            changes.append(f"Fight {num} time: {of['time'] or 'TBD'} → {nf['time']}")
        if nf["mat"] != of["mat"] and nf["mat"]:
            changes.append(f"Fight {num} mat: {of['mat'] or '?'} → {nf['mat']}")
        if nf["completed"] and not of.get("completed"):
            names = " vs ".join(c["name"] for c in nf["competitors"])
            winner = nf.get("winner", "")
            changes.append(f"Fight {num} FINISHED [{nf['phase']}]: {names}" +
                           (f" — winner: {winner}" if winner else ""))
        if nf["phase"] != of.get("phase") and nf["phase"] not in ("ROUND", ""):
            changes.append(f"Fight {num} advanced to {nf['phase']}")

    old_done = old.get("completed_fights", 0)
    new_done = new.get("completed_fights", 0)
    if new_done > old_done:
        changes.append(f"Progress: {new_done}/{new.get('total_fights',0)} fights done")

    if new.get("results_final") and not old.get("results_final"):
        gold = next((e["name"] for e in new.get("ranking",[]) if e["pos"]=="1"), "")
        changes.append(f"BRACKET COMPLETE — Gold: {gold}")

    return changes


# ── State persistence ─────────────────────────────────────────────────────────

def load_state(category_id):
    path = STATE_DIR / f"{category_id}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def save_state(category_id, state):
    path = STATE_DIR / f"{category_id}.json"
    path.write_text(json.dumps(state, indent=2))

def load_history(category_id):
    path = STATE_DIR / f"{category_id}_history.jsonl"
    if path.exists():
        return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    return []

def append_history(category_id, entry):
    path = STATE_DIR / f"{category_id}_history.jsonl"
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IBJJF Bracket Watcher")
    parser.add_argument("--tournament", required=True)
    parser.add_argument("--category",   help="Single category ID")
    parser.add_argument("--categories", nargs="+", help="Multiple category IDs")
    parser.add_argument("--interval",   type=int, default=30, help="Poll interval seconds")
    parser.add_argument("--once",       action="store_true", help="Fetch once and exit")
    args = parser.parse_args()

    cids = ([args.category] if args.category else []) + (args.categories or [])
    if not cids:
        print("Provide --category or --categories")
        return

    if args.once:
        state = fetch_bracket(args.tournament, cids[0])
        print(json.dumps(state, indent=2))
        return

    print(f"Watching {len(cids)} brackets, polling every {args.interval}s")
    while True:
        items = [(args.tournament, cid, "") for cid in cids]
        results = fetch_brackets_batch(items)
        ts = datetime.now().strftime("%H:%M:%S")
        for cid, state in results.items():
            if "error" in state:
                print(f"[{ts}] {cid}: ERROR {state['error']}")
                continue
            old = load_state(cid)
            changes = diff_states(old, state) if old else []
            save_state(cid, state)
            if changes:
                print(f"[{ts}] {state.get('division', cid)}:")
                for c in changes:
                    print(f"  {c}")
            else:
                print(f"[{ts}] {state.get('division', cid)}: {state['completed_fights']}/{state['total_fights']} done, no changes")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
