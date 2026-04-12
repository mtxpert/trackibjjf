"""
IBJJF Bracket Watcher
Polls category bracket pages on a schedule, detects changes in:
  - Match times (as schedule shifts during the day)
  - New rounds starting (QF → SF → Final)
  - Match results / scores (as fights complete)
  - Newly assigned mat numbers

Usage:
  python watcher.py --tournament 3106 --category 2817108 --interval 5
  python watcher.py --tournament 3106 --auto-find --min-competitors 6 --interval 5
"""

import asyncio
import json
import argparse
import time
import re
import os
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

BASE = "https://www.bjjcompsystem.com"
STATE_DIR = Path(__file__).parent / "bracket_states"
STATE_DIR.mkdir(exist_ok=True)


# ── Page parser ───────────────────────────────────────────────────────────────

def parse_bracket_state(text, category_id, category_name=""):
    """
    Parse a rendered bracket page into a structured state dict.
    Returns: { division, fights: [{num, mat, time, phase, competitors}], ranking: [...] }
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Division name
    division = category_name
    for ln in lines[:15]:
        if any(b in ln.upper() for b in ["WHITE","BLUE","PURPLE","BROWN","BLACK","NO-GI","NOGI"]):
            division = ln
            break

    # Parse fight blocks
    fight_re  = re.compile(r"^FIGHT\s+(\d+)\s*:\s*Mat\s+(\d+)$", re.IGNORECASE)
    phase_re  = re.compile(r"^(FINAL|SEMI.FINAL|QUARTER.FINAL|SF|QF|ROUND\s+\d+)$", re.IGNORECASE)
    time_re   = re.compile(r"^(\w{3}\s+\d{2}/\d{2}\s+at\s+\d{1,2}:\d{2}\s+[AP]M)$", re.IGNORECASE)
    score_re  = re.compile(r"^(\d+)\s*[x\-]\s*(\d+)$")
    seed_re   = re.compile(r"^\d{1,2}$")

    fights = []
    i = 0
    current_phase = ""

    while i < len(lines):
        # Track phase labels
        if phase_re.match(lines[i]):
            current_phase = lines[i]
            i += 1
            continue

        m = fight_re.match(lines[i])
        if not m:
            i += 1
            continue

        fight_num = m.group(1)
        mat       = m.group(2)
        fight_time = ""
        score     = ""
        competitors = []

        j = i + 1
        if j < len(lines) and time_re.match(lines[j]):
            fight_time = lines[j]; j += 1

        # Collect athletes until next fight or section
        while j < len(lines) and not fight_re.match(lines[j]) and not phase_re.match(lines[j]):
            ln = lines[j]
            # Score line e.g. "4 - 2" or "4x2"
            if score_re.match(ln.replace(" ","")):
                score = ln; j += 1; continue
            # Seed + name + team triplet
            if seed_re.match(ln) and j+1 < len(lines):
                name_ln = lines[j+1] if j+1 < len(lines) else ""
                team_ln = lines[j+2] if j+2 < len(lines) else ""
                if name_ln and name_ln.upper() == name_ln and name_ln not in ("BYE",):
                    competitors.append({
                        "seed": ln,
                        "name": name_ln.title(),
                        "team": team_ln if not seed_re.match(team_ln) else ""
                    })
                    j += 3 if (team_ln and not seed_re.match(team_ln)) else 2
                    continue
            j += 1

        fights.append({
            "fight_num":   fight_num,
            "mat":         mat,
            "time":        fight_time,
            "phase":       current_phase,
            "score":       score,
            "competitors": competitors,
            "completed":   bool(score),
        })
        i = j

    # Parse ranking table (at bottom of page)
    ranking = []
    try:
        rank_idx = next(k for k,l in enumerate(lines) if l.lower() == "ranking")
        k = rank_idx + 2
        while k < len(lines) - 1:
            if not re.match(r"^\d+$", lines[k]): break
            pos  = lines[k]
            name = lines[k+1] if k+1 < len(lines) else ""
            team_pts = lines[k+2] if k+2 < len(lines) else ""
            team = team_pts.split("\t")[0].strip()
            ranking.append({"pos": pos, "name": name, "team": team})
            k += 3
    except StopIteration:
        pass

    return {
        "category_id": category_id,
        "division":    division,
        "fetched_at":  datetime.now().isoformat(),
        "fights":      fights,
        "ranking":     ranking,
        "total_fights":    len(fights),
        "completed_fights": sum(1 for f in fights if f["completed"]),
    }


# ── Change detector ───────────────────────────────────────────────────────────

def diff_states(old, new):
    """Compare two bracket states and return a list of human-readable change strings."""
    changes = []

    old_fights = {f["fight_num"]: f for f in old.get("fights", [])}
    new_fights = {f["fight_num"]: f for f in new.get("fights", [])}

    for num, nf in new_fights.items():
        of = old_fights.get(num)
        if of is None:
            changes.append(f"🆕 Fight {num} appeared — Mat {nf['mat']} {nf['time']} {nf['phase']}")
            continue

        # Time changed
        if nf["time"] != of["time"] and nf["time"]:
            changes.append(f"⏰ Fight {num} time updated: {of['time'] or 'TBD'} → {nf['time']}")

        # Mat changed
        if nf["mat"] != of["mat"] and nf["mat"]:
            changes.append(f"📍 Fight {num} mat changed: {of['mat'] or '?'} → {nf['mat']}")

        # Fight completed (score appeared)
        if nf["completed"] and not of.get("completed"):
            names = " vs ".join(c["name"] for c in nf["competitors"])
            changes.append(f"✅ Fight {num} FINISHED: {names} — Score: {nf['score']}")

        # Phase progression
        if nf["phase"] != of.get("phase") and nf["phase"]:
            changes.append(f"🏆 Fight {num} phase: {of.get('phase','?')} → {nf['phase']}")

    # New completed fights count
    old_done = old.get("completed_fights", 0)
    new_done = new.get("completed_fights", 0)
    if new_done > old_done:
        changes.append(f"📊 Progress: {new_done}/{new.get('total_fights',0)} fights complete")

    return changes


# ── State persistence ─────────────────────────────────────────────────────────

def load_state(category_id):
    path = STATE_DIR / f"{category_id}.json"
    if path.exists():
        return json.loads(path.read_text())
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


# ── Single fetch ──────────────────────────────────────────────────────────────

async def fetch_bracket(tournament_id, category_id, category_name=""):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        url = f"{BASE}/tournaments/{tournament_id}/categories/{category_id}"
        await page.goto(url, wait_until="networkidle", timeout=25000)
        text = await page.inner_text("body")
        await browser.close()
    return parse_bracket_state(text, category_id, category_name)


async def fetch_multiple(tournament_id, category_ids, concurrency=6):
    """Fetch multiple categories concurrently."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        sem = asyncio.Semaphore(concurrency)

        async def fetch_one(cid, name=""):
            async with sem:
                page = await context.new_page()
                try:
                    url = f"{BASE}/tournaments/{tournament_id}/categories/{cid}"
                    await page.goto(url, wait_until="networkidle", timeout=20000)
                    text = await page.inner_text("body")
                    return parse_bracket_state(text, cid, name)
                except Exception as e:
                    return {"category_id": cid, "error": str(e)}
                finally:
                    await page.close()

        results = await asyncio.gather(*[
            fetch_one(cid) for cid in category_ids
        ])
        await browser.close()
    return results


# ── Poll loop ─────────────────────────────────────────────────────────────────

async def poll_bracket(tournament_id, category_id, category_name="", interval_minutes=5, log_fn=None):
    """
    Continuously poll a bracket page, detect and log changes.
    Stops when bracket is fully complete.
    """
    log = log_fn or print
    log(f"\n{'='*60}")
    log(f"WATCHING: {category_name or category_id}")
    log(f"URL: {BASE}/tournaments/{tournament_id}/categories/{category_id}")
    log(f"Polling every {interval_minutes} min")
    log(f"{'='*60}")

    while True:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            state = await fetch_bracket(tournament_id, category_id, category_name)
            old_state = load_state(category_id)

            if old_state:
                changes = diff_states(old_state, state)
                if changes:
                    log(f"\n[{ts}] CHANGES DETECTED:")
                    for c in changes:
                        log(f"  {c}")
                    append_history(category_id, {
                        "ts": ts, "changes": changes,
                        "completed": state["completed_fights"],
                        "total": state["total_fights"]
                    })
                else:
                    log(f"[{ts}] No changes — {state['completed_fights']}/{state['total_fights']} fights done")
            else:
                log(f"[{ts}] Initial snapshot — {state['total_fights']} fights, "
                    f"{state['completed_fights']} already done")
                log(f"  Division: {state['division']}")
                for f in state['fights'][:3]:
                    c_names = ", ".join(c["name"] for c in f["competitors"])
                    log(f"  Fight {f['fight_num']}: Mat {f['mat']} @ {f['time']} — {c_names}")

            save_state(category_id, state)

            # Done when all fights complete
            if state["total_fights"] > 0 and state["completed_fights"] >= state["total_fights"]:
                log(f"\n[{ts}] 🏆 BRACKET COMPLETE — all {state['total_fights']} fights finished")
                break

        except Exception as e:
            log(f"[{ts}] ERROR: {e}")

        await asyncio.sleep(interval_minutes * 60)


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IBJJF Bracket Watcher")
    parser.add_argument("--tournament", required=True, help="Tournament ID")
    parser.add_argument("--category",  help="Specific category ID to watch")
    parser.add_argument("--categories", nargs="+", help="Multiple category IDs")
    parser.add_argument("--interval",  type=int, default=5, help="Poll interval in minutes")
    parser.add_argument("--once",      action="store_true", help="Fetch once and exit")
    args = parser.parse_args()

    if args.once:
        cid = args.category or (args.categories[0] if args.categories else None)
        if not cid:
            print("Need --category for --once mode")
            return
        state = asyncio.run(fetch_bracket(args.tournament, cid))
        print(json.dumps(state, indent=2))
        return

    if args.categories:
        # Watch multiple brackets concurrently
        async def watch_all():
            await asyncio.gather(*[
                poll_bracket(args.tournament, cid, interval_minutes=args.interval)
                for cid in args.categories
            ])
        asyncio.run(watch_all())
    elif args.category:
        asyncio.run(poll_bracket(args.tournament, args.category,
                                  interval_minutes=args.interval))
    else:
        print("Provide --category or --categories")


if __name__ == "__main__":
    main()
