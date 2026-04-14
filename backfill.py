"""
backfill.py — One-time scrape of all past tournament brackets.

For each seeded IBJJF tournament: fetches all brackets by category_id,
saves state to local bracket_states/ and upserts to Supabase.

For each seeded NAGA event: discovers bracket IDs from Smoothcomp,
fetches each bracket, saves same way.

Run with: python3 backfill.py
"""

import json
import sys
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

# Load .env before any module that reads os.environ at import time
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

SEED_DIR = Path(__file__).parent / "seed_cache"

# ── helpers ───────────────────────────────────────────────────────────────────

def _try_save_supabase(category_id, tournament_id, tournament_name, division,
                       source, ranking, state, event_date):
    try:
        from results import save_bracket_final
        save_bracket_final(
            category_id=category_id,
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            division=division,
            source=source,
            ranking=ranking,
            state=state,
            event_date=event_date,
        )
    except Exception as e:
        log.warning("Supabase upsert failed for %s: %s", category_id, e)


def _save_local(category_id, state):
    """Save bracket state to local bracket_states dir."""
    try:
        from watcher import save_state
        save_state(category_id, state)
    except Exception as e:
        log.warning("Local save failed for %s: %s", category_id, e)


def _already_fetched(category_id) -> bool:
    """Return True if we already have a final result locally."""
    try:
        from watcher import load_state
        s = load_state(category_id)
        return bool(s and s.get("results_final"))
    except Exception:
        return False


# ── IBJJF backfill ────────────────────────────────────────────────────────────

def backfill_ibjjf():
    from watcher import fetch_brackets_batch

    tournaments = []
    for f in sorted(SEED_DIR.glob("*_roster.json")):
        d = json.loads(f.read_text())
        tid = str(d["tournament_id"])
        athletes = d.get("athletes", [])

        # Extract unique category IDs and infer event_date
        cat_ids = list({a["category_id"] for a in athletes if a.get("category_id")})

        import re
        dates = set()
        for a in athletes:
            m = re.search(r"(\d{2}/\d{2})", a.get("fight_time", ""))
            if m:
                dates.add(m.group(1))
        event_date = ""
        if dates:
            today = date.today()
            parsed = []
            for d_str in dates:
                mm, dd = d_str.split("/")
                try:
                    parsed.append(date(today.year, int(mm), int(dd)))
                except Exception:
                    pass
            if parsed:
                event_date = min(parsed).isoformat()

        # Get tournament name from seed tournaments.json
        name = f"Tournament {tid}"
        t_seed = SEED_DIR / "tournaments.json"
        if t_seed.exists():
            for t in json.loads(t_seed.read_text()):
                if str(t["id"]) == tid:
                    name = t["name"]
                    break

        tournaments.append({
            "tid": tid,
            "name": name,
            "cat_ids": cat_ids,
            "event_date": event_date,
        })

    total_cats = sum(len(t["cat_ids"]) for t in tournaments)
    log.info("IBJJF: %d tournaments, %d total brackets to fetch", len(tournaments), total_cats)

    done = skipped = errors = 0

    for t in tournaments:
        tid        = t["tid"]
        name       = t["name"]
        event_date = t["event_date"]
        cat_ids    = t["cat_ids"]

        # Skip brackets we already have final results for
        todo = [cid for cid in cat_ids if not _already_fetched(cid)]
        skip = len(cat_ids) - len(todo)
        skipped += skip
        if not todo:
            log.info("[%s] %s — all %d brackets already final, skipping", tid, name[:40], len(cat_ids))
            continue

        log.info("[%s] %s — fetching %d brackets (%d already done)", tid, name[:40], len(todo), skip)

        items = [(tid, cid, "") for cid in todo]
        results = fetch_brackets_batch(items, concurrency=20)

        for cid, state in results.items():
            if "error" in state:
                errors += 1
                continue
            _save_local(cid, state)
            _try_save_supabase(
                category_id=cid,
                tournament_id=tid,
                tournament_name=name,
                division=state.get("division", ""),
                source="ibjjf",
                ranking=state.get("ranking", []),
                state=state,
                event_date=event_date,
            )
            done += 1

        log.info("[%s] done — %d saved, %d errors", tid, len(results) - errors, errors)

    log.info("IBJJF backfill complete: %d saved, %d skipped (already final), %d errors",
             done, skipped, errors)
    return done, skipped, errors


# ── NAGA backfill ─────────────────────────────────────────────────────────────

def backfill_naga():
    from scraper_naga import _load_naga_cache, fetch_naga_bracket, _get_brackets_meta

    past_events = {eid: ev for eid, ev in _load_naga_cache().items()
                   if ev.get("is_past")}
    log.info("NAGA: %d past events to process", len(past_events))

    done = skipped = errors = 0

    for event_id, ev in past_events.items():
        name       = ev.get("name", f"NAGA {event_id}")
        event_date = ev.get("start", "")
        subdomain  = ev.get("subdomain", "naga")

        log.info("[%s] %s — discovering brackets...", event_id, name[:50])

        try:
            meta = _get_brackets_meta(event_id, subdomain)
        except Exception as e:
            log.warning("[%s] could not get brackets meta: %s", event_id, e)
            errors += 1
            continue

        if not meta:
            log.warning("[%s] no brackets found", event_id)
            continue

        log.info("[%s] found %d brackets", event_id, len(meta))


        for cid_int, bracket in meta.items():
            cid = str(cid_int)
            if not cid:
                continue
            if _already_fetched(cid):
                skipped += 1
                continue

            try:
                state = fetch_naga_bracket(event_id, int(cid), subdomain)
                if "error" in state:
                    errors += 1
                    continue
                _save_local(cid, state)
                _try_save_supabase(
                    category_id=cid,
                    tournament_id=event_id,
                    tournament_name=name,
                    division=state.get("division", bracket.get("name", "")),
                    source="naga",
                    ranking=state.get("ranking", []),
                    state=state,
                    event_date=event_date,
                )
                done += 1
            except Exception as e:
                log.warning("[%s] bracket %s failed: %s", event_id, cid, e)
                errors += 1

        log.info("[%s] done", event_id)

    log.info("NAGA backfill complete: %d saved, %d skipped, %d errors", done, skipped, errors)
    return done, skipped, errors


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t0 = time.time()

    log.info("=== Starting backfill ===")
    i_done, i_skip, i_err = backfill_ibjjf()
    n_done, n_skip, n_err = backfill_naga()

    elapsed = time.time() - t0
    log.info("=== Backfill complete in %.0fs ===", elapsed)
    log.info("IBJJF: %d saved, %d skipped, %d errors", i_done, i_skip, i_err)
    log.info("NAGA:  %d saved, %d skipped, %d errors", n_done, n_skip, n_err)
    log.info("Total: %d bracket results stored", i_done + n_done)
