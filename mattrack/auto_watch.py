"""
auto_watch.py — Automated bracket capture for all active bjjcompsystem tournaments.

Designed to run as a cron job so we never miss bracket data again.
bjjcompsystem.com deletes tournaments after they end — brackets not captured are lost forever.

Modes:
    python auto_watch.py                # full sweep: discover + fetch all active tournaments
    python auto_watch.py --sweep-only   # re-fetch only tournaments already in bracket_finals
    python auto_watch.py --tid 3102     # single tournament by ID

Schedule (Render cron or system cron):
    0 6 * * *       python auto_watch.py           # daily full discovery at 6 AM UTC
    0 */4 * * *     python auto_watch.py --sweep-only  # every 4h re-sweep active events
"""

import argparse
import logging
import os
import re
import sys
import time
from datetime import date, datetime

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
        logging.FileHandler("auto_watch.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("auto_watch")


# ── Supabase helpers ─────────────────────────────────────────────────────────

def _get_supabase():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        log.warning("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — will fetch but not save")
        return None
    from supabase import create_client
    return create_client(url, key)


def _get_known_finals(client) -> set[str]:
    """Return set of category_ids that already have results_final=True in bracket_finals."""
    if client is None:
        return set()
    try:
        # state_json->>results_final is stored inside the JSON; query for it
        resp = (
            client.table("bracket_finals")
            .select("category_id, state_json")
            .execute()
        )
        finals = set()
        for row in resp.data:
            state = row.get("state_json") or {}
            if state.get("results_final"):
                finals.add(row["category_id"])
        return finals
    except Exception as e:
        log.warning("Could not load known finals: %s", e)
        return set()


def _get_known_tournament_ids(client) -> set[str]:
    """Return set of tournament_ids already in bracket_finals."""
    if client is None:
        return set()
    try:
        resp = (
            client.table("bracket_finals")
            .select("tournament_id")
            .not_.is_("tournament_id", "null")
            .neq("tournament_id", "")
            .execute()
        )
        return {row["tournament_id"] for row in resp.data}
    except Exception as e:
        log.warning("Could not load known tournament_ids: %s", e)
        return set()


# ── Core capture logic ───────────────────────────────────────────────────────

def capture_tournament(tid: str, name: str, known_finals: set[str]) -> dict:
    """
    Fetch all brackets for a single tournament and save to Supabase.
    Skips categories already marked results_final (won't change).
    Returns {saved, skipped_final, skipped_empty, errors}.
    """
    from scraper import get_category_ids
    from watcher import fetch_brackets_batch
    from results import save_bracket_final

    stats = {"saved": 0, "skipped_final": 0, "skipped_empty": 0, "errors": 0, "categories": 0}

    try:
        cats = get_category_ids(tid)
    except Exception as e:
        if "404" in str(e):
            log.info("[%s] %s — no longer on bjjcompsystem (404), skipping", tid, name)
        else:
            log.error("[%s] Failed to get categories: %s", tid, e)
        stats["errors"] = 1
        return stats

    stats["categories"] = len(cats)
    if not cats:
        log.info("[%s] %s — no categories found", tid, name)
        return stats

    # Filter out categories we already have final results for
    todo = [(tid, c["id"], c["name"]) for c in cats if c["id"] not in known_finals]
    skipped = len(cats) - len(todo)
    stats["skipped_final"] = skipped

    if not todo:
        log.info("[%s] %s — all %d categories already final", tid, name, len(cats))
        return stats

    log.info("[%s] %s — fetching %d categories (%d already final)",
             tid, name, len(todo), skipped)

    # Fetch in batches of 50 to be respectful
    all_results = {}
    batch_size = 50
    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        log.info("[%s] Batch %d-%d of %d", tid, i + 1, min(i + batch_size, len(todo)), len(todo))
        batch_results = fetch_brackets_batch(batch, concurrency=15)
        all_results.update(batch_results)
        if i + batch_size < len(todo):
            time.sleep(0.5)

    # Save results
    for cid, state in all_results.items():
        if "error" in state:
            stats["errors"] += 1
            continue

        ranking = state.get("ranking", [])
        fights = state.get("fights", [])
        if not ranking and not fights:
            stats["skipped_empty"] += 1
            continue

        # Infer event_date from fight times
        event_date = ""
        for fight in fights:
            m = re.search(r'(\d{2})/(\d{2})', fight.get("time") or "")
            if m:
                y = date.today().year
                event_date = f"{y}-{m.group(1)}-{m.group(2)}"
                break

        try:
            save_bracket_final(
                category_id=cid,
                tournament_id=tid,
                tournament_name=name,
                division=state.get("division", ""),
                source="ibjjf",
                ranking=ranking,
                state=state,
                event_date=event_date,
            )
            stats["saved"] += 1
        except Exception as e:
            log.warning("[%s] Save failed for %s: %s", tid, cid, e)
            stats["errors"] += 1

    log.info("[%s] Done: %d saved, %d final-skipped, %d empty, %d errors",
             tid, stats["saved"], stats["skipped_final"], stats["skipped_empty"], stats["errors"])
    return stats


# ── Main modes ───────────────────────────────────────────────────────────────

def full_discovery(single_tid: str | None = None):
    """Discover all active tournaments on bjjcompsystem and capture their brackets."""
    from scraper import get_tournaments

    log.info("=" * 60)
    log.info("AUTO-WATCH: full discovery run at %s", datetime.now().isoformat())

    # Get known finals to skip
    client = _get_supabase()
    known_finals = _get_known_finals(client)
    log.info("Known final brackets: %d", len(known_finals))

    if single_tid:
        # Single tournament mode
        tournaments = [{"id": single_tid, "name": f"Tournament {single_tid}"}]
        # Try to get real name
        try:
            all_t = get_tournaments(use_cache_on_fail=True)
            for t in all_t:
                if str(t["id"]) == single_tid:
                    tournaments = [t]
                    break
        except Exception:
            pass
    else:
        # Discover all active tournaments
        log.info("Fetching tournament list from bjjcompsystem.com...")
        try:
            tournaments = get_tournaments(use_cache_on_fail=False)
        except Exception as e:
            log.error("Failed to fetch tournaments: %s", e)
            return

    log.info("Found %d tournaments on bjjcompsystem", len(tournaments))

    totals = {"saved": 0, "skipped_final": 0, "skipped_empty": 0, "errors": 0, "categories": 0}

    for t in tournaments:
        tid = str(t["id"])
        name = t.get("name", f"Tournament {tid}")

        stats = capture_tournament(tid, name, known_finals)
        for k in totals:
            totals[k] += stats[k]

    log.info("=" * 60)
    log.info("AUTO-WATCH COMPLETE: %d tournaments, %d categories, %d saved, %d already-final, %d empty, %d errors",
             len(tournaments), totals["categories"], totals["saved"],
             totals["skipped_final"], totals["skipped_empty"], totals["errors"])
    return totals


def sweep_only():
    """Re-check tournaments we already have data for — catch newly completed brackets."""
    log.info("=" * 60)
    log.info("AUTO-WATCH: sweep run at %s", datetime.now().isoformat())

    client = _get_supabase()
    known_tids = _get_known_tournament_ids(client)
    known_finals = _get_known_finals(client)
    log.info("Known tournament IDs: %d, Known finals: %d", len(known_tids), len(known_finals))

    if not known_tids:
        log.info("No known tournaments to sweep — run full discovery first")
        return

    # Get tournament names
    from scraper import get_tournaments
    try:
        all_t = get_tournaments(use_cache_on_fail=True)
        tid_to_name = {str(t["id"]): t.get("name", f"Tournament {t['id']}") for t in all_t}
    except Exception:
        tid_to_name = {}

    totals = {"saved": 0, "skipped_final": 0, "skipped_empty": 0, "errors": 0, "categories": 0}

    for tid in sorted(known_tids):
        name = tid_to_name.get(tid, f"Tournament {tid}")
        stats = capture_tournament(tid, name, known_finals)
        for k in totals:
            totals[k] += stats[k]

    log.info("=" * 60)
    log.info("SWEEP COMPLETE: %d tournaments, %d saved, %d already-final, %d empty, %d errors",
             len(known_tids), totals["saved"], totals["skipped_final"],
             totals["skipped_empty"], totals["errors"])
    return totals


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Auto-watch: capture all brackets from active bjjcompsystem tournaments"
    )
    parser.add_argument("--sweep-only", action="store_true",
                        help="Only re-check tournaments already in bracket_finals")
    parser.add_argument("--tid", type=str, default=None,
                        help="Single tournament ID to capture")
    args = parser.parse_args()

    if args.sweep_only:
        sweep_only()
    else:
        full_discovery(single_tid=args.tid)


if __name__ == "__main__":
    main()
