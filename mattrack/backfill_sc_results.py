"""
backfill_sc_results.py — Read bracket_finals.state_json for SC + misc sources
and propagate the rankings into fighter_results + tournament_results.

Use case: scrape_sc_brackets.py historically only wrote bracket_finals, so all
SC placements lived only in JSONB. This re-flattens them into the queryable
flat tables that power search and head-to-head.

Usage:
    python backfill_sc_results.py                    # all SC + misc brackets
    python backfill_sc_results.py --tid 27565        # just one event
    python backfill_sc_results.py --source naga      # one source
    python backfill_sc_results.py --dry-run          # count rows, no writes
"""
import argparse
import logging
import os
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Reuse the canonical flatten/upsert helpers from scrape_sc_brackets
sys.path.insert(0, os.path.dirname(__file__))
from scrape_sc_brackets import _flatten_results, _post, SUPABASE_URL, SUPABASE_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_sc")

SC_SOURCES = {"smoothcomp", "naga", "compnet", "fuji", "gi", "goodfight",
              "grapplingx", "newbreed", "nfc", "pbjjf", "rollalot",
              "subchallenge", "tco", "united", "misc", "adcc"}


def fetch_brackets(source: str | None, tid: str | None) -> list[dict]:
    """Page through bracket_finals 1000 rows at a time."""
    rows: list[dict] = []
    page = 0
    while True:
        params = {
            "select": "tournament_id,division,state_json,source",
            "order":  "tournament_id.asc",
            "offset": page * 1000,
            "limit":  1000,
        }
        if source:
            params["source"] = f"eq.{source}"
        elif tid:
            params["tournament_id"] = f"eq.{tid}"
        else:
            params["source"] = f"in.({','.join(SC_SOURCES)})"

        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/bracket_finals",
            params=params,
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=60,
        )
        if not r.ok:
            log.warning("Page %d fetch failed %s: %s", page, r.status_code, r.text[:200])
            break
        page_rows = r.json()
        rows.extend(page_rows)
        log.info("page %d: +%d (total %d)", page, len(page_rows), len(rows))
        if len(page_rows) < 1000:
            break
        page += 1
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tid")
    p.add_argument("--source")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not SUPABASE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        sys.exit(1)

    brackets = fetch_brackets(args.source, args.tid)
    log.info("Loaded %d bracket_finals rows to flatten", len(brackets))

    fr_total = 0
    tr_total = 0
    skipped = 0
    for i, b in enumerate(brackets):
        state = b.get("state_json") or {}
        if not state:
            skipped += 1
            continue
        # Make sure state has the keys _flatten_results expects
        state.setdefault("category_id",     state.get("category_id", ""))
        state.setdefault("tournament_id",   b.get("tournament_id"))
        state.setdefault("tournament_name", state.get("tournament_name", ""))
        state.setdefault("division",        b.get("division", ""))
        state.setdefault("source",          b.get("source", "smoothcomp"))

        _, tr_rows = _flatten_results(state)
        if not tr_rows:
            skipped += 1
            continue

        if args.dry_run:
            tr_total += len(tr_rows)
            continue

        _post("tournament_results", tr_rows,
              on_conflict="source,event_id,division,placement,athlete_name")
        tr_total += len(tr_rows)

        if (i + 1) % 50 == 0:
            log.info("  %d/%d brackets done — tr=%d", i + 1, len(brackets), tr_total)
        time.sleep(0.05)

    log.info("=" * 60)
    log.info("Backfill done: %d tournament_results, %d skipped", tr_total, skipped)


if __name__ == "__main__":
    main()
