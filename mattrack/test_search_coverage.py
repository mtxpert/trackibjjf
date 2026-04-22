#!/usr/bin/env python3
"""Integration test: verify the TrackBJJ search finds known public athletes.

Pulls ground-truth fixtures from our own DB:
  - Top 3 IBJJF adult black belts by ranking points
  - Gold medalists in the "PRO" divisions from the most recent 3 ADCC USA Opens
then runs each through search_athletes and reports misses.

Run: SUPABASE_URL=... SUPABASE_SERVICE_KEY=... python3 test_search_coverage.py
Exits non-zero on any miss.
"""
import os
import re
import sys
from collections import defaultdict

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY", "")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in env", file=sys.stderr)
    sys.exit(2)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_name(raw: str) -> str:
    """ADCC rows often store 'name / team' — strip the team off."""
    if not raw:
        return ""
    raw = raw.split(" / ")[0]
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def normalize(s: str) -> str:
    return re.sub(r"[^a-z\s]", "", (s or "").lower()).strip()


def name_matches(want: str, candidate: str) -> bool:
    a, b = normalize(want), normalize(candidate)
    if not a or not b:
        return False
    if a == b:
        return True
    a_tokens, b_tokens = set(a.split()), set(b.split())
    if a_tokens and b_tokens and a_tokens.issubset(b_tokens):
        return True
    if a_tokens and b_tokens and b_tokens.issubset(a_tokens):
        return True
    return a in b or b in a


def top_ibjjf_black_belts(n=3):
    res = (sb.table("ibjjf_athletes")
             .select("ibjjf_id,name,academy,points")
             .eq("belt", "black")
             .eq("ranking_category", "adult")
             .order("points", desc=True, nullsfirst=False)
             .limit(15)
             .execute())
    seen = set()
    out = []
    for row in (res.data or []):
        key = normalize(row["name"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= n:
            break
    return out


def last_adcc_qualifier_golds(n_events=3):
    """Return (events, list_of_gold_rows_in_top_tier_divisions)."""
    # Fetch distinct event_title+date by paginating past PostgREST's default
    # 1000-row cap (Phoenix alone has ~555 rows per event so naive .limit(N)
    # on the rows returns only a few events' worth).
    seen_titles = set()
    by_event = {}
    offset = 0
    page = 1000
    while True:
        res = (sb.table("tournament_results")
                 .select("event_title,event_date")
                 .eq("source", "adcc")
                 .ilike("event_title", "%USA%OPEN%")
                 .order("event_date", desc=True)
                 .range(offset, offset + page - 1)
                 .execute())
        data = res.data or []
        if not data:
            break
        for r in data:
            title = r["event_title"]
            if title not in by_event or (r.get("event_date") or "") > (by_event[title] or ""):
                by_event[title] = r.get("event_date")
            seen_titles.add(title)
        if len(data) < page or len(seen_titles) >= n_events + 3:
            break
        offset += page
    events = sorted(by_event.items(), key=lambda kv: kv[1] or "", reverse=True)[:n_events]

    golds = []
    for title, _ in events:
        # Top tier at recent ADCC USA Opens is "ADULT ADVANCED" (or "PRO" in older events).
        pr = (sb.table("tournament_results")
                .select("event_title,event_date,athlete_name,division,placement")
                .eq("source", "adcc")
                .eq("event_title", title)
                .eq("placement", 1)
                .or_("division.ilike.*ADULT*ADVANCED*,division.ilike.*PRO*")
                .limit(50)
                .execute())
        golds.extend(pr.data or [])
    return [{"title": t, "date": d} for t, d in events], golds


def run_search(q):
    try:
        res = sb.rpc("search_athletes", {"q": q.lower()}).execute()
        return res.data or []
    except Exception as e:
        return [{"_error": str(e)}]


def pct(x, total):
    return f"{(100*x/total):.0f}%" if total else "–"


def main():
    print("=" * 78)
    print("TEST: Top IBJJF Adult Black Belts — findable by name?")
    print("=" * 78)
    ibjjf_hits = 0
    ibjjf_total = 0
    for a in top_ibjjf_black_belts(3):
        ibjjf_total += 1
        name = a["name"]
        results = run_search(name)
        found_by_id = any(str(r.get("ibjjf_id") or "") == str(a["ibjjf_id"]) for r in results)
        found_by_name = any(name_matches(name, r.get("athlete_display") or "") for r in results)
        ok = found_by_id or found_by_name
        if ok:
            ibjjf_hits += 1
        marker = "PASS" if ok else "FAIL"
        pts = f"{a.get('points') or 0:.0f} pts"
        print(f"  [{marker}] {name:45s} ({a['ibjjf_id']}, {pts}) → {len(results)} hits"
              + (f" (id match)" if found_by_id else ("" if not found_by_name else " (name match)")))
        if not ok:
            print(f"         ↳ top results: "
                  + ", ".join(f"{r.get('athlete_display') or '?'}"
                              for r in results[:3] if isinstance(r, dict)))

    print()
    print("=" * 78)
    print("TEST: Last 3 ADCC USA Open — PRO division gold medalists findable by name?")
    print("=" * 78)
    events, golds = last_adcc_qualifier_golds(3)
    for e in events:
        print(f"  Event: {e['title']} ({e['date']})")
    adcc_hits = 0
    adcc_total = 0
    per_event = defaultdict(lambda: [0, 0])
    for g in golds:
        adcc_total += 1
        display_name = clean_name(g["athlete_name"])
        if not display_name:
            continue
        ev = g["event_title"]
        per_event[ev][1] += 1
        results = run_search(display_name)
        ok = any(name_matches(display_name, r.get("athlete_display") or "") for r in results)
        if ok:
            adcc_hits += 1
            per_event[ev][0] += 1
        marker = "PASS" if ok else "FAIL"
        div = (g.get("division") or "")[:32]
        print(f"  [{marker}] {display_name:35s} {div:32s}  ({len(results)} hits)")
    print()
    print("  per-event coverage:")
    for ev, (h, t) in per_event.items():
        print(f"    {ev:55s}  {h}/{t}  ({pct(h,t)})")

    print()
    print("=" * 78)
    print(f"SUMMARY")
    print("=" * 78)
    print(f"  IBJJF top black belts:  {ibjjf_hits}/{ibjjf_total}  ({pct(ibjjf_hits, ibjjf_total)})")
    print(f"  ADCC USA gold medalists: {adcc_hits}/{adcc_total} ({pct(adcc_hits, adcc_total)})")
    failures = (ibjjf_total - ibjjf_hits) + (adcc_total - adcc_hits)
    print(f"  TOTAL FAILURES: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
