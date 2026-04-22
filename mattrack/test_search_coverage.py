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


IBJJF_CATEGORIES = [
    ("male",   "gi"),
    ("male",   "nogi"),
    ("female", "gi"),
    ("female", "nogi"),
]

WEIGHT_ORDER = [
    "rooster", "light-feather", "feather", "light",
    "middle", "medium-heavy", "heavy", "super-heavy", "ultra-heavy",
]


def _exec_sql(query: str) -> list:
    """Execute SQL via Supabase Management API (read-only SELECTs here)."""
    import requests as _req
    pat = os.environ.get("SUPABASE_PAT", "")
    ref = os.environ.get("SUPABASE_PROJECT_REF", "kzqvfuqxtbrhlgphyntb")
    if not pat:
        raise RuntimeError("SUPABASE_PAT env var required for per-weight bucketing")
    r = _req.post(
        f"https://api.supabase.com/v1/projects/{ref}/database/query",
        json={"query": query},
        headers={"Authorization": f"Bearer {pat}", "User-Agent": "trackbjj-tests/1.0"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def top_ibjjf_per_weight(n_per_bucket=3, per_category_pool=500):
    """Top N adult black belts by (gender × gi/nogi × main weight class).

    ibjjf_athletes has no weight column, so we derive each athlete's main
    weight from the divisions they've competed in (tournament_results rows
    matching their ibjjf_athlete_id). Runs one query per (gender × gi_nogi)
    so that lower-scoring-but-still-top categories (e.g., Female NoGi) get
    fair representation instead of being crowded out by overall top-ranked.
    """
    buckets = {}
    for gender, gi_nogi in IBJJF_CATEGORIES:
        # Only 437/941k tournament_results rows have ibjjf_athlete_id populated
        # — so we join on lowercased name (via name_lower on ibjjf_athletes) rather
        # than ibjjf_athlete_id.
        sql = f"""
        WITH pool AS (
          SELECT ibjjf_id, name, name_lower, gender, gi_nogi, points
          FROM ibjjf_athletes
          WHERE belt = 'black' AND ranking_category = 'adult'
            AND gender = '{gender}' AND gi_nogi = '{gi_nogi}'
            AND points IS NOT NULL
          ORDER BY points DESC NULLS LAST
          LIMIT {per_category_pool}
        ),
        divs AS (
          SELECT p.ibjjf_id,
            CASE
              WHEN tr.division ILIKE '%light feather%'  OR tr.division ILIKE '%lightfeather%'  OR tr.division ILIKE '%galo-pluma%' THEN 'light-feather'
              WHEN tr.division ILIKE '%medium heavy%'   OR tr.division ILIKE '%mediumheavy%'   OR tr.division ILIKE '%meio pesado%' THEN 'medium-heavy'
              WHEN tr.division ILIKE '%super heavy%'    OR tr.division ILIKE '%superheavy%'    OR tr.division ILIKE '%super pesado%' THEN 'super-heavy'
              WHEN tr.division ILIKE '%ultra heavy%'    OR tr.division ILIKE '%ultraheavy%'    OR tr.division ILIKE '%pesadissimo%' THEN 'ultra-heavy'
              WHEN tr.division ILIKE '%rooster%'        OR tr.division ILIKE '%galo%'          THEN 'rooster'
              WHEN tr.division ILIKE '%feather%'        OR tr.division ILIKE '%pluma%'         THEN 'feather'
              WHEN tr.division ILIKE '%middle%'         OR tr.division ILIKE '%medio%'         THEN 'middle'
              WHEN tr.division ILIKE '%heavy%'          OR tr.division ILIKE '%pesado%'        THEN 'heavy'
              WHEN tr.division ILIKE '%light%'          OR tr.division ILIKE '%leve%'          THEN 'light'
              ELSE NULL
            END AS weight
          FROM tournament_results tr
          INNER JOIN pool p ON lower(tr.athlete_name) = p.name_lower
          WHERE tr.source = 'ibjjf'
        ),
        main AS (
          SELECT ibjjf_id, weight,
                 ROW_NUMBER() OVER (PARTITION BY ibjjf_id ORDER BY COUNT(*) DESC) AS rn
          FROM divs
          WHERE weight IS NOT NULL
          GROUP BY ibjjf_id, weight
        ),
        ranked AS (
          SELECT p.ibjjf_id, p.name, p.gender, p.gi_nogi, p.points, m.weight,
                 ROW_NUMBER() OVER (
                   PARTITION BY m.weight
                   ORDER BY p.points DESC NULLS LAST
                 ) AS rank_in_bucket
          FROM pool p
          INNER JOIN main m ON m.ibjjf_id = p.ibjjf_id AND m.rn = 1
        )
        SELECT ibjjf_id, name, gender, gi_nogi, points, weight, rank_in_bucket
        FROM ranked
        WHERE rank_in_bucket <= {n_per_bucket}
        ORDER BY weight, rank_in_bucket;
        """
        rows = _exec_sql(sql)
        for r in rows:
            key = (r["gender"], r["gi_nogi"], r["weight"])
            buckets.setdefault(key, []).append(r)
    return buckets


def top_ibjjf_per_category(n=3):
    """Fallback bucket-by-(gender × gi_nogi) — kept for comparison."""
    out = {}
    for gender, gi_nogi in IBJJF_CATEGORIES:
        res = (sb.table("ibjjf_athletes")
                 .select("ibjjf_id,name,academy,points,gi_nogi,gender")
                 .eq("belt", "black").eq("ranking_category", "adult")
                 .eq("gender", gender).eq("gi_nogi", gi_nogi)
                 .order("points", desc=True, nullsfirst=False)
                 .limit(15)
                 .execute())
        seen, picks = set(), []
        for row in (res.data or []):
            key = normalize(row["name"])
            if key in seen:
                continue
            seen.add(key)
            picks.append(row)
            if len(picks) >= n:
                break
        out[(gender, gi_nogi)] = picks
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


SOURCES_TO_TEST = [
    "smoothcomp", "ibjjf", "ajp", "uaejjf", "adcc",
    "gi", "naga", "sjjif", "fuji", "newbreed",
    "compnet", "grapplingx", "united", "subchallenge",
    "pbjjf", "goodfight", "rollalot",
]


def top_n_per_source(source: str, n: int = 15):
    """Top-N athletes by row count in a given source, grouping by lowered name
    (many sources store athlete_id as the literal string '\\N' so athlete_id is
    not a reliable identifier)."""
    sql = f"""
    SELECT
      MAX(athlete_name) AS athlete_name,
      lower(athlete_name) AS name_norm,
      MAX(team) AS team,
      COUNT(*) AS rows,
      MAX(event_date) AS last_seen
    FROM tournament_results
    WHERE source = '{source}'
      AND athlete_name IS NOT NULL
      AND length(trim(athlete_name)) > 2
    GROUP BY lower(athlete_name)
    ORDER BY rows DESC
    LIMIT {n};
    """
    rows = _exec_sql(sql)
    return rows


def main():
    print("=" * 78)
    print("TEST: Top 3 IBJJF Adult Black Belts per (gender × gi/nogi × weight) — findable?")
    print("=" * 78)
    ibjjf_hits = 0
    ibjjf_total = 0
    per_bucket = {}
    buckets = top_ibjjf_per_weight(3, per_category_pool=500)

    def weight_sort(weight):
        try:
            return WEIGHT_ORDER.index(weight)
        except ValueError:
            return 99

    ordered_keys = sorted(buckets.keys(), key=lambda k: (k[0], k[1], weight_sort(k[2])))
    for key in ordered_keys:
        gender, gi_nogi, weight = key
        bucket_label = f"{gender.title()} {gi_nogi.upper()} / {weight}"
        print(f"\n  {bucket_label}:")
        h, t = 0, 0
        for a in buckets[key]:
            ibjjf_total += 1
            t += 1
            name = a["name"]
            results = run_search(name)
            found_by_id = any(str(r.get("ibjjf_id") or "") == str(a["ibjjf_id"]) for r in results)
            found_by_name = any(name_matches(name, r.get("athlete_display") or "") for r in results)
            ok = found_by_id or found_by_name
            if ok:
                ibjjf_hits += 1
                h += 1
            marker = "PASS" if ok else "FAIL"
            pts = f"{a.get('points') or 0:.0f} pts"
            tag = " (id match)" if found_by_id else (" (name match)" if found_by_name else "")
            print(f"    [{marker}] #{a['rank_in_bucket']} {name[:38]:38s} ({str(a['ibjjf_id'])[:16]:16s}, {pts:>9s}) → {len(results)} hits{tag}")
            if not ok:
                tops = ", ".join(f"{r.get('athlete_display') or '?'}"
                                 for r in results[:3] if isinstance(r, dict))
                print(f"           ↳ top results: {tops}")
        per_bucket[bucket_label] = (h, t)

    print()
    print("  per-bucket coverage:")
    for label, (h, t) in per_bucket.items():
        print(f"    {label:38s}  {h}/{t}  ({pct(h,t)})")

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
    print("TEST: Per-source — top 15 athletes by row count findable via search")
    print("=" * 78)
    per_source = {}
    src_total_hits = 0
    src_total_fixtures = 0
    for source in SOURCES_TO_TEST:
        try:
            fixtures = top_n_per_source(source, 15)
        except Exception as e:
            print(f"\n  {source}: fetch failed ({e})")
            continue
        if not fixtures:
            print(f"\n  {source}: no fixtures")
            continue
        print(f"\n  {source}  ({len(fixtures)} fixtures, most-active athletes):")
        h, t = 0, 0
        for f in fixtures:
            name = f.get("athlete_name") or ""
            if not name:
                continue
            t += 1
            src_total_fixtures += 1
            results = run_search(name)
            ok = any(name_matches(name, r.get("athlete_display") or "") for r in results
                     if isinstance(r, dict))
            if ok:
                h += 1
                src_total_hits += 1
            marker = "PASS" if ok else "FAIL"
            team = (f.get("team") or "")[:22]
            print(f"    [{marker}] {name[:32]:32s} ({f.get('rows',0):>3} rows, {team:22s}) → {len(results)} hits")
        per_source[source] = (h, t)

    print()
    print("  per-source coverage:")
    for src, (h, t) in per_source.items():
        print(f"    {src:16s}  {h:>2}/{t:<2}  ({pct(h,t)})")

    print()
    print("=" * 78)
    print(f"SUMMARY")
    print("=" * 78)
    print(f"  IBJJF top black belts:  {ibjjf_hits}/{ibjjf_total}  ({pct(ibjjf_hits, ibjjf_total)})")
    print(f"  ADCC USA gold medalists: {adcc_hits}/{adcc_total} ({pct(adcc_hits, adcc_total)})")
    print(f"  Per-source top-15:      {src_total_hits}/{src_total_fixtures} ({pct(src_total_hits, src_total_fixtures)})")
    failures = (ibjjf_total - ibjjf_hits) + (adcc_total - adcc_hits) + (src_total_fixtures - src_total_hits)
    print(f"  TOTAL FAILURES: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
