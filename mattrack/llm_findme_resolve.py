"""
llm_findme_resolve.py — LLM-tier intelligence pass for unresolvable findme_reports.

Run AFTER nightly_findme_resolve.py:
    python nightly_findme_resolve.py 2>&1 | tee /tmp/resolve.log
    python llm_findme_resolve.py     2>&1 | tee /tmp/llm_resolve.log

Picks up reports that status='unresolvable' AND updated in the last 25 hours,
tries harder using external scrapes + multi-source agreement, then writes only
when at least TWO independent signals confirm the same identity.

Requires: SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── import shared helpers from the deterministic pass ────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from nightly_findme_resolve import (
    _get_sb,
    _normalize,
    _ensure_ibjjf_athletes_row,
    _fetch_ibjjf_profile_from_api,
    _ibjjf_name_search,
    _tr_name_search,
    _bjjmetrics_search,
    _get_link_by_sc,
    _get_link_by_ibjjf,
    _upsert_link,
    _close,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("llm_findme_resolve")

MAX_LLM_REPORTS = 25
LOOKBACK_HOURS  = 25

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Extra scrapers ────────────────────────────────────────────────────────────

def _scrape_smoothcomp_profile(sc_uid: str) -> dict:
    """
    GET https://smoothcomp.com/en/profile/<sc_uid>
    Returns {name, academy} or empty dict on any failure.
    """
    try:
        r = requests.get(
            f"https://smoothcomp.com/en/profile/{sc_uid}",
            headers=HTTP_HEADERS,
            timeout=15,
        )
        if r.status_code != 200:
            log.debug("Smoothcomp profile sc_uid=%s: HTTP %s", sc_uid, r.status_code)
            return {}
        soup = BeautifulSoup(r.text, "lxml")
        name = academy = None

        # Profile name is usually the first meaningful h1/h2
        for tag in soup.find_all(["h1", "h2"]):
            t = tag.get_text(strip=True)
            if t and len(t) < 80:
                name = t
                break

        # Academy: look for a tag that contains academy/team/club text nearby
        for tag in soup.find_all(class_=re.compile(r"academy|team|club|gym", re.I)):
            t = tag.get_text(strip=True)
            if t and len(t) < 100:
                academy = t
                break

        # Fallback: meta og:title
        if not name:
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                name = og["content"].strip()

        return {"name": name, "academy": academy}
    except Exception as e:
        log.debug("Smoothcomp profile sc_uid=%s: %s", sc_uid, e)
        return {}


def _scrape_bjjcompsystem_athlete(ibjjf_id: str) -> dict:
    """
    GET https://www.bjjcompsystem.com/athletes/<id>
    Returns {name, academy} or empty dict on any failure.
    """
    try:
        r = requests.get(
            f"https://www.bjjcompsystem.com/athletes/{ibjjf_id}",
            headers=HTTP_HEADERS,
            timeout=15,
        )
        if r.status_code != 200:
            log.debug("bjjcompsystem athlete id=%s: HTTP %s", ibjjf_id, r.status_code)
            return {}
        soup = BeautifulSoup(r.text, "lxml")
        name = academy = None

        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        for tag in soup.find_all(class_=re.compile(r"academy|team|gym|club", re.I)):
            t = tag.get_text(strip=True)
            if t and len(t) < 100:
                academy = t
                break

        return {"name": name, "academy": academy}
    except Exception as e:
        log.debug("bjjcompsystem id=%s: %s", ibjjf_id, e)
        return {}


# ── Disambiguation helpers ────────────────────────────────────────────────────

def _name_similarity(a: str, b: str) -> float:
    """Token-overlap Jaccard similarity on normalised names."""
    a_tok = set(_normalize(a).split())
    b_tok = set(_normalize(b).split())
    if not a_tok or not b_tok:
        return 0.0
    return len(a_tok & b_tok) / max(len(a_tok), len(b_tok))


def _ibjjf_middle_name_search(sb, name: str) -> list[dict]:
    """
    Search ibjjf_athletes via '%first%last%' to catch middle-name variants.
    E.g. 'Tyler Walker' → '%tyler%walker%' matches 'Tyler Michael Walker'.
    """
    parts = _normalize(name).split()
    if len(parts) < 2:
        return []
    pattern = "%" + "%".join([parts[0], parts[-1]]) + "%"
    try:
        res = (sb.table("ibjjf_athletes")
                 .select("ibjjf_id,name,name_lower,belt,academy")
                 .ilike("name_lower", pattern)
                 .limit(20)
                 .execute())
        return res.data or []
    except Exception as e:
        log.warning("ibjjf middle-name search %r: %s", name, e)
        return []


def _academy_hint(email: str, name_field: str) -> str | None:
    """
    Extract an academy hint from the report's email domain or name field.
    Returns a lowercase token string, or None if no useful hint found.
    """
    hints = []
    # Email domain (skip free providers)
    if email and "@" in email:
        domain = email.split("@")[-1].lower()
        generic = {"gmail.com","yahoo.com","hotmail.com","outlook.com",
                   "icloud.com","me.com","live.com","protonmail.com"}
        if domain not in generic:
            hints.append(domain.split(".")[0])

    # Name field may contain "Name / Academy Name" pattern
    if name_field and "/" in name_field:
        parts = name_field.split("/", 1)
        if len(parts[1].strip()) > 2:
            hints.append(_normalize(parts[1].strip()))

    return hints[0] if hints else None


def _academy_agrees(hint: str, candidate_academy: str | None) -> bool:
    """True if the academy hint appears in the candidate's academy string."""
    if not hint or not candidate_academy:
        return False
    return hint in _normalize(candidate_academy)


def _backfill_tr_ibjjf_id(sb, ibjjf_id: str, athlete_name: str):
    """
    Set tournament_results.ibjjf_athlete_id on ibjjf-source rows
    where ibjjf_athlete_id is null and athlete_name ILIKE matches.
    Capped at 200 rows per run.
    """
    name_lower = athlete_name.strip().lower()
    if not name_lower:
        return
    try:
        res = (sb.table("tournament_results")
                 .select("id")
                 .eq("source", "ibjjf")
                 .is_("ibjjf_athlete_id", "null")
                 .ilike("athlete_name", f"%{name_lower}%")
                 .limit(200)
                 .execute())
        rows = res.data or []
        if not rows:
            return
        ids = [r["id"] for r in rows]
        (sb.table("tournament_results")
           .update({"ibjjf_athlete_id": ibjjf_id})
           .in_("id", ids)
           .execute())
        log.info("  Backfilled %d tournament_results rows → ibjjf_id=%s", len(ids), ibjjf_id)
    except Exception as e:
        log.warning("tournament_results backfill ibjjf_id=%s: %s", ibjjf_id, e)


# ── Per-report LLM pass ───────────────────────────────────────────────────────

def process_unresolvable(sb, report: dict) -> tuple[str, str]:
    """
    Enhanced resolution attempt for a single unresolvable report.
    Returns (outcome, notes_string).
    outcome in {'resolved', 'unresolvable', 'error'}.

    Writing policy: only write if ≥2 independent signals point to the
    same ibjjf_id.  Never overwrite a conflicting sc_ibjjf_verified row.
    """
    rid      = report["id"]
    ibjjf_id = str(report.get("ibjjf_id") or "").strip() or None
    sc_uid   = str(report.get("sc_uid")   or "").strip() or None
    name     = (report.get("name")        or "").strip() or None
    email    = (report.get("email")       or "").strip() or None
    prev     = (report.get("resolution_notes") or "")

    log.info("LLM report %s: ibjjf_id=%s sc_uid=%s name=%r", rid, ibjjf_id, sc_uid, name)

    notes: list[str] = [f"[LLM] prior_notes: {prev[:120]}"]

    # signals[label] = ibjjf_id — each independent source earns one entry
    signals: dict[str, str] = {}

    acad_hint = _academy_hint(email or "", name or "")
    if acad_hint:
        notes.append(f"academy_hint: {acad_hint!r}")

    # ── Signal 1: IBJJF API profile ──────────────────────────────────────────
    ibjjf_profile: dict | None = None
    if ibjjf_id:
        ibjjf_profile = _fetch_ibjjf_profile_from_api(ibjjf_id)
        if ibjjf_profile and ibjjf_profile.get("name"):
            signals["ibjjf_api"] = ibjjf_id
            notes.append(
                f"IBJJF API: name={ibjjf_profile['name']!r} "
                f"belt={ibjjf_profile.get('belt','')!r} "
                f"academy={ibjjf_profile.get('academy','')!r}"
            )
            # Academy hint cross-check
            if acad_hint and _academy_agrees(acad_hint, ibjjf_profile.get("academy")):
                signals["ibjjf_api_academy"] = ibjjf_id
                notes.append(f"  ↳ academy confirms hint {acad_hint!r}")
        else:
            notes.append(f"IBJJF API: no profile for id={ibjjf_id}")

    # ── Signal 2: bjjcompsystem page scrape ──────────────────────────────────
    ibjjf_page: dict = {}
    if ibjjf_id:
        ibjjf_page = _scrape_bjjcompsystem_athlete(ibjjf_id)
        if ibjjf_page.get("name"):
            signals["bjjcompsystem_page"] = ibjjf_id
            notes.append(
                f"bjjcompsystem: name={ibjjf_page['name']!r} "
                f"academy={ibjjf_page.get('academy','')!r}"
            )
            if acad_hint and _academy_agrees(acad_hint, ibjjf_page.get("academy")):
                signals["bjjcompsystem_academy"] = ibjjf_id
                notes.append(f"  ↳ academy confirms hint {acad_hint!r}")
        else:
            notes.append(f"bjjcompsystem: no page data for id={ibjjf_id}")

    # ── Signal 3: ibjjf_athletes DB search (middle-name variant) ─────────────
    search_name = name
    # If we scraped a name and have no report name, use it as search term
    if not search_name and ibjjf_profile:
        search_name = ibjjf_profile.get("name")
    if not search_name and ibjjf_page:
        search_name = ibjjf_page.get("name")

    ibjjf_db_matches: list[dict] = []
    if search_name:
        # Standard search first
        ibjjf_db_matches = _ibjjf_name_search(sb, search_name)
        # Middle-name variant search if no standard hit or name has 3+ parts
        if not ibjjf_db_matches or len(search_name.split()) > 2:
            extra = _ibjjf_middle_name_search(sb, search_name)
            seen_ids = {m["ibjjf_id"] for m in ibjjf_db_matches}
            for m in extra:
                if m["ibjjf_id"] not in seen_ids:
                    ibjjf_db_matches.append(m)

        for m in ibjjf_db_matches:
            sim = _name_similarity(search_name, m["name"])
            notes.append(f"ibjjf_athletes: {m['name']!r} id={m['ibjjf_id']} sim={sim:.2f}")
            if sim >= 0.75:
                candidate_id = str(m["ibjjf_id"])
                signals[f"ibjjf_db:{candidate_id}"] = candidate_id
                if acad_hint and _academy_agrees(acad_hint, m.get("academy")):
                    signals[f"ibjjf_db_academy:{candidate_id}"] = candidate_id
                    notes.append(f"  ↳ academy confirms hint {acad_hint!r}")

    # ── Signal 4: bjjmetrics cross-ref ────────────────────────────────────────
    bm_matches: list[dict] = []
    if search_name:
        bm_matches = _bjjmetrics_search(search_name)
        for bm in bm_matches:
            bid = str(bm["ibjjf_id"])
            notes.append(f"bjjmetrics: {bm['name']!r} id={bid}")
            # Confirm if it matches a known ibjjf_id OR has high name similarity
            if ibjjf_id and bid == str(ibjjf_id):
                signals[f"bjjmetrics_confirms:{bid}"] = bid
                notes.append(f"  ↳ bjjmetrics confirms report ibjjf_id")
            elif _name_similarity(search_name, bm["name"]) >= 0.80:
                signals[f"bjjmetrics:{bid}"] = bid

    # ── Signal 5: Smoothcomp public profile (if sc_uid given) ─────────────────
    sc_profile: dict = {}
    if sc_uid:
        sc_profile = _scrape_smoothcomp_profile(sc_uid)
        sc_name    = sc_profile.get("name")
        sc_academy = sc_profile.get("academy")
        if sc_name:
            notes.append(f"SC profile: name={sc_name!r} academy={sc_academy!r}")
            # Cross-check SC name against ibjjf name
            ibjjf_display = (
                (ibjjf_profile or {}).get("name")
                or (ibjjf_page or {}).get("name")
                or search_name
                or ""
            )
            if ibjjf_display:
                sim = _name_similarity(sc_name, ibjjf_display)
                notes.append(f"  SC↔IBJJF name similarity: {sim:.2f}")
                if sim >= 0.75 and ibjjf_id:
                    signals["sc_name_cross"] = ibjjf_id
                    notes.append(f"  ↳ SC name cross-confirms ibjjf_id={ibjjf_id}")
            if acad_hint and _academy_agrees(acad_hint, sc_academy):
                if ibjjf_id:
                    signals["sc_academy"] = ibjjf_id
                notes.append(f"  ↳ SC academy confirms hint {acad_hint!r}")
        else:
            notes.append(f"SC profile: no data for sc_uid={sc_uid}")

    # ── Signal 6: tournament_results name search ──────────────────────────────
    tr_matches: list[dict] = []
    if search_name:
        tr_matches = _tr_name_search(sb, search_name)
        for tr in tr_matches:
            if tr.get("ibjjf_athlete_id"):
                tid = str(tr["ibjjf_athlete_id"])
                notes.append(f"tournament_results: athlete_name match → ibjjf_id={tid} source={tr.get('source')}")
                if ibjjf_id and tid == str(ibjjf_id):
                    signals[f"tr_confirms:{tid}"] = tid
                    notes.append(f"  ↳ TR confirms report ibjjf_id")
                else:
                    signals[f"tr:{tid}"] = tid

    # ── Decision: tally signals per candidate_id ─────────────────────────────
    counts: dict[str, int] = {}
    for val in signals.values():
        counts[val] = counts.get(val, 0) + 1

    notes.append(f"signal_counts: {dict(sorted(counts.items(), key=lambda x: -x[1]))}")

    # We need at least 2 independent signals converging on ONE candidate
    strong = {k: v for k, v in counts.items() if v >= 2}
    if len(strong) > 1:
        notes.append(f"ambiguous: {len(strong)} candidates each with ≥2 signals: {list(strong.keys())}")
        _close(sb, rid, "unresolvable", "; ".join(notes))
        return "unresolvable", "; ".join(notes)

    if len(strong) == 0:
        notes.append(f"insufficient evidence: best={dict(sorted(counts.items(), key=lambda x: -x[1])[:3])}")
        _close(sb, rid, "unresolvable", "; ".join(notes))
        return "unresolvable", "; ".join(notes)

    confirmed_ibjjf_id = list(strong.keys())[0]
    n_signals = strong[confirmed_ibjjf_id]
    notes.append(f"HIGH CONFIDENCE: ibjjf_id={confirmed_ibjjf_id} ({n_signals} independent signals)")

    # ── Conflict check before any write ──────────────────────────────────────
    if sc_uid:
        existing_by_sc = _get_link_by_sc(sb, sc_uid)
        if existing_by_sc:
            ex_id = str(existing_by_sc.get("ibjjf_athlete_id", ""))
            if ex_id and ex_id != confirmed_ibjjf_id:
                notes.append(
                    f"CONFLICT: sc_uid={sc_uid} already verified → ibjjf={ex_id}; "
                    f"we want {confirmed_ibjjf_id} — keeping existing, not overwriting"
                )
                _close(sb, rid, "unresolvable", "; ".join(notes))
                return "unresolvable", "; ".join(notes)
            if ex_id == confirmed_ibjjf_id:
                notes.append(f"sc_ibjjf_verified already correct: sc={sc_uid} → ibjjf={confirmed_ibjjf_id}")
                _close(sb, rid, "resolved", "; ".join(notes))
                return "resolved", "; ".join(notes)

        existing_by_ibjjf = _get_link_by_ibjjf(sb, confirmed_ibjjf_id)
        if existing_by_ibjjf:
            ex_sc = str(existing_by_ibjjf.get("sc_uid", ""))
            if ex_sc and ex_sc != sc_uid:
                notes.append(
                    f"CONFLICT: ibjjf_id={confirmed_ibjjf_id} already verified → sc={ex_sc}; "
                    f"report has sc_uid={sc_uid} — keeping existing, not overwriting"
                )
                _close(sb, rid, "unresolvable", "; ".join(notes))
                return "unresolvable", "; ".join(notes)

    # ── Writes ────────────────────────────────────────────────────────────────
    ibjjf_row = _ensure_ibjjf_athletes_row(sb, confirmed_ibjjf_id)
    ibjjf_name = (
        (ibjjf_row or {}).get("name")
        or (ibjjf_profile or {}).get("name")
        or (ibjjf_page or {}).get("name")
        or search_name
        or ""
    )

    # Write sc_ibjjf_verified if we have both IDs
    if sc_uid:
        try:
            _upsert_link(sb, sc_uid, confirmed_ibjjf_id, ibjjf_name)
            notes.append(
                f"WRITTEN sc_ibjjf_verified: sc={sc_uid} ↔ ibjjf={confirmed_ibjjf_id} "
                f"({ibjjf_name!r}) [{n_signals} signals]"
            )
            log.info("  Linked sc=%s ↔ ibjjf=%s (%s)", sc_uid, confirmed_ibjjf_id, ibjjf_name)
        except Exception as e:
            notes.append(f"sc_ibjjf_verified write failed: {e}")
            log.error("  sc_ibjjf_verified write report=%s: %s", rid, e)
            _close(sb, rid, "error", "; ".join(notes))
            return "error", "; ".join(notes)

    # Backfill tournament_results.ibjjf_athlete_id
    if ibjjf_name:
        _backfill_tr_ibjjf_id(sb, confirmed_ibjjf_id, ibjjf_name)

    _close(sb, rid, "resolved", "; ".join(notes))
    return "resolved", "; ".join(notes)


# ── Main ──────────────────────────────────────────────────────────────────────

def _parse_step1_log(path: str = "/tmp/resolve.log") -> dict:
    """
    Extract step-1 summary totals from the tee'd log file.
    Returns dict with keys: seen, resolved, unresolvable, errored.
    """
    totals = {"seen": 0, "resolved": 0, "unresolvable": 0, "errored": 0}
    try:
        with open(path) as f:
            for line in f:
                m = re.search(
                    r"seen:\s*(\d+).*resolved:\s*(\d+).*unresolvable:\s*(\d+).*errored:\s*(\d+)",
                    line,
                )
                if m:
                    totals["seen"]        = int(m.group(1))
                    totals["resolved"]    = int(m.group(2))
                    totals["unresolvable"] = int(m.group(3))
                    totals["errored"]     = int(m.group(4))
    except FileNotFoundError:
        pass
    return totals


def main():
    log.info("=== llm_findme_resolve starting ===")

    # ── Step 1 summary from log ───────────────────────────────────────────────
    step1 = _parse_step1_log("/tmp/resolve.log")
    print("\n── Step 1 (deterministic pass) ──────────────────────────────────────")
    if step1["seen"] == 0 and all(v == 0 for v in step1.values()):
        print("  (no summary found in /tmp/resolve.log — step 1 may not have run or produced errors)")
    else:
        print(
            f"  seen: {step1['seen']} | resolved: {step1['resolved']} | "
            f"unresolvable: {step1['unresolvable']} | errored: {step1['errored']}"
        )

    # ── Connect to Supabase ───────────────────────────────────────────────────
    try:
        sb = _get_sb()
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)

    # ── Query unresolvable reports from last 25 hours ─────────────────────────
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    try:
        res = (sb.table("findme_reports")
                 .select("id,name,ibjjf_id,sc_uid,email,status,resolution_notes,updated_at")
                 .eq("status", "unresolvable")
                 .gte("updated_at", cutoff)
                 .order("updated_at", desc=False)
                 .limit(MAX_LLM_REPORTS)
                 .execute())
        reports = res.data or []
    except Exception as e:
        log.error("Failed to fetch unresolvable reports: %s", e)
        print(f"\n  ERROR fetching reports: {e}")
        _print_final_summary(step1, [], 0, 0, 0, 0)
        sys.exit(1)

    log.info("Fetched %d unresolvable report(s) updated since %s", len(reports), cutoff[:16])

    # ── Process each report ───────────────────────────────────────────────────
    print(f"\n── Step 2 (LLM intelligence pass) ───────────────────────────────────")
    print(f"  {len(reports)} unresolvable report(s) to retry (window: last {LOOKBACK_HOURS}h)\n")

    per_report: list[tuple[str, str, str]] = []  # (report_id, outcome, notes)
    resolved = unresolvable = errored = skipped = 0

    for report in reports:
        rid = report["id"]
        try:
            outcome, notes = process_unresolvable(sb, report)
        except Exception as e:
            log.error("Uncaught error on report %s: %s", rid, e)
            try:
                _close(sb, rid, "unresolvable", f"[LLM] agent error: {e}")
            except Exception:
                pass
            outcome, notes = "error", f"agent error: {e}"

        per_report.append((rid, outcome, notes))

        icon = {"resolved": "✓", "unresolvable": "✗", "error": "!", "skipped": "~"}.get(outcome, "?")
        print(f"  {icon} [{rid[:8]}] {outcome.upper()}")
        # Print key evidence lines (those NOT starting with '[LLM] prior_notes')
        for line in notes.split(";"):
            line = line.strip()
            if line and not line.startswith("[LLM] prior_notes"):
                print(f"      {line}")
        print()

        if outcome == "resolved":
            resolved += 1
        elif outcome == "unresolvable":
            unresolvable += 1
        elif outcome == "error":
            errored += 1
        else:
            skipped += 1

    _print_final_summary(step1, per_report, resolved, unresolvable, errored, skipped)


def _print_final_summary(step1, per_report, resolved, unresolvable, errored, skipped):
    print("── Final Summary ─────────────────────────────────────────────────────")
    print(f"  Step 1 (deterministic) : seen={step1['seen']} resolved={step1['resolved']} "
          f"unresolvable={step1['unresolvable']} errored={step1['errored']}")
    print(f"  Step 2 (LLM pass)      : resolved={resolved} unresolvable={unresolvable} "
          f"errored={errored} skipped={skipped}")
    total_resolved   = step1.get("resolved", 0) + resolved
    total_unresolv   = step1.get("unresolvable", 0) + unresolvable
    total_err        = step1.get("errored", 0) + errored
    total_seen       = step1.get("seen", 0) + resolved + unresolvable + errored + skipped
    print(f"  Combined totals        : resolved={total_resolved} unresolvable={total_unresolv} "
          f"errored={total_err} total_processed={total_seen}")
    print("─────────────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
