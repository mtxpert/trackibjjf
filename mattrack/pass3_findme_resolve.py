"""
pass3_findme_resolve.py — Claude reasoning pass for findme_reports still pending/unresolvable.

Queries findme_reports where status IN ('pending','unresolvable')
AND updated_at > now() - 25 hours, then applies multi-source web search
requiring ≥2 independent sources before writing anything.

Reads /tmp/p1.log and /tmp/p2.log for earlier pass summaries.
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
)

from llm_findme_resolve import (
    _scrape_smoothcomp_profile,
    _scrape_bjjcompsystem_athlete,
    _name_similarity,
    _ibjjf_middle_name_search,
    _academy_hint,
    _academy_agrees,
    _backfill_tr_ibjjf_id,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pass3_findme")

MAX_REPORTS   = 30
LOOKBACK_HOURS = 25

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Extra web scrapers for Pass 3 ─────────────────────────────────────────────

def _scrape_ibjjf_athlete_search(name: str) -> list[dict]:
    """
    GET https://www.ibjjf.com/athletes/?q=<name>
    Returns list of {ibjjf_id, name} or empty list on failure.
    """
    try:
        r = requests.get(
            "https://www.ibjjf.com/athletes/",
            params={"q": name},
            headers=HTTP_HEADERS,
            timeout=20,
        )
        if r.status_code != 200:
            log.debug("IBJJF web search %r: HTTP %s", name, r.status_code)
            return []
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        seen: set[str] = set()
        # athlete profile links: /athletes/<id>/ or /athlete/<id>
        for a_tag in soup.find_all("a", href=re.compile(r"/athlete[s]?/(\d+)", re.I)):
            m = re.search(r"/athlete[s]?/(\d+)", a_tag["href"])
            if m:
                aid = m.group(1)
                if aid not in seen:
                    seen.add(aid)
                    label = a_tag.get_text(strip=True) or ""
                    results.append({"ibjjf_id": aid, "name": label})
        return results
    except Exception as e:
        log.debug("IBJJF web search %r: %s", name, e)
        return []


def _scrape_smoothcomp_global_search(name: str) -> list[dict]:
    """
    GET https://smoothcomp.com/en/search/global?q=<name>
    Returns list of {sc_uid, name} or empty list on failure.
    """
    try:
        r = requests.get(
            "https://smoothcomp.com/en/search/global",
            params={"q": name},
            headers=HTTP_HEADERS,
            timeout=20,
        )
        if r.status_code != 200:
            log.debug("SC global search %r: HTTP %s", name, r.status_code)
            return []
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        seen: set[str] = set()
        # profile links: /en/profile/<id>
        for a_tag in soup.find_all("a", href=re.compile(r"/en/profile/(\d+)", re.I)):
            m = re.search(r"/en/profile/(\d+)", a_tag["href"])
            if m:
                uid = m.group(1)
                if uid not in seen:
                    seen.add(uid)
                    label = a_tag.get_text(strip=True) or ""
                    results.append({"sc_uid": uid, "name": label})
        return results
    except Exception as e:
        log.debug("SC global search %r: %s", name, e)
        return []


def _scrape_bjjmetrics_search(name: str) -> list[dict]:
    """
    GET https://bjjmetrics.com/?s=<name>
    Returns list of {ibjjf_id, name} or empty list.
    """
    try:
        r = requests.get(
            "https://bjjmetrics.com/",
            params={"s": name},
            headers=HTTP_HEADERS,
            timeout=20,
        )
        if r.status_code != 200:
            log.debug("bjjmetrics.com search %r: HTTP %s", name, r.status_code)
            return []
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        seen: set[str] = set()
        for a_tag in soup.find_all("a", href=re.compile(r"/athlete[s]?/(\d+)", re.I)):
            m = re.search(r"/athlete[s]?/(\d+)", a_tag["href"])
            if m:
                aid = m.group(1)
                if aid not in seen:
                    seen.add(aid)
                    label = a_tag.get_text(strip=True) or ""
                    results.append({"ibjjf_id": aid, "name": label})
        # Also use the existing bjjmetrics helper (different URL) as fallback
        if not results:
            results = _bjjmetrics_search(name)
        return results
    except Exception as e:
        log.debug("bjjmetrics.com search %r: %s", name, e)
        return _bjjmetrics_search(name)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _close_pass3(sb, report_id, status: str, notes: str):
    """Update findme_reports with pass3 status, notes, and resolved_at if resolved."""
    payload: dict = {
        "status": status,
        "resolution_notes": notes[:1000],
    }
    if status == "resolved":
        payload["resolved_at"] = datetime.now(timezone.utc).isoformat()
    try:
        sb.table("findme_reports").update(payload).eq("id", report_id).execute()
    except Exception as e:
        log.error("Close report %s failed: %s", report_id, e)


def _backfill_tr_sc_uid(sb, sc_uid: str, athlete_name: str):
    """Set tournament_results.athlete_id on smoothcomp rows where it's null + name matches."""
    name_lower = athlete_name.strip().lower()
    if not name_lower or not sc_uid:
        return
    try:
        res = (sb.table("tournament_results")
                 .select("id")
                 .eq("source", "smoothcomp")
                 .is_("athlete_id", "null")
                 .ilike("athlete_name", f"%{name_lower}%")
                 .limit(200)
                 .execute())
        rows = res.data or []
        if not rows:
            return
        ids = [r["id"] for r in rows]
        (sb.table("tournament_results")
           .update({"athlete_id": sc_uid})
           .in_("id", ids)
           .execute())
        log.info("  Backfilled %d SC tournament_results rows → sc_uid=%s", len(ids), sc_uid)
    except Exception as e:
        log.warning("tournament_results SC backfill sc_uid=%s: %s", sc_uid, e)


# ── Per-report pass 3 logic ───────────────────────────────────────────────────

def process_report_pass3(sb, report: dict) -> tuple[str, str]:
    """
    Full multi-source resolution attempt.
    Returns (outcome, notes_string) where outcome in {'resolved','unresolvable','error'}.
    Writes only when ≥2 independent signals agree on the same ibjjf_id.
    """
    rid      = report["id"]
    ibjjf_id = str(report.get("ibjjf_id") or "").strip() or None
    sc_uid   = str(report.get("sc_uid")   or "").strip() or None
    name     = (report.get("name")        or "").strip() or None
    email    = (report.get("email")       or "").strip() or None
    notes_in = report.get("notes") or report.get("resolution_notes") or ""

    log.info("Pass3 report %s: ibjjf_id=%s sc_uid=%s name=%r", rid, ibjjf_id, sc_uid, name)

    ev: list[str] = []  # evidence log

    # Derive search name: use report name, or fall back to IBJJF API fetch
    search_name = name

    acad_hint = _academy_hint(email or "", name or "")
    if acad_hint:
        ev.append(f"academy_hint={acad_hint!r}")

    # signals[label] = ibjjf_id — each independent source earns one entry
    signals: dict[str, str] = {}

    # ── Source A: IBJJF API profile ───────────────────────────────────────────
    ibjjf_profile: dict = {}
    if ibjjf_id:
        ibjjf_profile = _fetch_ibjjf_profile_from_api(ibjjf_id) or {}
        if ibjjf_profile.get("name"):
            signals["ibjjf_api"] = ibjjf_id
            ev.append(
                f"IBJJF_API name={ibjjf_profile['name']!r} "
                f"belt={ibjjf_profile.get('belt','')!r} "
                f"academy={ibjjf_profile.get('academy','')!r}"
            )
            if not search_name:
                search_name = ibjjf_profile["name"]
            if acad_hint and _academy_agrees(acad_hint, ibjjf_profile.get("academy")):
                signals["ibjjf_api_academy"] = ibjjf_id
                ev.append(f"  ↳ IBJJF_API academy confirms hint")
        else:
            ev.append(f"IBJJF_API: no profile for id={ibjjf_id}")

    # ── Source B: ibjjf.com web search ────────────────────────────────────────
    ibjjf_web_hits: list[dict] = []
    if search_name:
        ibjjf_web_hits = _scrape_ibjjf_athlete_search(search_name)
        ev.append(f"ibjjf.com search {search_name!r}: {len(ibjjf_web_hits)} hit(s)")
        for h in ibjjf_web_hits:
            bid = str(h["ibjjf_id"])
            sim = _name_similarity(search_name, h["name"]) if h["name"] else 0.0
            ev.append(f"  ibjjf.com: id={bid} name={h['name']!r} sim={sim:.2f}")
            if ibjjf_id and bid == str(ibjjf_id):
                signals["ibjjf_web_match"] = bid
                ev.append(f"    ↳ confirms report ibjjf_id")
            elif sim >= 0.80:
                signals[f"ibjjf_web:{bid}"] = bid

    # ── Source C: bjjcompsystem page ──────────────────────────────────────────
    ibjjf_page: dict = {}
    if ibjjf_id:
        ibjjf_page = _scrape_bjjcompsystem_athlete(ibjjf_id) or {}
        if ibjjf_page.get("name"):
            signals["bjjcompsystem"] = ibjjf_id
            ev.append(
                f"bjjcompsystem: name={ibjjf_page['name']!r} "
                f"academy={ibjjf_page.get('academy','')!r}"
            )
            if not search_name:
                search_name = ibjjf_page["name"]
            if acad_hint and _academy_agrees(acad_hint, ibjjf_page.get("academy")):
                signals["bjjcompsystem_academy"] = ibjjf_id
                ev.append(f"  ↳ bjjcompsystem academy confirms hint")
        else:
            ev.append(f"bjjcompsystem: no page for id={ibjjf_id}")

    # ── Source D: ibjjf_athletes DB ───────────────────────────────────────────
    if search_name:
        db_hits = _ibjjf_name_search(sb, search_name)
        if not db_hits or len(search_name.split()) > 2:
            db_hits += _ibjjf_middle_name_search(sb, search_name)
        seen_db: set[str] = set()
        for m in db_hits:
            mid = str(m["ibjjf_id"])
            if mid in seen_db:
                continue
            seen_db.add(mid)
            sim = _name_similarity(search_name, m["name"])
            ev.append(f"ibjjf_athletes DB: id={mid} name={m['name']!r} sim={sim:.2f}")
            if sim >= 0.75:
                signals[f"ibjjf_db:{mid}"] = mid
                if acad_hint and _academy_agrees(acad_hint, m.get("academy")):
                    signals[f"ibjjf_db_academy:{mid}"] = mid
                    ev.append(f"  ↳ DB academy confirms hint")

    # ── Source E: bjjmetrics.com search ───────────────────────────────────────
    if search_name:
        bm_hits = _scrape_bjjmetrics_search(search_name)
        ev.append(f"bjjmetrics.com search {search_name!r}: {len(bm_hits)} hit(s)")
        for bm in bm_hits:
            bid = str(bm["ibjjf_id"])
            sim = _name_similarity(search_name, bm["name"]) if bm["name"] else 0.0
            ev.append(f"  bjjmetrics: id={bid} name={bm['name']!r} sim={sim:.2f}")
            if ibjjf_id and bid == str(ibjjf_id):
                signals["bjjmetrics_confirms"] = bid
                ev.append(f"    ↳ confirms report ibjjf_id")
            elif sim >= 0.80:
                signals[f"bjjmetrics:{bid}"] = bid

    # ── Source F: Smoothcomp global search ────────────────────────────────────
    sc_search_hits: list[dict] = []
    sc_confirmed_uid: str | None = sc_uid  # start with reported sc_uid if given
    if search_name:
        sc_search_hits = _scrape_smoothcomp_global_search(search_name)
        ev.append(f"smoothcomp search {search_name!r}: {len(sc_search_hits)} hit(s)")
        for sh in sc_search_hits:
            uid = str(sh["sc_uid"])
            sim = _name_similarity(search_name, sh["name"]) if sh["name"] else 0.0
            ev.append(f"  SC search: sc_uid={uid} name={sh['name']!r} sim={sim:.2f}")
            if sc_uid and uid == sc_uid:
                ev.append(f"    ↳ SC search confirms reported sc_uid")
            elif sim >= 0.90 and not sc_confirmed_uid:
                sc_confirmed_uid = uid
                ev.append(f"    ↳ high-confidence SC match → sc_uid={uid}")

    # ── Source G: Smoothcomp profile (if sc_uid known) ───────────────────────
    if sc_uid:
        sc_prof = _scrape_smoothcomp_profile(sc_uid) or {}
        sc_pname = sc_prof.get("name")
        sc_pacad = sc_prof.get("academy")
        if sc_pname:
            ev.append(f"SC profile: name={sc_pname!r} academy={sc_pacad!r}")
            ibjjf_display = (
                ibjjf_profile.get("name") or ibjjf_page.get("name") or search_name or ""
            )
            if ibjjf_display:
                sim = _name_similarity(sc_pname, ibjjf_display)
                ev.append(f"  SC↔IBJJF name sim={sim:.2f}")
                if sim >= 0.75 and ibjjf_id:
                    signals["sc_name_cross"] = ibjjf_id
                    ev.append(f"  ↳ SC name cross-confirms ibjjf_id={ibjjf_id}")
            if acad_hint and _academy_agrees(acad_hint, sc_pacad):
                if ibjjf_id:
                    signals["sc_academy"] = ibjjf_id
                ev.append(f"  ↳ SC academy confirms hint {acad_hint!r}")
        else:
            ev.append(f"SC profile: no data for sc_uid={sc_uid}")

    # ── Source H: tournament_results name search ──────────────────────────────
    if search_name:
        tr_hits = _tr_name_search(sb, search_name)
        for tr in tr_hits:
            if tr.get("ibjjf_athlete_id"):
                tid = str(tr["ibjjf_athlete_id"])
                ev.append(f"tournament_results: name match → ibjjf_id={tid} source={tr.get('source')}")
                if ibjjf_id and tid == str(ibjjf_id):
                    signals[f"tr_confirms:{tid}"] = tid
                    ev.append(f"  ↳ TR confirms report ibjjf_id")
                else:
                    signals[f"tr:{tid}"] = tid

    # ── Decision: tally signals per candidate_id ──────────────────────────────
    counts: dict[str, int] = {}
    for val in signals.values():
        counts[val] = counts.get(val, 0) + 1

    ev.append(f"signal_counts: {dict(sorted(counts.items(), key=lambda x: -x[1]))}")

    strong = {k: v for k, v in counts.items() if v >= 2}

    if len(strong) > 1:
        reason = f"ambiguous: {len(strong)} candidates each with ≥2 signals: {list(strong.keys())}"
        ev.append(reason)
        _close_pass3(sb, rid, "unresolvable", "auto-unresolvable pass3: " + "; ".join(ev))
        return "unresolvable", "; ".join(ev)

    if len(strong) == 0:
        reason = f"insufficient evidence: best={dict(sorted(counts.items(), key=lambda x: -x[1])[:3])}"
        ev.append(reason)
        _close_pass3(sb, rid, "unresolvable", "auto-unresolvable pass3: " + "; ".join(ev))
        return "unresolvable", "; ".join(ev)

    confirmed_ibjjf_id = list(strong.keys())[0]
    n_signals = strong[confirmed_ibjjf_id]
    ev.append(f"HIGH_CONFIDENCE: ibjjf_id={confirmed_ibjjf_id} ({n_signals} signals)")

    # ── Conflict check ────────────────────────────────────────────────────────
    if sc_uid:
        existing_by_sc = _get_link_by_sc(sb, sc_uid)
        if existing_by_sc:
            ex_id = str(existing_by_sc.get("ibjjf_athlete_id", ""))
            if ex_id and ex_id != confirmed_ibjjf_id:
                reason = (
                    f"conflict: ibjjf_id already linked to different sc_uid — "
                    f"sc_uid={sc_uid} already verified to ibjjf={ex_id}; "
                    f"report wants {confirmed_ibjjf_id}"
                )
                ev.append(reason)
                _close_pass3(sb, rid, "unresolvable", "auto-unresolvable pass3: " + "; ".join(ev))
                return "unresolvable", "; ".join(ev)
            if ex_id == confirmed_ibjjf_id:
                ev.append(f"sc_ibjjf_verified already correct: sc={sc_uid} → ibjjf={confirmed_ibjjf_id}")
                _close_pass3(sb, rid, "resolved", "auto-resolved pass3: " + "; ".join(ev))
                return "resolved", "; ".join(ev)

        existing_by_ibjjf = _get_link_by_ibjjf(sb, confirmed_ibjjf_id)
        if existing_by_ibjjf:
            ex_sc = str(existing_by_ibjjf.get("sc_uid", ""))
            if ex_sc and ex_sc != sc_uid:
                reason = (
                    f"conflict: ibjjf_id already linked to different sc_uid — "
                    f"ibjjf_id={confirmed_ibjjf_id} already verified to sc={ex_sc}; "
                    f"report has sc_uid={sc_uid}"
                )
                ev.append(reason)
                _close_pass3(sb, rid, "unresolvable", "auto-unresolvable pass3: " + "; ".join(ev))
                return "unresolvable", "; ".join(ev)

    # ── Writes ────────────────────────────────────────────────────────────────
    ibjjf_row = _ensure_ibjjf_athletes_row(sb, confirmed_ibjjf_id)
    ibjjf_name = (
        (ibjjf_row or {}).get("name")
        or ibjjf_profile.get("name")
        or ibjjf_page.get("name")
        or search_name
        or ""
    )

    # 1. Upsert sc_ibjjf_verified if we have both IDs
    if sc_uid:
        try:
            _upsert_link(sb, sc_uid, confirmed_ibjjf_id, ibjjf_name)
            ev.append(
                f"WRITTEN sc_ibjjf_verified: sc={sc_uid} ↔ ibjjf={confirmed_ibjjf_id} "
                f"({ibjjf_name!r}) [{n_signals} signals]"
            )
            log.info("  Linked sc=%s ↔ ibjjf=%s (%s)", sc_uid, confirmed_ibjjf_id, ibjjf_name)
        except Exception as e:
            ev.append(f"sc_ibjjf_verified write failed: {e}")
            log.error("  sc_ibjjf_verified write report=%s: %s", rid, e)
            _close_pass3(sb, rid, "unresolvable", "pass3 error: " + "; ".join(ev))
            return "error", "; ".join(ev)

    # 2. Backfill tournament_results
    if ibjjf_name:
        _backfill_tr_ibjjf_id(sb, confirmed_ibjjf_id, ibjjf_name)
    if sc_uid and ibjjf_name:
        _backfill_tr_sc_uid(sb, sc_uid, ibjjf_name)

    # 3. Resolve the report
    evidence_summary = "; ".join(ev)
    _close_pass3(
        sb, rid, "resolved",
        f"auto-resolved pass3: {evidence_summary}"
    )
    return "resolved", evidence_summary


# ── Log parsing helpers ────────────────────────────────────────────────────────

def _parse_pass_log(path: str) -> dict:
    """Extract resolved/unresolvable/errored counts from a tee'd log file."""
    totals = {"seen": 0, "resolved": 0, "unresolvable": 0, "errored": 0}
    try:
        with open(path) as f:
            for line in f:
                # nightly_findme_resolve summary line
                m = re.search(
                    r"seen:\s*(\d+).*?resolved:\s*(\d+).*?unresolvable:\s*(\d+).*?errored:\s*(\d+)",
                    line,
                )
                if m:
                    totals["seen"]         = int(m.group(1))
                    totals["resolved"]     = int(m.group(2))
                    totals["unresolvable"] = int(m.group(3))
                    totals["errored"]      = int(m.group(4))
                    continue
                # llm_findme_resolve Step 2 line
                m2 = re.search(
                    r"Step 2.*?resolved=(\d+)\s+unresolvable=(\d+)\s+errored=(\d+)",
                    line,
                )
                if m2:
                    totals["resolved"]     = int(m2.group(1))
                    totals["unresolvable"] = int(m2.group(2))
                    totals["errored"]      = int(m2.group(3))
    except FileNotFoundError:
        pass
    return totals


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== pass3_findme_resolve starting ===")

    pass1 = _parse_pass_log("/tmp/p1.log")
    pass2 = _parse_pass_log("/tmp/p2.log")

    try:
        sb = _get_sb()
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    try:
        res = (sb.table("findme_reports")
                 .select("id,name,ibjjf_id,sc_uid,email,status,resolution_notes,notes,updated_at")
                 .in_("status", ["pending", "unresolvable"])
                 .gte("updated_at", cutoff)
                 .order("updated_at", desc=False)
                 .limit(MAX_REPORTS)
                 .execute())
        reports = res.data or []
    except Exception as e:
        # 'notes' column may not exist — retry without it
        log.warning("Fetch with 'notes' failed (%s), retrying without", e)
        try:
            res = (sb.table("findme_reports")
                     .select("id,name,ibjjf_id,sc_uid,email,status,resolution_notes,updated_at")
                     .in_("status", ["pending", "unresolvable"])
                     .gte("updated_at", cutoff)
                     .order("updated_at", desc=False)
                     .limit(MAX_REPORTS)
                     .execute())
            reports = res.data or []
        except Exception as e2:
            log.error("Failed to fetch reports: %s", e2)
            reports = []

    log.info("Pass3: %d report(s) to process (pending+unresolvable, last %dh)", len(reports), LOOKBACK_HOURS)

    p3_resolved = p3_unresolvable = p3_error = 0
    still_unresolvable: list[tuple] = []
    errors: list[str] = []

    for report in reports:
        rid    = report["id"]
        rname  = report.get("name") or "(no name)"
        try:
            outcome, notes = process_report_pass3(sb, report)
        except Exception as e:
            log.error("Uncaught error on report %s: %s", rid, e)
            errors.append(f"report {rid}: {e}")
            try:
                _close_pass3(sb, rid, "unresolvable", f"pass3 agent error: {e}")
            except Exception:
                pass
            outcome, notes = "error", str(e)

        icon = {"resolved": "✓", "unresolvable": "✗", "error": "!"}.get(outcome, "?")
        print(f"  {icon} [{str(rid)[:8]}] {outcome.upper()} — {rname!r}")
        for line in notes.split(";"):
            line = line.strip()
            if line:
                print(f"      {line}")
        print()

        if outcome == "resolved":
            p3_resolved += 1
        elif outcome == "error":
            p3_error += 1
            still_unresolvable.append((rid, rname, notes[:120]))
        else:
            p3_unresolvable += 1
            still_unresolvable.append((rid, rname, notes[:120]))

    # ── Final summary block ───────────────────────────────────────────────────
    reports_processed = pass1.get("seen", 0) + pass2.get("seen", 0) + len(reports)
    pass1_resolved    = pass1.get("resolved", 0)
    pass2_resolved    = pass2.get("resolved", 0)
    pass3_resolved    = p3_resolved

    print()
    print("=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"  reports_processed  : {reports_processed}")
    print(f"  pass1_resolved     : {pass1_resolved}")
    print(f"  pass2_resolved     : {pass2_resolved}")
    print(f"  pass3_resolved     : {pass3_resolved}")
    print(f"  still_unresolvable : {len(still_unresolvable)}")
    if still_unresolvable:
        for (rep_id, aname, reason) in still_unresolvable:
            short_reason = reason.split(";")[-1].strip() if reason else "—"
            print(f"    - {rep_id} | {aname!r} | {short_reason}")
    if errors:
        print(f"  errors             : {len(errors)}")
        for err in errors:
            print(f"    - {err}")
    else:
        print(f"  errors             : 0")
    print("=" * 70)


if __name__ == "__main__":
    main()
