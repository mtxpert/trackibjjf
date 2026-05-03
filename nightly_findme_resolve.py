"""
nightly_findme_resolve.py — Nightly automated resolver for findme_reports.

Steps:
  1. Deterministic pass: resolve reports with both IDs, single-name matches,
     IBJJF API backfill, bjjmetrics cross-ref.
  2. Intelligence pass: dig deeper into 'unresolvable' rows via scraping,
     middle-name variants, academy/team hints, cross-source agreement.

Write rules (HIGH CONFIDENCE ONLY — two independent signals required):
  - ibjjf_athletes: upsert on ibjjf_id
  - sc_ibjjf_verified: insert only if no row for sc_uid AND no row for ibjjf_id
  - tournament_results.ibjjf_athlete_id: PATCH where source='ibjjf' and name matches

Bail early with non-zero exit if Supabase is unreachable.
"""

from __future__ import annotations

import os
import sys
import time
import re
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

import requests
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("findme")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

IBJJF_API_BASE = "https://api.ibjjfdb.com"
IBJJF_COMP_BASE = "https://www.bjjcompsystem.com"
SC_BASE = "https://smoothcomp.com"
BJJMETRICS_BASE = "https://www.bjjmetrics.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

INTELLIGENCE_CAP = 25

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------
_sb: Client | None = None


def _get_sb() -> Client:
    global _sb
    if _sb is not None:
        return _sb
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL / SUPABASE_SERVICE_KEY not set", flush=True)
        sys.exit(1)
    try:
        _sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        # Connectivity probe
        _sb.table("findme_reports").select("id").limit(1).execute()
    except Exception as e:
        print(f"ERROR: Supabase unreachable — {e}", flush=True)
        sys.exit(1)
    return _sb


def _sql(query: str) -> list[dict]:
    """Run raw SQL via Supabase management API (fallback when PostgREST is awkward)."""
    mgmt_pat = os.environ.get("SUPABASE_MGMT_PAT", "")
    project_ref = re.search(r"https://([^.]+)\.supabase\.co", SUPABASE_URL)
    if not mgmt_pat or not project_ref:
        raise RuntimeError("SUPABASE_MGMT_PAT or project ref not available")
    resp = requests.post(
        f"https://api.supabase.com/v1/projects/{project_ref.group(1)}/database/query",
        headers={
            "Authorization": f"Bearer {mgmt_pat}",
            "Content-Type": "application/json",
        },
        json={"query": query},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Rate-limited HTTP helpers
# ---------------------------------------------------------------------------
_last_req: dict[str, float] = {}


def _get(url: str, min_gap: float = 0.6, **kwargs) -> requests.Response:
    host = re.sub(r"https?://([^/]+).*", r"\1", url)
    since = time.time() - _last_req.get(host, 0)
    if since < min_gap:
        time.sleep(min_gap - since)
    r = requests.get(url, headers=HEADERS, timeout=(5, 20), **kwargs)
    _last_req[host] = time.time()
    return r


def _ibjjf_photo_valid(ibjjf_id: str) -> Optional[bool]:
    """Return True if the IBJJF athlete photo endpoint returns 200 (real ID)."""
    try:
        r = requests.head(
            f"{IBJJF_API_BASE}/Athletes/{ibjjf_id}/Photo",
            headers=HEADERS,
            timeout=(4, 10),
            allow_redirects=True,
        )
        _last_req[IBJJF_API_BASE] = time.time()
        time.sleep(0.5)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# IBJJF public API
# ---------------------------------------------------------------------------
def _fetch_ibjjf_profile_from_api(ibjjf_id: str) -> Optional[dict]:
    """Fetch athlete profile from IBJJF API. Returns dict or None."""
    try:
        r = _get(
            f"{IBJJF_API_BASE}/Athletes/{ibjjf_id}",
            min_gap=0.5,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        return data
    except Exception as e:
        log.debug("IBJJF API fetch %s: %s", ibjjf_id, e)
        return None


def _ibjjf_name_search(name: str) -> list[dict]:
    """Search ibjjf_athletes table by fuzzy name match."""
    sb = _get_sb()
    parts = name.lower().split()
    if not parts:
        return []
    try:
        # Try exact name_lower first
        r = sb.table("ibjjf_athletes").select("*").ilike(
            "name_lower", f"%{name.lower()}%"
        ).limit(10).execute()
        return r.data or []
    except Exception as e:
        log.debug("ibjjf_name_search error: %s", e)
        return []


def _tr_name_search(name: str) -> list[dict]:
    """Search tournament_results by athlete_name (IBJJF rows only)."""
    sb = _get_sb()
    try:
        r = (
            sb.table("tournament_results")
            .select("ibjjf_athlete_id, athlete_name, source")
            .eq("source", "ibjjf")
            .ilike("athlete_name", f"%{name}%")
            .limit(20)
            .execute()
        )
        return r.data or []
    except Exception as e:
        log.debug("tr_name_search error: %s", e)
        return []


def _bjjmetrics_search(name: str) -> list[dict]:
    """Search bjjmetrics for an athlete name. Returns list of candidate dicts."""
    try:
        r = _get(
            f"{BJJMETRICS_BASE}/athletes",
            params={"q": name},
            min_gap=0.7,
        )
        if r.status_code != 200:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        for row in soup.select("table tbody tr")[:10]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                link = row.find("a", href=True)
                href = link["href"] if link else ""
                ibjjf_id_m = re.search(r"/athletes/(\d+)", href)
                results.append(
                    {
                        "name": cols[0].get_text(strip=True),
                        "academy": cols[1].get_text(strip=True) if len(cols) > 1 else "",
                        "ibjjf_id": ibjjf_id_m.group(1) if ibjjf_id_m else None,
                        "href": href,
                    }
                )
        return results
    except Exception as e:
        log.debug("bjjmetrics_search error: %s", e)
        return []


def _sc_profile_scrape(sc_uid: str) -> Optional[dict]:
    """Scrape Smoothcomp public profile for name / academy hints."""
    try:
        r = _get(f"{SC_BASE}/en/profile/{sc_uid}", min_gap=0.7)
        if r.status_code != 200:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        name_el = soup.select_one("h1.profile-name, .profile__name, h1")
        academy_el = soup.select_one(".profile-academy, .profile__academy, .academy-name")
        return {
            "name": name_el.get_text(strip=True) if name_el else None,
            "academy": academy_el.get_text(strip=True) if academy_el else None,
        }
    except Exception as e:
        log.debug("sc_profile_scrape %s: %s", sc_uid, e)
        return None


def _ibjjf_athlete_page(ibjjf_id: str) -> Optional[dict]:
    """Scrape bjjcompsystem athlete page for name/academy."""
    try:
        r = _get(f"{IBJJF_COMP_BASE}/athletes/{ibjjf_id}", min_gap=0.7)
        if r.status_code != 200:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        name_el = soup.select_one("h1, .athlete-name, .athlete__name")
        academy_el = soup.select_one(".academy-name, .athlete-academy, .team-name")
        belt_el = soup.select_one(".belt, .athlete-belt")
        return {
            "name": name_el.get_text(strip=True) if name_el else None,
            "academy": academy_el.get_text(strip=True) if academy_el else None,
            "belt": belt_el.get_text(strip=True).lower() if belt_el else None,
        }
    except Exception as e:
        log.debug("ibjjf_athlete_page %s: %s", ibjjf_id, e)
        return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _ensure_ibjjf_athletes_row(
    ibjjf_id: str,
    name: str,
    belt: Optional[str] = None,
    academy: Optional[str] = None,
) -> None:
    """Upsert a row in ibjjf_athletes."""
    sb = _get_sb()
    row: dict = {
        "ibjjf_id": ibjjf_id,
        "name": name,
        "name_lower": name.lower(),
    }
    if belt:
        row["belt"] = belt.lower()
    if academy:
        row["academy"] = academy
    try:
        sb.table("ibjjf_athletes").upsert(row, on_conflict="ibjjf_id").execute()
        log.info("ibjjf_athletes upserted: %s (%s)", name, ibjjf_id)
    except Exception as e:
        log.warning("ibjjf_athletes upsert failed for %s: %s", ibjjf_id, e)


def _safe_link_sc_ibjjf(
    sc_uid: str,
    ibjjf_id: str,
    report_id: str,
) -> tuple[bool, str]:
    """
    Insert into sc_ibjjf_verified only if no conflicting row exists.
    Returns (success, message).
    """
    sb = _get_sb()
    try:
        # Check existing rows
        existing_sc = (
            sb.table("sc_ibjjf_verified")
            .select("sc_uid, ibjjf_athlete_id")
            .eq("sc_uid", sc_uid)
            .execute()
        )
        existing_ibjjf = (
            sb.table("sc_ibjjf_verified")
            .select("sc_uid, ibjjf_athlete_id")
            .eq("ibjjf_athlete_id", ibjjf_id)
            .execute()
        )
        if existing_sc.data:
            row = existing_sc.data[0]
            if row["ibjjf_athlete_id"] == ibjjf_id:
                return True, "already_linked"
            return (
                False,
                f"CONFLICT: sc_uid {sc_uid} already linked to ibjjf_id "
                f"{row['ibjjf_athlete_id']} (attempted {ibjjf_id})",
            )
        if existing_ibjjf.data:
            row = existing_ibjjf.data[0]
            if row["sc_uid"] == sc_uid:
                return True, "already_linked"
            return (
                False,
                f"CONFLICT: ibjjf_id {ibjjf_id} already linked to sc_uid "
                f"{row['sc_uid']} (attempted {sc_uid})",
            )
        # Safe to insert
        sb.table("sc_ibjjf_verified").insert(
            {"sc_uid": sc_uid, "ibjjf_athlete_id": ibjjf_id}
        ).execute()
        log.info("sc_ibjjf_verified linked: sc=%s ibjjf=%s", sc_uid, ibjjf_id)
        return True, "inserted"
    except Exception as e:
        return False, f"DB error: {e}"


def _patch_tournament_results(ibjjf_id: str, athlete_name: str) -> int:
    """
    Backfill ibjjf_athlete_id on tournament_results rows where source='ibjjf'
    and athlete_name matches, but ibjjf_athlete_id is NULL.
    Returns count of rows updated.
    """
    sb = _get_sb()
    try:
        r = (
            sb.table("tournament_results")
            .update({"ibjjf_athlete_id": ibjjf_id})
            .eq("source", "ibjjf")
            .ilike("athlete_name", athlete_name)
            .is_("ibjjf_athlete_id", "null")
            .execute()
        )
        count = len(r.data) if r.data else 0
        if count:
            log.info(
                "tournament_results: patched %d rows for %s (%s)",
                count, athlete_name, ibjjf_id,
            )
        return count
    except Exception as e:
        log.warning("tournament_results patch failed for %s: %s", ibjjf_id, e)
        return 0


def _close_report(
    report_id: str,
    status: str,
    notes: str,
) -> None:
    """Update findme_reports status and resolution_notes."""
    sb = _get_sb()
    try:
        sb.table("findme_reports").update(
            {
                "status": status,
                "resolution_notes": notes[:1000],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", report_id).execute()
    except Exception as e:
        log.warning("close_report failed for %s: %s", report_id, e)


# ---------------------------------------------------------------------------
# Deterministic pass helpers
# ---------------------------------------------------------------------------
def _resolve_both_ids(report: dict) -> tuple[bool, str]:
    """When both sc_uid and ibjjf_id are present, just link them."""
    sc_uid = report.get("sc_uid")
    ibjjf_id = report.get("ibjjf_id")
    name = report.get("name", "")

    # Validate IBJJF ID
    valid = _ibjjf_photo_valid(str(ibjjf_id))
    if valid is False:
        return False, f"IBJJF ID {ibjjf_id} returned 404 — bogus ID"
    if valid is None:
        log.debug("IBJJF photo check inconclusive for %s", ibjjf_id)

    # Try to backfill profile data
    profile = _fetch_ibjjf_profile_from_api(str(ibjjf_id))
    if profile:
        canon_name = profile.get("name") or profile.get("fullName") or name
        belt = profile.get("belt") or profile.get("beltName")
        academy = profile.get("academy") or profile.get("teamName")
        _ensure_ibjjf_athletes_row(str(ibjjf_id), canon_name, belt, academy)
    else:
        _ensure_ibjjf_athletes_row(str(ibjjf_id), name)

    ok, msg = _safe_link_sc_ibjjf(str(sc_uid), str(ibjjf_id), report["id"])
    if not ok:
        return False, msg

    if msg != "already_linked":
        _patch_tournament_results(str(ibjjf_id), name)

    sources = "both IDs provided by user"
    if profile:
        sources += "; IBJJF API profile confirmed"
    return True, f"Linked sc={sc_uid} ↔ ibjjf={ibjjf_id}. Sources: {sources}."


def _resolve_ibjjf_only(report: dict) -> tuple[bool, str]:
    """When only ibjjf_id is provided, verify + backfill ibjjf_athletes."""
    ibjjf_id = str(report.get("ibjjf_id", ""))
    name = report.get("name", "")

    valid = _ibjjf_photo_valid(ibjjf_id)
    if valid is False:
        return False, f"IBJJF ID {ibjjf_id} returned 404 — bogus ID"

    profile = _fetch_ibjjf_profile_from_api(ibjjf_id)
    if profile:
        canon_name = profile.get("name") or profile.get("fullName") or name
        belt = profile.get("belt") or profile.get("beltName")
        academy = profile.get("academy") or profile.get("teamName")
        _ensure_ibjjf_athletes_row(ibjjf_id, canon_name, belt, academy)
        _patch_tournament_results(ibjjf_id, canon_name)
        return True, (
            f"IBJJF-only report. Profile confirmed from API: {canon_name}. "
            f"ibjjf_athletes upserted; tournament_results backfilled."
        )
    else:
        if valid is True:
            _ensure_ibjjf_athletes_row(ibjjf_id, name)
            return True, (
                f"IBJJF ID {ibjjf_id} verified (photo endpoint 200). "
                f"Name from report used: {name}."
            )
        return False, f"IBJJF ID {ibjjf_id} could not be confirmed (API timeout/error)."


def _resolve_sc_only(report: dict) -> tuple[bool, str]:
    """When only sc_uid is provided, try to find a matching ibjjf_athletes row by name."""
    sc_uid = str(report.get("sc_uid", ""))
    name = report.get("name", "")

    candidates = _ibjjf_name_search(name)
    if len(candidates) == 1:
        ibjjf_id = candidates[0]["ibjjf_id"]
        ok, msg = _safe_link_sc_ibjjf(sc_uid, str(ibjjf_id), report["id"])
        if not ok:
            return False, msg
        _patch_tournament_results(str(ibjjf_id), name)
        return True, (
            f"SC-only report. Unique name match in ibjjf_athletes: "
            f"{candidates[0]['name']} (ibjjf_id={ibjjf_id}). Linked."
        )
    if len(candidates) > 1:
        cand_str = "; ".join(
            f"{c['name']} ({c['ibjjf_id']})" for c in candidates[:5]
        )
        return False, (
            f"[NEEDS REVIEW] SC-only report with {len(candidates)} name candidates: {cand_str}"
        )
    return False, f"No matching ibjjf_athletes row found for name '{name}'."


def _resolve_name_only(report: dict) -> tuple[bool, str]:
    """When only a name is provided, attempt unique match across sources."""
    name = report.get("name", "")
    if not name:
        return False, "No name provided in report."

    candidates = _ibjjf_name_search(name)
    tr_hits = _tr_name_search(name)
    bm_hits = _bjjmetrics_search(name)

    ibjjf_ids_seen: set[str] = set()
    for c in candidates:
        if c.get("ibjjf_id"):
            ibjjf_ids_seen.add(str(c["ibjjf_id"]))
    for t in tr_hits:
        if t.get("ibjjf_athlete_id"):
            ibjjf_ids_seen.add(str(t["ibjjf_athlete_id"]))
    for b in bm_hits:
        if b.get("ibjjf_id"):
            ibjjf_ids_seen.add(str(b["ibjjf_id"]))

    if len(ibjjf_ids_seen) == 1:
        ibjjf_id = ibjjf_ids_seen.pop()
        _patch_tournament_results(ibjjf_id, name)
        return True, (
            f"Name-only report. Single IBJJF ID {ibjjf_id} agreed upon by "
            f"ibjjf_athletes+tournament_results+bjjmetrics. tournament_results backfilled."
        )
    if len(ibjjf_ids_seen) > 1:
        return False, (
            f"[NEEDS REVIEW] Name-only report. Multiple IBJJF IDs: "
            + ", ".join(sorted(ibjjf_ids_seen))
        )
    return False, f"Name-only report: no IBJJF match found for '{name}'."


# ---------------------------------------------------------------------------
# Deterministic pass
# ---------------------------------------------------------------------------
def run_deterministic_pass() -> dict:
    sb = _get_sb()
    stats = {"seen": 0, "resolved": 0, "unresolvable": 0, "errored": 0}

    try:
        rows = (
            sb.table("findme_reports")
            .select("*")
            .eq("status", "pending")
            .execute()
        )
    except Exception as e:
        log.error("Failed to fetch pending findme_reports: %s", e)
        stats["errored"] += 1
        return stats

    reports = rows.data or []
    stats["seen"] = len(reports)
    log.info("Deterministic pass: %d pending reports", len(reports))

    for report in reports:
        rid = report.get("id", "?")
        sc_uid = report.get("sc_uid")
        ibjjf_id = report.get("ibjjf_id")
        name = report.get("name", "")

        try:
            if sc_uid and ibjjf_id:
                ok, notes = _resolve_both_ids(report)
            elif ibjjf_id:
                ok, notes = _resolve_ibjjf_only(report)
            elif sc_uid:
                ok, notes = _resolve_sc_only(report)
            else:
                ok, notes = _resolve_name_only(report)

            if ok:
                _close_report(rid, "resolved", notes)
                stats["resolved"] += 1
                log.info("RESOLVED %s: %s", rid, notes[:120])
            else:
                _close_report(rid, "unresolvable", notes)
                stats["unresolvable"] += 1
                log.info("UNRESOLVABLE %s: %s", rid, notes[:120])

        except Exception as e:
            log.error("Error processing report %s: %s", rid, e)
            _close_report(rid, "unresolvable", f"Processing error: {e}"[:1000])
            stats["errored"] += 1

    return stats


# ---------------------------------------------------------------------------
# Intelligence pass
# ---------------------------------------------------------------------------
def run_intelligence_pass() -> dict:
    sb = _get_sb()
    stats = {
        "reviewed": 0,
        "linked": 0,
        "left_unresolvable": 0,
        "needs_review": 0,
    }
    links_created: list[tuple] = []
    needs_review: list[tuple] = []
    conflicts: list[tuple] = []

    try:
        rows = (
            sb.table("findme_reports")
            .select("*")
            .eq("status", "unresolvable")
            .gte(
                "updated_at",
                (datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )).isoformat(),
            )
            .limit(INTELLIGENCE_CAP)
            .execute()
        )
    except Exception as e:
        log.error("Failed to fetch unresolvable reports: %s", e)
        return stats

    reports = rows.data or []
    stats["reviewed"] = len(reports)
    log.info("Intelligence pass: %d unresolvable reports to dig into", len(reports))

    for report in reports:
        rid = report.get("id", "?")
        sc_uid = report.get("sc_uid")
        ibjjf_id = report.get("ibjjf_id")
        name = report.get("name", "")
        email = report.get("email", "") or ""
        prev_notes = report.get("resolution_notes", "") or ""

        log.info("Intelligence: examining report %s (%s)", rid, name)

        try:
            resolved, notes, link_tuple, conflict_tuple = _intelligence_resolve(
                rid, sc_uid, ibjjf_id, name, email, prev_notes
            )
        except Exception as e:
            log.error("Intelligence error for %s: %s", rid, e)
            notes = f"Intelligence pass error: {e}"
            resolved = False
            link_tuple = None
            conflict_tuple = None

        if resolved:
            _close_report(rid, "resolved", notes)
            stats["linked"] += 1
            if link_tuple:
                links_created.append(link_tuple)
            log.info("INTELLIGENCE RESOLVED %s: %s", rid, notes[:120])
        else:
            if notes.startswith("[NEEDS REVIEW]"):
                stats["needs_review"] += 1
                needs_review.append((rid, name, notes))
            elif notes.startswith("CONFLICT:"):
                conflicts.append((rid, name, conflict_tuple, notes))
                stats["left_unresolvable"] += 1
            else:
                stats["left_unresolvable"] += 1
            _close_report(rid, "unresolvable", notes)
            log.info("INTELLIGENCE UNRESOLVABLE %s: %s", rid, notes[:120])

    return stats, links_created, needs_review, conflicts


def _intelligence_resolve(
    rid: str,
    sc_uid: Optional[str],
    ibjjf_id: Optional[str],
    name: str,
    email: str,
    prev_notes: str,
) -> tuple[bool, str, Optional[tuple], Optional[tuple]]:
    """
    Deeper resolution attempt. Returns (resolved, notes, link_tuple, conflict_tuple).
    """
    signals: list[str] = []
    candidate_ibjjf_id: Optional[str] = None

    # ── Middle-name variant search ─────────────────────────────────────────
    parts = name.lower().split()
    if len(parts) >= 3:
        # Search by first + last only
        first, last = parts[0], parts[-1]
        try:
            sb = _get_sb()
            r = (
                sb.table("ibjjf_athletes")
                .select("*")
                .ilike("name_lower", f"%{first}%{last}%")
                .limit(10)
                .execute()
            )
            middle_matches = r.data or []
            log.debug(
                "Middle-name search (%s %s): %d hits", first, last, len(middle_matches)
            )
            if len(middle_matches) == 1:
                m = middle_matches[0]
                signals.append(f"unique middle-name variant match: {m['name']} (ibjjf={m['ibjjf_id']})")
                candidate_ibjjf_id = candidate_ibjjf_id or str(m["ibjjf_id"])
            elif len(middle_matches) > 1:
                signals.append(
                    f"{len(middle_matches)} middle-name candidates: "
                    + ", ".join(str(m["ibjjf_id"]) for m in middle_matches[:4])
                )
        except Exception as e:
            log.debug("middle-name search error: %s", e)

    # ── IBJJF ID validity + page scrape ───────────────────────────────────
    if ibjjf_id:
        ibjjf_id = str(ibjjf_id)
        valid = _ibjjf_photo_valid(ibjjf_id)
        if valid is True:
            signals.append(f"IBJJF photo endpoint 200 (ID is real)")
            page_data = _ibjjf_athlete_page(ibjjf_id)
            if page_data and page_data.get("name"):
                signals.append(
                    f"bjjcompsystem page: name={page_data['name']}, "
                    f"academy={page_data.get('academy')}"
                )
                candidate_ibjjf_id = ibjjf_id
        elif valid is False:
            return (
                False,
                f"IBJJF ID {ibjjf_id} confirmed bogus (photo 404).",
                None,
                None,
            )

    # ── Smoothcomp profile scrape ──────────────────────────────────────────
    sc_name: Optional[str] = None
    sc_academy: Optional[str] = None
    if sc_uid:
        sc_data = _sc_profile_scrape(str(sc_uid))
        if sc_data:
            sc_name = sc_data.get("name")
            sc_academy = sc_data.get("academy")
            if sc_name:
                signals.append(f"Smoothcomp profile name: {sc_name}")
            if sc_academy:
                signals.append(f"Smoothcomp profile academy: {sc_academy}")
            # Re-search with SC name if different
            if sc_name and sc_name.lower() != name.lower():
                extra = _ibjjf_name_search(sc_name)
                if len(extra) == 1:
                    signals.append(
                        f"unique ibjjf_athletes match on SC name: "
                        f"{extra[0]['name']} ({extra[0]['ibjjf_id']})"
                    )
                    candidate_ibjjf_id = candidate_ibjjf_id or str(extra[0]["ibjjf_id"])

    # ── bjjmetrics cross-ref ───────────────────────────────────────────────
    search_name = sc_name or name
    bm_hits = _bjjmetrics_search(search_name)
    if bm_hits:
        bm_ids = [h["ibjjf_id"] for h in bm_hits if h.get("ibjjf_id")]
        if len(set(bm_ids)) == 1:
            signals.append(
                f"bjjmetrics unique match: {bm_hits[0]['name']} "
                f"(ibjjf={bm_ids[0]}, academy={bm_hits[0].get('academy')})"
            )
            candidate_ibjjf_id = candidate_ibjjf_id or bm_ids[0]
        elif bm_ids:
            signals.append(f"bjjmetrics {len(bm_ids)} candidates: " + ", ".join(set(bm_ids)))

    # ── Academy / team hint disambiguation ────────────────────────────────
    academy_hint: Optional[str] = sc_academy
    if not academy_hint and "@" in email:
        domain = email.split("@")[-1].split(".")[0]
        if len(domain) > 3:
            academy_hint = domain

    if academy_hint and candidate_ibjjf_id is None and name:
        db_candidates = _ibjjf_name_search(name)
        matching = [
            c for c in db_candidates
            if academy_hint.lower() in (c.get("academy") or "").lower()
        ]
        if len(matching) == 1:
            signals.append(
                f"academy hint '{academy_hint}' disambiguated to "
                f"{matching[0]['name']} ({matching[0]['ibjjf_id']})"
            )
            candidate_ibjjf_id = str(matching[0]["ibjjf_id"])

    # ── Confidence gate: need ≥2 independent signals pointing same ID ─────
    if not candidate_ibjjf_id:
        if signals:
            return (
                False,
                f"[NEEDS REVIEW] Signals found but no single candidate ID: "
                + "; ".join(signals),
                None,
                None,
            )
        return False, "Intelligence pass: no additional signals found.", None, None

    # Count signals that actually confirmed this specific ID
    confirming = [s for s in signals if candidate_ibjjf_id in s]
    if len(confirming) < 2 and not (ibjjf_id and ibjjf_id == candidate_ibjjf_id and sc_uid):
        cand_summary = f"candidate ibjjf_id={candidate_ibjjf_id}"
        return (
            False,
            f"[NEEDS REVIEW] {cand_summary}, only {len(confirming)} confirming signal(s). "
            + "; ".join(signals[:4]),
            None,
            None,
        )

    # ── Attempt to link ───────────────────────────────────────────────────
    # First ensure ibjjf_athletes row exists
    profile = _fetch_ibjjf_profile_from_api(candidate_ibjjf_id)
    if profile:
        canon_name = profile.get("name") or profile.get("fullName") or search_name
        belt = profile.get("belt") or profile.get("beltName")
        academy = profile.get("academy") or profile.get("teamName") or sc_academy
        _ensure_ibjjf_athletes_row(candidate_ibjjf_id, canon_name, belt, academy)
    else:
        canon_name = search_name
        _ensure_ibjjf_athletes_row(candidate_ibjjf_id, canon_name, None, sc_academy)

    if sc_uid:
        ok, msg = _safe_link_sc_ibjjf(str(sc_uid), candidate_ibjjf_id, rid)
        if not ok:
            return (
                False,
                msg,
                None,
                (rid, name, None, f"sc={sc_uid}↔ibjjf={candidate_ibjjf_id}"),
            )
        if msg != "already_linked":
            _patch_tournament_results(candidate_ibjjf_id, canon_name)
        notes = (
            f"Intelligence resolved. Signals: {'; '.join(signals[:5])}. "
            f"Linked sc={sc_uid} ↔ ibjjf={candidate_ibjjf_id}."
        )
        return (
            True,
            notes,
            (str(sc_uid), candidate_ibjjf_id, canon_name),
            None,
        )
    else:
        # IBJJF-only resolve — backfill tournament_results
        _patch_tournament_results(candidate_ibjjf_id, canon_name)
        notes = (
            f"Intelligence resolved (IBJJF only). Signals: {'; '.join(signals[:5])}. "
            f"ibjjf_athletes upserted; tournament_results backfilled."
        )
        return True, notes, None, None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    today = date.today().isoformat()
    print(f"\n=== Nightly findme resolver — {today} ===", flush=True)

    # Connectivity check (will sys.exit(1) if unreachable)
    _get_sb()

    # Step 1 — Deterministic pass
    det = run_deterministic_pass()

    # Step 2 — Intelligence pass
    intel_result = run_intelligence_pass()
    if isinstance(intel_result, tuple) and len(intel_result) == 4:
        intel, links_created, needs_review, conflicts = intel_result
    else:
        intel = {"reviewed": 0, "linked": 0, "left_unresolvable": 0, "needs_review": 0}
        links_created, needs_review, conflicts = [], [], []

    # ── Summary ─────────────────────────────────────────────────────────────
    print(
        f"Deterministic phase: seen={det['seen']} resolved={det['resolved']} "
        f"unresolvable={det['unresolvable']} errored={det['errored']}",
        flush=True,
    )
    print(
        f"Intelligence phase:  reviewed={intel['reviewed']} linked={intel['linked']} "
        f"left_unresolvable={intel['left_unresolvable']} needs_review={intel['needs_review']}",
        flush=True,
    )
    print(
        "Links created: "
        + (str(links_created) if links_created else "[]"),
        flush=True,
    )
    print(
        "[NEEDS REVIEW]: "
        + (
            str([(r[0], r[1], r[2][:200]) for r in needs_review])
            if needs_review
            else "[]"
        ),
        flush=True,
    )
    print(
        "Conflicts: "
        + (str(conflicts) if conflicts else "[]"),
        flush=True,
    )

    rc = 0 if (det["errored"] == 0) else 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
