"""
nightly_findme_resolve.py — Process pending findme_reports and identify/link athletes.

Cron-ready (run nightly, e.g. 3 AM):
    python nightly_findme_resolve.py

Requires env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY

Per-report logic:
  1. IBJJF side  — ensure ibjjf_athletes row exists; check tournament_results coverage.
  2. SC side     — check tournament_results for smoothcomp source rows.
  3. Link        — if both IDs present, upsert sc_ibjjf_verified.
  4. Name-only   — search by name; cross-ref bjjmetrics.com; link only on exact 1 match.
  5. Close       — mark report resolved or unresolvable with notes.
"""

import logging
import os
import re
import sys
import unicodedata

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("findme_resolve")

SUPABASE_URL        = os.environ.get("SUPABASE_URL", "https://kzqvfuqxtbrhlgphyntb.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MAX_REPORTS = 50

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ── Supabase client ───────────────────────────────────────────────────────────

def _get_sb():
    if not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_SERVICE_KEY is not set")
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── IBJJF athlete helpers ─────────────────────────────────────────────────────

def _fetch_ibjjf_profile_from_api(ibjjf_id: str) -> dict | None:
    """
    Try api2.ibjjfdb.com (unauthenticated public endpoint) for basic profile info.
    Returns dict with 'name', 'belt', 'academy' or None on any failure.
    """
    try:
        r = requests.get(
            f"https://api2.ibjjfdb.com/admin/athletes/{ibjjf_id}",
            headers={
                "User-Agent": HTTP_HEADERS["User-Agent"],
                "Origin":  "https://app.ibjjfdb.com",
                "Referer": "https://app.ibjjfdb.com/",
                "Accept":  "application/json, text/plain, */*",
            },
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        attrs = data.get("data", {}).get("attributes", {})
        belt    = attrs.get("belt") or {}
        academy = attrs.get("academy") or {}
        return {
            "name":    attrs.get("name", ""),
            "belt":    belt.get("name", "") if isinstance(belt, dict) else str(belt),
            "academy": academy.get("name", "") if isinstance(academy, dict) else str(academy),
        }
    except Exception as e:
        log.debug("IBJJF API fetch for id=%s: %s", ibjjf_id, e)
        return None


def _ensure_ibjjf_athletes_row(sb, ibjjf_id: str) -> dict | None:
    """
    Return the ibjjf_athletes row for ibjjf_id, creating it from the API if absent.
    Returns None if the row can't be found or created.
    """
    try:
        res = (sb.table("ibjjf_athletes")
                 .select("ibjjf_id,name,name_lower,belt,academy")
                 .eq("ibjjf_id", ibjjf_id)
                 .limit(1)
                 .execute())
        if res.data:
            return res.data[0]
    except Exception as e:
        log.warning("ibjjf_athletes lookup id=%s: %s", ibjjf_id, e)
        return None

    # Not in DB — try to fetch from IBJJF API
    profile = _fetch_ibjjf_profile_from_api(ibjjf_id)
    if not profile or not profile.get("name"):
        log.info("IBJJF API returned no profile for id=%s", ibjjf_id)
        return None

    name = profile["name"].strip()
    row = {
        "ibjjf_id":  ibjjf_id,
        "name":      name,
        "name_lower": name.lower(),
        "belt":      (profile.get("belt") or "").lower() or None,
        "academy":   profile.get("academy") or None,
    }
    try:
        sb.table("ibjjf_athletes").upsert(row, on_conflict="ibjjf_id").execute()
        log.info("Upserted ibjjf_athletes id=%s name=%s", ibjjf_id, name)
        return row
    except Exception as e:
        log.warning("ibjjf_athletes upsert id=%s: %s", ibjjf_id, e)
        return None


def _count_ibjjf_results(sb, ibjjf_id: str) -> int:
    """Count tournament_results rows for this IBJJF athlete."""
    try:
        res = (sb.table("tournament_results")
                 .select("event_id")
                 .eq("source", "ibjjf")
                 .eq("ibjjf_athlete_id", ibjjf_id)
                 .limit(1)
                 .execute())
        return len(res.data)
    except Exception:
        return 0


def _count_sc_results(sb, sc_uid: str) -> int:
    """Count tournament_results rows for this Smoothcomp athlete."""
    try:
        res = (sb.table("tournament_results")
                 .select("event_id")
                 .eq("source", "smoothcomp")
                 .eq("athlete_id", sc_uid)
                 .limit(1)
                 .execute())
        return len(res.data)
    except Exception:
        return 0


# ── Linking helpers ───────────────────────────────────────────────────────────

def _get_link_by_sc(sb, sc_uid: str) -> dict | None:
    try:
        res = (sb.table("sc_ibjjf_verified")
                 .select("sc_uid,ibjjf_athlete_id,ibjjf_name")
                 .eq("sc_uid", sc_uid)
                 .limit(1)
                 .execute())
        return res.data[0] if res.data else None
    except Exception as e:
        log.warning("sc_ibjjf_verified lookup sc_uid=%s: %s", sc_uid, e)
        return None


def _get_link_by_ibjjf(sb, ibjjf_id: str) -> dict | None:
    try:
        res = (sb.table("sc_ibjjf_verified")
                 .select("sc_uid,ibjjf_athlete_id,ibjjf_name")
                 .eq("ibjjf_athlete_id", ibjjf_id)
                 .limit(1)
                 .execute())
        return res.data[0] if res.data else None
    except Exception as e:
        log.warning("sc_ibjjf_verified lookup ibjjf_id=%s: %s", ibjjf_id, e)
        return None


def _upsert_link(sb, sc_uid: str, ibjjf_id: str, ibjjf_name: str):
    sb.table("sc_ibjjf_verified").upsert({
        "sc_uid":           sc_uid,
        "ibjjf_athlete_id": ibjjf_id,
        "ibjjf_name":       ibjjf_name,
    }, on_conflict="sc_uid").execute()


# ── Name-only search helpers ──────────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase and strip combining accents for fuzzy comparison."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()


def _ibjjf_name_search(sb, name: str) -> list[dict]:
    name_lower = name.lower().strip()
    try:
        res = (sb.table("ibjjf_athletes")
                 .select("ibjjf_id,name,name_lower,belt,academy")
                 .ilike("name_lower", f"%{name_lower}%")
                 .limit(20)
                 .execute())
        return res.data or []
    except Exception as e:
        log.warning("ibjjf_athletes name search '%s': %s", name, e)
        return []


def _tr_name_search(sb, name: str) -> list[dict]:
    name_lower = name.lower().strip()
    try:
        res = (sb.table("tournament_results")
                 .select("athlete_id,ibjjf_athlete_id,athlete_name,source")
                 .ilike("athlete_name", f"%{name_lower}%")
                 .limit(20)
                 .execute())
        return res.data or []
    except Exception as e:
        log.warning("tournament_results name search '%s': %s", name, e)
        return []


def _bjjmetrics_search(name: str) -> list[dict]:
    """
    Search bjjmetrics.com for athletes matching name.
    Returns list of {ibjjf_id, name} dicts. Empty on any failure.
    """
    try:
        r = requests.get(
            "https://www.bjjmetrics.com/athletes",
            params={"q": name},
            headers=HTTP_HEADERS,
            timeout=15,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        seen: dict[str, dict] = {}
        for a_tag in soup.find_all("a", href=re.compile(r"/athlete/(\d+)")):
            m = re.search(r"/athlete/(\d+)", a_tag["href"])
            if m:
                aid = m.group(1)
                seen[aid] = {"ibjjf_id": aid, "name": a_tag.get_text(strip=True)}
        return list(seen.values())
    except Exception as e:
        log.debug("bjjmetrics search '%s': %s", name, e)
        return []


# ── Per-report logic ──────────────────────────────────────────────────────────

def _close(sb, report_id, status: str, notes: str):
    try:
        sb.table("findme_reports").update({
            "status":           status,
            "resolution_notes": notes[:1000],
        }).eq("id", report_id).execute()
    except Exception as e:
        log.error("Close report %s failed: %s", report_id, e)


def process_report(sb, report: dict) -> str:
    """
    Process one pending findme_reports row.
    Returns 'resolved', 'unresolvable', or 'error'.
    """
    rid      = report["id"]
    ibjjf_id = str(report.get("ibjjf_id") or "").strip() or None
    sc_uid   = str(report.get("sc_uid") or "").strip() or None
    name     = (report.get("name") or "").strip() or None

    log.info("Report %s: ibjjf_id=%s sc_uid=%s name=%s", rid, ibjjf_id, sc_uid, name)

    notes: list[str] = []

    # ── 1. IBJJF side ────────────────────────────────────────────────────────
    ibjjf_row: dict | None = None
    n_ibjjf = 0
    if ibjjf_id:
        ibjjf_row = _ensure_ibjjf_athletes_row(sb, ibjjf_id)
        if ibjjf_row:
            notes.append(f"ibjjf_athletes ok: id={ibjjf_id} name={ibjjf_row.get('name','')!r}")
        else:
            notes.append(f"ibjjf_athletes missing for id={ibjjf_id} (API fetch failed)")

        n_ibjjf = _count_ibjjf_results(sb, ibjjf_id)
        if n_ibjjf == 0:
            notes.append("no ibjjf tournament_results rows (historical scrape pending)")
        else:
            notes.append(f"{n_ibjjf}+ ibjjf tournament_result rows present")

    # ── 2. SC side ────────────────────────────────────────────────────────────
    n_sc = 0
    if sc_uid:
        n_sc = _count_sc_results(sb, sc_uid)
        if n_sc == 0:
            notes.append(f"no sc tournament_results for sc_uid={sc_uid} (brackets may not be scraped yet)")
        else:
            notes.append(f"{n_sc}+ sc tournament_result rows for sc_uid={sc_uid}")

    # ── 3. Link (both IDs present) ───────────────────────────────────────────
    if ibjjf_id and sc_uid:
        ibjjf_name = ibjjf_row.get("name", "") if ibjjf_row else ""

        existing_by_sc = _get_link_by_sc(sb, sc_uid)
        if existing_by_sc:
            existing_ibjjf = str(existing_by_sc.get("ibjjf_athlete_id", ""))
            if existing_ibjjf and existing_ibjjf != ibjjf_id:
                notes.append(
                    f"CONFLICT: sc_uid={sc_uid} already linked to ibjjf_id={existing_ibjjf}; "
                    f"report requests {ibjjf_id} — keeping existing, not overwriting"
                )
                _close(sb, rid, "unresolvable", "; ".join(notes))
                return "unresolvable"
            notes.append(f"link already exists: sc_uid={sc_uid} ↔ ibjjf_id={existing_ibjjf}")
        else:
            # Check if this ibjjf_id is already linked to a different sc_uid
            existing_by_ibjjf = _get_link_by_ibjjf(sb, ibjjf_id)
            if existing_by_ibjjf:
                existing_sc = str(existing_by_ibjjf.get("sc_uid", ""))
                if existing_sc and existing_sc != sc_uid:
                    notes.append(
                        f"CONFLICT: ibjjf_id={ibjjf_id} already linked to sc_uid={existing_sc}; "
                        f"report requests sc_uid={sc_uid} — keeping existing, not overwriting"
                    )
                    _close(sb, rid, "unresolvable", "; ".join(notes))
                    return "unresolvable"

            try:
                _upsert_link(sb, sc_uid, ibjjf_id, ibjjf_name)
                notes.append(f"linked: sc_uid={sc_uid} ↔ ibjjf_id={ibjjf_id} ({ibjjf_name!r})")
                log.info("Linked sc_uid=%s ↔ ibjjf_id=%s (%s)", sc_uid, ibjjf_id, ibjjf_name)
            except Exception as e:
                notes.append(f"link upsert failed: {e}")
                log.error("Link upsert report %s: %s", rid, e)
                _close(sb, rid, "unresolvable", "; ".join(notes))
                return "error"

        _close(sb, rid, "resolved", "; ".join(notes))
        return "resolved"

    # ── IBJJF-only ───────────────────────────────────────────────────────────
    if ibjjf_id and not sc_uid:
        existing = _get_link_by_ibjjf(sb, ibjjf_id)
        if existing:
            notes.append(f"already linked to sc_uid={existing.get('sc_uid')}")
        else:
            notes.append("no sc_uid — cannot link")

        if ibjjf_row:
            _close(sb, rid, "resolved", "; ".join(notes))
            return "resolved"
        else:
            _close(sb, rid, "unresolvable", "; ".join(notes))
            return "unresolvable"

    # ── SC-only ───────────────────────────────────────────────────────────────
    if sc_uid and not ibjjf_id:
        if n_sc > 0:
            _close(sb, rid, "resolved", "; ".join(notes))
            return "resolved"
        else:
            notes.append("no sc results and no ibjjf_id — cannot find athlete")
            _close(sb, rid, "unresolvable", "; ".join(notes))
            return "unresolvable"

    # ── Name-only ─────────────────────────────────────────────────────────────
    if name and not ibjjf_id and not sc_uid:
        ibjjf_matches = _ibjjf_name_search(sb, name)
        tr_matches    = _tr_name_search(sb, name)
        bm_matches    = _bjjmetrics_search(name)

        # Collect all unique ibjjf_id candidates across all sources
        candidate_ids: set[str] = set()
        for m in ibjjf_matches:
            candidate_ids.add(str(m["ibjjf_id"]))
        for m in tr_matches:
            if m.get("ibjjf_athlete_id"):
                candidate_ids.add(str(m["ibjjf_athlete_id"]))
        for m in bm_matches:
            candidate_ids.add(str(m["ibjjf_id"]))

        # Collect sc_uids from tournament_results name matches
        sc_candidate_ids: set[str] = set()
        for m in tr_matches:
            if m.get("athlete_id") and m.get("source") == "smoothcomp":
                sc_candidate_ids.add(str(m["athlete_id"]))

        if len(candidate_ids) == 1:
            found_ibjjf_id = candidate_ids.pop()
            row = _ensure_ibjjf_athletes_row(sb, found_ibjjf_id)
            found_name = row.get("name", name) if row else name
            notes.append(f"name-only: single match ibjjf_id={found_ibjjf_id} ({found_name!r})")

            if len(sc_candidate_ids) == 1:
                found_sc_uid = sc_candidate_ids.pop()
                existing = _get_link_by_sc(sb, found_sc_uid)
                if not existing:
                    try:
                        _upsert_link(sb, found_sc_uid, found_ibjjf_id, found_name)
                        notes.append(f"linked: sc_uid={found_sc_uid} ↔ ibjjf_id={found_ibjjf_id}")
                    except Exception as e:
                        notes.append(f"link failed: {e}")
                else:
                    notes.append(f"sc_uid={found_sc_uid} already linked")
            elif len(sc_candidate_ids) > 1:
                notes.append(f"ambiguous sc_uids {sorted(sc_candidate_ids)} — skipped link")

            _close(sb, rid, "resolved", "; ".join(notes))
            return "resolved"

        elif not candidate_ids:
            notes.append(f"no candidates found for name={name!r}")
        else:
            sample = sorted(candidate_ids)[:10]
            notes.append(f"ambiguous: {len(candidate_ids)} ibjjf candidates {sample}")

        _close(sb, rid, "unresolvable", "; ".join(notes))
        return "unresolvable"

    # No usable data at all
    notes.append("no ibjjf_id, sc_uid, or name in report")
    _close(sb, rid, "unresolvable", "; ".join(notes))
    return "unresolvable"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== findme_resolve starting ===")

    try:
        sb = _get_sb()
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)

    try:
        res = (sb.table("findme_reports")
                 .select("id,name,ibjjf_id,sc_uid,email,status,resolution_notes,created_at")
                 .eq("status", "pending")
                 .order("created_at", desc=False)
                 .limit(MAX_REPORTS)
                 .execute())
        reports = res.data or []
    except Exception as e:
        log.error("Failed to fetch pending reports: %s", e)
        sys.exit(1)

    log.info("Fetched %d pending report(s)", len(reports))

    seen = len(reports)
    resolved = unresolvable = errored = 0

    for report in reports:
        try:
            outcome = process_report(sb, report)
        except Exception as e:
            log.error("Uncaught error on report %s: %s", report.get("id"), e)
            try:
                _close(sb, report["id"], "unresolvable", f"agent error: {e}")
            except Exception:
                pass
            outcome = "error"

        if outcome == "resolved":
            resolved += 1
        elif outcome == "unresolvable":
            unresolvable += 1
        else:
            errored += 1

    log.info("=== findme_resolve done ===")
    print(
        f"\nSummary — seen: {seen} | resolved: {resolved} | "
        f"unresolvable: {unresolvable} | errored: {errored}"
    )


if __name__ == "__main__":
    main()
