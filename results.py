"""
results.py — Persist final bracket results and fighter profiles in Supabase.

Two tables:
  bracket_finals   — one row per completed bracket (survives server restarts)
  fighter_results  — one row per athlete per division (foundation for profiles)

Called from app.py whenever results_final=True on any bracket.
Also provides load_bracket_finals() for warm startup.
"""

import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL        = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

_client: Client | None = None


def _get_client() -> Client | None:
    global _client
    if _client is not None:
        return _client
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return None
    try:
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        return _client
    except Exception as e:
        logger.warning("results: Supabase client init failed: %s", e)
        return None


# ── Write ─────────────────────────────────────────────────────────────────────

def save_bracket_final(
    category_id:     str,
    tournament_id:   str,
    tournament_name: str,
    division:        str,
    source:          str,          # 'ibjjf' or 'naga'
    ranking:         list,         # [{pos, name}]
    state:           dict,         # full bracket state dict
    event_date:      str = "",     # YYYY-MM-DD of first fight day
) -> None:
    """
    Upsert a completed bracket to bracket_finals and derive fighter_results rows.
    Fires-and-forgets — exceptions are logged but never raised.
    """
    client = _get_client()
    if client is None:
        return
    try:
        # ── bracket_finals ────────────────────────────────────────────────────
        client.table("bracket_finals").upsert({
            "category_id":     category_id,
            "tournament_id":   tournament_id,
            "tournament_name": tournament_name,
            "division":        division,
            "source":          source,
            "ranking":         ranking,
            "state_json":      state,
            "event_date":      event_date,
        }).execute()

        # ── fighter_results ───────────────────────────────────────────────────
        # Build name→display and name→team maps from fight competitors
        name_display: dict[str, str] = {}
        name_team:    dict[str, str] = {}
        for fight in state.get("fights", []):
            for comp in fight.get("competitors", []):
                raw = comp.get("name", "").strip()
                if raw and raw.lower() not in ("bye", ""):
                    key = raw.lower()
                    name_display.setdefault(key, raw)
                    name_team.setdefault(key, comp.get("team", ""))

        rows = []
        placed: set[str] = set()

        for r in ranking:
            key = r["name"].lower()
            placed.add(key)
            rows.append({
                "athlete_name":    key,
                "athlete_display": name_display.get(key, r["name"]),
                "team":            name_team.get(key, ""),
                "tournament_id":   tournament_id,
                "tournament_name": tournament_name,
                "category_id":     category_id,
                "division":        division,
                "source":          source,
                "placement":       r["pos"],   # '1' / '2' / '3'
                "event_date":      event_date,
            })

        # Anyone who fought but isn't on the podium = eliminated
        for key in name_display:
            if key not in placed:
                rows.append({
                    "athlete_name":    key,
                    "athlete_display": name_display[key],
                    "team":            name_team.get(key, ""),
                    "tournament_id":   tournament_id,
                    "tournament_name": tournament_name,
                    "category_id":     category_id,
                    "division":        division,
                    "source":          source,
                    "placement":       "eliminated",
                    "event_date":      event_date,
                })

        if rows:
            client.table("fighter_results").upsert(rows).execute()

    except Exception as e:
        logger.error("save_bracket_final(%s): %s", category_id, e)


# ── Read ──────────────────────────────────────────────────────────────────────

def load_bracket_finals() -> dict:
    """
    Load all saved bracket finals from Supabase.
    Returns dict of category_id → state_json dict.
    Called once at startup to pre-populate _brackets.
    """
    client = _get_client()
    if client is None:
        return {}
    try:
        resp = (
            client.table("bracket_finals")
            .select("category_id, state_json")
            .execute()
        )
        out = {}
        for row in resp.data or []:
            state = row.get("state_json") or {}
            if isinstance(state, dict):
                state["results_final"] = True   # always true in this table
                out[row["category_id"]] = state
        logger.info("load_bracket_finals: loaded %d brackets from Supabase", len(out))
        return out
    except Exception as e:
        logger.error("load_bracket_finals: %s", e)
        return {}


def get_fighter_profile(name: str) -> list:
    """
    Return all results for a fighter by name (case-insensitive).
    Sorted newest event first.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        resp = (
            client.table("fighter_results")
            .select("athlete_display, team, tournament_name, division, source, placement, event_date")
            .ilike("athlete_name", name.lower())
            .order("event_date", desc=True)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.error("get_fighter_profile(%s): %s", name, e)
        return []
