"""
ibjjf_rankings.py — Fetch IBJJF ranking positions for a specific athlete.

Given a slug and division params, hits the IBJJF ranking pages to find
the athlete's position. Results are cached in ibjjf_rankings_cache table.
"""

import re
import time
import logging
from datetime import date, datetime, timedelta

import requests
import psycopg2.extras

log = logging.getLogger(__name__)

BASE_URL = "https://ibjjf.com/{year}-athletes-ranking"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://ibjjf.com/",
}

GI_TYPES = {
    "gi":   "ranking-geral-gi",
    "nogi": "ranking-geral-no-gi",
}

# Maps common division weight strings → IBJJF filter slug
WEIGHT_SLUG_MAP = {
    "rooster":      "rooster",
    "light feather": "lightfeather",
    "lightfeather": "lightfeather",
    "feather":      "feather",
    "light":        "light",
    "middle":       "middle",
    "medium heavy": "mediumheavy",
    "mediumheavy":  "mediumheavy",
    "medium-heavy": "mediumheavy",
    "heavy":        "heavy",
    "super heavy":  "superheavy",
    "superheavy":   "superheavy",
    "super-heavy":  "superheavy",
    "ultra heavy":  "ultraheavy",
    "ultraheavy":   "ultraheavy",
    "ultra-heavy":  "ultraheavy",
    "open class":   "openclass",
    "openclass":    "openclass",
    "open-class":   "openclass",
}

WEIGHT_DISPLAY = {
    "rooster":      "Rooster",
    "lightfeather": "Light Feather",
    "feather":      "Feather",
    "light":        "Light",
    "middle":       "Middle",
    "mediumheavy":  "Medium Heavy",
    "heavy":        "Heavy",
    "superheavy":   "Super Heavy",
    "ultraheavy":   "Ultra Heavy",
    "openclass":    "Open Class",
}


def weight_slug_from_division(division: str) -> str | None:
    """Extract IBJJF weight slug from a division string like 'Purple / Master 6 / Male / Ultra Heavy'."""
    if not division:
        return None
    d = division.lower()
    # Sort by length descending so "super heavy" matches before "heavy"
    for key in sorted(WEIGHT_SLUG_MAP, key=len, reverse=True):
        if key in d:
            return WEIGHT_SLUG_MAP[key]
    return None


def _find_on_page(html: str, slug: str) -> tuple[int | None, float | None]:
    """Search HTML for athlete slug, return (rank_position, points)."""
    trs = re.findall(r"<tr>(.*?)</tr>", html, re.S)
    for tr in trs:
        if f"/athletes/{slug}" not in tr:
            continue
        m_pos = re.search(r"class=['\"]position['\"]>(\d+)", tr)
        m_pts = re.search(r"class=['\"]pontuation['\"][^>]*>([\d.]+)", tr)
        return (
            int(m_pos.group(1)) if m_pos else None,
            float(m_pts.group(1)) if m_pts else None,
        )
    return None, None


def fetch_rank(slug: str, belt: str, gender: str, ranking_category: str,
               age_division: str | None, gi_label: str, weight: str = "",
               name_search: str = "", year: int | None = None) -> dict:
    """
    Fetch rank for one (gi/nogi, weight) combination.
    Uses filters[search] with the athlete's last name for accuracy —
    the weight-specific page only shows correct weight points when filtered by name.
    Returns {"rank": int|None, "points": float|None}.
    """
    if year is None:
        year = date.today().year
    gi_type = GI_TYPES.get(gi_label, GI_TYPES["gi"])
    params = {
        "filters[ranking_category]": ranking_category,
        "filters[gender]":           gender,
        "filters[s]":                gi_type,
        "filters[belt]":             belt,
        "commit":                    "Search",
    }
    if age_division:
        params["filters[age_division]"] = age_division
    if weight:
        # When using name search, IBJJF uses filters[weight]; without search, filters[weight_division]
        weight_key = "filters[weight]" if name_search else "filters[weight_division]"
        params[weight_key] = weight
    if name_search:
        params["filters[search]"] = name_search

    url = BASE_URL.format(year=year)
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning("ibjjf_rankings fetch error: %s", e)
        return {"rank": None, "points": None}

    rank, pts = _find_on_page(resp.text, slug)
    if rank is not None:
        return {"rank": rank, "points": pts}

    # Fallback: paginate if name search didn't find them (e.g. unusual name)
    for page in range(2, 25):
        p = {**params, "page": page}
        try:
            resp = requests.get(url, params=p, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            log.warning("ibjjf_rankings fetch error page %d: %s", page, e)
            break
        rank, pts = _find_on_page(resp.text, slug)
        if rank is not None:
            return {"rank": rank, "points": pts}
        if 'rel="next"' not in resp.text and "rel='next'" not in resp.text:
            break
        time.sleep(0.3)

    return {"rank": None, "points": None}


def get_rankings(cur, slug: str, belt: str, gender: str,
                 ranking_category: str, age_division: str | None,
                 weight: str = "", year: int | None = None,
                 cache_hours: int = 24) -> dict:
    """
    Return all 4 ranking dicts, using DB cache (ibjjf_rankings_cache).

    Returns {
        "overall_gi":  {"rank": int|None, "points": float|None},
        "overall_nogi":{"rank": int|None, "points": float|None},
        "weight_gi":   {"rank": int|None, "points": float|None},
        "weight_nogi": {"rank": int|None, "points": float|None},
        "weight_display": str,  # human-readable weight class name
    }
    """
    if not slug:
        return _empty_rankings(weight)

    cutoff = datetime.utcnow() - timedelta(hours=cache_hours)
    combos = [
        ("gi",   ""),
        ("nogi", ""),
        ("gi",   weight),
        ("nogi", weight),
    ]

    result = {}
    needs_fetch = []

    for gi_label, w in combos:
        key = f"{'overall' if not w else 'weight'}_{gi_label}"
        cur.execute("""
            SELECT rank_position, points, cached_at
            FROM ibjjf_rankings_cache
            WHERE slug=%s AND gi_nogi=%s AND weight=%s
        """, (slug, gi_label, w or ""))
        row = cur.fetchone()
        if row and row["cached_at"] and row["cached_at"] > cutoff:
            result[key] = {"rank": row["rank_position"], "points": row["points"]}
        else:
            needs_fetch.append((gi_label, w, key))

    # Derive last name from slug (e.g. "michael-bambic" → "bambic") for name search
    name_search = slug.split("-")[-1].title() if slug else ""

    for gi_label, w, key in needs_fetch:
        data = fetch_rank(slug, belt, gender, ranking_category, age_division,
                          gi_label, w, name_search=name_search, year=year)
        result[key] = data
        cur.execute("""
            INSERT INTO ibjjf_rankings_cache (slug, gi_nogi, weight, rank_position, points, cached_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (slug, gi_nogi, weight) DO UPDATE SET
                rank_position = EXCLUDED.rank_position,
                points = EXCLUDED.points,
                cached_at = NOW()
        """, (slug, gi_label, w or "", data["rank"], data["points"]))
        try:
            cur.connection.commit()
        except Exception:
            pass
        time.sleep(0.4)

    result["weight_display"] = WEIGHT_DISPLAY.get(weight, weight.replace("-", " ").title()) if weight else None
    return result


def _empty_rankings(weight: str = "") -> dict:
    return {
        "overall_gi":   {"rank": None, "points": None},
        "overall_nogi": {"rank": None, "points": None},
        "weight_gi":    {"rank": None, "points": None},
        "weight_nogi":  {"rank": None, "points": None},
        "weight_display": WEIGHT_DISPLAY.get(weight, None),
    }


def ensure_cache_table(cur):
    """Create ibjjf_rankings_cache if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ibjjf_rankings_cache (
            slug      TEXT    NOT NULL,
            gi_nogi   TEXT    NOT NULL,
            weight    TEXT    NOT NULL DEFAULT '',
            rank_position INT,
            points    FLOAT,
            cached_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (slug, gi_nogi, weight)
        )
    """)
    cur.connection.commit()
