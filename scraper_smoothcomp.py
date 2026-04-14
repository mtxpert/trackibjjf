"""
Smoothcomp unified event discovery.
Single source covering NAGA, CompNet, TCO, ADCC, Grappling Industries,
New Breed, Fuji, Good Fight, PBJJF, and 130+ other orgs.
"""
import re, json, time, requests
from datetime import date as _date

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Subdomain → stable org key
_SUB_ORG = {
    "naga":                   "naga",
    "compnet":                "compnet",
    "adcc":                   "adcc",
    "adcc-spain":             "adcc",
    "grapplingindustries":    "gi",
    "newbreedbjj":            "newbreed",
    "fujibjj":                "fuji",
    "pbjjf":                  "pbjjf",
    "goodfight":              "goodfight",
    "united":                 "united",
    "submissionchallenge":    "subchallenge",
    "grapplingx":             "grapplingx",
    "mdl":                    "mdl",
    "rollalot":               "rollalot",
    "impactbjj":              "impactbjj",
    "empiregrappling":        "empiregrappling",
    "allstarsbjj":            "allstarsbjj",
    "prosportgrappling":      "prosport",
    "agf":                    "agf",
    "bjj247":                 "bjj247",
    "grapplecity":            "grapplecity",
    "avagrappling":           "ava",
    "nabjjf":                 "nabjjf",
    "wsojj":                  "wsojj",
    "copa-bjj":               "copabjj",
    "grappling-games":        "grapplinggames",
    "stealthgrappling":       "stealth",
    "kakutogi":               "adcc",   # ADCC events on kakutogi sub
}

# Title pattern → org key (for no-subdomain events on smoothcomp.com)
_TITLE_PATTERNS = [
    (r"tap cancer",             "tco"),
    (r"grapplingindustries|grappling industries", "gi"),
    (r"\bnaga\b",               "naga"),
]

# In-memory cache: (timestamp, data)
_CACHE: tuple = (0, [])
_CACHE_TTL = 1800  # 30 minutes


def _parse_events_js(html: str) -> list:
    idx = html.find("var events")
    if idx == -1:
        return []
    chunk = html[idx:]
    try:
        start = chunk.index("[")
        depth = 0
        for j, c in enumerate(chunk[start:], start):
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    return json.loads(chunk[start : j + 1])
    except Exception:
        pass
    return []


def _detect_org(url: str, title: str) -> str:
    m = re.match(r"https://([^.]+)\.smoothcomp\.com", url)
    if m:
        sub = m.group(1)
        return _SUB_ORG.get(sub, sub)
    tl = title.lower()
    for pattern, org in _TITLE_PATTERNS:
        if re.search(pattern, tl):
            return org
    return "other"


def _normalize(e: dict) -> dict:
    today = _date.today()
    url   = e.get("url", "")
    title = e.get("title", "")
    org   = _detect_org(url, title)

    m          = re.match(r"https://([^.]+)\.smoothcomp\.com", url)
    subdomain  = m.group(1) if m else ""

    start_iso  = (e.get("startdate") or "")[:10]
    end_iso    = (e.get("enddate")   or "")[:10]
    is_past    = bool(end_iso and _date.fromisoformat(end_iso) < today)

    city         = e.get("location_city", "").strip()
    country      = e.get("location_country_human", "").strip()
    country_code = e.get("location_country", "").strip()

    try:
        lat = float(e.get("location_lat") or 0) or None
        lng = float(e.get("location_long") or 0) or None
    except (ValueError, TypeError):
        lat = lng = None

    return {
        "id":           str(e["id"]),
        "name":         title,
        "start":        start_iso,
        "end":          end_iso,
        "city":         city,
        "country":      country,
        "country_code": country_code,
        "lat":          lat,
        "lng":          lng,
        "cover_image":  e.get("cover_image", ""),
        "url":          url,
        "org":          org,
        "subdomain":    subdomain,
        "source":       "smoothcomp",
        "is_past":      is_past,
    }


def get_smoothcomp_events(force_refresh: bool = False) -> list:
    """Fetch all upcoming Smoothcomp events (cached 30 min)."""
    global _CACHE
    ts, cached = _CACHE
    if not force_refresh and cached and (time.time() - ts) < _CACHE_TTL:
        return cached

    resp = requests.get(
        "https://smoothcomp.com/en/events/upcoming",
        headers=HEADERS,
        timeout=(5, 25),
    )
    resp.raise_for_status()
    raw    = _parse_events_js(resp.text)
    result = [_normalize(e) for e in raw]
    _CACHE = (time.time(), result)
    return result
