"""Shared helpers for scrape_* scripts.

Keeps placeholder-name and structural filters centralized so each source
(AJP, UAEJJF, ADCC, etc.) applies the same sanity checks before an
athlete row lands in tournament_results.
"""

_PLACEHOLDER_NAMES = {
    "unknown user",
    "not defined",
    "winner not determined",
    "no athlete",
    "tbd",
    "bye",
    "n/a",
}


def is_placeholder_name(name: str) -> bool:
    """True when the scraper should skip this row — the source page did not
    contain a real athlete identity.

    Catches:
      - Exact matches on known placeholders (see _PLACEHOLDER_NAMES).
      - Any variant of 'unknown user' (AJP/UAEJJF emit 'unknown user a',
        'unknown user -', etc. when registration lacks a user).
    """
    if not name:
        return True
    n = name.strip().lower()
    if not n or len(n) < 2:
        return True
    if n in _PLACEHOLDER_NAMES:
        return True
    if n.startswith("unknown user"):
        return True
    return False
