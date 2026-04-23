"""Shared helpers for scrape_* scripts.

Keeps placeholder-name and structural filters centralized so each source
(AJP, UAEJJF, ADCC, etc.) applies the same sanity checks before an
athlete row lands in tournament_results.
"""

import re


_PLACEHOLDER_EXACT = {
    "no athlete",
    "tbd",
    "bye",
    "n/a",
    "na",
    "null",
    "undefined",
    "-",
}

# Substring fragments — match anywhere in the (lowercased, whitespace-collapsed)
# name. Catches variants like "winner not determined)" with a stray paren,
# "unknown user a", "UNKNOWN USER -", etc.
_PLACEHOLDER_FRAGMENTS = (
    "unknown user",
    "winner not determined",
    "not defined",
    "no name",
    "no entry",
    "forfeit",
    "disqualified",
    "did not show",
    "did not compete",
)


def is_placeholder_name(name: str) -> bool:
    """True when the scraper should skip this row — the source page did not
    contain a real athlete identity.

    Catches exact-match placeholders (BYE, TBD, N/A, etc.) and fragment-match
    ones (any variant of 'unknown user', 'winner not determined)' with stray
    punctuation, etc.). Also rejects names that after stripping punctuation
    still have fewer than 3 letters.
    """
    if not name:
        return True
    n = re.sub(r"\s+", " ", name.strip().lower())
    if not n or len(n) < 2:
        return True
    if n in _PLACEHOLDER_EXACT:
        return True
    for frag in _PLACEHOLDER_FRAGMENTS:
        if frag in n:
            return True
    letters_only = re.sub(r"[^a-z]", "", n)
    if len(letters_only) < 3:
        return True
    return False
