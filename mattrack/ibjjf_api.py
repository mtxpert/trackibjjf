"""
ibjjf_api.py — IBJJF core.ibjjf.com + api2.ibjjfdb.com API client.

Provides:
  - login(email, password) → (athlete_id, jwt_token)
  - get_active_registrations(athlete_id, token) → list of upcoming championship dicts
  - get_athlete_profile(token) → full profile dict

Usage (standalone):
    python ibjjf_api.py --email mbambic@gmail.com --password '...'
"""

import argparse
import base64
import json
import logging
import sys

import requests

log = logging.getLogger("ibjjf_api")

CORE_URL  = "https://core.ibjjf.com"
API2_URL  = "https://api2.ibjjfdb.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Origin":  "https://app.ibjjfdb.com",
    "Referer": "https://app.ibjjfdb.com/",
    "Accept":  "application/json, text/plain, */*",
    "Content-Type": "application/json",
}


def _decode_jwt_payload(token: str) -> dict:
    """Decode JWT payload without verification (we trust IBJJF's own server)."""
    try:
        payload_b64 = token.split(".")[1]
        # Add padding
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return {}


def login(email: str, password: str) -> tuple[str, str]:
    """
    Log into IBJJF and return (athlete_id, jwt_token).
    Raises on failure.
    """
    # Step 1: lookup (gets userId for the login call)
    r = requests.post(
        f"{CORE_URL}/users/login",
        json={"user": {"email": email, "password": password}},
        headers=HEADERS,
        timeout=20,
    )
    if r.status_code != 200:
        raise ValueError(f"IBJJF login failed: {r.status_code} {r.text[:200]}")

    data = r.json()
    token = data["data"]["attributes"]["token"]
    payload = _decode_jwt_payload(token)
    athlete_id = str(payload.get("athleteId", ""))

    if not athlete_id:
        raise ValueError("No athleteId in JWT payload")

    log.info("Logged in: athleteId=%s", athlete_id)
    return athlete_id, token


def get_athlete_profile(token: str) -> dict:
    """Return full athlete profile dict from /users/me."""
    r = requests.get(
        f"{CORE_URL}/users/me",
        headers={**HEADERS, "Authorization": f"Bearer {token}"},
        timeout=20,
    )
    r.raise_for_status()
    attrs = r.json()["data"]["attributes"]
    athlete = attrs.get("athlete", {})
    return {
        "athlete_id":  str(athlete.get("id", "")),
        "name":        athlete.get("name", ""),
        "belt":        athlete.get("belt", {}).get("name", ""),
        "belt_id":     athlete.get("beltId"),
        "academy":     athlete.get("academy", {}).get("name", ""),
        "academy_id":  athlete.get("academyId"),
        "birth_date":  athlete.get("birthDate", ""),
        "gender":      athlete.get("gender", {}).get("name", ""),
        "photo_url":   athlete.get("photo", ""),
    }


def get_active_registrations(athlete_id: str, token: str) -> list[dict]:
    """Return upcoming/active championship registrations for the athlete."""
    r = requests.get(
        f"{API2_URL}/admin/championships/championshipsRegistrations",
        params={"athleteId": athlete_id, "championshipStatus": "active", "pageSize": 50},
        headers={**HEADERS, "Authorization": f"Bearer {token}"},
        timeout=20,
    )
    r.raise_for_status()
    items = r.json().get("list", [])
    results = []
    for item in items:
        champ = item.get("championship", {})
        date_start = (champ.get("date", {}) or {}).get("start", "") or ""
        results.append({
            "registration_id":  str(item.get("id", "")),
            "athlete_id":       str(item.get("athleteId", "")),
            "status":           item.get("status", ""),
            "championship_id":  str(champ.get("id", "")),
            "championship_name": champ.get("name", ""),
            "championship_abbr": champ.get("abbr", ""),
            "event_date":       date_start[:10] if date_start else "",
            "city":             champ.get("localAddressCity", ""),
            "state":            champ.get("localAddressState", ""),
            "academy_id":       str((item.get("academyTeam") or {}).get("id", "")),
            "academy_name":     (item.get("academyTeam") or {}).get("name", ""),
        })
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--email",    required=True)
    ap.add_argument("--password", required=True)
    args = ap.parse_args()

    athlete_id, token = login(args.email, args.password)
    profile = get_athlete_profile(token)
    regs = get_active_registrations(athlete_id, token)

    print(f"\nAthlete: {profile['name']} (ID: {athlete_id})")
    print(f"Belt: {profile['belt']} | Academy: {profile['academy']}")
    print(f"DOB: {profile['birth_date']} | Gender: {profile['gender']}")
    print(f"\nActive registrations ({len(regs)}):")
    for r in regs:
        print(f"  [{r['championship_id']}] {r['championship_name']}")
        print(f"    Date: {r['event_date']} | Status: {r['status']} | Team: {r['academy_name']}")


if __name__ == "__main__":
    main()
