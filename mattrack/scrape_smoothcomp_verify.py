"""
scrape_smoothcomp_verify.py — Smoothcomp login verification.

Provides:
  verify_sc_login(email, password) -> dict with {sc_user_id, sc_name, email}

The sc_user_id returned is the Smoothcomp user/athlete ID which matches
the athlete profile URL at smoothcomp.com/en/profile/{sc_user_id}.
"""

import logging
import re

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("scrape_smoothcomp_verify")

SC_BASE = "https://smoothcomp.com"
LOGIN_URL = f"{SC_BASE}/en/auth/login"
PROFILE_URL = f"{SC_BASE}/en/account/profile"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get_csrf_token(session: requests.Session) -> str:
    """GET the login page and extract the CSRF _token field."""
    r = session.get(LOGIN_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "_token"})
    if not token_input:
        raise ValueError("Could not find CSRF _token on Smoothcomp login page")
    return token_input["value"]


def _extract_user_id_from_page(html: str) -> str | None:
    """
    Try to find the Smoothcomp user/athlete ID from the profile page HTML.
    Looks for patterns like:
      - /en/profile/123456
      - data-user-id="123456"
      - /en/athlete/123456
    """
    patterns = [
        r"/en/profile/(\d+)",
        r"/en/athlete/(\d+)",
        r'data-user-id=["\'](\d+)["\']',
        r'data-athlete-id=["\'](\d+)["\']',
        r'"userId"\s*:\s*(\d+)',
        r'"user_id"\s*:\s*(\d+)',
        r'"athleteId"\s*:\s*(\d+)',
    ]
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            return m.group(1)
    return None


def _extract_display_name(html: str) -> str:
    """Try to extract the athlete's display name from the profile page."""
    soup = BeautifulSoup(html, "html.parser")

    # Try og:title meta tag
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    # Try <title> tag
    title = soup.find("title")
    if title:
        text = title.get_text(strip=True)
        # Remove site suffix like " | Smoothcomp"
        text = re.sub(r"\s*[|\-–]\s*Smoothcomp.*$", "", text, flags=re.I)
        if text:
            return text

    # Try h1/h2 with name-like content
    for tag in soup.find_all(["h1", "h2"]):
        text = tag.get_text(strip=True)
        if text and len(text) > 2 and len(text) < 80:
            return text

    return ""


def verify_sc_login(email: str, password: str) -> dict:
    """
    Attempt Smoothcomp login and return user info on success.

    Returns:
        {
            "sc_user_id": str,  # Smoothcomp user ID
            "sc_name": str,     # Display name
            "email": str,       # Email used to login
        }

    Raises:
        ValueError: on bad credentials or if user ID can't be found
        requests.HTTPError: on network/server errors
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # Step 1: GET login page for CSRF token
    log.info("Fetching Smoothcomp login page for CSRF token")
    csrf_token = _get_csrf_token(session)
    log.info("Got CSRF token: %s...", csrf_token[:8])

    # Step 2: POST login credentials
    log.info("POSTing credentials for %s", email)
    payload = {
        "_token": csrf_token,
        "email": email,
        "password": password,
        "_next_url": "/en/account/profile",
    }
    r = session.post(
        LOGIN_URL,
        data=payload,
        headers={**HEADERS, "Referer": LOGIN_URL, "Content-Type": "application/x-www-form-urlencoded"},
        allow_redirects=True,
        timeout=20,
    )
    r.raise_for_status()

    # Detect login failure: if we ended up back at the login URL or see error indicators
    final_url = r.url
    log.info("Post-login URL: %s", final_url)

    if "auth/login" in final_url or "login" in final_url.lower():
        # Check for error messages in the page
        soup = BeautifulSoup(r.text, "html.parser")
        error_el = soup.find(class_=re.compile(r"error|alert|invalid|danger", re.I))
        err_msg = error_el.get_text(strip=True) if error_el else "Invalid credentials"
        raise ValueError(f"Login failed: {err_msg[:200]}")

    # Step 3: Extract user ID from the redirected page or fetch profile
    user_id = _extract_user_id_from_page(r.text)
    sc_name = _extract_display_name(r.text)

    # If we didn't land on profile page, fetch it explicitly
    if not user_id or "account/profile" not in final_url:
        log.info("Fetching profile page explicitly")
        pr = session.get(PROFILE_URL, headers=HEADERS, timeout=20)
        pr.raise_for_status()
        if "auth/login" in pr.url:
            raise ValueError("Session not established after login")
        user_id = _extract_user_id_from_page(pr.text) or user_id
        if not sc_name:
            sc_name = _extract_display_name(pr.text)

    if not user_id:
        # Try fetching the user's public profile link from the account page
        log.warning("Could not extract user ID from profile page HTML — trying account/settings")
        sr = session.get(f"{SC_BASE}/en/account/settings", headers=HEADERS, timeout=20)
        if sr.status_code == 200:
            user_id = _extract_user_id_from_page(sr.text)

    if not user_id:
        raise ValueError(
            "Login succeeded but could not determine Smoothcomp user ID from profile page. "
            "The page structure may have changed."
        )

    log.info("Smoothcomp login verified: user_id=%s name=%s", user_id, sc_name)
    return {
        "sc_user_id": user_id,
        "sc_name": sc_name,
        "email": email,
    }
