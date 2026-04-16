"""
meta_api.py — Meta (Facebook/Instagram) OAuth and Graph API client.

Provides:
  get_oauth_url(sc_uid, redirect_uri) -> str
  exchange_code(code, redirect_uri) -> dict (short-lived token info)
  get_long_lived_token(short_token) -> dict (60-day token info)
  get_instagram_user_id(token) -> str
  get_recent_posts(ig_user_id, token, limit=9) -> list of post dicts

App credentials are loaded from environment variables:
  META_APP_ID     (default: 2117140528845445)
  META_APP_SECRET (default: 50515783c9b95bc307a0efa4b915f735)
"""

import logging
import os

import requests

log = logging.getLogger("meta_api")

APP_ID     = os.environ.get("META_APP_ID",     "2117140528845445")
APP_SECRET = os.environ.get("META_APP_SECRET", "50515783c9b95bc307a0efa4b915f735")
GRAPH_BASE = "https://graph.facebook.com/v18.0"

# Scopes needed: instagram_basic to read media, pages_show_list to find pages
OAUTH_SCOPES = "instagram_basic,pages_show_list,instagram_content_publish"
OAUTH_BASE   = "https://www.facebook.com/dialog/oauth"


def get_oauth_url(sc_uid: str, redirect_uri: str) -> str:
    """Return the Facebook OAuth dialog URL. sc_uid is passed as state."""
    params = {
        "client_id":    APP_ID,
        "redirect_uri": redirect_uri,
        "scope":        OAUTH_SCOPES,
        "state":        str(sc_uid),
        "response_type": "code",
    }
    qs = "&".join(f"{k}={requests.utils.quote(str(v), safe='')}" for k, v in params.items())
    return f"{OAUTH_BASE}?{qs}"


def exchange_code(code: str, redirect_uri: str) -> dict:
    """
    Exchange OAuth code for a short-lived user access token.

    Returns:
        {"access_token": str, "token_type": str}
    """
    r = requests.get(
        f"{GRAPH_BASE}/oauth/access_token",
        params={
            "client_id":     APP_ID,
            "client_secret": APP_SECRET,
            "redirect_uri":  redirect_uri,
            "code":          code,
        },
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise ValueError(f"Token exchange error: {data['error'].get('message', data)}")
    log.info("Got short-lived token (type: %s)", data.get("token_type"))
    return data


def get_long_lived_token(short_token: str) -> dict:
    """
    Exchange a short-lived token for a long-lived token (~60 days).

    Returns:
        {"access_token": str, "token_type": str, "expires_in": int}
    """
    r = requests.get(
        f"{GRAPH_BASE}/oauth/access_token",
        params={
            "grant_type":        "fb_exchange_token",
            "client_id":         APP_ID,
            "client_secret":     APP_SECRET,
            "fb_exchange_token": short_token,
        },
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise ValueError(f"Long-lived token error: {data['error'].get('message', data)}")
    log.info("Got long-lived token (expires_in: %s s)", data.get("expires_in"))
    return data


def get_instagram_user_id(token: str) -> dict:
    """
    Find the Instagram Business/Creator account connected to this token.

    Returns:
        {"ig_user_id": str, "ig_username": str}

    Raises ValueError if no Instagram account is found.
    """
    # List pages the user manages
    r = requests.get(
        f"{GRAPH_BASE}/me/accounts",
        params={"access_token": token, "fields": "id,name,instagram_business_account"},
        timeout=20,
    )
    r.raise_for_status()
    pages = r.json().get("data", [])
    log.info("Found %d Facebook pages", len(pages))

    for page in pages:
        ig = page.get("instagram_business_account")
        if ig and ig.get("id"):
            ig_id = ig["id"]
            # Fetch username
            ur = requests.get(
                f"{GRAPH_BASE}/{ig_id}",
                params={"access_token": token, "fields": "id,username"},
                timeout=20,
            )
            ur.raise_for_status()
            ud = ur.json()
            return {"ig_user_id": ig_id, "ig_username": ud.get("username", "")}

    # Fallback: try /me with instagram_basic scope (personal IG accounts)
    r2 = requests.get(
        "https://graph.instagram.com/me",
        params={"access_token": token, "fields": "id,username"},
        timeout=20,
    )
    if r2.status_code == 200:
        d = r2.json()
        if d.get("id"):
            return {"ig_user_id": d["id"], "ig_username": d.get("username", "")}

    raise ValueError(
        "No Instagram Business account found. "
        "Make sure your Instagram is connected to a Facebook Page."
    )


def get_recent_posts(ig_user_id: str, token: str, limit: int = 9) -> list[dict]:
    """
    Fetch recent Instagram media posts.

    Returns list of:
        {
            "id": str,
            "media_type": str,  # IMAGE, VIDEO, CAROUSEL_ALBUM
            "media_url": str,
            "thumbnail_url": str,  # for videos
            "permalink": str,
            "timestamp": str,
        }
    """
    r = requests.get(
        f"{GRAPH_BASE}/{ig_user_id}/media",
        params={
            "access_token": token,
            "fields": "id,media_type,media_url,thumbnail_url,permalink,timestamp",
            "limit": limit,
        },
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise ValueError(f"Media fetch error: {data['error'].get('message', data)}")
    posts = data.get("data", [])
    log.info("Fetched %d Instagram posts for user %s", len(posts), ig_user_id)
    return posts
