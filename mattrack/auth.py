"""
auth.py — JWT verification and plan lookup for mattrack.

All functions fail silently and return safe defaults so the app continues
to work if Supabase is unreachable.
"""

import os
import time
import logging
import requests
from jose import jwt, JWTError, jwk
from jose.utils import base64url_decode
from supabase import create_client, Client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://your-project.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "your-anon-key")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "your-service-key")
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "your-jwt-secret")

_service_client: Client | None = None

# JWKS cache — refreshed every 24 hours
_jwks_cache: dict = {}
_jwks_fetched_at: float = 0.0
_JWKS_TTL = 86400  # 24 hours

def _prewarm_jwks() -> None:
    """Pre-fetch JWKS at startup in a background thread so the first auth call is fast."""
    import threading
    def _fetch():
        try:
            r = requests.get(f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json", timeout=10)
            if r.ok:
                global _jwks_cache, _jwks_fetched_at
                keys = r.json().get("keys", [])
                _jwks_cache = {k["kid"]: k for k in keys}
                _jwks_fetched_at = time.time()
                log.info("JWKS pre-warmed: %d keys", len(_jwks_cache))
        except Exception as e:
            log.warning("JWKS pre-warm failed: %s", e)
    threading.Thread(target=_fetch, daemon=True).start()


def _get_jwks() -> dict:
    """Return cached JWKS keys, refreshing if stale."""
    global _jwks_cache, _jwks_fetched_at
    if time.time() - _jwks_fetched_at > _JWKS_TTL or not _jwks_cache:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json", timeout=5
            )
            if r.ok:
                keys = r.json().get("keys", [])
                _jwks_cache = {k["kid"]: k for k in keys}
                _jwks_fetched_at = time.time()
        except Exception:
            pass
    return _jwks_cache


def _get_service_client() -> Client | None:
    """Return a cached Supabase service-role client, or None on error."""
    global _service_client
    if _service_client is not None:
        return _service_client
    try:
        _service_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        return _service_client
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_user_from_token(request) -> dict | None:
    """
    Extract and verify the JWT from the Authorization header.

    Returns the decoded payload dict (contains at minimum ``sub`` and
    ``email``) on success, or ``None`` on any failure (missing header,
    invalid/expired token, bad secret, etc.).
    """
    try:
        auth_header: str = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header.removeprefix("Bearer ").strip()
        if not token:
            return None

        # Get key ID from token header
        header = jwt.get_unverified_header(token)
        alg = header.get("alg", "HS256")
        kid = header.get("kid")

        if alg == "ES256" and kid:
            # Verify against Supabase JWKS (asymmetric key)
            jwks = _get_jwks()
            key_data = jwks.get(kid)
            if not key_data:
                log.warning("JWT kid=%s not found in JWKS (cached keys: %s)", kid, list(jwks.keys()))
                return None
            public_key = jwk.construct(key_data)
            payload = jwt.decode(
                token,
                public_key,
                algorithms=["ES256"],
                audience="authenticated",
            )
        else:
            # Fallback: HS256 with JWT secret
            payload = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                options={"verify_aud": False},
            )

        return payload
    except JWTError as e:
        log.warning("JWT verification failed: %s", e)
        return None
    except Exception as e:
        log.warning("get_user_from_token unexpected error: %s", e)
        return None


def _query_user_row(user_id: str, fields: str = "plan,sub_status") -> dict:
    """
    Fetch one row from public.users via the Supabase REST API directly.
    Uses requests with a 5-second timeout to avoid hanging the web worker.
    Falls back to empty dict on any error.
    """
    try:
        url = f"{SUPABASE_URL}/rest/v1/users?select={fields}&id=eq.{user_id}&limit=1"
        r = requests.get(url, headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        }, timeout=5)
        rows = r.json()
        return rows[0] if rows else {}
    except Exception as e:
        log.warning("_query_user_row error for user_id=%s: %s", user_id, e)
        return {}


def get_user_plan(user_id: str) -> str:
    """
    Look up the plan for *user_id* in ``public.users``.

    Returns one of ``'free'``, ``'individual'``, ``'gym'``, ``'affiliate'``.
    Falls back to ``'free'`` on any error (DB unavailable, user not found,
    unexpected schema, etc.).
    """
    try:
        data = _query_user_row(user_id, "plan")
        if not data:
            log.warning("get_user_plan: no row found for user_id=%s", user_id)
            return "free"
        plan = data.get("plan", "free")
        log.info("get_user_plan: user_id=%s plan=%s", user_id, plan)
        return plan if plan in ("free", "individual", "gym", "affiliate") else "free"
    except Exception as e:
        log.warning("get_user_plan error for user_id=%s: %s", user_id, e)
        return "free"


def is_plan_active(user_id: str) -> bool:
    """
    Return ``True`` if *user_id* has an active paid subscription.

    Active means ``plan != 'free'`` **and** ``sub_status == 'active'``.
    Returns ``False`` on any error or if the user is on the free tier.
    """
    try:
        data = _query_user_row(user_id, "plan,sub_status")
        if not data:
            return False
        return data.get("plan", "free") != "free" and data.get("sub_status") == "active"
    except Exception:
        return False
