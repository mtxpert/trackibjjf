"""
auth.py — JWT verification and plan lookup for mattrack.

All functions fail silently and return safe defaults so the app continues
to work if Supabase is unreachable.
"""

import os
from jose import jwt, JWTError
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://your-project.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "your-anon-key")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "your-service-key")
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "your-jwt-secret")

_service_client: Client | None = None


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

        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
        return payload
    except (JWTError, Exception):
        return None


def get_user_plan(user_id: str) -> str:
    """
    Look up the plan for *user_id* in ``public.users``.

    Returns one of ``'free'``, ``'individual'``, ``'gym'``, ``'affiliate'``.
    Falls back to ``'free'`` on any error (DB unavailable, user not found,
    unexpected schema, etc.).
    """
    try:
        client = _get_service_client()
        if client is None:
            return "free"

        response = (
            client.table("users")
            .select("plan")
            .eq("id", user_id)
            .single()
            .execute()
        )

        data = response.data
        if not data:
            return "free"

        plan = data.get("plan", "free")
        return plan if plan in ("free", "individual", "gym", "affiliate") else "free"
    except Exception:
        return "free"


def is_plan_active(user_id: str) -> bool:
    """
    Return ``True`` if *user_id* has an active paid subscription.

    Active means ``plan != 'free'`` **and** ``sub_status == 'active'``.
    Returns ``False`` on any error or if the user is on the free tier.
    """
    try:
        client = _get_service_client()
        if client is None:
            return False

        response = (
            client.table("users")
            .select("plan, sub_status")
            .eq("id", user_id)
            .single()
            .execute()
        )

        data = response.data
        if not data:
            return False

        return data.get("plan", "free") != "free" and data.get("sub_status") == "active"
    except Exception:
        return False
