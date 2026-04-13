"""
payments.py — Stripe integration for mattrack.

Handles checkout session creation, webhook processing, access-code
generation, and access-code redemption.
"""

import os
import secrets
import string

import stripe
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://your-project.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "your-service-key")

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "your-webhook-secret")

PRICE_IDS: dict[str, str] = {
    "individual": os.environ.get("STRIPE_PRICE_INDIVIDUAL", "price_individual_placeholder"),
    "gym": os.environ.get("STRIPE_PRICE_GYM", "price_gym_placeholder"),
    "affiliate": os.environ.get("STRIPE_PRICE_AFFILIATE", "price_affiliate_placeholder"),
}

_CODE_ALPHABET = string.ascii_uppercase + string.digits
_CODE_SEGMENT_LEN = 4

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


def _update_user(user_id: str, updates: dict) -> None:
    """Apply *updates* to the public.users row for *user_id*. Fails silently."""
    try:
        client = _get_service_client()
        if client is None:
            return
        client.table("users").update(updates).eq("id", user_id).execute()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_checkout_session(
    user_id: str,
    email: str,
    plan: str,
    success_url: str,
    cancel_url: str,
) -> str | None:
    """
    Create a Stripe Checkout Session for the given *plan*.

    Returns the hosted checkout URL on success, or ``None`` on failure.

    :param user_id: Supabase auth user UUID — stored as metadata and
                    ``client_reference_id`` so the webhook can correlate it.
    :param email:   Pre-fill the customer email on the Stripe-hosted page.
    :param plan:    One of ``'individual'``, ``'gym'``, ``'affiliate'``.
    :param success_url: Redirect URL after successful payment.
    :param cancel_url:  Redirect URL when the user abandons the checkout.
    """
    try:
        price_id = PRICE_IDS.get(plan)
        if not price_id:
            return None

        session = stripe.checkout.Session.create(
            mode="subscription",
            client_reference_id=user_id,
            customer_email=email,
            line_items=[{"price": price_id, "quantity": 1}],
            metadata={"user_id": user_id, "plan": plan},
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return session.url
    except stripe.StripeError:
        return None
    except Exception:
        return None


def handle_webhook(payload: bytes, sig_header: str) -> tuple[bool, str]:
    """
    Verify and process an incoming Stripe webhook.

    :param payload:    Raw request body bytes.
    :param sig_header: Value of the ``Stripe-Signature`` HTTP header.
    :returns:          ``(True, 'ok')`` on success, ``(False, reason)`` on failure.
    """
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.errors.SignatureVerificationError:
        return False, "invalid signature"
    except Exception as exc:
        return False, str(exc)

    event_type: str = event["type"]
    data_obj = event["data"]["object"]

    try:
        if event_type == "checkout.session.completed":
            _handle_checkout_completed(data_obj)

        elif event_type == "customer.subscription.updated":
            _handle_subscription_updated(data_obj)

        elif event_type == "customer.subscription.deleted":
            _handle_subscription_deleted(data_obj)

        elif event_type == "invoice.payment_failed":
            _handle_payment_failed(data_obj)

    except Exception:
        # Log-worthy but don't return an error to Stripe (avoid retries for
        # events we've already partially processed).
        pass

    return True, "ok"


def _handle_checkout_completed(session: dict) -> None:
    user_id = (session.get("metadata") or {}).get("user_id") or session.get(
        "client_reference_id"
    )
    if not user_id:
        return

    plan = (session.get("metadata") or {}).get("plan", "individual")
    customer_id = session.get("customer")
    sub_id = session.get("subscription")

    _update_user(
        user_id,
        {
            "stripe_customer_id": customer_id,
            "stripe_sub_id": sub_id,
            "plan": plan,
            "sub_status": "active",
        },
    )


def _handle_subscription_updated(subscription: dict) -> None:
    sub_id = subscription.get("id")
    if not sub_id:
        return

    status = subscription.get("status", "")

    try:
        client = _get_service_client()
        if client is None:
            return
        client.table("users").update({"sub_status": status}).eq(
            "stripe_sub_id", sub_id
        ).execute()
    except Exception:
        pass


def _handle_subscription_deleted(subscription: dict) -> None:
    sub_id = subscription.get("id")
    if not sub_id:
        return

    try:
        client = _get_service_client()
        if client is None:
            return
        client.table("users").update(
            {"plan": "free", "sub_status": "canceled"}
        ).eq("stripe_sub_id", sub_id).execute()
    except Exception:
        pass


def _handle_payment_failed(invoice: dict) -> None:
    sub_id = invoice.get("subscription")
    if not sub_id:
        return

    try:
        client = _get_service_client()
        if client is None:
            return
        # Mark past_due but do NOT revoke access — give user time to update card.
        client.table("users").update({"sub_status": "past_due"}).eq(
            "stripe_sub_id", sub_id
        ).execute()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Access codes
# ---------------------------------------------------------------------------


def _random_code() -> str:
    """Return a random code in the format ``TRACK-XXXX``."""
    segment = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(_CODE_SEGMENT_LEN))
    return f"TRACK-{segment}"


def generate_access_codes(pack_id: str, count: int) -> list[str]:
    """
    Generate *count* unique access codes for *pack_id* and persist them.

    Codes follow the pattern ``TRACK-XXXX`` where X is an uppercase letter
    or digit.  Uniqueness is enforced by regenerating on collision (extremely
    rare at the scale we expect).

    Returns the list of generated code strings, or ``[]`` on any error.
    """
    try:
        client = _get_service_client()
        if client is None:
            return []

        # Fetch existing codes for this pack to avoid duplicates.
        existing_resp = (
            client.table("access_codes")
            .select("code")
            .eq("pack_id", pack_id)
            .execute()
        )
        existing: set[str] = {row["code"] for row in (existing_resp.data or [])}

        codes: list[str] = []
        while len(codes) < count:
            code = _random_code()
            if code not in existing and code not in codes:
                codes.append(code)

        rows = [{"code": c, "pack_id": pack_id} for c in codes]
        client.table("access_codes").insert(rows).execute()
        return codes
    except Exception:
        return []


def redeem_access_code(code: str, user_id: str) -> bool:
    """
    Validate and redeem *code* for *user_id*.

    Marks the code as redeemed and upgrades the user to the
    ``'individual'`` plan.

    Returns ``True`` on success, ``False`` if the code is invalid, already
    redeemed, or if any error occurs.
    """
    try:
        client = _get_service_client()
        if client is None:
            return False

        response = (
            client.table("access_codes")
            .select("id, redeemed_by")
            .eq("code", code)
            .single()
            .execute()
        )

        data = response.data
        if not data:
            return False

        if data.get("redeemed_by") is not None:
            return False

        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        # Conditional update: only succeeds if redeemed_by is still NULL.
        # This prevents a race where two requests both pass the check above.
        result = (
            client.table("access_codes")
            .update({"redeemed_by": user_id, "redeemed_at": now})
            .eq("id", data["id"])
            .is_("redeemed_by", "null")
            .execute()
        )

        if not (result.data):
            return False  # another request got there first

        _update_user(user_id, {"plan": "individual", "sub_status": "active"})
        return True
    except Exception:
        return False
