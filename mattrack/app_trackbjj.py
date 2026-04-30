"""
trackbjj.net — BJJ Athlete Repository
Runs locally on port 5951, accessible at http://172.23.93.61:5951

Companion to mattrack.net (port 5950). Shares the same Supabase DB.
"""

import difflib
import os
import sys
import re
import unicodedata
import logging
import datetime

# Ensure the directory containing this file is on sys.path so local modules
# (auth.py, payments.py, etc.) are importable regardless of CWD.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template, request, g, redirect, url_for, flash, make_response
from dotenv import load_dotenv
from supabase import create_client
from auth import get_user_from_token, get_user_plan, is_plan_active
from payments import create_checkout_session, redeem_access_code

import ibjjf_api
import scrape_smoothcomp_verify
import meta_api
import ibjjf_rankings

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from jinja2 import ChoiceLoader, FileSystemLoader
app = Flask(__name__, template_folder="trackbjj/templates", static_folder="trackbjj/static")
# Also search templates/ (repo-relative) so {% include 'shared/header.html' %} works
app.jinja_loader = ChoiceLoader([
    app.jinja_loader,
    FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
])
app.secret_key = os.environ.get("SECRET_KEY", "trackbjj-dev-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MATTRACK_PORT = int(os.environ.get("MATTRACK_PORT", 5950))
TRACKBJJ_PORT = int(os.environ.get("PORT", 5951))
INSTAGRAM_CALLBACK_PATH = "/auth/instagram/callback"

# Single shared client — supabase-py is stateless (HTTP), safe to reuse
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def _base_url(port):
    """Return base URL. Uses HTTPS and no port when behind Render/proxy."""
    host = request.host.split(":")[0]
    if request.headers.get("X-Forwarded-Proto") == "https":
        return f"https://{host}"
    return f"http://{host}:{port}"


MATTRACK_PROD_URL  = os.environ.get("MATTRACK_URL",  "https://www.mattrack.net")
TRACKBJJ_PROD_URL  = os.environ.get("TRACKBJJ_URL",  "https://www.trackbjj.net")


@app.context_processor
def inject_urls():
    # In production (HTTPS), use canonical domain URLs
    if request.headers.get("X-Forwarded-Proto") == "https":
        site_url    = TRACKBJJ_PROD_URL
        sibling_url = MATTRACK_PROD_URL
    else:
        site_url    = _base_url(TRACKBJJ_PORT)
        sibling_url = _base_url(MATTRACK_PORT)
    return dict(
        trackbjj_url=site_url,
        mattrack_url=sibling_url,
        # shared/header.html variables
        site_name="TrackBJJ",
        site_subtitle="BJJ Athlete Repository",
        site_url=site_url,
        site_icon="",
        sibling_name="MatTrack",
        sibling_url=sibling_url,
        header_home_fn="",
        show_lang=True,
    )


def normalize(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


# ── Division fingerprinting ─────────────────────────────────────────────────────

BELT_MAP = {
    # Kids-tier colored belts (IBJJF + common youth systems). Mapped to their
    # own bucket values so belt_matches properly separates them from adult
    # belts — black vs yellow must NOT be treated as "missing/permissive".
    "yellow": "yellow", "amarela": "yellow", "amarelo": "yellow",
    "orange": "orange", "laranja": "orange",
    "green":  "green",  "verde":   "green",
    "grey":   "gray",   "gray":    "gray",   "cinza":   "gray",
    # Adult belts
    "white": "white", "beginner": "white",
    "blue": "blue", "intermediate": None,
    "purple": "purple", "brown": "brown",
    "black": "black", "coral": "black", "red": "black",
    "branca": "white", "branco": "white", "azul": "blue",
    "roxa": "purple", "roxo": "purple", "marrom": "brown",
    "preta": "black", "preto": "black",
}

AGE_MAP = {
    # Youth age brackets — IBJJF has Pee Wee / Mighty Mite / Peewee / Teen /
    # Juvenile. Mapped to 'youth' so they don't cross-match an adult
    # fingerprint via None-permissive age_matches.
    "pee wee": "youth", "peewee": "youth",
    "mighty mite": "youth",
    "kids": "youth", "juvenile": "youth", "teen": "youth",
    "menor": "youth", "infantil": "youth",
    "adult": "adult", "adults": "adult",
    "master 1": "m1", "master1": "m1", "masters (30": "m1",
    "master 2": "m2", "master2": "m2", "masters (35": "m2",
    "master 3": "m3", "master3": "m3", "masters (40": "m3",
    "master 4": "m4", "master4": "m4", "masters (45": "m4",
    "master 5": "m5", "master5": "m5", "masters (50": "m5", "master 5 (51": "m5",
    "master 6": "m6", "master6": "m6", "masters (55": "m6", "master 6 (57": "m6",
    "master 7": "m7", "master7": "m7", "masters (60": "m7",
}

# Deliberately does NOT include 'youth' — age_matches compares ranks and
# returns False when either side is outside this map, so youth rows are
# cleanly rejected when fingerprint is adult/master and vice-versa.
AGE_RANK  = {"adult": 0, "m1": 1, "m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6, "m7": 7}
# Same pattern — only adult belts get ranks, so kids colors (yellow, orange,
# green, gray) register as rank=-1 and fail belt_matches against any adult belt.
BELT_RANK = {"white": 0, "blue": 1, "purple": 2, "brown": 3, "black": 4}


WEIGHT_BUCKETS = {
    # Match LONGEST keys first (light feather before light, super heavy before heavy, etc.)
    "galo":          0, "rooster":        0,
    "pluma":         1, "galo-pluma":     1, "galo pluma": 1,
    "light feather": 1, "lightfeather":   1, "light-feather": 1,
    "feather":       2,
    "super heavy":   7, "superheavy":     7, "super-heavy":  7, "super pesado": 7,
    "ultra heavy":   8, "ultraheavy":     8, "ultra-heavy":  8, "pesadissimo":  8,
    "medium heavy":  5, "mediumheavy":    5, "medium-heavy": 5, "meio pesado":  5,
    "heavy":         6, "pesado":         6,
    "middle":        4, "medio":          4, "médio":        4,
    "light":         3, "leve":           3,
    "open class":    9, "openclass":      9, "absolute":     9, "open weight":  9,
}


def _extract_weight_bucket(d: str) -> int | None:
    """Pick the weight-class bucket, preferring longer keys so 'super heavy'
    wins over 'heavy' and 'light feather' wins over 'light' or 'feather'."""
    for key in sorted(WEIGHT_BUCKETS, key=len, reverse=True):
        if key in d:
            return WEIGHT_BUCKETS[key]
    return None


def parse_division(div: str) -> dict:
    if not div:
        return {}
    d = div.lower()
    belt   = next((v for k, v in BELT_MAP.items() if k in d), None)
    age    = next((v for k, v in AGE_MAP.items()  if k in d), None)
    gender = "female" if re.search(r"\bfem|\bwom|\bf\b", d) else "male"
    weight = _extract_weight_bucket(d)
    return {"belt": belt, "age": age, "gender": gender, "weight": weight}


def athlete_fingerprint(divisions: list) -> dict:
    belts, ages, genders, weights = [], [], [], []
    for div in divisions:
        fp = parse_division(div)
        if fp.get("belt"):  belts.append(fp["belt"])
        if fp.get("age"):   ages.append(fp["age"])
        if fp.get("weight") is not None: weights.append(fp["weight"])
        genders.append(fp.get("gender", "male"))
    def mode(lst): return max(set(lst), key=lst.count) if lst else None
    highest_belt = max(belts, key=lambda b: BELT_RANK.get(b, -1)) if belts else None
    return {"belt": highest_belt, "age": mode(ages), "gender": mode(genders), "weight": mode(weights)}


def weight_matches(fp_weight, row_weight, tolerance: int = 2) -> bool:
    """Return True if the weight buckets are within `tolerance` (or either is
    unknown). Open Class / Absolute (bucket 9) always matches since champions
    frequently enter absolute brackets across weights."""
    if fp_weight is None or row_weight is None:
        return True
    if fp_weight == 9 or row_weight == 9:
        return True
    return abs(fp_weight - row_weight) <= tolerance


def age_matches(athlete_age: str, div_age: str) -> bool:
    if not athlete_age or not div_age:
        return True
    if athlete_age == div_age:
        return True
    ra, rb = AGE_RANK.get(athlete_age, -1), AGE_RANK.get(div_age, -1)
    return ra >= 0 and rb >= 0 and abs(ra - rb) <= 1


def belt_matches(athlete_belt: str, div_belt: str) -> bool:
    if not athlete_belt or not div_belt:
        return True
    if athlete_belt == div_belt:
        return True
    ra = BELT_RANK.get(athlete_belt, -1)
    rb = BELT_RANK.get(div_belt, -1)
    return ra >= 0 and rb >= 0 and abs(ra - rb) <= 1


def first_name_score(a: str, b: str) -> float:
    fa = normalize(a).split()[0] if a else ""
    fb = normalize(b).split()[0] if b else ""
    if not fa or not fb:
        return 0.0
    if fa == fb:
        return 1.0
    if len(fa) == 1 and fb.startswith(fa): return 0.85
    if len(fb) == 1 and fa.startswith(fb): return 0.85
    if fa.startswith(fb) or fb.startswith(fa): return 0.80
    return difflib.SequenceMatcher(None, fa, fb).ratio()


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/api/auth/me")
def api_auth_me():
    """Return current user plan. Called on page load to restore session."""
    try:
        user = get_user_from_token(request)
        if not user:
            return jsonify({"plan": "free", "authenticated": False})
        email = user.get("email", "")
        plan = get_user_plan(user["sub"], email=email)
        return jsonify({
            "authenticated": True,
            "email": email,
            "plan": plan,
            "active": is_plan_active(user["sub"], email=email),
        })
    except Exception as e:
        log.error("api_auth_me error: %s", e, exc_info=True)
        return jsonify({"plan": "free", "authenticated": False, "error": str(e)}), 200


@app.route("/api/stripe/checkout", methods=["POST"])
def api_stripe_checkout():
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    data = request.json or {}
    plan = data.get("plan", "individual")
    if plan not in ("individual", "gym", "affiliate"):
        return jsonify({"error": "Invalid plan"}), 400
    base_url = os.environ.get("APP_URL", "https://www.trackbjj.net")
    try:
        url = create_checkout_session(
            user_id     = user["sub"],
            email       = user.get("email", ""),
            plan        = plan,
            success_url = f"{base_url}/?checkout=success",
            cancel_url  = f"{base_url}/?checkout=cancel",
        )
        return jsonify({"url": url})
    except Exception as e:
        log.error("checkout failed user=%s: %s", user.get("sub"), e)
        return jsonify({"error": "Failed to create checkout session"}), 500


@app.route("/api/billing/portal", methods=["POST"])
def api_billing_portal():
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    try:
        import stripe
        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
        row = sb.table("users").select("stripe_customer_id").eq("id", user["sub"]).single().execute()
        customer_id = (row.data or {}).get("stripe_customer_id")
        if not customer_id:
            return jsonify({"error": "No billing account found"}), 404
        base_url = os.environ.get("APP_URL", "https://www.trackbjj.net")
        portal = stripe.billing_portal.Session.create(
            customer   = customer_id,
            return_url = base_url,
        )
        return jsonify({"url": portal.url})
    except Exception as e:
        log.error("billing portal failed user=%s: %s", user.get("sub"), e)
        return jsonify({"error": "Failed to open billing portal"}), 500


@app.route("/api/gym/redeem", methods=["POST"])
def api_gym_redeem():
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    code = (request.json or {}).get("code", "").strip().upper()
    if not code:
        return jsonify({"error": "Code required"}), 400
    ok = redeem_access_code(code, user["sub"])
    if ok:
        return jsonify({"success": True, "plan": "individual"})
    return jsonify({"error": "Invalid or already used code"}), 400


@app.route("/api/gym/codes")
def api_gym_codes():
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    try:
        packs = sb.table("gym_packs").select("id,school_name,max_codes,sub_status,plan").eq("owner_id", user["sub"]).execute()
        result = []
        for pack in (packs.data or []):
            codes = sb.table("access_codes").select("code,redeemed_by,redeemed_at,created_at").eq("pack_id", pack["id"]).execute()
            result.append({**pack, "codes": codes.data or []})
        return jsonify(result)
    except Exception as e:
        log.error("gym codes fetch failed user=%s: %s", user.get("sub"), e)
        return jsonify({"error": "Failed to load codes"}), 500


@app.route("/")
def index():
    return render_template("trackbjj/index.html")


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    return render_template("trackbjj/search.html", q=q)


@app.route("/findme", methods=["GET", "POST"])
def findme():
    """Auth-based lookup — user logs in with IBJJF and/or Smoothcomp to
    auto-verify their IDs. We find them, link them in sc_ibjjf_verified,
    and redirect to their profile. If we can't find them despite verified
    IDs, save a report for nightly Claude review.
    """
    from flask import session

    if request.method == "GET":
        return render_template(
            "trackbjj/findme.html",
            verified_ibjjf_id=session.get("findme_ibjjf_id"),
            verified_ibjjf_name=session.get("findme_ibjjf_name"),
            verified_sc_uid=session.get("findme_sc_uid"),
            verified_sc_email=session.get("findme_sc_email"),
        )

    action = (request.form.get("action") or "").strip()

    # --- IBJJF auth ---
    if action == "ibjjf":
        email = (request.form.get("email") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not email or not password:
            flash("Enter IBJJF email and password.", "error")
            return redirect(url_for("findme"))
        try:
            athlete_id, token = ibjjf_api.login(email, password)
            profile = ibjjf_api.get_athlete_profile(token)
            ibjjf_name = profile.get("name") or profile.get("display_name") or ""
        except Exception as e:
            flash(f"IBJJF login failed: {e}", "error")
            return redirect(url_for("findme"))
        # Upsert their ibjjf_athletes row so they exist in our directory even
        # if they're not in the top-ranked cache we scrape
        try:
            sb.table("ibjjf_athletes").upsert({
                "ibjjf_id":    str(athlete_id),
                "name":        ibjjf_name,
                "name_lower":  (ibjjf_name or "").lower(),
                "belt":        (profile.get("belt") or "").lower() or None,
                "academy":     profile.get("academy") or profile.get("team") or None,
                "gender":      profile.get("gender") or None,
            }, on_conflict="ibjjf_id").execute()
        except Exception as e:
            log.warning("findme ibjjf_athletes upsert failed: %s", e)
        session["findme_ibjjf_id"] = str(athlete_id)
        session["findme_ibjjf_name"] = ibjjf_name
        return _findme_resolve(session)

    # --- Smoothcomp auth ---
    if action == "smoothcomp":
        email = (request.form.get("email") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not email or not password:
            flash("Enter Smoothcomp email and password.", "error")
            return redirect(url_for("findme"))
        try:
            result = scrape_smoothcomp_verify.verify_sc_login(email, password)
        except Exception as e:
            flash(f"Smoothcomp login failed: {e}", "error")
            return redirect(url_for("findme"))
        session["findme_sc_uid"] = str(result["sc_user_id"])
        session["findme_sc_email"] = result.get("email") or email
        return _findme_resolve(session)

    # --- Reset / clear ---
    if action == "reset":
        for k in ("findme_ibjjf_id", "findme_ibjjf_name",
                  "findme_sc_uid", "findme_sc_email"):
            session.pop(k, None)
        return redirect(url_for("findme"))

    return redirect(url_for("findme"))


def _findme_resolve(session):
    """After an auth step, check if we have enough info to find the athlete.
    Saves a findme_reports entry with the verified ID(s). Marks the report
    `resolved` if we can auto-route the user to a profile, otherwise leaves it
    `pending` for the nightly agent."""
    ibjjf_id = session.get("findme_ibjjf_id")
    sc_uid = session.get("findme_sc_uid")

    report_id = None
    try:
        ins_res = sb.table("findme_reports").insert({
            "name":       session.get("findme_ibjjf_name") or None,
            "ibjjf_id":   ibjjf_id or None,
            "sc_uid":     sc_uid or None,
            "email":      session.get("findme_sc_email") or None,
            "user_agent": request.headers.get("User-Agent", "")[:500],
            "ip":         request.headers.get("X-Forwarded-For", "").split(",")[0].strip(),
            "status":     "pending",
            "resolution_notes": "verified auth: " + ",".join(
                filter(None, ["ibjjf" if ibjjf_id else "", "sc" if sc_uid else ""])
            ),
        }).execute()
        if ins_res.data:
            report_id = ins_res.data[0].get("id")
    except Exception as e:
        log.warning("findme report save failed: %s", e)

    def _mark_resolved(notes):
        if not report_id:
            return
        try:
            sb.table("findme_reports").update({
                "status": "resolved",
                "resolution_notes": notes,
            }).eq("id", report_id).execute()
        except Exception as e:
            log.warning("findme report mark resolved failed: %s", e)

    def _cleanup_session():
        for k in ("findme_ibjjf_id", "findme_ibjjf_name",
                  "findme_sc_uid", "findme_sc_email",
                  "claim_intent_ibjjf_id", "claim_intent_sc_uid"):
            session.pop(k, None)

    # If both IDs: link them immediately and go to profile
    if ibjjf_id and sc_uid:
        try:
            sb.table("sc_ibjjf_verified").upsert({
                "sc_uid": sc_uid,
                "ibjjf_athlete_id": ibjjf_id,
                "ibjjf_name": session.get("findme_ibjjf_name") or "",
            }, on_conflict="sc_uid").execute()
            _mark_resolved("auto-linked sc_uid ↔ ibjjf_id on auth")
        except Exception as e:
            log.warning("findme link failed: %s", e)
            _mark_resolved(f"link attempted but failed: {e}")
        _cleanup_session()
        return redirect(url_for("athlete_profile", sc_uid=sc_uid))

    # SC alone — if we have any SC tournament data, go to profile
    if sc_uid and not ibjjf_id:
        try:
            sb.table("sc_smoothcomp_verified").upsert({
                "sc_uid": sc_uid,
                "sc_user_id": sc_uid,
                "sc_email": session.get("findme_sc_email") or "",
                "sc_name": "",
            }, on_conflict="sc_uid").execute()
        except Exception:
            pass
        try:
            sc_res = (sb.table("tournament_results")
                        .select("athlete_id")
                        .eq("source", "smoothcomp")
                        .eq("athlete_id", sc_uid)
                        .limit(1).execute())
            if sc_res.data:
                _mark_resolved("SC-only resolved via sc_uid → profile")
                _cleanup_session()
                return redirect(url_for("athlete_profile", sc_uid=sc_uid))
        except Exception:
            pass

    # IBJJF alone — route to linked sc_uid if any, else slim IBJJF profile
    if ibjjf_id and not sc_uid:
        try:
            v_res = (sb.table("sc_ibjjf_verified")
                       .select("sc_uid")
                       .eq("ibjjf_athlete_id", ibjjf_id)
                       .limit(1).execute())
            if v_res.data:
                sc = v_res.data[0]["sc_uid"]
                _mark_resolved("IBJJF-only resolved via existing link")
                _cleanup_session()
                return redirect(url_for("athlete_profile", sc_uid=sc))
        except Exception:
            pass
        _mark_resolved("IBJJF-only profile — no SC link yet")
        target = ibjjf_id
        _cleanup_session()
        return redirect(url_for("ibjjf_athlete_profile", ibjjf_id=target))

    # Not resolvable — show the page with verified-so-far state
    return redirect(url_for("findme"))


@app.route("/claim-me", methods=["GET", "POST"])
def claim_me():
    """Unified claim-by-auth entrypoint. Takes an intent (?ibjjf_id=... &sc_uid=...)
    and verifies the user is the rightful owner via IBJJF and/or Smoothcomp login.
    Rejects claim-jacking: auth'd IDs must match the intent, and an IBJJF/SC ID
    already linked to a different profile cannot be re-linked."""
    from flask import session

    intent_ibjjf = request.args.get("ibjjf_id")
    intent_sc = request.args.get("sc_uid")
    if intent_ibjjf:
        session["claim_intent_ibjjf_id"] = str(intent_ibjjf)
    if intent_sc:
        session["claim_intent_sc_uid"] = str(intent_sc)
    intent_ibjjf = session.get("claim_intent_ibjjf_id")
    intent_sc = session.get("claim_intent_sc_uid")

    if request.method == "GET":
        intent_info = {}
        if intent_ibjjf:
            try:
                ia_res = (sb.table("ibjjf_athletes")
                            .select("ibjjf_id,name,belt,academy")
                            .eq("ibjjf_id", str(intent_ibjjf))
                            .limit(1).execute())
                if ia_res.data:
                    intent_info["ibjjf"] = ia_res.data[0]
            except Exception:
                pass
            intent_info.setdefault("ibjjf", {"ibjjf_id": intent_ibjjf})
        if intent_sc:
            intent_info["sc"] = {"sc_uid": intent_sc}
        return render_template(
            "trackbjj/claim_me.html",
            intent=intent_info,
            verified_ibjjf_id=session.get("findme_ibjjf_id"),
            verified_ibjjf_name=session.get("findme_ibjjf_name"),
            verified_sc_uid=session.get("findme_sc_uid"),
        )

    action = (request.form.get("action") or "").strip()

    if action == "ibjjf":
        email = (request.form.get("email") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not email or not password:
            flash("Enter IBJJF email and password.", "error")
            return redirect(url_for("claim_me"))
        try:
            athlete_id, token = ibjjf_api.login(email, password)
            profile = ibjjf_api.get_athlete_profile(token)
            ibjjf_name = profile.get("name") or profile.get("display_name") or ""
        except Exception as e:
            flash(f"IBJJF login failed: {e}", "error")
            return redirect(url_for("claim_me"))

        # Intent mismatch — user logged in as a different IBJJF account
        if intent_ibjjf and str(athlete_id) != str(intent_ibjjf):
            flash(
                f"You logged in as IBJJF #{athlete_id}, but this claim is for "
                f"IBJJF #{intent_ibjjf}. Claim rejected.",
                "error",
            )
            return redirect(url_for("claim_me"))

        # Conflict — this IBJJF id is already linked to a different sc_uid
        try:
            existing = (sb.table("sc_ibjjf_verified")
                          .select("sc_uid")
                          .eq("ibjjf_athlete_id", str(athlete_id))
                          .limit(1).execute())
        except Exception:
            existing = None
        if existing and existing.data:
            linked_sc = str(existing.data[0]["sc_uid"])
            if intent_sc and linked_sc != str(intent_sc):
                flash(
                    "That IBJJF account is already claimed on a different profile. "
                    "If this is a mistake, contact support.",
                    "error",
                )
                return redirect(url_for("claim_me"))

        try:
            sb.table("ibjjf_athletes").upsert({
                "ibjjf_id":    str(athlete_id),
                "name":        ibjjf_name,
                "name_lower":  (ibjjf_name or "").lower(),
                "belt":        (profile.get("belt") or "").lower() or None,
                "academy":     profile.get("academy") or profile.get("team") or None,
                "gender":      profile.get("gender") or None,
            }, on_conflict="ibjjf_id").execute()
        except Exception as e:
            log.warning("claim_me ibjjf_athletes upsert failed: %s", e)

        session["findme_ibjjf_id"] = str(athlete_id)
        session["findme_ibjjf_name"] = ibjjf_name
        # Trust the sc_uid from intent (came from a searched profile card): if
        # that sc_uid isn't already linked to a DIFFERENT ibjjf_id, treat it as
        # the target for linking on this claim.
        if intent_sc and not session.get("findme_sc_uid"):
            try:
                sc_existing = (sb.table("sc_ibjjf_verified")
                                 .select("ibjjf_athlete_id")
                                 .eq("sc_uid", str(intent_sc))
                                 .limit(1).execute())
            except Exception:
                sc_existing = None
            conflict = False
            if sc_existing and sc_existing.data:
                linked_ibjjf = str(sc_existing.data[0].get("ibjjf_athlete_id") or "")
                if linked_ibjjf and linked_ibjjf != str(athlete_id):
                    flash(
                        "That profile is already linked to a different IBJJF account.",
                        "error",
                    )
                    conflict = True
            if conflict:
                return redirect(url_for("claim_me"))
            session["findme_sc_uid"] = str(intent_sc)
        return _findme_resolve(session)

    if action == "smoothcomp":
        email = (request.form.get("email") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not email or not password:
            flash("Enter Smoothcomp email and password.", "error")
            return redirect(url_for("claim_me"))
        try:
            result = scrape_smoothcomp_verify.verify_sc_login(email, password)
        except Exception as e:
            flash(f"Smoothcomp login failed: {e}", "error")
            return redirect(url_for("claim_me"))

        sc_authed = str(result["sc_user_id"])
        if intent_sc and sc_authed != str(intent_sc):
            flash(
                f"You logged in as SC #{sc_authed}, but this claim is for "
                f"SC #{intent_sc}. Claim rejected.",
                "error",
            )
            return redirect(url_for("claim_me"))

        try:
            existing = (sb.table("sc_ibjjf_verified")
                          .select("ibjjf_athlete_id")
                          .eq("sc_uid", sc_authed)
                          .limit(1).execute())
        except Exception:
            existing = None
        if existing and existing.data and intent_ibjjf:
            linked_ibjjf = str(existing.data[0]["ibjjf_athlete_id"] or "")
            if linked_ibjjf and linked_ibjjf != str(intent_ibjjf):
                flash(
                    "That Smoothcomp account is already linked to a different IBJJF profile.",
                    "error",
                )
                return redirect(url_for("claim_me"))

        session["findme_sc_uid"] = sc_authed
        session["findme_sc_email"] = result.get("email") or email
        # If we have an IBJJF intent (came from searched row), trust it for
        # linking provided it isn't already bound to a different sc_uid.
        if intent_ibjjf and not session.get("findme_ibjjf_id"):
            try:
                ib_existing = (sb.table("sc_ibjjf_verified")
                                 .select("sc_uid")
                                 .eq("ibjjf_athlete_id", str(intent_ibjjf))
                                 .limit(1).execute())
            except Exception:
                ib_existing = None
            conflict = False
            if ib_existing and ib_existing.data:
                linked_sc = str(ib_existing.data[0].get("sc_uid") or "")
                if linked_sc and linked_sc != sc_authed:
                    flash(
                        "That IBJJF profile is already claimed on a different account.",
                        "error",
                    )
                    conflict = True
            if conflict:
                return redirect(url_for("claim_me"))
            session["findme_ibjjf_id"] = str(intent_ibjjf)
            try:
                ia_res = (sb.table("ibjjf_athletes")
                            .select("name")
                            .eq("ibjjf_id", str(intent_ibjjf))
                            .limit(1).execute())
                if ia_res.data:
                    session["findme_ibjjf_name"] = ia_res.data[0].get("name") or ""
            except Exception:
                pass
        return _findme_resolve(session)

    if action == "reset":
        for k in ("claim_intent_ibjjf_id", "claim_intent_sc_uid",
                  "findme_ibjjf_id", "findme_ibjjf_name",
                  "findme_sc_uid", "findme_sc_email"):
            session.pop(k, None)
        return redirect(url_for("findme"))

    return redirect(url_for("claim_me"))


# ── Team / Event URL routes ────────────────────────────────────────────────────

SUPABASE_PROJECT_REF = os.environ.get("SUPABASE_PROJECT_REF", "kzqvfuqxtbrhlgphyntb")


def _rest_get(path: str, params: dict = None, limit: int = None):
    """Query Supabase PostgREST with service role key. Returns list of rows."""
    import requests as _req
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Accept": "application/json",
    }
    if limit:
        headers["Range"] = f"0-{limit - 1}"
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{path}"
    r = _req.get(url, params=params or {}, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json() or []


def team_slug(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


app.jinja_env.filters["team_slug"] = team_slug


def _sql_escape(s: str) -> str:
    return (s or "").replace("'", "''")


@app.route("/team/<team_slug_in>")
def team_profile(team_slug_in):
    try:
        return _team_profile_inner(team_slug_in)
    except Exception:
        import traceback as _tb
        log.error("team_profile 500 for slug=%s:\n%s", team_slug_in, _tb.format_exc())
        raise


def _team_profile_inner(team_slug_in):
    slug = team_slug(team_slug_in)
    if not slug:
        return render_template("trackbjj/not_found.html"), 404

    # Fuzzy-match teams via ilike (PostgREST uses * for wildcards, not %),
    # then filter to exact slug match client-side.
    pattern = slug.replace("-", "*")
    import requests as _req
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Accept": "application/json",
        "Range": "0-4999",
    }
    q_url = (f"{SUPABASE_URL.rstrip('/')}/rest/v1/tournament_results"
             f"?select=team&team=ilike.*{pattern}*")
    resp = _req.get(q_url, headers=headers, timeout=15)
    rows = resp.json() if resp.ok else []
    if not isinstance(rows, list):
        log.error("team ilike query returned non-list: %s", str(rows)[:200])
        rows = []

    # Filter client-side to exact slug match
    team_counts: dict[str, int] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        t = (r.get("team") or "").strip()
        if not t:
            continue
        if team_slug(t) == slug:
            team_counts[t] = team_counts.get(t, 0) + 1

    if not team_counts:
        return render_template("trackbjj/not_found.html",
                               message=f"No team matches \u201C{team_slug_in}\u201D."), 404

    team_names = sorted(team_counts.keys(), key=lambda x: -team_counts[x])
    canonical_name = team_names[0]
    teams = [{"team": n, "result_count": team_counts[n]} for n in team_names]

    # Fetch athletes from matching teams (use "in.(name1,name2)" filter)
    team_filter = "(" + ",".join(f'"{n.replace(chr(34), chr(92)+chr(34))}"' for n in team_names) + ")"
    ath_resp = _req.get(
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/tournament_results",
        params={
            "select": "athlete_id,athlete_display,athlete_name,event_date,placement,source,team",
            "team": f"in.{team_filter}",
            "source": "eq.smoothcomp",
            "athlete_id": "not.is.null",
        },
        headers={**headers, "Range": "0-19999"},
        timeout=30,
    )
    ath_rows = ath_resp.json() if ath_resp.ok else []
    if not isinstance(ath_rows, list):
        log.error("team athletes query returned non-list: %s", str(ath_rows)[:200])
        ath_rows = []

    # Aggregate athletes client-side
    by_ath: dict = {}
    for r in ath_rows:
        aid = r.get("athlete_id")
        if not aid:
            continue
        if aid not in by_ath:
            by_ath[aid] = {
                "athlete_id": aid,
                "display_name": r.get("athlete_display") or r.get("athlete_name") or "",
                "result_count": 0, "gold": 0, "silver": 0, "bronze": 0,
                "last_seen": "", "sources": set(),
            }
        entry = by_ath[aid]
        entry["result_count"] += 1
        pl = r.get("placement")
        if pl == 1: entry["gold"] += 1
        elif pl == 2: entry["silver"] += 1
        elif pl == 3: entry["bronze"] += 1
        ed = r.get("event_date") or ""
        if ed > entry["last_seen"]:
            entry["last_seen"] = ed
        if r.get("source"):
            entry["sources"].add(r["source"])
    athletes = sorted(
        by_ath.values(),
        key=lambda x: (x["last_seen"] or "", x["result_count"]),
        reverse=True,
    )[:500]
    for a in athletes:
        a["sources"] = ",".join(sorted(a["sources"]))

    # Recent wins (placement=1)
    win_resp = _req.get(
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/tournament_results",
        params={
            "select": "source,event_id,event_title,event_date,division,placement,athlete_id,athlete_display,athlete_name",
            "team": f"in.{team_filter}",
            "placement": "eq.1",
            "event_date": "not.is.null",
            "order": "event_date.desc",
        },
        headers={**headers, "Range": "0-29"},
        timeout=15,
    )
    win_rows = win_resp.json() if win_resp.ok else []
    if not isinstance(win_rows, list):
        win_rows = []
    recent_wins = [
        {**r, "display_name": r.get("athlete_display") or r.get("athlete_name") or ""}
        for r in win_rows if isinstance(r, dict)
    ]

    total_results = sum(team_counts.values())

    return render_template(
        "trackbjj/team.html",
        team_slug=slug,
        team_name=canonical_name,
        team_variants=team_names,
        athletes=athletes,
        recent_wins=recent_wins,
        athlete_count=len(athletes),
        total_results=total_results,
        now_year=datetime.date.today().year,
    )


@app.route("/event/<source>/<event_id>")
def event_profile(source, event_id):
    try:
        return _event_profile_inner(source, event_id)
    except Exception:
        import traceback as _tb
        log.error("event_profile 500 for %s/%s:\n%s", source, event_id, _tb.format_exc())
        raise


def _event_profile_inner(source, event_id):
    import requests as _req
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Accept": "application/json",
    }

    # Fetch the full roster (includes event_title/event_date)
    roster = _req.get(
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/tournament_results",
        params={
            "select": "athlete_id,athlete_display,athlete_name,team,division,placement,status,event_title,event_date",
            "source": f"eq.{source}",
            "event_id": f"eq.{event_id}",
            "order": "placement.asc.nullslast,division.asc.nullslast",
        },
        headers={**headers, "Range": "0-4999"},
        timeout=30,
    ).json() or []

    if not roster:
        return render_template("trackbjj/not_found.html",
                               message=f"No event matches {source}/{event_id}."), 404

    # Derive event metadata from most common values in roster
    titles: dict[str, int] = {}
    dates: dict[str, int] = {}
    for r in roster:
        t = r.get("event_title")
        d = r.get("event_date")
        if t: titles[t] = titles.get(t, 0) + 1
        if d: dates[d] = dates.get(d, 0) + 1
    event_title = max(titles.items(), key=lambda x: x[1])[0] if titles else "Event"
    event_date = max(dates.items(), key=lambda x: x[1])[0] if dates else None

    team_counts: dict[str, int] = {}
    for r in roster:
        t = (r.get("team") or "").strip()
        if t:
            team_counts[t] = team_counts.get(t, 0) + 1
    top_teams = sorted(team_counts.items(), key=lambda x: -x[1])[:10]

    external_url = None
    if source == "smoothcomp":
        external_url = f"https://smoothcomp.com/en/event/{event_id}"
    elif source == "ibjjf":
        external_url = f"https://www.ibjjf.com/events/{event_id}"

    has_results = any(r.get("placement") for r in roster)

    return render_template(
        "trackbjj/event.html",
        source=source,
        event_id=event_id,
        event_title=event_title,
        event_date=event_date,
        roster=roster,
        athlete_count=len(roster),
        has_results=has_results,
        top_teams=top_teams,
        external_url=external_url,
        team_slug_fn=team_slug,
    )


@app.route("/ibjjf-athlete/<ibjjf_id>")
def ibjjf_athlete_profile(ibjjf_id):
    """Slim profile for IBJJF-only athletes (no sc_uid yet).
    If this IBJJF ID is already linked via sc_ibjjf_verified, 302 to the
    canonical sc_uid-keyed profile. Otherwise render a Claim CTA page.
    """
    # If already linked, redirect to the full profile
    try:
        v_res = (sb.table("sc_ibjjf_verified")
                   .select("sc_uid")
                   .eq("ibjjf_athlete_id", str(ibjjf_id))
                   .limit(1).execute())
        if v_res.data:
            return redirect(url_for("athlete_profile", sc_uid=v_res.data[0]["sc_uid"]))
    except Exception as e:
        log.warning("ibjjf_athlete_profile verified lookup failed: %s", e)

    # Fetch the IBJJF athlete record
    try:
        ia_res = (sb.table("ibjjf_athletes")
                    .select("ibjjf_id,name,slug,belt,academy,points,ranking_category,age_division,gi_nogi,gender")
                    .eq("ibjjf_id", str(ibjjf_id))
                    .limit(1).execute())
    except Exception as e:
        log.warning("ibjjf_athlete_profile ibjjf_athletes lookup failed: %s", e)
        ia_res = None
    if not (ia_res and ia_res.data):
        return render_template("trackbjj/not_found.html",
                               message=f"No IBJJF athlete found with ID {ibjjf_id}."), 404
    ia = ia_res.data[0]

    # Results for this IBJJF ID
    try:
        results_res = (sb.table("tournament_results")
                         .select("event_title,event_date,division,placement,team,event_id,status")
                         .eq("source", "ibjjf")
                         .eq("ibjjf_athlete_id", str(ibjjf_id))
                         .or_("status.is.null,status.neq.registered")
                         .order("event_date", desc=True)
                         .limit(500)
                         .execute())
        results = results_res.data or []
    except Exception as e:
        log.warning("ibjjf_athlete_profile results lookup failed: %s", e)
        results = []

    gold   = sum(1 for r in results if r.get("placement") == 1)
    silver = sum(1 for r in results if r.get("placement") == 2)
    bronze = sum(1 for r in results if r.get("placement") == 3)
    stats = {"events": len({r.get("event_id") for r in results}),
             "divisions": len(results),
             "gold": gold, "silver": silver, "bronze": bronze}

    return render_template(
        "trackbjj/ibjjf_athlete.html",
        ibjjf_id=ia.get("ibjjf_id"),
        name=ia.get("name") or "IBJJF Athlete",
        belt=(ia.get("belt") or "").title(),
        academy=ia.get("academy") or "",
        slug=ia.get("slug") or "",
        points=ia.get("points"),
        ranking_category=ia.get("ranking_category") or "",
        age_division=ia.get("age_division") or "",
        gi_nogi=ia.get("gi_nogi") or "",
        gender=ia.get("gender") or "",
        results=results,
        stats=stats,
        now_year=datetime.date.today().year,
    )


@app.route("/athlete-by-name/<path:name>")
def athlete_by_name_profile(name):
    """Slim profile for athletes with no sc_uid and no ibjjf_athletes row.
    Looks up tournament_results by case-insensitive athlete_name match.
    Backed by idx_tr_name_lower on LOWER(athlete_name) for fast lookups.
    """
    name = (name or "").strip()
    if not name:
        return render_template("trackbjj/not_found.html",
                               message="No athlete name provided."), 404

    try:
        results_res = (sb.table("tournament_results")
                         .select("source,event_title,event_date,division,placement,team,event_id,status,athlete_name")
                         .ilike("athlete_name", name)
                         .or_("status.is.null,status.neq.registered")
                         .order("event_date", desc=True)
                         .limit(500)
                         .execute())
        results = results_res.data or []
    except Exception as e:
        log.warning("athlete_by_name_profile results lookup failed: %s", e)
        results = []

    if not results:
        return render_template("trackbjj/not_found.html",
                               message=f'No results found for "{name}".'), 404

    display_name = next((r["athlete_name"] for r in results if r.get("athlete_name")), name)
    team = next((r["team"] for r in results if r.get("team")), "")

    gold   = sum(1 for r in results if r.get("placement") == 1)
    silver = sum(1 for r in results if r.get("placement") == 2)
    bronze = sum(1 for r in results if r.get("placement") == 3)
    stats = {"events": len({r.get("event_id") for r in results if r.get("event_id")}),
             "divisions": len(results),
             "gold": gold, "silver": silver, "bronze": bronze}

    return render_template(
        "trackbjj/ibjjf_athlete.html",
        ibjjf_id=None,
        name=display_name,
        belt="",
        academy=team,
        slug="",
        points=None,
        ranking_category="",
        age_division="",
        gi_nogi="",
        gender="",
        results=results,
        stats=stats,
        hint_name=display_name,
        now_year=datetime.date.today().year,
    )


@app.route("/athlete/<sc_uid>")
def athlete_profile(sc_uid):
    """Athlete profile page anchored by Smoothcomp user_id."""
    try:
        return _athlete_profile_inner(sc_uid)
    except Exception:
        import traceback as _tb
        log.error("athlete_profile 500 for sc_uid=%s:\n%s", sc_uid, _tb.format_exc())
        raise


def _athlete_profile_inner(sc_uid):
    # Get all Smoothcomp rows for this user_id
    sc_res = (sb.table("tournament_results")
               .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,athlete_id,event_id")
               .eq("source", "smoothcomp")
               .eq("athlete_id", str(sc_uid))
               .order("event_date", desc=True)
               .execute())
    sc_rows = sc_res.data

    if not sc_rows:
        return render_template("trackbjj/not_found.html"), 404

    # Derive identity from SC rows
    names = list({r["athlete_display"] or r["athlete_name"] for r in sc_rows})
    display_name = max(names, key=len)
    norm_display = normalize(display_name)
    parts      = norm_display.split()
    last_name  = parts[-1] if parts else ""
    first_name = parts[0]  if parts else ""

    # Build fingerprint from SC divisions FIRST — we need it to belt-filter
    # the team lookup below (stops another "Roiter Lima" White-Belt Master 1
    # registration from overwriting the Black-Belt Adult's team).
    fp = athlete_fingerprint([r["division"] for r in sc_rows if r["division"]])
    # Also defined once here so both upcoming_rows and the fallback query
    # below can gate on it — no signal means we can't fuzzy-match safely.
    fp_has_signal = (fp.get("belt") is not None
                     or fp.get("age") is not None
                     or fp.get("weight") is not None)

    def _date_int(d):
        try:
            return int((d or "0000-00-00").replace("-", ""))
        except (ValueError, AttributeError):
            return 0

    # Team from most recent registration/result. Pulls by last_name, then
    # drops rows whose division belt doesn't match the SC fingerprint so
    # same-name, different-belt people don't pollute the team display.
    team_res = (sb.table("tournament_results")
                 .select("team,status,event_date,division,athlete_name")
                 .ilike("athlete_name", f"%{last_name}%")
                 .not_.is_("team", "null")
                 .order("event_date", desc=True)
                 .limit(80)
                 .execute())
    team_rows_raw = []
    for r in (team_res.data or []):
        row_name = normalize(r.get("athlete_name") or "")
        if first_name_score(first_name, row_name) < 0.70:
            continue
        if fp.get("belt"):
            row_fp = parse_division(r.get("division") or "")
            if row_fp.get("belt") and not belt_matches(fp["belt"], row_fp["belt"]):
                continue
        team_rows_raw.append(r)
    team_rows_raw.sort(key=lambda r: (0 if r.get("status") == "registered" else 1,
                                      -_date_int(r.get("event_date"))))
    team = team_rows_raw[0]["team"] if team_rows_raw else next(
        (r["team"] for r in sc_rows if r["team"]), "Unknown"
    )

    # Check for manually verified claim first
    verified_res = (sb.table("sc_ibjjf_verified")
                     .select("*")
                     .eq("sc_uid", sc_uid)
                     .execute())
    verified_claim = verified_res.data[0] if verified_res.data else None
    profile_photo_url = (verified_claim or {}).get("photo_url") or ""

    # Look up in ibjjf_athletes
    ibjjf_match = None
    if verified_claim:
        ia_res = (sb.table("ibjjf_athletes")
                   .select("ibjjf_id,name,slug,belt,academy,points,ranking_category,age_division,gi_nogi,gender")
                   .eq("ibjjf_id", verified_claim["ibjjf_athlete_id"])
                   .execute())
        ibjjf_match = ia_res.data[0] if ia_res.data else None
        if not ibjjf_match:
            ibjjf_match = {
                "ibjjf_id": verified_claim["ibjjf_athlete_id"],
                "name": verified_claim["ibjjf_name"],
                "slug": None, "belt": verified_claim["belt"],
                "academy": None, "points": None, "gender": None,
                "ranking_category": None, "age_division": None, "gi_nogi": None,
            }

    if not ibjjf_match and last_name:
        cand_res = (sb.table("ibjjf_athletes")
                     .select("ibjjf_id,name,slug,belt,academy,points,ranking_category,age_division,gi_nogi,gender")
                     .ilike("name_lower", f"%{last_name}%")
                     .order("points", desc=True, nullsfirst=False)
                     .limit(20)
                     .execute())
        candidates = cand_res.data or []
        # Exclude IBJJF athletes already claimed by a DIFFERENT sc_uid — their
        # data belongs to whoever verified them, not an anonymous name-alike.
        if candidates:
            try:
                cand_ids = [str(c["ibjjf_id"]) for c in candidates if c.get("ibjjf_id") is not None]
                v_res = (sb.table("sc_ibjjf_verified")
                           .select("ibjjf_athlete_id,sc_uid")
                           .in_("ibjjf_athlete_id", cand_ids)
                           .execute())
                taken = {str(vr["ibjjf_athlete_id"]) for vr in (v_res.data or [])
                         if str(vr.get("sc_uid") or "") != str(sc_uid)}
                if taken:
                    candidates = [c for c in candidates if str(c["ibjjf_id"]) not in taken]
            except Exception as e:
                log.warning("fuzzy-match claim exclusion failed: %s", e)
        best_score = 0.0
        for c in candidates:
            cname = normalize(c["name"] or "")
            score = first_name_score(first_name, cname)
            if fp.get("belt") and c["belt"]:
                if not belt_matches(fp["belt"], c["belt"]):
                    continue
            if score > best_score and score >= 0.70:
                best_score = score
                ibjjf_match = dict(c)

    # Upcoming IBJJF registrations matched by name — only run if we have
    # enough of a fingerprint to filter by. Otherwise every Tyler Walker's
    # upcoming brackets would appear on a kid-Tyler-Walker's profile.
    upcoming_rows = []
    if last_name and fp_has_signal:
        reg_res = (sb.table("tournament_results")
                    .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,status,event_id,ibjjf_athlete_id")
                    .eq("source", "ibjjf")
                    .eq("status", "registered")
                    .ilike("athlete_name", f"%{last_name}%")
                    .order("event_date")
                    .execute())
        reg_rows = reg_res.data or []
        # Exclude registrations whose ibjjf_athlete_id belongs to a different claimed sc_uid.
        if reg_rows:
            tagged_ids = {str(r["ibjjf_athlete_id"]) for r in reg_rows
                          if r.get("ibjjf_athlete_id") is not None}
            taken = set()
            if tagged_ids:
                try:
                    v_res = (sb.table("sc_ibjjf_verified")
                               .select("ibjjf_athlete_id,sc_uid")
                               .in_("ibjjf_athlete_id", list(tagged_ids))
                               .execute())
                    taken = {str(vr["ibjjf_athlete_id"]) for vr in (v_res.data or [])
                             if str(vr.get("sc_uid") or "") != str(sc_uid)}
                except Exception as e:
                    log.warning("upcoming-reg claim exclusion failed: %s", e)
            if taken:
                reg_rows = [r for r in reg_rows
                            if str(r.get("ibjjf_athlete_id") or "") not in taken]
        for row in reg_rows:
            row_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
            if first_name_score(first_name, row_name) < 0.70:
                continue
            # Belt + age + weight filter — the bracket must plausibly belong
            # to THIS athlete. Otherwise a different same-name athlete leaks
            # (e.g., a White Master 1 bracket on a Black Adult's profile).
            row_fp = parse_division(row.get("division") or "")
            if fp.get("belt") and row_fp.get("belt") and not belt_matches(fp["belt"], row_fp["belt"]):
                continue
            if fp.get("age") and row_fp.get("age") and not age_matches(fp["age"], row_fp["age"]):
                continue
            if not weight_matches(fp.get("weight"), row_fp.get("weight")):
                continue
            upcoming_rows.append(dict(row, _source="ibjjf"))

    # Fetch bracket-mates for each upcoming registration (everyone else in
    # the same event + division). Batched per-(event,division) — typical
    # athlete has 1-5 upcoming registrations.
    for row in upcoming_rows:
        eid = row.get("event_id")
        div = row.get("division")
        src = row.get("source") or "ibjjf"
        if not (eid and div):
            row["bracket_mates"] = []
            continue
        try:
            mates_res = (sb.table("tournament_results")
                           .select("athlete_name,athlete_display,team,athlete_id")
                           .eq("source", src)
                           .eq("event_id", str(eid))
                           .eq("division", div)
                           .eq("status", "registered")
                           .limit(200)
                           .execute())
            self_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
            mates = []
            for m in (mates_res.data or []):
                mn = normalize(m.get("athlete_display") or m.get("athlete_name") or "")
                if mn == self_name:
                    continue
                mates.append({
                    "name": m.get("athlete_display") or m.get("athlete_name") or "",
                    "team": m.get("team") or "",
                    "sc_uid": m.get("athlete_id") or None,
                })
            mates.sort(key=lambda x: x["name"].lower())
            row["bracket_mates"] = mates
        except Exception as e:
            log.warning("bracket-mates fetch failed: %s", e)
            row["bracket_mates"] = []

    # IBJJF results
    ibjjf_rows = []
    ibjjf_verified = bool(verified_claim)
    if ibjjf_match:
        ir_res = (sb.table("tournament_results")
                   .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,event_id")
                   .eq("source", "ibjjf")
                   .eq("ibjjf_athlete_id", ibjjf_match["ibjjf_id"])
                   .or_("status.is.null,status.neq.registered")
                   .order("event_date")
                   .execute())
        ibjjf_rows = ir_res.data or []

    # Strict name-match pull: any IBJJF row whose athlete_name is an exact
    # superset of this SC profile's display_name (token-level). Catches the
    # athlete's career history across belt changes — e.g., Roiter Lima (SC)
    # matches his 45 'Roiter Lima Silva Junior' IBJJF rows spanning Juvenile
    # Blue (2016) through Adult Black (2026). Belt filter doesn't apply here
    # because the name match is already a strong signal.
    sc_norm = norm_display.strip()
    sc_tokens = set(sc_norm.split())
    if len(sc_tokens) >= 2:
        try:
            nm_res = (sb.table("tournament_results")
                       .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,event_id,ibjjf_athlete_id")
                       .eq("source", "ibjjf")
                       .ilike("athlete_name", f"{sc_norm}%")
                       .or_("status.is.null,status.neq.registered")
                       .order("event_date")
                       .limit(300)
                       .execute())
            # Filter to exact-superset token match so we don't pick up
            # 'roiter lima silva junior' for an SC named 'roiter' alone,
            # and exclude rows tagged to a different sc_uid via verified claim.
            taken_ibjjf_ids = set()
            if nm_res.data:
                tagged = {str(r["ibjjf_athlete_id"]) for r in nm_res.data
                          if r.get("ibjjf_athlete_id") is not None}
                if tagged:
                    try:
                        v_res = (sb.table("sc_ibjjf_verified")
                                   .select("ibjjf_athlete_id,sc_uid")
                                   .in_("ibjjf_athlete_id", list(tagged))
                                   .execute())
                        taken_ibjjf_ids = {str(vr["ibjjf_athlete_id"])
                                           for vr in (v_res.data or [])
                                           if str(vr.get("sc_uid") or "") != str(sc_uid)}
                    except Exception:
                        pass
            for row in (nm_res.data or []):
                row_name = normalize(row.get("athlete_name") or "")
                row_tokens = set(row_name.split())
                if not sc_tokens.issubset(row_tokens):
                    continue
                if str(row.get("ibjjf_athlete_id") or "") in taken_ibjjf_ids:
                    continue
                key = (row.get("event_id"), row.get("division"), row.get("placement"))
                if not any((r.get("event_id"), r.get("division"), r.get("placement")) == key
                           for r in ibjjf_rows):
                    ibjjf_rows.append(row)
        except Exception as e:
            log.warning("name-match IBJJF pull failed: %s", e)

    # Explicit cross-source links: IBJJF rows tagged with this sc_uid
    # (backfilled manually or by nightly agent for athletes whose IBJJF results
    # don't have ibjjf_athlete_id populated but we know they belong to this SC profile)
    try:
        explicit_res = (sb.table("tournament_results")
                         .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,event_id")
                         .eq("source", "ibjjf")
                         .eq("athlete_id", str(sc_uid))
                         .or_("status.is.null,status.neq.registered")
                         .order("event_date")
                         .execute())
        for row in (explicit_res.data or []):
            # Dedupe by (event_id, division, placement)
            key = (row.get("event_id"), row.get("division"), row.get("placement"))
            if not any((r.get("event_id"), r.get("division"), r.get("placement")) == key
                       for r in ibjjf_rows):
                ibjjf_rows.append(row)
    except Exception as e:
        log.warning("explicit IBJJF link query failed: %s", e)

    # Skip fuzzy fallback when we have no fingerprint at all — without belt,
    # age, OR weight signals, any name-match would scoop up every other
    # Tyler Walker in the DB and stamp their IBJJF rows on a kid's profile.
    if not ibjjf_rows and last_name and fp_has_signal:
        fb_res = (sb.table("tournament_results")
                   .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,event_id,ibjjf_athlete_id")
                   .eq("source", "ibjjf")
                   .ilike("athlete_name", f"%{last_name}%")
                   .or_("status.is.null,status.neq.registered")
                   .order("event_date")
                   .execute())
        fb_rows = fb_res.data or []
        # Strip any rows whose ibjjf_athlete_id is already claimed by a
        # different sc_uid — those belong to the verified owner.
        if fb_rows:
            tagged_ids = {str(r["ibjjf_athlete_id"]) for r in fb_rows
                          if r.get("ibjjf_athlete_id") is not None}
            taken = set()
            if tagged_ids:
                try:
                    v_res = (sb.table("sc_ibjjf_verified")
                               .select("ibjjf_athlete_id,sc_uid")
                               .in_("ibjjf_athlete_id", list(tagged_ids))
                               .execute())
                    taken = {str(vr["ibjjf_athlete_id"]) for vr in (v_res.data or [])
                             if str(vr.get("sc_uid") or "") != str(sc_uid)}
                except Exception as e:
                    log.warning("fallback IBJJF claim exclusion failed: %s", e)
            if taken:
                fb_rows = [r for r in fb_rows
                           if str(r.get("ibjjf_athlete_id") or "") not in taken]
        for row in fb_rows:
            row_fp = parse_division(row.get("division") or "")
            if not age_matches(fp.get("age"), row_fp.get("age")):
                continue
            if fp.get("gender") and row_fp.get("gender") and fp["gender"] != row_fp["gender"]:
                continue
            if not belt_matches(fp.get("belt"), row_fp.get("belt")):
                continue
            if not weight_matches(fp.get("weight"), row_fp.get("weight")):
                continue
            row_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
            if first_name_score(first_name, row_name) < 0.70:
                continue
            ibjjf_rows.append(row)

    # Unified result list
    all_rows = []
    for r in sc_rows:
        all_rows.append(dict(r, _source="smoothcomp"))
    for r in ibjjf_rows:
        all_rows.append(dict(r, _source="ibjjf"))
    all_rows.sort(key=lambda r: (r["event_date"] or "0000-00-00"))

    gold   = sum(1 for r in all_rows if r["placement"] == 1)
    silver = sum(1 for r in all_rows if r["placement"] == 2)
    bronze = sum(1 for r in all_rows if r["placement"] == 3)
    stats = {
        "events":    len({r["event_title"] for r in all_rows}),
        "divisions": len(all_rows),
        "gold":      gold,
        "silver":    silver,
        "bronze":    bronze,
    }

    # Social links
    sl_res = sb.table("athlete_social_links").select("*").eq("sc_uid", sc_uid).execute()
    social_links = sl_res.data[0] if sl_res.data else None

    # SC verification status
    sc_ver_res = sb.table("sc_smoothcomp_verified").select("sc_name").eq("sc_uid", sc_uid).execute()
    sc_verified = bool(sc_ver_res.data)

    # Instagram posts
    instagram_posts = []
    if social_links and social_links.get("instagram_token") and social_links.get("instagram_user_id"):
        try:
            instagram_posts = meta_api.get_recent_posts(
                social_links["instagram_user_id"],
                social_links["instagram_token"],
                limit=9,
            )
        except Exception as e:
            log.warning("Failed to fetch Instagram posts for sc_uid=%s: %s", sc_uid, e)

    # IBJJF rankings
    ibjjf_rank_data = None
    _gender = (ibjjf_match or {}).get("gender") or fp.get("gender")
    if ibjjf_match and ibjjf_match.get("slug") and ibjjf_match.get("belt") and _gender:
        weight_slug = None
        weight_counts: dict[str, int] = {}
        for row in ibjjf_rows:
            div = row.get("division") or ""
            w = ibjjf_rankings.weight_slug_from_division(div)
            if w and w != "openclass":
                weight_counts[w] = weight_counts.get(w, 0) + 1
        if weight_counts:
            weight_slug = max(weight_counts, key=weight_counts.get)

        try:
            ibjjf_rank_data = ibjjf_rankings.get_rankings(
                sb=sb,
                slug=ibjjf_match["slug"],
                belt=ibjjf_match["belt"],
                gender=_gender,
                ranking_category=ibjjf_match.get("ranking_category") or "adult",
                age_division=ibjjf_match.get("age_division"),
                weight=weight_slug or "",
            )
        except Exception as e:
            log.warning("Failed to fetch IBJJF rankings for sc_uid=%s: %s", sc_uid, e)

    # Match history — separate try/except so a failing IBJJF RPC doesn't block SC results
    match_history = []
    if ibjjf_match and ibjjf_match.get("ibjjf_id"):
        try:
            mh_res = sb.rpc("get_match_history_ibjjf",
                            {"p_ibjjf_athlete_id": str(ibjjf_match["ibjjf_id"])}).execute()
            match_history.extend(mh_res.data or [])
        except Exception as e:
            log.warning("get_match_history_ibjjf failed for sc_uid=%s: %s", sc_uid, e)
    try:
        sc_mh_res = sb.rpc("get_match_history_sc", {"p_sc_uid": str(sc_uid)}).execute()
        match_history.extend(sc_mh_res.data or [])
    except Exception as e:
        log.warning("get_match_history_sc failed for sc_uid=%s: %s", sc_uid, e)

    # Deduplicate by (event_date, division, opponent_name) and sort newest first
    seen = set()
    unique_matches = []
    for m in sorted(match_history, key=lambda x: (x.get("event_date") or ""), reverse=True):
        key = (m.get("event_date"), m.get("division"), (m.get("opponent_name") or "").lower())
        if key not in seen:
            seen.add(key)
            unique_matches.append(m)
    match_history = unique_matches

    # Head-to-head aggregates vs repeat opponents
    h2h: dict[str, dict] = {}
    for m in match_history:
        opp = m.get("opponent_name") or "Unknown"
        if opp not in h2h:
            h2h[opp] = {"wins": 0, "losses": 0, "opponent_sc_uid": m.get("opponent_sc_uid")}
        if m.get("result") == "Win":
            h2h[opp]["wins"] += 1
        else:
            h2h[opp]["losses"] += 1
    h2h_repeat = {k: v for k, v in h2h.items() if v["wins"] + v["losses"] > 1}

    return render_template("trackbjj/athlete.html",
                           sc_uid=sc_uid,
                           profile_photo_url=profile_photo_url,
                           display_name=display_name,
                           names=names,
                           team=team,
                           fp=fp,
                           sc_rows=sc_rows,
                           ibjjf_rows=ibjjf_rows,
                           ibjjf_verified=ibjjf_verified,
                           ibjjf_match=ibjjf_match,
                           all_rows=all_rows,
                           upcoming_rows=upcoming_rows,
                           stats=stats,
                           social_links=social_links,
                           sc_verified=sc_verified,
                           instagram_posts=instagram_posts,
                           ibjjf_rank_data=ibjjf_rank_data,
                           match_history=match_history,
                           h2h_repeat=h2h_repeat,
                           now_year=datetime.date.today().year)


# ── Claim profile ──────────────────────────────────────────────────────────────

@app.route("/claim/<sc_uid>", methods=["GET", "POST"])
def claim_profile(sc_uid):
    # Legacy entrypoint — redirect to the unified /claim-me flow with sc_uid intent.
    return redirect(url_for("claim_me", sc_uid=sc_uid), code=302)


# ── Smoothcomp verification ────────────────────────────────────────────────────

@app.route("/verify-sc/<sc_uid>", methods=["GET", "POST"])
def verify_sc(sc_uid):
    # Legacy entrypoint — redirect to the unified /claim-me flow with sc_uid intent.
    return redirect(url_for("claim_me", sc_uid=sc_uid), code=302)


# ── Social links ───────────────────────────────────────────────────────────────

@app.route("/social/<sc_uid>", methods=["GET", "POST"])
def social_links(sc_uid):
    sl_res = sb.table("athlete_social_links").select("*").eq("sc_uid", sc_uid).execute()
    existing = sl_res.data[0] if sl_res.data else None

    if request.method == "GET":
        return render_template("trackbjj/social.html", sc_uid=sc_uid, existing=existing)

    instagram_handle = request.form.get("instagram_handle", "").strip().lstrip("@")
    facebook_url     = request.form.get("facebook_url", "").strip()
    youtube_url      = request.form.get("youtube_url", "").strip()

    upsert_data = {"sc_uid": sc_uid}
    if instagram_handle:
        upsert_data["instagram_handle"] = instagram_handle
    if facebook_url:
        upsert_data["facebook_url"] = facebook_url
    if youtube_url:
        upsert_data["youtube_url"] = youtube_url

    sb.table("athlete_social_links").upsert(upsert_data, on_conflict="sc_uid").execute()

    flash("Social links updated!", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── Meta / Instagram OAuth ─────────────────────────────────────────────────────

@app.route("/auth/instagram/<sc_uid>")
def instagram_auth_start(sc_uid):
    redirect_uri = _base_url(TRACKBJJ_PORT) + INSTAGRAM_CALLBACK_PATH
    oauth_url = meta_api.get_oauth_url(sc_uid, redirect_uri)
    return redirect(oauth_url)


@app.route("/auth/instagram/callback")
def instagram_auth_callback():
    code     = request.args.get("code", "").strip()
    sc_uid   = request.args.get("state", "").strip()
    error    = request.args.get("error", "")
    err_desc = request.args.get("error_description", "")

    if error:
        flash(f"Instagram authorization failed: {err_desc or error}", "error")
        return redirect(url_for("athlete_profile", sc_uid=sc_uid) if sc_uid else url_for("index"))

    if not code or not sc_uid:
        flash("Invalid OAuth callback — missing code or state.", "error")
        return redirect(url_for("index"))

    redirect_uri = _base_url(TRACKBJJ_PORT) + INSTAGRAM_CALLBACK_PATH

    try:
        short       = meta_api.exchange_code(code, redirect_uri)
        long        = meta_api.get_long_lived_token(short["access_token"])
        long_token  = long["access_token"]
        expires_in  = long.get("expires_in", 5184000)
        ig_info     = meta_api.get_instagram_user_id(long_token)
        ig_user_id  = ig_info["ig_user_id"]
        ig_username = ig_info.get("ig_username", "")
    except Exception as e:
        log.error("Instagram OAuth failed for sc_uid=%s: %s", sc_uid, e)
        flash(f"Instagram connection failed: {e}", "error")
        return redirect(url_for("athlete_profile", sc_uid=sc_uid))

    expires_at = (datetime.datetime.utcnow() + datetime.timedelta(seconds=int(expires_in))).isoformat()

    sb.table("athlete_social_links").upsert({
        "sc_uid":                  sc_uid,
        "instagram_user_id":       ig_user_id,
        "instagram_token":         long_token,
        "instagram_token_expires": expires_at,
        "instagram_handle":        ig_username or None,
    }, on_conflict="sc_uid").execute()

    flash(f"Instagram connected! (@{ig_username})", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── API ────────────────────────────────────────────────────────────────────────

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    result = sb.rpc("search_athletes", {"q": normalize(q)}).execute()
    rows = result.data or []
    # Normalize to consistent shape for the frontend.
    # Includes IBJJF-only athletes (no sc_uid) — they're rendered with a
    # "Claim" CTA on the search card.
    # First pass: find SC-keyed rows and remember their normalized display
    # name tokens so we can merge compatible loose-IBJJF aggregates into them.
    sc_token_map = []
    for r in rows:
        if r.get("sc_uid"):
            n = normalize(r.get("athlete_display") or r.get("athlete_name") or "")
            if n:
                sc_token_map.append((r, set(n.split())))

    out = []
    merged_loose = set()  # indexes of loose rows merged into SC cards
    for idx, r in enumerate(rows):
        sc_uid = r.get("sc_uid")
        ibjjf_id = r.get("ibjjf_id")
        display = r.get("athlete_display") or r.get("athlete_name", "")
        # Rows with neither key are "loose" IBJJF tournament matches — keep
        # them if they have a display name so the user can still see the
        # aggregated history and claim it via /claim-me. If the loose row's
        # name is a token-superset of a SC-keyed row's name, merge it into
        # that SC card so the user sees ONE result instead of two.
        if not sc_uid and not ibjjf_id and display:
            loose_tokens = set(normalize(display).split())
            for sc_row, sc_tokens in sc_token_map:
                if sc_tokens and sc_tokens.issubset(loose_tokens):
                    sc_row.setdefault("_merged_loose_count", 0)
                    sc_row["_merged_loose_count"] += r.get("result_count") or 0
                    sc_row.setdefault("_merged_loose_sources", set()).update(r.get("sources") or [])
                    sc_row["_merged_loose_last_seen"] = max(
                        sc_row.get("_merged_loose_last_seen") or "",
                        r.get("last_seen") or "",
                    )
                    merged_loose.add(idx)
                    break
        if not sc_uid and not ibjjf_id and not display:
            continue
        # Skip loose rows that got merged into a SC card above.
        if idx in merged_loose:
            continue
        country = r.get("country") or ""
        if country in (r"\N", "\\N", "\\\\N"):
            country = ""
        sources = list(r.get("sources") or [])
        if isinstance(r.get("sources"), str):
            sources = [s.strip() for s in r.get("sources").split(",") if s.strip()]
        event_count = r.get("result_count", 0) or 0
        last_seen = r.get("last_seen", "")
        # Fold in merged loose-IBJJF totals for SC cards.
        if r.get("_merged_loose_count"):
            event_count += r["_merged_loose_count"]
            for s in r.get("_merged_loose_sources", []):
                if s not in sources:
                    sources.append(s)
            if r.get("_merged_loose_last_seen") and r["_merged_loose_last_seen"] > last_seen:
                last_seen = r["_merged_loose_last_seen"]
        out.append({
            "athlete_id":   sc_uid,
            "ibjjf_id":     ibjjf_id,
            "claimed":      bool(r.get("claimed")),
            "display_name": display,
            "team":         r.get("team") or "",
            "country":      country,
            "event_count":  event_count,
            "last_seen":    last_seen,
            "sources":      sources,
        })
    return jsonify(out)


@app.route("/api/athlete/<sc_uid>/results")
def api_athlete_results(sc_uid):
    res = (sb.table("tournament_results")
             .select("source,event_date,event_title,division,placement,athlete_name,athlete_display,team,event_id")
             .eq("athlete_id", str(sc_uid))
             .eq("source", "smoothcomp")
             .order("event_date", desc=True)
             .execute())
    return jsonify(res.data or [])


@app.route("/api/stats")
def api_stats():
    import requests as req
    SVCKEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
    SBURL  = os.environ.get("SUPABASE_URL", "")
    try:
        r = req.get(
            f"{SBURL}/rest/v1/site_stats?select=key,value",
            headers={"apikey": SVCKEY, "Authorization": f"Bearer {SVCKEY}"},
            timeout=5,
        )
        rows = r.json() if r.ok else []
        stats = {row["key"]: row["value"] for row in rows if isinstance(row, dict)}
    except Exception:
        stats = {}
    return jsonify({
        "smoothcomp_athletes": stats.get("smoothcomp_athletes", 0),
        "total_rows":          stats.get("total_results", 0),
        "total_events":        stats.get("total_events", 0),
    })


@app.route("/auth-relay")
def auth_relay():
    """Cross-domain session relay — lets sibling sites inherit the session via iframe postMessage."""
    html = """<!DOCTYPE html>
<html><head><meta charset="utf-8"></head><body>
<script>
var TRUSTED=['https://www.mattrack.net','https://mattrack.net','https://www.trackbjj.net','https://trackbjj.net'];
var SB_KEY='sb-kzqvfuqxtbrhlgphyntb-auth-token';
window.addEventListener('message',function(e){
  if(!TRUSTED.includes(e.origin))return;
  if(e.data&&e.data.type==='get-session'){
    try{var d=localStorage.getItem(SB_KEY);e.source.postMessage({type:'session-response',session:d?JSON.parse(d):null},e.origin);}
    catch(err){e.source.postMessage({type:'session-response',session:null},e.origin);}
  }
});
</script></body></html>"""
    resp = make_response(html)
    resp.headers["Content-Type"] = "text/html"
    resp.headers["Cache-Control"] = "no-store"
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("TRACKBJJ_PORT", 5951))
    log.info("trackbjj.net dev server → http://0.0.0.0:%d", port)
    log.info("Access from Windows at http://172.23.93.61:%d", port)
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=True)
