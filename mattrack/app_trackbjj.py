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
    "white": "white", "beginner": "white",
    "blue": "blue", "intermediate": None,
    "purple": "purple", "brown": "brown",
    "black": "black", "coral": "black", "red": "black",
    "branca": "white", "branco": "white", "azul": "blue",
    "roxa": "purple", "roxo": "purple", "marrom": "brown",
    "preta": "black", "preto": "black",
}

AGE_MAP = {
    "adult": "adult", "adults": "adult",
    "master 1": "m1", "master1": "m1", "masters (30": "m1",
    "master 2": "m2", "master2": "m2", "masters (35": "m2",
    "master 3": "m3", "master3": "m3", "masters (40": "m3",
    "master 4": "m4", "master4": "m4", "masters (45": "m4",
    "master 5": "m5", "master5": "m5", "masters (50": "m5", "master 5 (51": "m5",
    "master 6": "m6", "master6": "m6", "masters (55": "m6", "master 6 (57": "m6",
    "master 7": "m7", "master7": "m7", "masters (60": "m7",
}

AGE_RANK  = {"adult": 0, "m1": 1, "m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6, "m7": 7}
BELT_RANK = {"white": 0, "blue": 1, "purple": 2, "brown": 3, "black": 4}


def parse_division(div: str) -> dict:
    if not div:
        return {}
    d = div.lower()
    belt   = next((v for k, v in BELT_MAP.items() if k in d), None)
    age    = next((v for k, v in AGE_MAP.items()  if k in d), None)
    gender = "female" if re.search(r"\bfem|\bwom|\bf\b", d) else "male"
    return {"belt": belt, "age": age, "gender": gender}


def athlete_fingerprint(divisions: list) -> dict:
    belts, ages, genders = [], [], []
    for div in divisions:
        fp = parse_division(div)
        if fp.get("belt"):  belts.append(fp["belt"])
        if fp.get("age"):   ages.append(fp["age"])
        genders.append(fp.get("gender", "male"))
    def mode(lst): return max(set(lst), key=lst.count) if lst else None
    highest_belt = max(belts, key=lambda b: BELT_RANK.get(b, -1)) if belts else None
    return {"belt": highest_belt, "age": mode(ages), "gender": mode(genders)}


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
        plan = get_user_plan(user["sub"])
        return jsonify({
            "authenticated": True,
            "email": user.get("email", ""),
            "plan": plan,
            "active": is_plan_active(user["sub"]),
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


@app.route("/athlete/<sc_uid>")
def athlete_profile(sc_uid):
    """Athlete profile page anchored by Smoothcomp user_id."""
    try:
        return _athlete_profile_inner(sc_uid)
    except Exception:
        import traceback as _tb
        tb_str = _tb.format_exc()
        log.error("athlete_profile 500 for sc_uid=%s:\n%s", sc_uid, tb_str)
        return f"<pre>500 debug:\n{tb_str}</pre>", 500


def _athlete_profile_inner(sc_uid):
    # Get all Smoothcomp rows for this user_id
    sc_res = (sb.table("tournament_results")
               .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,athlete_id")
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

    # Team from most recent registration, then most recent result
    team_res = (sb.table("tournament_results")
                 .select("team,status,event_date")
                 .ilike("athlete_name", f"%{last_name}%")
                 .not_.is_("team", "null")
                 .order("event_date", desc=True)
                 .limit(50)
                 .execute())
    team_rows_raw = team_res.data or []
    # Prefer 'registered' status for most current team
    team_rows_raw.sort(key=lambda r: (0 if r.get("status") == "registered" else 1,
                                      -(int((r.get("event_date") or "0000-00-00").replace("-", "")) )))
    team = team_rows_raw[0]["team"] if team_rows_raw else next(
        (r["team"] for r in sc_rows if r["team"]), "Unknown"
    )

    # Build fingerprint from SC divisions
    fp = athlete_fingerprint([r["division"] for r in sc_rows if r["division"]])

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

    # Upcoming IBJJF registrations matched by name
    upcoming_rows = []
    if last_name:
        reg_res = (sb.table("tournament_results")
                    .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source,status,event_id")
                    .eq("source", "ibjjf")
                    .eq("status", "registered")
                    .ilike("athlete_name", f"%{last_name}%")
                    .order("event_date")
                    .execute())
        for row in (reg_res.data or []):
            row_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
            if first_name_score(first_name, row_name) >= 0.50:
                upcoming_rows.append(dict(row, _source="ibjjf"))

    # IBJJF results
    ibjjf_rows = []
    ibjjf_verified = bool(verified_claim)
    if ibjjf_match:
        ir_res = (sb.table("tournament_results")
                   .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source")
                   .eq("source", "ibjjf")
                   .eq("ibjjf_athlete_id", ibjjf_match["ibjjf_id"])
                   .or_("status.is.null,status.neq.registered")
                   .order("event_date")
                   .execute())
        ibjjf_rows = ir_res.data or []

    if not ibjjf_rows and last_name:
        fb_res = (sb.table("tournament_results")
                   .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source")
                   .eq("source", "ibjjf")
                   .ilike("athlete_name", f"%{last_name}%")
                   .or_("status.is.null,status.neq.registered")
                   .order("event_date")
                   .execute())
        for row in (fb_res.data or []):
            row_fp = parse_division(row.get("division") or "")
            if not age_matches(fp.get("age"), row_fp.get("age")):
                continue
            if fp.get("gender") and row_fp.get("gender") and fp["gender"] != row_fp["gender"]:
                continue
            if not belt_matches(fp.get("belt"), row_fp.get("belt")):
                continue
            row_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
            if first_name_score(first_name, row_name) < 0.50:
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

    # Match history (adjacent-placement inference)
    match_history = []
    try:
        if ibjjf_match and ibjjf_match.get("ibjjf_id"):
            mh_res = sb.rpc("get_match_history_ibjjf",
                            {"p_ibjjf_athlete_id": str(ibjjf_match["ibjjf_id"])}).execute()
            match_history.extend(mh_res.data or [])
        sc_mh_res = sb.rpc("get_match_history_sc", {"p_sc_uid": str(sc_uid)}).execute()
        match_history.extend(sc_mh_res.data or [])
    except Exception as e:
        log.warning("Failed to fetch match history for sc_uid=%s: %s", sc_uid, e)

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
    existing_res = sb.table("sc_ibjjf_verified").select("ibjjf_athlete_id,ibjjf_name").eq("sc_uid", sc_uid).execute()
    existing = existing_res.data[0] if existing_res.data else None

    if request.method == "GET":
        return render_template("trackbjj/claim.html", sc_uid=sc_uid, existing=existing)

    email    = request.form.get("email", "").strip()
    password = request.form.get("password", "").strip()

    if not email or not password:
        flash("Email and password are required.", "error")
        return render_template("trackbjj/claim.html", sc_uid=sc_uid, existing=existing)

    try:
        athlete_id, token = ibjjf_api.login(email, password)
        profile = ibjjf_api.get_athlete_profile(token)
    except Exception as e:
        flash(f"IBJJF login failed: {e}", "error")
        return render_template("trackbjj/claim.html", sc_uid=sc_uid, existing=existing)

    ibjjf_id   = profile.get("athlete_id") or athlete_id
    ibjjf_name = profile.get("name", "")
    belt       = profile.get("belt", "")
    academy    = profile.get("academy", "")
    photo_url  = profile.get("photo_url", "") or ""

    verified_row = {
        "sc_uid":           sc_uid,
        "ibjjf_athlete_id": ibjjf_id,
        "ibjjf_name":       ibjjf_name,
        "belt":             belt,
        "academy":          academy,
    }
    if photo_url:
        verified_row["photo_url"] = photo_url

    sb.table("sc_ibjjf_verified").upsert(verified_row, on_conflict="sc_uid").execute()

    ibjjf_gender = (profile.get("gender") or "").lower() or None
    upsert_data = {
        "ibjjf_id":         ibjjf_id,
        "name":             ibjjf_name,
        "name_lower":       ibjjf_name.lower(),
        "ranking_category": "adult",
        "ranking_year":     datetime.date.today().year,
    }
    if belt:
        upsert_data["belt"] = belt.lower()
    if ibjjf_gender:
        upsert_data["gender"] = ibjjf_gender

    sb.table("ibjjf_athletes").upsert(upsert_data, on_conflict="ibjjf_id").execute()

    flash(f"Profile claimed! Verified as {ibjjf_name} (IBJJF ID: {ibjjf_id})", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── Smoothcomp verification ────────────────────────────────────────────────────

@app.route("/verify-sc/<sc_uid>", methods=["GET", "POST"])
def verify_sc(sc_uid):
    existing_res = sb.table("sc_smoothcomp_verified").select("sc_name,sc_email").eq("sc_uid", sc_uid).execute()
    existing = existing_res.data[0] if existing_res.data else None

    if request.method == "GET":
        return render_template("trackbjj/verify_sc.html", sc_uid=sc_uid, existing=existing)

    email    = request.form.get("email", "").strip()
    password = request.form.get("password", "").strip()

    if not email or not password:
        flash("Email and password are required.", "error")
        return render_template("trackbjj/verify_sc.html", sc_uid=sc_uid, existing=existing)

    try:
        result = scrape_smoothcomp_verify.verify_sc_login(email, password)
    except Exception as e:
        flash(f"Smoothcomp login failed: {e}", "error")
        return render_template("trackbjj/verify_sc.html", sc_uid=sc_uid, existing=existing)

    returned_id = str(result["sc_user_id"])
    if returned_id != str(sc_uid):
        flash(
            f"Login succeeded but your Smoothcomp ID ({returned_id}) "
            f"does not match this profile (#{sc_uid}). "
            "Make sure you're logging in with the account that owns this profile.",
            "error",
        )
        return render_template("trackbjj/verify_sc.html", sc_uid=sc_uid, existing=existing)

    sb.table("sc_smoothcomp_verified").upsert({
        "sc_uid":    sc_uid,
        "sc_email":  result["email"],
        "sc_user_id": returned_id,
        "sc_name":   result["sc_name"],
    }, on_conflict="sc_uid").execute()

    flash(f"Smoothcomp profile verified! Welcome, {result['sc_name']}.", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


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
    # Normalize to consistent shape for the frontend
    out = []
    for r in rows:
        sc_uid = r.get("sc_uid")
        if not sc_uid:
            continue  # no profile page exists for athletes without a sc_uid
        country = r.get("country") or ""
        if country in (r"\N", "\\N", "\\\\N"):
            country = ""
        sources = r.get("sources") or []
        if isinstance(sources, str):
            sources = [s.strip() for s in sources.split(",") if s.strip()]
        out.append({
            "athlete_id":   sc_uid,
            "display_name": r.get("athlete_display") or r.get("athlete_name", ""),
            "team":         r.get("team") or "",
            "country":      country,
            "event_count":  r.get("result_count", 0),
            "last_seen":    r.get("last_seen", ""),
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


@app.route("/api/debug/athlete/<sc_uid>")
def api_debug_athlete(sc_uid):
    """Temporary debug endpoint — trace athlete profile steps."""
    import traceback
    steps = {}
    try:
        sc_res = (sb.table("tournament_results")
                   .select("athlete_name,athlete_display,team,event_date,division,placement")
                   .eq("source", "smoothcomp").eq("athlete_id", str(sc_uid))
                   .order("event_date", desc=True).execute())
        steps["sc_rows"] = len(sc_res.data or [])
        if not sc_res.data:
            return jsonify({"steps": steps, "error": "no sc_rows → 404"})
        sc_rows = sc_res.data
        names = list({r["athlete_display"] or r["athlete_name"] for r in sc_rows})
        steps["names"] = names
        display_name = max(names, key=len)
        steps["display_name"] = display_name
        norm_display = normalize(display_name)
        parts = norm_display.split()
        last_name = parts[-1] if parts else ""
        first_name = parts[0] if parts else ""
        steps["last_name"] = last_name
        team_res = (sb.table("tournament_results")
                     .select("team,status,event_date")
                     .ilike("athlete_name", f"%{last_name}%")
                     .not_.is_("team", "null")
                     .order("event_date", desc=True).limit(50).execute())
        steps["team_rows"] = len(team_res.data or [])
        fp = athlete_fingerprint([r["division"] for r in sc_rows if r["division"]])
        steps["fp"] = fp
        verified_res = (sb.table("sc_ibjjf_verified").select("*").eq("sc_uid", sc_uid).execute())
        steps["verified_claim"] = bool(verified_res.data)
        cand_res = (sb.table("ibjjf_athletes")
                     .select("ibjjf_id,name,slug,belt")
                     .ilike("name_lower", f"%{last_name}%")
                     .order("points", desc=True, nullsfirst=False).limit(20).execute())
        steps["ibjjf_candidates"] = len(cand_res.data or [])
        reg_res = (sb.table("tournament_results")
                    .select("athlete_name,division,status")
                    .eq("source", "ibjjf").eq("status", "registered")
                    .ilike("athlete_name", f"%{last_name}%")
                    .order("event_date").execute())
        steps["upcoming_ibjjf"] = len(reg_res.data or [])
        fb_res = (sb.table("tournament_results")
                   .select("athlete_name,division,placement")
                   .eq("source", "ibjjf")
                   .ilike("athlete_name", f"%{last_name}%")
                   .or_("status.is.null,status.neq.registered")
                   .order("event_date").execute())
        steps["fallback_ibjjf"] = len(fb_res.data or [])
        sl_res = sb.table("athlete_social_links").select("*").eq("sc_uid", sc_uid).execute()
        steps["social_links"] = bool(sl_res.data)
        sc_ver_res = sb.table("sc_smoothcomp_verified").select("sc_name").eq("sc_uid", sc_uid).execute()
        steps["sc_verified"] = bool(sc_ver_res.data)
        try:
            sc_mh_res = sb.rpc("get_match_history_sc", {"p_sc_uid": str(sc_uid)}).execute()
            match_history = list(sc_mh_res.data or [])
            steps["match_history_sc"] = len(match_history)
            # Test h2h processing
            h2h = {}
            for m in match_history:
                opp = m.get("opponent_name") or "Unknown"
                if opp not in h2h:
                    h2h[opp] = {"wins": 0, "losses": 0, "opponent_sc_uid": m.get("opponent_sc_uid")}
                if m.get("result") == "Win":
                    h2h[opp]["wins"] += 1
                else:
                    h2h[opp]["losses"] += 1
            steps["h2h_entries"] = len(h2h)
            steps["match_keys"] = list(match_history[0].keys()) if match_history else []
        except Exception as e:
            steps["match_history_err"] = str(e)
        # Test ibjjf_rows fallback processing
        fp = athlete_fingerprint([r["division"] for r in sc_res.data if r["division"]])
        norm_display2 = normalize(max(list({r["athlete_display"] or r["athlete_name"] for r in sc_res.data}), key=len))
        parts2 = norm_display2.split()
        last_name2 = parts2[-1] if parts2 else ""
        first_name2 = parts2[0] if parts2 else ""
        try:
            fb_res2 = (sb.table("tournament_results")
                       .select("athlete_name,athlete_display,team,event_date,event_title,division,placement,source")
                       .eq("source", "ibjjf")
                       .ilike("athlete_name", f"%{last_name2}%")
                       .or_("status.is.null,status.neq.registered")
                       .order("event_date").execute())
            ibjjf_rows_test = []
            for row in (fb_res2.data or []):
                row_fp = parse_division(row.get("division") or "")
                if not age_matches(fp.get("age"), row_fp.get("age")):
                    continue
                if fp.get("gender") and row_fp.get("gender") and fp["gender"] != row_fp["gender"]:
                    continue
                if not belt_matches(fp.get("belt"), row_fp.get("belt")):
                    continue
                row_name = normalize(row.get("athlete_display") or row.get("athlete_name") or "")
                if first_name_score(first_name2, row_name) < 0.50:
                    continue
                ibjjf_rows_test.append(row)
            steps["ibjjf_rows_filtered"] = len(ibjjf_rows_test)
            # Test all_rows assembly and sort
            all_rows_test = []
            for r in sc_res.data:
                all_rows_test.append(dict(r, _source="smoothcomp"))
            for r in ibjjf_rows_test:
                all_rows_test.append(dict(r, _source="ibjjf"))
            all_rows_test.sort(key=lambda r: (r["event_date"] or "0000-00-00"))
            steps["all_rows"] = len(all_rows_test)
        except Exception as e:
            import traceback as tb2
            steps["ibjjf_filter_err"] = tb2.format_exc()
        return jsonify({"ok": True, "steps": steps})
    except Exception:
        steps["traceback"] = traceback.format_exc()
        return jsonify({"ok": False, "steps": steps}), 500


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
