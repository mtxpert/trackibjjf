"""
trackbjj.net — BJJ Athlete Repository
Runs locally on port 5951, accessible at http://172.23.93.61:5951

Companion to mattrack.net (port 5950). Shares the same local Postgres DB.
"""

import difflib
import os
import re
import unicodedata
import logging

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, render_template, request, g, redirect, url_for, flash
from dotenv import load_dotenv

import ibjjf_api
import scrape_smoothcomp_verify
import meta_api
import ibjjf_rankings

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__, template_folder="trackbjj/templates", static_folder="trackbjj/static")
app.secret_key = os.environ.get("SECRET_KEY", "trackbjj-dev-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

DSN = os.environ.get("LOCAL_PG_DSN", "dbname=mattrack")
MATTRACK_PORT = int(os.environ.get("MATTRACK_PORT", 5950))
TRACKBJJ_PORT = int(os.environ.get("PORT", 5951))
INSTAGRAM_CALLBACK_PATH = "/auth/instagram/callback"


def _base_url(port):
    """Return base URL using the same host the browser used."""
    host = request.host.split(":")[0]
    return f"http://{host}:{port}"


@app.context_processor
def inject_urls():
    return dict(
        trackbjj_url=_base_url(TRACKBJJ_PORT),
        mattrack_url=_base_url(MATTRACK_PORT),
    )


def get_db():
    if "db" not in g:
        g.db = psycopg2.connect(DSN, cursor_factory=psycopg2.extras.RealDictCursor)
    return g.db


def _ensure_tables():
    """Create any missing tables on startup."""
    try:
        db = psycopg2.connect(DSN, cursor_factory=psycopg2.extras.RealDictCursor)
        cur = db.cursor()
        ibjjf_rankings.ensure_cache_table(cur)
        db.close()
    except Exception as e:
        log.warning("Could not ensure tables: %s", e)


_ensure_tables()


@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db:
        db.close()


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

AGE_RANK = {"adult": 0, "m1": 1, "m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6, "m7": 7}
BELT_RANK = {"white": 0, "blue": 1, "purple": 2, "brown": 3, "black": 4}


def parse_division(div: str) -> dict:
    if not div:
        return {}
    d = div.lower()
    belt = next((v for k, v in BELT_MAP.items() if k in d), None)
    age  = next((v for k, v in AGE_MAP.items()  if k in d), None)
    gender = "female" if re.search(r"\bfem|\bwom|\bf\b", d) else "male"
    return {"belt": belt, "age": age, "gender": gender}


def athlete_fingerprint(divisions: list) -> dict:
    """Return belt/age/gender fingerprint from a list of division strings.
    Uses HIGHEST belt seen (athletes progress — never go backward).
    Uses modal age group (most common)."""
    belts, ages, genders = [], [], []
    for div in divisions:
        fp = parse_division(div)
        if fp.get("belt"):  belts.append(fp["belt"])
        if fp.get("age"):   ages.append(fp["age"])
        genders.append(fp.get("gender", "male"))
    def mode(lst): return max(set(lst), key=lst.count) if lst else None
    # Use highest belt rank — an athlete's belt only goes up
    highest_belt = max(belts, key=lambda b: BELT_RANK.get(b, -1)) if belts else None
    return {"belt": highest_belt, "age": mode(ages), "gender": mode(genders)}


def age_matches(athlete_age: str, div_age: str) -> bool:
    """True if ages match exactly or are adjacent (athlete aged up a bracket)."""
    if not athlete_age or not div_age:
        return True  # unknown — don't filter
    if athlete_age == div_age:
        return True
    ra, rb = AGE_RANK.get(athlete_age, -1), AGE_RANK.get(div_age, -1)
    return ra >= 0 and rb >= 0 and abs(ra - rb) <= 1


def belt_matches(athlete_belt: str, div_belt: str) -> bool:
    """True if belts are within 1 level (allows for progression over time).
    A brown-belt athlete could have purple-belt historical results — that's fine.
    But a brown-belt athlete should NOT have white/blue results (different person).
    """
    if not athlete_belt or not div_belt:
        return True  # unknown — don't filter
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
    db = get_db()
    cur = db.cursor()

    # Get all Smoothcomp rows for this user_id
    cur.execute("""
        SELECT athlete_name, athlete_display, team, event_date, event_title,
               division, placement, source, athlete_id
        FROM tournament_results
        WHERE source = 'smoothcomp' AND athlete_id = %s
        ORDER BY event_date DESC
    """, (str(sc_uid),))
    sc_rows = cur.fetchall()

    if not sc_rows:
        return render_template("trackbjj/not_found.html"), 404

    # Derive identity from SC rows
    names = list({r["athlete_display"] or r["athlete_name"] for r in sc_rows})
    display_name = max(names, key=len)
    # Use team from most recent registration first, then most recent completed result
    cur.execute("""
        SELECT team FROM tournament_results
        WHERE lower(athlete_name) LIKE %s AND team IS NOT NULL
        ORDER BY
            CASE WHEN status = 'registered' THEN 0 ELSE 1 END,
            event_date DESC NULLS LAST
        LIMIT 1
    """, (f"%{normalize(display_name).split()[-1]}%",))
    team_row = cur.fetchone()
    team = team_row["team"] if team_row else next((r["team"] for r in sc_rows if r["team"]), "Unknown")
    norm_display = normalize(display_name)
    parts = norm_display.split()
    last_name  = parts[-1] if parts else ""
    first_name = parts[0]  if parts else ""

    # Build fingerprint from SC divisions
    fp = athlete_fingerprint([r["division"] for r in sc_rows if r["division"]])

    # Check for manually verified claim first
    cur.execute("SELECT ibjjf_athlete_id, ibjjf_name, belt FROM sc_ibjjf_verified WHERE sc_uid = %s", (sc_uid,))
    verified_claim = cur.fetchone()

    # Look up in ibjjf_athletes (either from ranking scraper or from claim)
    ibjjf_match = None
    if verified_claim:
        cur.execute("SELECT ibjjf_id, name, slug, belt, academy, points, ranking_category, age_division, gi_nogi FROM ibjjf_athletes WHERE ibjjf_id = %s", (verified_claim["ibjjf_athlete_id"],))
        ibjjf_match = cur.fetchone()
        if not ibjjf_match:
            # Build a minimal match from the verified claim
            ibjjf_match = {
                "ibjjf_id": verified_claim["ibjjf_athlete_id"],
                "name": verified_claim["ibjjf_name"],
                "slug": None, "belt": verified_claim["belt"],
                "academy": None, "points": None,
                "ranking_category": None, "age_division": None, "gi_nogi": None,
            }

    if not ibjjf_match and last_name:
        cur.execute("""
            SELECT ibjjf_id, name, slug, belt, academy, points, ranking_category, age_division, gi_nogi
            FROM ibjjf_athletes
            WHERE name_lower LIKE %s
            ORDER BY points DESC NULLS LAST
            LIMIT 20
        """, (f"%{last_name}%",))
        candidates = cur.fetchall()
        best_score = 0.0
        for c in candidates:
            cname = normalize(c["name"] or "")
            score = first_name_score(first_name, cname)
            # Belt check: if we have belt info from rankings, must be within 2 levels
            if fp.get("belt") and c["belt"]:
                if not belt_matches(fp["belt"], c["belt"]):
                    continue
            if score > best_score and score >= 0.70:
                best_score = score
                ibjjf_match = dict(c)

    # Find upcoming IBJJF registrations matched by name (exact-ish)
    upcoming_rows = []
    if last_name:
        cur.execute("""
            SELECT athlete_name, athlete_display, team, event_date, event_title,
                   division, placement, source, status, event_id
            FROM tournament_results
            WHERE source = 'ibjjf' AND status = 'registered'
              AND lower(athlete_name) LIKE %s
            ORDER BY event_date ASC
        """, (f"%{last_name}%",))
        reg_candidates = cur.fetchall()
        for row in reg_candidates:
            row_name = normalize(row["athlete_display"] or row["athlete_name"] or "")
            if first_name_score(first_name, row_name) >= 0.50:
                upcoming_rows.append(dict(row, _source="ibjjf"))

    # Find IBJJF results — verified athlete_id takes priority
    ibjjf_rows = []
    ibjjf_verified = bool(verified_claim)  # True if user explicitly claimed their profile
    if ibjjf_match:
        # Query by ibjjf_athlete_id (separate namespace from SC athlete_id)
        cur.execute("""
            SELECT athlete_name, athlete_display, team, event_date, event_title,
                   division, placement, source
            FROM tournament_results
            WHERE source = 'ibjjf' AND ibjjf_athlete_id = %s
              AND (status IS NULL OR status != 'registered')
            ORDER BY event_date ASC
        """, (ibjjf_match["ibjjf_id"],))
        ibjjf_rows = cur.fetchall()

    if not ibjjf_rows and last_name:
        # Fuzzy fallback: filter by last name + belt + age + first name
        cur.execute("""
            SELECT athlete_name, athlete_display, team, event_date, event_title,
                   division, placement, source
            FROM tournament_results
            WHERE source = 'ibjjf' AND lower(athlete_name) LIKE %s
              AND (status IS NULL OR status != 'registered')
            ORDER BY event_date ASC
        """, (f"%{last_name}%",))
        candidates = cur.fetchall()

        for row in candidates:
            row_fp = parse_division(row["division"] or "")
            if not age_matches(fp.get("age"), row_fp.get("age")):
                continue
            if fp.get("gender") and row_fp.get("gender") and fp["gender"] != row_fp["gender"]:
                continue
            if not belt_matches(fp.get("belt"), row_fp.get("belt")):
                continue
            row_name = normalize(row["athlete_display"] or row["athlete_name"] or "")
            if first_name_score(first_name, row_name) < 0.50:
                continue
            ibjjf_rows.append(row)

    # Unified result list: SC + matched IBJJF, oldest → newest
    all_rows = []
    for r in sc_rows:
        all_rows.append(dict(r, _source="smoothcomp"))
    for r in ibjjf_rows:
        all_rows.append(dict(r, _source="ibjjf"))
    all_rows.sort(key=lambda r: (r["event_date"] or "0000-00-00"))

    # Stats across both sources
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
    cur.execute("SELECT * FROM athlete_social_links WHERE sc_uid = %s", (sc_uid,))
    social_links = cur.fetchone()

    # SC verification status
    cur.execute("SELECT sc_name FROM sc_smoothcomp_verified WHERE sc_uid = %s", (sc_uid,))
    sc_verified_row = cur.fetchone()
    sc_verified = bool(sc_verified_row)

    # Instagram posts (only if we have a stored token)
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

    # IBJJF rankings (only for verified athletes with a slug)
    ibjjf_rank_data = None
    _gender = (ibjjf_match or {}).get("gender") or fp.get("gender")
    if ibjjf_match and ibjjf_match.get("slug") and ibjjf_match.get("belt") and _gender:
        # Infer weight class from IBJJF results (most common non-open weight)
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
                cur=cur,
                slug=ibjjf_match["slug"],
                belt=ibjjf_match["belt"],
                gender=_gender,
                ranking_category=ibjjf_match.get("ranking_category") or "adult",
                age_division=ibjjf_match.get("age_division"),
                weight=weight_slug or "",
            )
        except Exception as e:
            log.warning("Failed to fetch IBJJF rankings for sc_uid=%s: %s", sc_uid, e)

    return render_template("trackbjj/athlete.html",
                           sc_uid=sc_uid,
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
                           now_year=__import__('datetime').date.today().year)


# ── Claim profile ──────────────────────────────────────────────────────────────

@app.route("/claim/<sc_uid>", methods=["GET", "POST"])
def claim_profile(sc_uid):
    """Allow an athlete to verify their IBJJF identity by logging in."""
    db = get_db()
    cur = db.cursor()

    # Check if already claimed
    cur.execute("SELECT ibjjf_athlete_id, ibjjf_name FROM sc_ibjjf_verified WHERE sc_uid = %s", (sc_uid,))
    existing = cur.fetchone()

    if request.method == "GET":
        return render_template("trackbjj/claim.html", sc_uid=sc_uid, existing=existing)

    # POST: attempt IBJJF login
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

    # Upsert into sc_ibjjf_verified
    cur.execute("""
        INSERT INTO sc_ibjjf_verified (sc_uid, ibjjf_athlete_id, ibjjf_name, belt, academy)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sc_uid) DO UPDATE SET
            ibjjf_athlete_id = EXCLUDED.ibjjf_athlete_id,
            ibjjf_name       = EXCLUDED.ibjjf_name,
            belt             = EXCLUDED.belt,
            academy          = EXCLUDED.academy,
            verified_at      = now()
    """, (sc_uid, ibjjf_id, ibjjf_name, belt, academy))

    # Also upsert into ibjjf_athletes with the ID so matching works
    ibjjf_gender = (profile.get("gender") or "").lower() or None
    cur.execute("""
        INSERT INTO ibjjf_athletes (ibjjf_id, name, name_lower, belt, gender, ranking_category, ranking_year)
        VALUES (%s, %s, %s, %s, %s, 'adult', %s)
        ON CONFLICT (ibjjf_id) DO UPDATE SET
            name      = EXCLUDED.name,
            name_lower = EXCLUDED.name_lower,
            belt      = COALESCE(EXCLUDED.belt, ibjjf_athletes.belt),
            gender    = COALESCE(EXCLUDED.gender, ibjjf_athletes.gender)
    """, (ibjjf_id, ibjjf_name, ibjjf_name.lower(), belt.lower() if belt else None,
          ibjjf_gender, __import__('datetime').date.today().year))

    db.commit()

    flash(f"Profile claimed! Verified as {ibjjf_name} (IBJJF ID: {ibjjf_id})", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── Smoothcomp verification ────────────────────────────────────────────────────

@app.route("/verify-sc/<sc_uid>", methods=["GET", "POST"])
def verify_sc(sc_uid):
    """Allow an athlete to verify they own a Smoothcomp profile."""
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT sc_name, sc_email FROM sc_smoothcomp_verified WHERE sc_uid = %s", (sc_uid,))
    existing = cur.fetchone()

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

    cur.execute("""
        INSERT INTO sc_smoothcomp_verified (sc_uid, sc_email, sc_user_id, sc_name, verified_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (sc_uid) DO UPDATE SET
            sc_email    = EXCLUDED.sc_email,
            sc_user_id  = EXCLUDED.sc_user_id,
            sc_name     = EXCLUDED.sc_name,
            verified_at = now()
    """, (sc_uid, result["email"], returned_id, result["sc_name"]))
    db.commit()

    flash(f"Smoothcomp profile verified! Welcome, {result['sc_name']}.", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── Social links ───────────────────────────────────────────────────────────────

@app.route("/social/<sc_uid>", methods=["GET", "POST"])
def social_links(sc_uid):
    """Allow an athlete to add/update their social media links."""
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT * FROM athlete_social_links WHERE sc_uid = %s", (sc_uid,))
    existing = cur.fetchone()

    if request.method == "GET":
        return render_template("trackbjj/social.html", sc_uid=sc_uid, existing=existing)

    instagram_handle = request.form.get("instagram_handle", "").strip().lstrip("@")
    facebook_url     = request.form.get("facebook_url", "").strip()
    youtube_url      = request.form.get("youtube_url", "").strip()

    cur.execute("""
        INSERT INTO athlete_social_links (sc_uid, instagram_handle, facebook_url, youtube_url, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (sc_uid) DO UPDATE SET
            instagram_handle = COALESCE(NULLIF(EXCLUDED.instagram_handle, ''), athlete_social_links.instagram_handle),
            facebook_url     = COALESCE(NULLIF(EXCLUDED.facebook_url, ''),     athlete_social_links.facebook_url),
            youtube_url      = COALESCE(NULLIF(EXCLUDED.youtube_url, ''),      athlete_social_links.youtube_url),
            updated_at       = now()
    """, (sc_uid, instagram_handle or None, facebook_url or None, youtube_url or None))
    db.commit()

    flash("Social links updated!", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── Meta / Instagram OAuth ─────────────────────────────────────────────────────

@app.route("/auth/instagram/<sc_uid>")
def instagram_auth_start(sc_uid):
    """Redirect the athlete to Facebook's OAuth dialog."""
    redirect_uri = _base_url(TRACKBJJ_PORT) + INSTAGRAM_CALLBACK_PATH
    oauth_url = meta_api.get_oauth_url(sc_uid, redirect_uri)
    return redirect(oauth_url)


@app.route("/auth/instagram/callback")
def instagram_auth_callback():
    """Handle Facebook OAuth callback, exchange code, store token."""
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
        # Exchange code for short-lived token
        short = meta_api.exchange_code(code, redirect_uri)
        short_token = short["access_token"]

        # Upgrade to long-lived token (~60 days)
        long = meta_api.get_long_lived_token(short_token)
        long_token   = long["access_token"]
        expires_in   = long.get("expires_in", 5184000)  # default 60 days

        # Find Instagram user ID
        ig_info = meta_api.get_instagram_user_id(long_token)
        ig_user_id  = ig_info["ig_user_id"]
        ig_username = ig_info.get("ig_username", "")

    except Exception as e:
        log.error("Instagram OAuth failed for sc_uid=%s: %s", sc_uid, e)
        flash(f"Instagram connection failed: {e}", "error")
        return redirect(url_for("athlete_profile", sc_uid=sc_uid))

    import datetime
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=int(expires_in))

    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO athlete_social_links
            (sc_uid, instagram_user_id, instagram_token, instagram_token_expires,
             instagram_handle, updated_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (sc_uid) DO UPDATE SET
            instagram_user_id      = EXCLUDED.instagram_user_id,
            instagram_token        = EXCLUDED.instagram_token,
            instagram_token_expires = EXCLUDED.instagram_token_expires,
            instagram_handle       = COALESCE(EXCLUDED.instagram_handle, athlete_social_links.instagram_handle),
            updated_at             = now()
    """, (sc_uid, ig_user_id, long_token, expires_at, ig_username or None))
    db.commit()

    flash(f"Instagram connected! (@{ig_username})", "success")
    return redirect(url_for("athlete_profile", sc_uid=sc_uid))


# ── API ────────────────────────────────────────────────────────────────────────

@app.route("/api/search")
def api_search():
    """Search athletes by name. Returns top matches with their SC user_id."""
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    db = get_db()
    cur = db.cursor()

    # Search by normalized name, group by user_id
    cur.execute("""
        SELECT
            athlete_id,
            MAX(athlete_display) as display_name,
            MAX(team) as team,
            COUNT(DISTINCT event_id) as event_count,
            MAX(event_date) as last_seen,
            string_agg(DISTINCT source, ',') as sources
        FROM tournament_results
        WHERE lower(athlete_name) LIKE %s
          AND athlete_id ~ '^[0-9]+$'
        GROUP BY athlete_id
        ORDER BY event_count DESC
        LIMIT 20
    """, (f"%{normalize(q)}%",))
    rows = cur.fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/athlete/<sc_uid>/results")
def api_athlete_results(sc_uid):
    """All results for a Smoothcomp user_id."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT source, event_date, event_title, division, placement,
               athlete_name, athlete_display, team, event_id
        FROM tournament_results
        WHERE athlete_id = %s AND source = 'smoothcomp'
        ORDER BY event_date DESC
    """, (str(sc_uid),))
    return jsonify([dict(r) for r in cur.fetchall()])


@app.route("/api/stats")
def api_stats():
    """DB summary stats."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT source, COUNT(*) rows,
               COUNT(DISTINCT CASE WHEN athlete_id ~ '^[0-9]+$' THEN athlete_id END) unique_athletes
        FROM tournament_results GROUP BY source ORDER BY source
    """)
    rows = cur.fetchall()
    cur.execute("SELECT COUNT(DISTINCT athlete_id) FROM tournament_results WHERE athlete_id ~ '^[0-9]+$' AND source='smoothcomp'")
    sc_athletes = cur.fetchone()["count"]
    return jsonify({"by_source": [dict(r) for r in rows], "smoothcomp_athletes": sc_athletes})


if __name__ == "__main__":
    port = int(os.environ.get("TRACKBJJ_PORT", 5951))
    log.info("trackbjj.net dev server → http://0.0.0.0:%d", port)
    log.info("Access from Windows at http://172.23.93.61:%d", port)
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=True)
