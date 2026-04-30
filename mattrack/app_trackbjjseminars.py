"""
trackbjjseminars.net — One-time BJJ Seminar Listings

Sibling to trackmat / trackbjj / trackopenmat. Shares Supabase auth.
Local dev port: 5953.
"""

import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request, redirect, url_for, flash, make_response
from dotenv import load_dotenv
from jinja2 import ChoiceLoader, FileSystemLoader
from supabase import create_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from auth import get_user_from_token, get_user_plan, is_plan_active

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder="trackbjjseminars/templates",
    static_folder="trackbjjseminars/static",
)
app.jinja_loader = ChoiceLoader([
    app.jinja_loader,
    FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
])
app.secret_key = os.environ.get("SECRET_KEY", "trackbjjseminars-dev-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
PROD_URL = os.environ.get("TRACKBJJSEMINARS_URL", "https://www.trackbjjseminars.net")

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL and SUPABASE_SERVICE_KEY else None


def _base_url(port):
    host = request.host.split(":")[0]
    if request.headers.get("X-Forwarded-Proto") == "https":
        return f"https://{host}"
    return f"http://{host}:{port}"


@app.context_processor
def inject_chrome():
    if request.headers.get("X-Forwarded-Proto") == "https":
        site_url = PROD_URL
    else:
        site_url = _base_url(int(os.environ.get("PORT", 5953)))
    return dict(
        site_name="TrackSeminars",
        site_subtitle="BJJ Seminars Worldwide",
        site_url=site_url,
        site_icon="📅",
        site_key="seminars",
        sibling_name="TrackBJJ",
        sibling_url="https://www.trackbjj.net",
        header_home_fn="",
        show_lang=True,
    )


def _slugify(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Homepage — upcoming seminars chronologically."""
    if not sb:
        return render_template("trackbjjseminars/index.html", upcoming=[], total=0)
    now = datetime.now(timezone.utc).isoformat()
    try:
        rows = (sb.table("seminars")
                  .select("slug,title,instructor_name,host_city,host_state,host_country,start_datetime,cost_usd,gi_required,topic,level")
                  .gte("start_datetime", now)
                  .eq("approved", True)
                  .order("start_datetime")
                  .limit(50)
                  .execute()).data or []
        cnt = (sb.table("seminars")
                 .select("id", count="exact")
                 .gte("start_datetime", now)
                 .eq("approved", True)
                 .limit(1)
                 .execute())
        total = cnt.count or 0
    except Exception as e:
        log.warning("index: %s", e)
        rows, total = [], 0
    return render_template("trackbjjseminars/index.html", upcoming=rows, total=total)


@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    return render_template("trackbjjseminars/search.html", q=q)


@app.route("/seminar/<slug>")
def seminar_detail(slug):
    if not sb:
        flash("Database unavailable", "error")
        return redirect(url_for("index"))
    try:
        rows = (sb.table("seminars").select("*").eq("slug", slug).limit(1).execute()).data or []
    except Exception as e:
        log.warning("seminar fetch %s: %s", slug, e)
        rows = []
    if not rows:
        return render_template("trackbjjseminars/not_found.html", slug=slug), 404
    return render_template("trackbjjseminars/seminar.html", seminar=rows[0])


@app.route("/submit")
def submit_form():
    """Submission form for new seminars (auth required client-side)."""
    return render_template("trackbjjseminars/submit.html")


# ── APIs ───────────────────────────────────────────────────────────────────────

@app.route("/api/seminars")
def api_seminars():
    """List/search upcoming seminars. ?q=, ?city=, ?limit="""
    if not sb:
        return jsonify({"seminars": []})
    q = (request.args.get("q") or "").strip().lower()
    city = (request.args.get("city") or "").strip().lower()
    limit = max(1, min(int(request.args.get("limit", 50) or 50), 200))
    now = datetime.now(timezone.utc).isoformat()

    try:
        query = (sb.table("seminars")
                   .select("slug,title,instructor_name,host_city,host_state,host_country,"
                           "start_datetime,cost_usd,gi_required,topic,level,registration_url")
                   .gte("start_datetime", now)
                   .eq("approved", True))
        if q:
            query = query.or_(f"title.ilike.%{q}%,instructor_name.ilike.%{q}%,topic.ilike.%{q}%")
        if city:
            query = query.ilike("host_city", f"%{city}%")
        rows = query.order("start_datetime").limit(limit).execute().data or []
    except Exception as e:
        log.warning("api_seminars: %s", e)
        rows = []
    return jsonify({"seminars": rows, "count": len(rows)})


@app.route("/api/seminars", methods=["POST"])
def api_seminars_create():
    """Create a new seminar. Requires authenticated user. Pending until approved."""
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "auth_required"}), 401
    if not sb:
        return jsonify({"error": "db_unavailable"}), 503

    body = request.get_json(silent=True) or {}
    title = (body.get("title") or "").strip()
    instructor = (body.get("instructor_name") or "").strip()
    start_dt = (body.get("start_datetime") or "").strip()
    if not title or not instructor or not start_dt:
        return jsonify({"error": "missing_required_fields"}), 400

    base_slug = _slugify(f"{instructor}-{title}-{start_dt[:10]}")[:80] or "seminar"
    slug = base_slug
    try:
        # Ensure slug uniqueness (light retry)
        for i in range(5):
            existing = sb.table("seminars").select("slug").eq("slug", slug).limit(1).execute().data
            if not existing:
                break
            slug = f"{base_slug}-{i+2}"

        row = {
            "slug":               slug,
            "title":               title,
            "instructor_name":     instructor,
            "co_instructors":      body.get("co_instructors") or [],
            "host_school_id":      body.get("host_school_id"),
            "host_venue_name":     body.get("host_venue_name"),
            "host_address":        body.get("host_address"),
            "host_city":           body.get("host_city"),
            "host_state":          body.get("host_state"),
            "host_country":        body.get("host_country"),
            "start_datetime":      start_dt,
            "end_datetime":        body.get("end_datetime"),
            "timezone":            body.get("timezone") or "America/New_York",
            "cost_usd":            body.get("cost_usd"),
            "registration_url":    body.get("registration_url"),
            "registration_deadline": body.get("registration_deadline"),
            "max_attendees":       body.get("max_attendees"),
            "level":               body.get("level") or "All Levels",
            "gi_required":         body.get("gi_required"),
            "topic":               body.get("topic"),
            "description_md":      body.get("description_md"),
            "flyer_url":           body.get("flyer_url"),
            "video_promo_url":     body.get("video_promo_url"),
            "approved":            False,  # pending review
            "created_by_user_id":  user["sub"],
        }
        sb.table("seminars").insert(row).execute()
    except Exception as e:
        log.warning("api_seminars create: %s", e)
        return jsonify({"error": "create_failed"}), 500

    return jsonify({"ok": True, "slug": slug, "status": "pending_review"})


@app.route("/api/auth/me")
def api_auth_me():
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
        log.warning("api_auth_me: %s", e)
        return jsonify({"plan": "free", "authenticated": False})


@app.route("/auth-relay")
def auth_relay():
    html = """<!DOCTYPE html>
<html><head><meta charset="utf-8"></head><body>
<script>
var TRUSTED=['https://www.trackmat.net','https://trackmat.net','https://www.trackbjj.net','https://trackbjj.net','https://www.trackopenmat.net','https://trackopenmat.net','https://www.trackbjjseminars.net','https://trackbjjseminars.net'];
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


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "site": "trackbjjseminars"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5953))
    log.info("trackbjjseminars.net dev → http://0.0.0.0:%d", port)
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=True)
