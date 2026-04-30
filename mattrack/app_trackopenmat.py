"""
trackopenmat.net — BJJ School & Open Mat Directory

Sibling to trackmat.net / trackbjj.net / trackbjjseminars.net.
Shares the same Supabase project (single auth across all 4 sites).
Local dev port: 5952.
"""

import logging
import os
import re
import sys
import unicodedata

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
    template_folder="trackopenmat/templates",
    static_folder="trackopenmat/static",
)
app.jinja_loader = ChoiceLoader([
    app.jinja_loader,
    FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
])
app.secret_key = os.environ.get("SECRET_KEY", "trackopenmat-dev-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
TRACKOPENMAT_PROD_URL = os.environ.get("TRACKOPENMAT_URL", "https://www.trackopenmat.net")

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL and SUPABASE_SERVICE_KEY else None


def _base_url(port):
    host = request.host.split(":")[0]
    if request.headers.get("X-Forwarded-Proto") == "https":
        return f"https://{host}"
    return f"http://{host}:{port}"


@app.context_processor
def inject_chrome():
    if request.headers.get("X-Forwarded-Proto") == "https":
        site_url = TRACKOPENMAT_PROD_URL
    else:
        site_url = _base_url(int(os.environ.get("PORT", 5952)))
    return dict(
        site_name="TrackOpenMat",
        site_subtitle="School & Open Mat Directory",
        site_url=site_url,
        site_icon="🏠",
        site_key="openmat",
        sibling_name="TrackBJJ",
        sibling_url="https://www.trackbjj.net",
        header_home_fn="",
        show_lang=True,
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _slugify(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


# ── Routes — pages ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Homepage — featured/claimed schools + city search."""
    if not sb:
        return render_template("trackopenmat/index.html", featured=[], total=0)
    try:
        # Claimed schools (those with real owners) up top, recently added next
        claimed = (sb.table("schools")
                     .select("team_slug,display_name,city,state_province,country")
                     .eq("claimed", True)
                     .order("updated_at", desc=True)
                     .limit(12)
                     .execute()).data or []
        cnt = (sb.table("schools").select("id", count="exact").limit(1).execute())
        total = cnt.count or 0
    except Exception as e:
        log.warning("index fetch: %s", e)
        claimed, total = [], 0
    return render_template("trackopenmat/index.html", featured=claimed, total=total)


@app.route("/search")
def search():
    """Search schools by name or city."""
    q = (request.args.get("q") or "").strip()
    return render_template("trackopenmat/search.html", q=q)


@app.route("/school/<slug>")
def school_detail(slug):
    """Detail page for one school."""
    if not sb:
        flash("Database unavailable", "error")
        return redirect(url_for("index"))
    try:
        rows = (sb.table("schools")
                  .select("*")
                  .eq("team_slug", slug)
                  .limit(1)
                  .execute()).data or []
    except Exception as e:
        log.warning("school fetch %s: %s", slug, e)
        rows = []
    if not rows:
        return render_template("trackopenmat/not_found.html", slug=slug), 404
    school = rows[0]

    try:
        sched = (sb.table("school_schedules")
                   .select("day_of_week,start_time,end_time,class_type,level,coach_name,notes")
                   .eq("school_id", school["id"])
                   .order("day_of_week")
                   .order("start_time")
                   .execute()).data or []
    except Exception:
        sched = []

    by_day = {i: [] for i in range(7)}
    for s in sched:
        by_day[int(s["day_of_week"])].append(s)
    schedule = [{"day": DAYS[i], "classes": by_day[i]} for i in range(7)]

    return render_template(
        "trackopenmat/school.html",
        school=school,
        schedule=schedule,
        days=DAYS,
    )


@app.route("/openmats")
def openmats():
    """Browse all upcoming open mats (any school marked Open Mat in schedule)."""
    return render_template("trackopenmat/openmats.html")


@app.route("/claim/<slug>")
def claim(slug):
    """Claim a school — auth required, sets up school_claims row."""
    return render_template("trackopenmat/claim.html", slug=slug)


# ── Routes — APIs ──────────────────────────────────────────────────────────────

@app.route("/api/schools")
def api_schools():
    """Search/list endpoint. Optional ?q= name/city filter, ?limit=, ?city="""
    if not sb:
        return jsonify({"schools": []})
    q = (request.args.get("q") or "").strip().lower()
    city = (request.args.get("city") or "").strip().lower()
    limit = max(1, min(int(request.args.get("limit", 50) or 50), 200))

    try:
        query = sb.table("schools").select(
            "team_slug,display_name,city,state_province,country,claimed,head_instructor_name"
        )
        if q:
            query = query.ilike("display_name", f"%{q}%")
        if city:
            query = query.ilike("city", f"%{city}%")
        rows = (query.order("claimed", desc=True)
                     .order("display_name")
                     .limit(limit)
                     .execute()).data or []
    except Exception as e:
        log.warning("api_schools: %s", e)
        rows = []
    return jsonify({"schools": rows, "count": len(rows)})


@app.route("/api/openmats")
def api_openmats():
    """List all schools that have an Open Mat scheduled, with day/time + city."""
    if not sb:
        return jsonify({"openmats": []})
    try:
        rows = (sb.table("school_schedules")
                  .select("day_of_week,start_time,end_time,notes,schools(team_slug,display_name,city,state_province)")
                  .eq("class_type", "Open Mat")
                  .order("day_of_week")
                  .limit(500)
                  .execute()).data or []
    except Exception as e:
        log.warning("api_openmats: %s", e)
        rows = []

    out = []
    for r in rows:
        s = r.get("schools") or {}
        out.append({
            "school_slug":   s.get("team_slug"),
            "school_name":   s.get("display_name"),
            "city":          s.get("city"),
            "state":         s.get("state_province"),
            "day":           DAYS[int(r["day_of_week"])],
            "day_idx":       int(r["day_of_week"]),
            "start_time":    r.get("start_time"),
            "end_time":      r.get("end_time"),
            "notes":         r.get("notes"),
        })
    return jsonify({"openmats": out, "count": len(out)})


@app.route("/api/claim/<slug>", methods=["POST"])
def api_claim(slug):
    """Submit a claim for a school. Requires authenticated user."""
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "auth_required"}), 401

    if not sb:
        return jsonify({"error": "db_unavailable"}), 503

    try:
        rows = (sb.table("schools").select("id").eq("team_slug", slug).limit(1).execute()).data or []
        if not rows:
            return jsonify({"error": "school_not_found"}), 404
        school_id = rows[0]["id"]

        sb.table("school_claims").upsert({
            "school_id":           school_id,
            "user_id":             user["sub"],
            "role":                "owner",
            "verified":            False,
            "verification_method": "email_match_pending",
        }, on_conflict="school_id,user_id").execute()
    except Exception as e:
        log.warning("claim %s: %s", slug, e)
        return jsonify({"error": "claim_failed"}), 500

    return jsonify({"ok": True, "status": "pending_verification"})


@app.route("/api/auth/me")
def api_auth_me():
    """Plan/auth status for the shared header."""
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
    """Cross-domain session relay for the 4-site family."""
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
    return jsonify({"ok": True, "site": "trackopenmat"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5952))
    log.info("trackopenmat.net dev → http://0.0.0.0:%d", port)
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=True)
