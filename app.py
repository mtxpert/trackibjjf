"""
MatTrack — Tournament Tracker
Scalable architecture:
  - Roster cache: one Playwright scrape per tournament serves all users
  - Client-side filtering: search is a JS .filter(), zero server load
  - Shared bracket state: background poller updates once per interval for all users
  - Server-Sent Events: pushes bracket changes to all connected clients simultaneously
"""

from flask import Flask, render_template, jsonify, request, Response, send_file
import os, threading, time, json, queue, re, logging, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import watcher as _watcher
import scraper as _scraper

load_dotenv()

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mattrack-secret-key")

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=[],
    storage_uri="memory://",
)

BASE_URL = "https://www.bjjcompsystem.com"

# ── Tournament timezone lookup ────────────────────────────────────────────────

_TOURNEY_TZ = {
    'houston':       'America/Chicago',
    'dallas':        'America/Chicago',
    'chicago':       'America/Chicago',
    'new york':      'America/New_York',
    'miami':         'America/New_York',
    'atlanta':       'America/New_York',
    'boston':        'America/New_York',
    'washington':    'America/New_York',
    'los angeles':   'America/Los_Angeles',
    'orange county': 'America/Los_Angeles',
    'san francisco': 'America/Los_Angeles',
    'seattle':       'America/Los_Angeles',
    'denver':        'America/Denver',
    'phoenix':       'America/Phoenix',
    'milan':         'Europe/Rome',
    'rome':          'Europe/Rome',
    'paris':         'Europe/Paris',
    'london':        'Europe/London',
    'portugal':      'Europe/Lisbon',
    'lisbon':        'Europe/Lisbon',
    'madrid':        'Europe/Madrid',
    'barcelona':     'Europe/Madrid',
    'recife':        'America/Recife',
    'rio':           'America/Sao_Paulo',
    'sao paulo':     'America/Sao_Paulo',
    'toronto':       'America/Toronto',
    'montreal':      'America/Toronto',
    'dubai':         'Asia/Dubai',
    'abu dhabi':     'Asia/Dubai',
    'tokyo':         'Asia/Tokyo',
    'sydney':        'Australia/Sydney',
}

def _tournament_tz(tournament_name):
    name_lower = (tournament_name or '').lower()
    for kw, tz in _TOURNEY_TZ.items():
        if kw in name_lower:
            return tz
    return 'America/New_York'   # default

_FIGHT_TIME_RE = re.compile(
    r'\w{3}\s+(\d{2})/(\d{2})\s+at\s+(\d{1,2}:\d{2}\s*[AP]M)', re.IGNORECASE
)

def _fight_time_to_utc(fight_time_str, tz_name):
    """Convert 'Sat 04/12 at 2:30 PM' (tournament-local) → UTC ISO string."""
    m = _FIGHT_TIME_RE.search(fight_time_str or '')
    if not m:
        return None
    try:
        month, day, time_str = int(m.group(1)), int(m.group(2)), m.group(3).strip()
        year = datetime.now().year
        tz   = ZoneInfo(tz_name)
        dt   = datetime.strptime(f"{year}-{month:02d}-{day:02d} {time_str}", "%Y-%m-%d %I:%M %p")
        return dt.replace(tzinfo=tz).astimezone(ZoneInfo('UTC')).isoformat()
    except Exception:
        return None


# ── In-memory stores ──────────────────────────────────────────────────────────
_jobs        = {}   # search jobs (legacy live-scrape path)
_build_jobs  = {}   # roster build jobs
_brackets    = {}   # category_id -> latest bracket state (shared by all users)

# Pre-load completed brackets from Supabase so past-tournament results survive
# server restarts without re-fetching from bjjcompsystem / Smoothcomp.
def _warm_brackets_from_db():
    try:
        from results import load_bracket_finals
        loaded = load_bracket_finals()
        _brackets.update(loaded)
        logger.info("Warmed %d bracket finals from Supabase", len(loaded))
    except Exception as e:
        logger.warning("Could not warm brackets from Supabase: %s", e)

threading.Thread(target=_warm_brackets_from_db, daemon=True).start()

# ── NAGA routing helpers ──────────────────────────────────────────────────────
def _is_naga_tournament(tournament_id):
    """NAGA Smoothcomp IDs are 5-digit numbers (≥10000); IBJJF IDs are 4-digit (≤9999)."""
    try:
        return str(tournament_id).isdigit() and int(tournament_id) >= 10000
    except (ValueError, TypeError):
        return False

def _naga_event_id(tournament_id):
    return str(tournament_id)

def _naga_subdomain(tournament_name=""):
    """Infer subdomain from tournament name; default to 'naga'."""
    name_lower = (tournament_name or "").lower()
    if "compnet" in name_lower:
        return "compnet"
    return "naga"


def _subdomain_to_source(subdomain: str) -> str:
    """Map Smoothcomp subdomain to MatTrack source string."""
    return "compnet" if subdomain == "compnet" else "naga"

# ── Shared bracket watcher ────────────────────────────────────────────────────
# category_id -> {tournament_id, last_fetched, interval_sec}
_watch_registry = {}
_watch_lock     = threading.Lock()

# SSE subscriber queues: tournament_id -> list[queue.Queue]
_sse_clients = {}
_sse_lock    = threading.Lock()


def _send_push_notifications(category_id: str, division: str, changes: list) -> None:
    """Send web push to all subscribers watching category_id. Runs in daemon thread."""
    try:
        from pywebpush import webpush, WebPushException
        from supabase import create_client
        import json as _json

        vapid_private = os.environ.get("VAPID_PRIVATE_KEY", "")
        vapid_email   = os.environ.get("VAPID_EMAIL", "mailto:info@mattrack.net")
        if not vapid_private:
            return

        sb = create_client(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_KEY", ""),
        )
        rows = sb.table("push_subscriptions") \
                 .select("endpoint,p256dh,auth") \
                 .contains("category_ids", [category_id]) \
                 .execute()

        if not rows.data:
            return

        # Build a concise notification body from changes
        notable = [c for c in changes if any(k in c for k in ("Mat", "FINISHED", "COMPLETE", "time:"))]
        body = notable[0] if notable else changes[0]
        short_div = division.split(" ")[:4]
        title = "🥋 " + " ".join(short_div) if division else "🥋 MatTrack"

        payload = _json.dumps({"title": title, "body": body, "tag": f"cat-{category_id}", "url": "/"})

        private_pem = (
            "-----BEGIN PRIVATE KEY-----\n" +
            vapid_private + "\n-----END PRIVATE KEY-----"
            if "BEGIN" not in vapid_private else vapid_private
        )

        for row in rows.data:
            try:
                webpush(
                    subscription_info={
                        "endpoint": row["endpoint"],
                        "keys": {"p256dh": row["p256dh"], "auth": row["auth"]},
                    },
                    data=payload,
                    vapid_private_key=private_pem,
                    vapid_claims={"sub": vapid_email},
                )
            except WebPushException as e:
                if "410" in str(e) or "404" in str(e):
                    # Subscription expired — remove it
                    try:
                        sb.table("push_subscriptions").delete().eq("endpoint", row["endpoint"]).execute()
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception as e:
        logger.error("push notification send failed cat=%s: %s", category_id, e)


def _sse_push(tournament_id, event_data):
    with _sse_lock:
        qs = list(_sse_clients.get(tournament_id, []))
    for q in qs:
        try:
            q.put_nowait(event_data)
        except queue.Full:
            pass   # slow client — drop event rather than block


def _tournament_is_live(tournament_id: str) -> bool:
    """Return True only if the tournament's start date is today (day-of only)."""
    today = date.today().isoformat()
    # Check all known tournament lists for this id
    try:
        from pathlib import Path as _P
        import json as _j
        seed = _P(__file__).parent / "seed_cache" / "tournaments.json"
        if seed.exists():
            for t in _j.loads(seed.read_text()):
                if str(t.get("id")) == str(tournament_id):
                    start = t.get("start") or t.get("date", "")
                    end   = t.get("end", start)
                    return start <= today <= end
    except Exception:
        pass
    # NAGA: check naga seed cache
    try:
        from scraper_naga import _load_naga_cache
        ev = _load_naga_cache().get(str(tournament_id))
        if ev:
            start = ev.get("start", "")
            end   = ev.get("end", start)
            return bool(start) and start <= today <= (end or start)
    except Exception:
        pass
    return False


def register_watch(tournament_id, category_id, interval_sec=30):
    """Register a category for background polling.
    Skips if bracket is already final (in DB or memory) or tournament isn't today."""
    with _watch_lock:
        # Already finished — never re-poll
        cached = _brackets.get(category_id)
        if cached and cached.get("results_final"):
            return
        # Only poll live (day-of) tournaments
        if not _tournament_is_live(tournament_id):
            return
        existing = _watch_registry.get(category_id, {})
        _watch_registry[category_id] = {
            "tournament_id": tournament_id,
            "last_fetched":  existing.get("last_fetched", 0),
            "interval_sec":  interval_sec,
        }


def _persist_final_bracket(cid, tournament_id, state, source="ibjjf", tournament_name=""):
    """Fire-and-forget: save a results_final bracket to Supabase in a daemon thread."""
    def _save():
        try:
            from results import save_bracket_final
            # Infer event_date from fight times in state
            import re as _re
            event_date = ""
            for fight in state.get("fights", []):
                m = _re.search(r'(\d{2})/(\d{2})', fight.get("time") or "")
                if m:
                    from datetime import date as _date
                    y = _date.today().year
                    event_date = f"{y}-{m.group(1)}-{m.group(2)}"
                    break
            save_bracket_final(
                category_id=cid,
                tournament_id=tournament_id,
                tournament_name=tournament_name or tournament_id,
                division=state.get("division", ""),
                source=source,
                ranking=state.get("ranking", []),
                state=state,
                event_date=event_date,
            )
        except Exception as e:
            logger.warning("_persist_final_bracket(%s): %s", cid, e)
    threading.Thread(target=_save, daemon=True).start()


def _process_batch_results(batch_results, tid_by_cid):
    """
    After a fetch_brackets_batch run: update shared state, push SSE events,
    remove completed brackets from the watch registry.
    """
    from watcher import diff_states, save_state, load_state

    for cid, state in batch_results.items():
        tournament_id = tid_by_cid.get(cid, "")
        if "error" in state:
            continue
        try:
            old     = _brackets.get(cid) or load_state(cid)
            changes = diff_states(old, state) if old else []
            save_state(cid, state)
            state["changes"]     = changes
            state["bracket_url"] = f"{BASE_URL}/tournaments/{tournament_id}/categories/{cid}"
            _brackets[cid]       = state

            if state.get("results_final"):
                # Read source BEFORE popping — after pop, the entry is gone
                info = _watch_registry.get(cid) or {}
                with _watch_lock:
                    _watch_registry.pop(cid, None)
                # Persist to Supabase so results survive restarts
                _persist_final_bracket(cid, tournament_id, state, source=info.get("source", "ibjjf"))
            else:
                with _watch_lock:
                    if cid in _watch_registry:
                        _watch_registry[cid]["last_fetched"] = time.time()

            if changes:
                _sse_push(tournament_id, {
                    "type":        "bracket_update",
                    "category_id": cid,
                    "changes":     changes,
                })
                # Fire web push notifications to subscribed users
                threading.Thread(
                    target=_send_push_notifications,
                    args=(cid, state.get("division", ""), changes),
                    daemon=True,
                ).start()
                # Clear immediately so /api/refresh doesn't show them again
                state["changes"] = []
        except Exception:
            pass


def _naga_register_watch(tournament_id, category_id, subdomain="naga", interval_sec=30):
    """Register a NAGA bracket for background polling. Day-of only."""
    cid = str(category_id)
    with _watch_lock:
        cached = _brackets.get(cid)
        if cached and cached.get("results_final"):
            return
        if not _tournament_is_live(str(tournament_id)):
            return
        existing = _watch_registry.get(cid, {})
        _watch_registry[cid] = {
            "tournament_id": str(tournament_id),
            "last_fetched":  existing.get("last_fetched", 0),
            "interval_sec":  interval_sec,
            "source":        _subdomain_to_source(subdomain),
            "subdomain":     subdomain,
        }


def _background_poller():
    """
    Single long-running daemon thread.
    Fetches all stale brackets concurrently via thread pool (no Playwright),
    pushes changes via SSE, removes completed brackets from the registry.
    ~2s to refresh 166 brackets; polls every 30s.
    Handles both IBJJF and NAGA brackets.
    """
    from watcher import fetch_brackets_batch

    while True:
        now = time.time()
        ibjjf_refresh = []
        naga_refresh  = []
        tid_by_cid    = {}

        with _watch_lock:
            for cid, info in list(_watch_registry.items()):
                if (now - info["last_fetched"]) >= info["interval_sec"]:
                    tid = info["tournament_id"]
                    tid_by_cid[cid] = tid
                    if info.get("source") in ("naga", "compnet"):
                        naga_refresh.append((tid, cid, info.get("subdomain", "naga")))
                    else:
                        ibjjf_refresh.append((tid, cid, ""))

        if ibjjf_refresh:
            try:
                batch_results = fetch_brackets_batch(ibjjf_refresh, concurrency=20)
                _process_batch_results(batch_results, tid_by_cid)
            except Exception:
                pass

        if naga_refresh:
            try:
                from scraper_naga import fetch_naga_brackets_batch
                batch_results = fetch_naga_brackets_batch(naga_refresh, concurrency=10)
                _process_batch_results(batch_results, tid_by_cid)
            except Exception:
                pass


        time.sleep(10)   # check every 10s for newly stale entries


# Start background poller once at import time
threading.Thread(target=_background_poller, daemon=True).start()


def _ingest_bracket_results(tid, bracket_results):
    """Store bracket states and register non-final ones for watching."""
    from watcher import save_state as _save_state
    for cid, state in bracket_results.items():
        if "error" in state:
            continue
        _save_state(cid, state)
        state["bracket_url"] = f"{BASE_URL}/tournaments/{tid}/categories/{cid}"
        _brackets[cid] = state
        if not state.get("results_final"):
            register_watch(tid, cid)


def _build_one_tournament(t):
    """Build roster + ingest brackets for a single tournament. Runs in thread pool."""
    from scraper import build_roster
    tid    = t["id"]
    job_id = f"roster_{tid}"
    if _build_jobs.get(job_id, {}).get("status") == "running":
        return
    job = {"status": "running", "progress": 0, "total": 0, "current_cat": ""}
    _build_jobs[job_id] = job
    try:
        bracket_results = build_roster(tid, job)
        _ingest_bracket_results(tid, bracket_results)
    except Exception as e:
        job["status"] = "error"
        job["error"]  = str(e)


def _auto_discover():
    """
    Reads the seeded tournament list and builds rosters for any not yet cached.
    Uses the seed_cache/tournaments.json to avoid any live network calls at startup.
    """
    import time as _time
    import json as _json
    from pathlib import Path as _Path
    from scraper import load_roster_cache
    _time.sleep(10)   # let Flask fully start and serve initial requests

    while True:
        try:
            # Read from seed file — no live network call on startup
            seed = _Path(__file__).parent / "seed_cache" / "tournaments.json"
            if seed.exists():
                tournaments = _json.loads(seed.read_text())
            else:
                from scraper import get_tournaments
                tournaments = get_tournaments()

            from datetime import date as _date
            today = _date.today().isoformat()
            for t in tournaments:
                try:
                    start = t.get("start") or t.get("date", "")
                    end   = t.get("end", start)
                    # Only build rosters for today's tournaments
                    if not (start <= today <= (end or start)):
                        continue
                    if not load_roster_cache(t["id"]):
                        _build_one_tournament(t)
                        _time.sleep(5)
                except Exception:
                    pass

        except Exception:
            pass

        _time.sleep(3600)


threading.Thread(target=_auto_discover, daemon=True).start()


# ── Helpers ───────────────────────────────────────────────────────────────────


def refresh_bracket(tournament_id, category_id, category_name=""):
    """Fetch a bracket page and store the shared state."""
    from watcher import fetch_bracket, diff_states, save_state, load_state
    try:
        state   = fetch_bracket(tournament_id, category_id, category_name)
        old     = _brackets.get(category_id) or load_state(category_id)
        changes = diff_states(old, state) if old else []
        save_state(category_id, state)
        state["changes"]     = changes
        state["bracket_url"] = f"{BASE_URL}/tournaments/{tournament_id}/categories/{category_id}"
        _brackets[category_id] = state
        return state
    except Exception as e:
        logger.error("bracket fetch failed cat=%s: %s", category_id, e, exc_info=True)
        err = {"error": "Failed to fetch bracket", "category_id": category_id,
               "bracket_url": f"{BASE_URL}/tournaments/{tournament_id}/categories/{category_id}"}
        _brackets[category_id] = err
        return err


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/browser")
def tournament_browser():
    return send_file(os.path.join(os.path.dirname(__file__), "tournament_browser.html"))


_IBJJF_LOGO_CACHE: dict = {}   # {ts, svg_bytes}

@app.route("/api/org-logo/ibjjf")
def org_logo_ibjjf():
    """Proxy the IBJJF logo SVG at a stable URL (resolves webpack hash automatically)."""
    import re as _re
    cached = _IBJJF_LOGO_CACHE
    if cached.get("ts") and time.time() - cached["ts"] < 86400:
        return Response(cached["svg"], mimetype="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=86400"})
    try:
        hdrs = {"User-Agent": "Mozilla/5.0"}
        page = requests.get("https://ibjjf.com", headers=hdrs, timeout=8).text
        m = _re.search(r'(/packs/media/images/ibjjf/logo-ibjjf[^\'"]+\.svg)', page)
        if m:
            svg = requests.get("https://ibjjf.com" + m.group(1), headers=hdrs, timeout=8).content
            cached["svg"] = svg
            cached["ts"]  = time.time()
            return Response(svg, mimetype="image/svg+xml",
                            headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        logger.error("ibjjf logo proxy failed: %s", e)
    return Response(status=404)


@app.route("/api/geocode")
def api_geocode():
    """Geocode a city/state string. Uses static dict first, then Nominatim."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "no query"}), 400
    from scraper import _geocode, _CITY_COORDS
    # Try static dict
    coords = _geocode(q, "")
    if coords:
        return jsonify({"lat": coords[0], "lng": coords[1], "source": "static"})
    # Nominatim fallback
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "json", "limit": 1},
            headers={"User-Agent": "MatTrack/1.0"},
            timeout=5,
        )
        data = resp.json()
        if data:
            return jsonify({"lat": float(data[0]["lat"]), "lng": float(data[0]["lon"]), "source": "nominatim"})
    except Exception as e:
        logger.warning("Nominatim geocode failed for %r: %s", q, e)
    return jsonify({"error": "not found"}), 404


@app.route("/api/browser-events")
def api_browser_events():
    """All events for the tournament browser: Smoothcomp (all orgs) + IBJJF schedule."""
    from scraper_smoothcomp import get_smoothcomp_events
    from scraper import get_ibjjf_schedule

    sc, ibjjf = [], []
    try:
        sc = get_smoothcomp_events()
    except Exception as e:
        logger.error("get_smoothcomp_events failed: %s", e)
    try:
        ibjjf = [dict(ev, org="ibjjf") for ev in get_ibjjf_schedule()]
    except Exception as e:
        logger.error("get_ibjjf_schedule failed: %s", e)

    return jsonify(sc + ibjjf)


@app.route("/api/tournaments")
def api_tournaments():
    from scraper_naga import get_naga_events

    # IBJJF — merge bjjcompsystem (brackets built) with ibjjf.com schedule (full calendar)
    try:
        from scraper import get_tournaments, get_ibjjf_schedule
        bracket_events = get_tournaments()   # events on bjjcompsystem (have rosters)
        schedule       = get_ibjjf_schedule()  # all events on ibjjf.com/events/championships

        # Index bracket events by start date; also build name→location map from schedule
        brackets_by_start = {}
        for bt in bracket_events:
            if bt.get("start"):
                brackets_by_start.setdefault(bt["start"], []).append(bt)

        # Secondary lookup: location by slug keywords (for bracket events not in upcoming API)
        sched_loc_by_slug = {}
        for ev in schedule:
            slug_key = ev.get("id", "")  # championship id == slug fallback
            name_key = ev.get("name", "").lower()
            if ev.get("location"):
                sched_loc_by_slug[slug_key] = ev["location"]
                sched_loc_by_slug[name_key] = ev["location"]

        ibjjf = []
        matched_bracket_ids = set()
        for ev in schedule:
            match = None
            if ev.get("start"):
                candidates = brackets_by_start.get(ev["start"], [])
                for bt in candidates:
                    if bt["id"] not in matched_bracket_ids:
                        match = bt
                        matched_bracket_ids.add(bt["id"])
                        break
            if match:
                # Use bjjcompsystem id (for roster lookup) + location from schedule
                ibjjf.append(dict(match, source="ibjjf", has_brackets=True,
                                  location=ev.get("location", "")))
            else:
                ibjjf.append(ev)  # future/no-bracket event from schedule

        # Add any bracket events that didn't match a schedule entry
        # (typically events already underway / just past — not in "upcoming" API)
        for bt in bracket_events:
            if bt["id"] not in matched_bracket_ids:
                loc = (bt.get("location") or
                       sched_loc_by_slug.get(bt.get("name", "").lower(), ""))
                ibjjf.append(dict(bt, source="ibjjf", has_brackets=True, location=loc))
    except Exception as e:
        logger.error("ibjjf schedule merge failed: %s", e)
        try:
            from scraper import get_tournaments
            ibjjf = [dict(t, source="ibjjf", has_brackets=True) for t in get_tournaments()]
        except Exception as e2:
            logger.error("get_tournaments fallback failed: %s", e2)
            ibjjf = []

    # NAGA
    try:
        naga = get_naga_events()
    except Exception as e:
        logger.error("get_naga_events failed: %s", e)
        naga = []

    # CompNet — sourced from Smoothcomp (same platform, no separate scraper needed)
    try:
        from scraper_smoothcomp import get_smoothcomp_events
        compnet = [dict(e, source="compnet") for e in get_smoothcomp_events()
                   if e.get("org") == "compnet"]
    except Exception as e:
        logger.error("compnet from smoothcomp failed: %s", e)
        compnet = []

    return jsonify(ibjjf + naga + compnet)


# ── Roster cache endpoints ────────────────────────────────────────────────────

@app.route("/api/roster/<tournament_id>", methods=["PUT"])
def api_roster_upload(tournament_id):
    """Upload a pre-built roster from a local machine. Requires X-Upload-Key header."""
    expected = os.environ.get("UPLOAD_KEY", "")
    if not expected or request.headers.get("X-Upload-Key") != expected:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    if not data or "athletes" not in data:
        return jsonify({"error": "invalid payload"}), 400
    from scraper import save_roster_cache
    save_roster_cache(tournament_id, data)
    return jsonify({"ok": True, "athletes": len(data["athletes"])})


@app.route("/api/roster/<tournament_id>")
def api_roster(tournament_id):
    """
    Serve the full tournament roster JSON.
    Client downloads once, stores in localStorage, filters entirely in JS.
    """
    from scraper import load_roster_cache
    cache = load_roster_cache(tournament_id)
    if not cache:
        return jsonify({"error": "No roster cache — trigger a build first"}), 404
    # Allow browsers/CDN to cache for up to 1 hour
    resp = jsonify(cache)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@app.route("/api/teams/<tournament_id>")
def api_teams(tournament_id):
    """
    Return unique team names + athlete counts for typeahead.
    Small payload (~2KB) — no full roster sent to client.
    """
    from scraper import load_roster_cache
    cache = load_roster_cache(tournament_id)
    if not cache:
        return jsonify({"error": "building"}), 503
    team_map = {}
    for a in cache.get("athletes", []):
        t = (a.get("team") or "").strip()
        if t:
            team_map[t] = team_map.get(t, 0) + 1
    teams = sorted(
        [{"name": t, "count": c} for t, c in team_map.items()],
        key=lambda x: -x["count"]
    )
    athletes = sorted(
        [{"name": a["name"], "team": a.get("team", "")} for a in cache.get("athletes", [])],
        key=lambda x: x["name"]
    )
    return jsonify({"teams": teams, "athletes": athletes, "athlete_count": len(athletes)})


@app.route("/api/cache/<tournament_id>", methods=["GET"])
def api_cache_status(tournament_id):
    from scraper import load_roster_cache
    cache = load_roster_cache(tournament_id)
    if not cache:
        return jsonify({"status": "none"})
    return jsonify({
        "status":        "ready",
        "built_at":      cache.get("built_at"),
        "total_cats":    cache.get("total_cats", 0),
        "athlete_count": len(cache.get("athletes", [])),
    })


@app.route("/api/cache/<tournament_id>", methods=["POST"])
@limiter.limit("10 per hour")
def api_cache_build(tournament_id):
    expected = os.environ.get("UPLOAD_KEY", "")
    if not expected or request.headers.get("X-Upload-Key") != expected:
        return jsonify({"error": "unauthorized"}), 401
    from scraper import build_roster
    job_id = f"roster_{tournament_id}"
    if job_id in _build_jobs and _build_jobs[job_id].get("status") == "running":
        return jsonify({"job_id": job_id, "already_running": True})
    _build_jobs[job_id] = {"status": "running", "progress": 0, "total": 0, "current_cat": ""}
    threading.Thread(
        target=build_roster,
        args=(tournament_id, _build_jobs[job_id]),
        daemon=True,
    ).start()
    return jsonify({"job_id": job_id})


@app.route("/api/cache/<tournament_id>/status")
def api_cache_job_status(tournament_id):
    job = _build_jobs.get(f"roster_{tournament_id}")
    if not job:
        return jsonify({"status": "none"})
    return jsonify(job)


@app.route("/api/cache/all", methods=["POST"])
@limiter.limit("5 per day")
def api_cache_build_all():
    """
    Rebuild roster cache for every active tournament.
    Called by the nightly cron — no client ever triggers a slow scrape.
    Runs builds sequentially to avoid hammering bjjcompsystem with
    multiple concurrent Playwright browsers.
    """
    from scraper import get_tournaments, build_roster

    def run_all():
        try:
            tournaments = get_tournaments()
        except Exception as e:
            _build_jobs["__all__"]["error"] = str(e)
            _build_jobs["__all__"]["status"] = "error"
            return

        _build_jobs["__all__"]["tournaments"] = [t["id"] for t in tournaments]
        results = []
        for t in tournaments:
            tid    = t["id"]
            job_id = f"roster_{tid}"
            if _build_jobs.get(job_id, {}).get("status") == "running":
                results.append({"id": tid, "skipped": "already running"})
                continue
            job = {"status": "running", "progress": 0, "total": 0, "current_cat": ""}
            _build_jobs[job_id] = job
            build_roster(tid, job)   # sequential — blocks until done
            results.append({"id": tid, "status": job.get("status"), "athletes": job.get("athlete_count", 0)})

        _build_jobs["__all__"]["status"]  = "done"
        _build_jobs["__all__"]["results"] = results

    expected = os.environ.get("UPLOAD_KEY", "")
    if not expected or request.headers.get("X-Upload-Key") != expected:
        return jsonify({"error": "unauthorized"}), 401

    if _build_jobs.get("__all__", {}).get("status") == "running":
        return jsonify({"already_running": True})

    _build_jobs["__all__"] = {"status": "running", "tournaments": [], "results": []}
    threading.Thread(target=run_all, daemon=True).start()
    return jsonify({"started": True})


@app.route("/api/cache/all/status")
def api_cache_all_status():
    job = _build_jobs.get("__all__")
    if not job:
        return jsonify({"status": "none"})
    return jsonify(job)


# ── NAGA club list ────────────────────────────────────────────────────────────

@app.route("/api/naga-clubs/<event_id>")
def api_naga_clubs(event_id):
    if not re.match(r'^\d+$', str(event_id)):
        return jsonify([])
    from scraper_naga import get_naga_clubs
    tournament_name = request.args.get("name", "")
    subdomain = _naga_subdomain(tournament_name)
    clubs = get_naga_clubs(event_id, subdomain)
    return jsonify(clubs)


# ── Search (live scrape fallback) ─────────────────────────────────────────────

@app.route("/api/search", methods=["POST"])
def api_search():
    data          = request.json or {}
    tournament_id = data.get("tournament_id", "").strip()
    school_name   = data.get("school_name", "").strip()
    tournament_name = data.get("tournament_name", "").strip()

    if not tournament_id or not school_name:
        return jsonify({"error": "tournament_id and school_name are required"}), 400

    job_id = f"{tournament_id}_{school_name.replace(' ', '_')}_{int(time.time())}"

    # ── NAGA / CompNet / Smoothcomp path ─────────────────────────────────────
    if _is_naga_tournament(tournament_id):
        from scraper_naga import build_naga_roster
        subdomain = _naga_subdomain(tournament_name)
        source    = _subdomain_to_source(subdomain)
        athletes  = build_naga_roster(tournament_id, school_name, subdomain)
        for a in athletes:
            a["source"] = source
        if athletes:
            label = "CompNet" if source == "compnet" else "NAGA"
            _jobs[job_id] = {
                "status":      "done",
                "progress":    len(athletes),
                "total":       len(athletes),
                "current_cat": f"{label} · {len(athletes)} division entries",
                "athletes":    athletes,
                "from_cache":  False,
                "source":      source,
            }
            return jsonify({"job_id": job_id})
        return jsonify({"error": "school_not_found", "message": f"No athletes found for '{school_name}'"}), 404

    # ── IBJJF path ────────────────────────────────────────────────────────────
    from scraper import load_roster_cache, filter_roster
    cache = load_roster_cache(tournament_id)
    if cache:
        athletes = filter_roster(cache, school_name)
        _jobs[job_id] = {
            "status":      "done",
            "progress":    cache.get("total_cats", 0),
            "total":       cache.get("total_cats", 0),
            "current_cat": f"Cached · built {cache.get('built_at','')[:10]}",
            "athletes":    athletes,
            "from_cache":  True,
        }
        return jsonify({"job_id": job_id})

    # Roster still building at startup — tell client to retry shortly
    return jsonify({"error": "roster_building", "retry_ms": 3000}), 503


@app.route("/api/search/<job_id>")
def api_search_status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


# ── Bracket (single category) ─────────────────────────────────────────────────

@app.route("/api/bracket/<tournament_id>/<category_id>")
def api_bracket(tournament_id, category_id):
    cached = _brackets.get(category_id)
    if cached:
        return jsonify(cached)
    state = {"category_id": category_id, "status": "fetching",
             "bracket_url": f"{BASE_URL}/tournaments/{tournament_id}/categories/{category_id}"}
    threading.Thread(target=refresh_bracket, args=(tournament_id, category_id), daemon=True).start()
    return jsonify(state)


# ── Pick-screen status preview (no payment gate — read-only from cached state) ─

@app.route("/api/pick-statuses/<tournament_id>")
def api_pick_statuses(tournament_id):
    """Return placement/eliminated/fight_time keyed by athlete name (lowercase).
    Used for pick-screen preview only — no payment limit since it's just browsing."""
    from scraper import load_roster_cache
    from watcher import load_state

    cache = load_roster_cache(tournament_id)
    if not cache:
        return jsonify({"statuses": {}})

    tournament_name = request.args.get("name", "")
    tz_name = _tournament_tz(tournament_name)
    statuses = {}

    for a in cache.get("athletes", []):
        cid = a.get("category_id", "")
        if not cid:
            continue
        state = _brackets.get(cid) or load_state(cid)
        if not state:
            continue
        name_lower    = a["name"].lower()
        results_final = state.get("results_final", False)
        placement     = _get_placement(name_lower, state)

        rec = {}
        if placement:
            rec["placement"]  = placement
            rec["eliminated"] = False
        elif results_final:
            rec["eliminated"] = True
        else:
            rec["eliminated"] = _check_eliminated(name_lower, state)

        if not results_final and not rec.get("eliminated"):
            for fight in state.get("fights", []):
                if not _fight_is_upcoming(fight):
                    continue
                for comp in fight.get("competitors", []):
                    if name_lower in comp["name"].lower():
                        rec["fight_time"]     = fight["time"]
                        rec["fight_time_utc"] = _fight_time_to_utc(fight["time"], tz_name)
                        rec["mat_name"]       = fight.get("mat", "")
                        break
                else:
                    continue
                break

        statuses[name_lower] = rec

    return jsonify({"statuses": statuses})


# ── Refresh (lightweight — reads shared state, schedules background fetch) ────

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """
    Returns cached bracket state instantly (no Playwright per request).
    Registers categories for background watching; poller updates them automatically.
    At 10K users this endpoint does zero Playwright — just dict lookups.
    """
    data             = request.json or {}
    tournament_id    = data.get("tournament_id", "")
    tournament_name  = data.get("tournament_name", "")
    athletes         = data.get("athletes", [])

    # Enforce free tier limit server-side
    import os as _os
    if not _os.environ.get("DEV_BYPASS_AUTH"):
        from auth import get_user_from_token, is_plan_active
        _user = get_user_from_token(request)
        _paid = _user and is_plan_active(_user["sub"])
        if not _paid and len(data.get("athletes", [])) > 1:
            return jsonify({"error": "limit_reached", "plan_required": "individual"}), 402

    if not tournament_id or not athletes:
        return jsonify({"error": "tournament_id and athletes required"}), 400

    # ── NAGA / Smoothcomp refresh path ───────────────────────────────────────
    if _is_naga_tournament(tournament_id):
        return _naga_refresh(tournament_id, tournament_name, athletes)

    # ── IBJJF refresh path ────────────────────────────────────────────────────
    tz_name = _tournament_tz(tournament_name)

    cat_ids = list({a["category_id"] for a in athletes if a.get("category_id")})
    if not cat_ids:
        return jsonify({"updated": athletes, "changes": []})

    is_live = _tournament_is_live(tournament_id)

    # Register for background watching (day-of only — register_watch also guards this)
    if is_live:
        for cid in cat_ids:
            register_watch(tournament_id, cid)

        # Kick off immediate fetch for categories not yet in memory
        for cid in cat_ids:
            if cid not in _brackets:
                threading.Thread(target=refresh_bracket, args=(tournament_id, cid), daemon=True).start()

    # Return current cached state (instant)
    all_changes = []
    updated     = []
    from scraper import load_roster_cache  # noqa — only for typing hints
    from watcher import load_state

    for athlete in athletes:
        cid   = athlete.get("category_id", "")
        state = _brackets.get(cid) or load_state(cid)
        a     = dict(athlete)
        a["bracket_url"] = f"{BASE_URL}/tournaments/{tournament_id}/categories/{cid}" if cid else ""

        if state:
            name_lower    = athlete["name"].lower()
            results_final = state.get("results_final", False)
            placement     = _get_placement(name_lower, state)

            # Always clear stale fight info first; re-populate only if upcoming fight found
            a.pop("fight_time",     None)
            a.pop("fight_time_utc", None)
            a.pop("mat",            None)
            a.pop("fight_num",      None)

            # Only show fight info for upcoming fights (today/future, not completed)
            if not results_final:
                for fight in state.get("fights", []):
                    if not _fight_is_upcoming(fight):
                        continue
                    for comp in fight.get("competitors", []):
                        if name_lower in comp["name"].lower():
                            a["mat"]            = fight["mat"]
                            a["fight_time"]     = fight["time"]
                            a["fight_time_utc"] = _fight_time_to_utc(fight["time"], tz_name)
                            a["fight_num"]      = fight["fight_num"]
                            break
                    else:
                        continue
                    break

            a["placement"]  = placement
            if placement:
                a["eliminated"] = False
            elif results_final:
                a["eliminated"] = True
            else:
                a["eliminated"] = _check_eliminated(name_lower, state)
            if state.get("changes"):
                all_changes.extend(state["changes"])

        updated.append(a)

    # Clear consumed changes so they don't repeat on next poll
    for cid in cat_ids:
        if cid in _brackets and _brackets[cid].get("changes"):
            _brackets[cid]["changes"] = []

    return jsonify({"updated": updated, "changes": all_changes})


def _naga_refresh(tournament_id, tournament_name, athletes):
    """Handle /api/refresh for NAGA/Smoothcomp events (naga + compnet)."""
    from scraper_naga import fetch_naga_bracket
    subdomain = _naga_subdomain(tournament_name)
    source    = _subdomain_to_source(subdomain)
    updated   = []
    all_changes = []

    # Fetch/update brackets for each unique category_id
    cat_ids = list({a["category_id"] for a in athletes if a.get("category_id")})

    # Register NAGA brackets for background polling
    for cid in cat_ids:
        _naga_register_watch(tournament_id, cid, subdomain)

    # Fetch any not yet in cache (or re-fetch to get latest)
    for cid in cat_ids:
        if cid not in _brackets:
            state = fetch_naga_bracket(tournament_id, int(cid), subdomain)
            if "error" not in state:
                state["source"] = source
                _brackets[cid] = state
                if state.get("results_final"):
                    _persist_final_bracket(cid, tournament_id, state,
                                           source=source,
                                           tournament_name=tournament_name)

    for athlete in athletes:
        cid   = athlete.get("category_id", "")
        state = _brackets.get(cid)
        a     = dict(athlete)

        # Clear stale fight info
        a.pop("fight_time",     None)
        a.pop("fight_time_utc", None)
        a.pop("mat",            None)
        a.pop("fight_num",      None)

        if state and "error" not in state:
            name_lower    = athlete["name"].lower()
            results_final = state.get("results_final", False)

            # Find placement from ranking
            placement = None
            for r in state.get("ranking", []):
                if r["name"].lower() == name_lower:
                    placement = r["pos"]
                    break

            # Find upcoming fight
            if not results_final:
                for fight in state.get("fights", []):
                    if fight.get("completed"):
                        continue
                    for comp in fight.get("competitors", []):
                        if name_lower in comp.get("name", "").lower():
                            a["mat"]            = fight.get("mat", "")
                            a["fight_time"]     = fight.get("time", "")
                            a["fight_time_utc"] = fight.get("time_utc", "")
                            a["fight_num"]      = fight.get("fight_num", "")
                            a["mat_name"]       = fight.get("mat", "")
                            break
                    else:
                        continue
                    break

            a["placement"] = placement
            if placement:
                a["eliminated"] = False
            elif results_final:
                a["eliminated"] = True
            else:
                # Eliminated if they lost a completed fight with no upcoming fights
                has_upcoming = any(
                    not f.get("completed") and any(
                        name_lower in c.get("name","").lower()
                        for c in f.get("competitors",[])
                    )
                    for f in state.get("fights", [])
                )
                lost = any(
                    f.get("completed") and any(
                        c.get("name","").lower() == name_lower and not c.get("winner")
                        for c in f.get("competitors", [])
                    )
                    for f in state.get("fights", [])
                )
                a["eliminated"] = lost and not has_upcoming

            a["bracket_url"] = (
                f"https://{subdomain}.smoothcomp.com/en/event/{tournament_id}/bracket/{cid}"
            )

        updated.append(a)

    return jsonify({"updated": updated, "changes": all_changes})


# ── Server-Sent Events — push bracket changes to all connected clients ─────────

@app.route("/api/events/<tournament_id>")
def api_events(tournament_id):
    """
    SSE stream. Client connects once; server pushes when brackets change.
    All 10K users watching the same tournament share ONE Playwright run per interval.
    """
    q = queue.Queue(maxsize=50)
    with _sse_lock:
        _sse_clients.setdefault(tournament_id, []).append(q)

    def generate():
        try:
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            while True:
                try:
                    event = q.get(timeout=25)
                    yield f"data: {json.dumps(event)}\n\n"
                except queue.Empty:
                    yield "data: {\"type\":\"heartbeat\"}\n\n"  # keep-alive
        finally:
            with _sse_lock:
                clients = _sse_clients.get(tournament_id, [])
                if q in clients:
                    clients.remove(q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

_DATE_IN_TIME_RE = re.compile(r'(\d{2})/(\d{2})')
_TIME_OF_DAY_RE  = re.compile(r'at\s+(\d{1,2}):(\d{2})\s*([AP]M)', re.IGNORECASE)

def _fight_is_upcoming(fight):
    """
    A fight is upcoming if ALL of:
      - not marked completed (score present or name-repeat)
      - scheduled today or later
      - scheduled time has not yet passed (with 30-min grace for fights in progress)
    """
    if fight.get("completed"):
        return False

    time_str = fight.get("time") or ""

    # Date check
    dm = _DATE_IN_TIME_RE.search(time_str)
    if not dm:
        return False  # no date/time info — don't block elimination detection
    fight_date = date(date.today().year, int(dm.group(1)), int(dm.group(2)))
    # Use UTC-12 as the reference date so tournaments in any timezone are treated
    # as "today" even when the Render server has already rolled past UTC midnight.
    today_safe = (datetime.utcnow() - timedelta(hours=12)).date()
    if fight_date > today_safe:
        return True
    if fight_date < today_safe:
        return False

    # Same day — check time (use UTC-5 as conservative tournament timezone)
    tm = _TIME_OF_DAY_RE.search(time_str)
    if not tm:
        return True
    hour, minute, ampm = int(tm.group(1)), int(tm.group(2)), tm.group(3).upper()
    if ampm == 'PM' and hour != 12:
        hour += 12
    elif ampm == 'AM' and hour == 12:
        hour = 0
    now = datetime.utcnow()
    # Convert fight time to UTC using UTC-5 (Central) as default — 30 min grace
    fight_utc_hour = hour + 5
    fight_minutes  = fight_utc_hour * 60 + minute
    now_minutes    = now.hour * 60 + now.minute
    return now_minutes < (fight_minutes + 30)


def _check_eliminated(name_lower, state):
    """
    Grey name detection (via watcher) already set winner/completed on each fight.
    An athlete is eliminated if:
      - They lost a fight (explicit winner that isn't them), OR
      - All their fights are in the past + at least one completed + no upcoming
    Athletes who placed (bronze/silver/gold) are handled by _get_placement.
    """
    fights_with_athlete = []
    for fight in state.get("fights", []):
        for comp in fight.get("competitors", []):
            if name_lower in comp["name"].lower():
                fights_with_athlete.append(fight)
                break

    if not fights_with_athlete:
        return False

    # Lost any fight → eliminated (check before upcoming, grey-name winner is definitive)
    for fight in fights_with_athlete:
        winner = fight.get("winner", "")
        if winner and name_lower not in winner:
            return True

    # Upcoming fight → still in it
    if any(_fight_is_upcoming(f) for f in fights_with_athlete):
        return False

    # No explicit winner — use completion as fallback (all fights past, at least one done)
    return any(f.get("completed") for f in fights_with_athlete)


_MEDAL_POS = {"1", "2", "3"}

def _get_placement(name_lower, state):
    """
    Returns medal position string ("1","2","3") if athlete appears
    in the ranking table at a podium position, else None.
    """
    for entry in state.get("ranking", []):
        if name_lower in entry.get("name", "").lower():
            pos = entry.get("pos", "")
            return pos if pos in _MEDAL_POS else None
    return None


def _in_ranking(name_lower, state):
    """Returns True if athlete appears anywhere in the ranking table."""
    for entry in state.get("ranking", []):
        if name_lower in entry.get("name", "").lower():
            return True
    return False


# ── Auth & billing endpoints ──────────────────────────────────────────────────

@app.route("/api/auth/me")
def api_auth_me():
    """Return current user's plan. Called on page load to restore session state."""
    from auth import get_user_from_token, get_user_plan, is_plan_active
    auth_header = request.headers.get("Authorization", "")
    has_token = bool(auth_header.startswith("Bearer ") and len(auth_header) > 10)
    user = get_user_from_token(request)
    if not user:
        logger.info("/api/auth/me → unauthenticated (has_token=%s)", has_token)
        return jsonify({"plan": "free", "authenticated": False})
    plan_err = None
    try:
        from auth import _get_service_client
        sc = _get_service_client()
        if sc is None:
            plan_err = "service_client_none"
        else:
            resp = sc.table("users").select("plan,sub_status").eq("id", user["sub"]).single().execute()
            plan_err = f"data={resp.data}"
    except Exception as e:
        plan_err = str(e)
    plan = get_user_plan(user["sub"])
    logger.info("/api/auth/me → user=%s plan=%s err=%s", user.get("email", user["sub"][:8]), plan, plan_err)
    return jsonify({
        "authenticated": True,
        "user_id": user["sub"],
        "email": user.get("email", ""),
        "plan": plan,
        "active": is_plan_active(user["sub"]),
        "_debug_plan": plan_err,
    })


@app.route("/api/auth/debug")
def api_auth_debug():
    """Auth diagnostic — returns token decode info without failing silently."""
    from auth import get_user_from_token
    import time as _time
    auth_header = request.headers.get("Authorization", "")
    has_token = auth_header.startswith("Bearer ") and len(auth_header) > 10
    if not has_token:
        return jsonify({"ok": False, "reason": "no_token"})
    token = auth_header.removeprefix("Bearer ").strip()
    try:
        from jose import jwt as _jwt
        header  = _jwt.get_unverified_header(token)
        payload = _jwt.get_unverified_claims(token)
        exp     = payload.get("exp", 0)
        now     = int(_time.time())
        user    = get_user_from_token(request)
        return jsonify({
            "ok":        user is not None,
            "alg":       header.get("alg"),
            "kid":       header.get("kid"),
            "sub":       payload.get("sub", "")[:8] + "…",
            "email":     payload.get("email", ""),
            "exp":       exp,
            "now":       now,
            "expired":   exp < now,
            "ttl_sec":   exp - now,
            "verified":  user is not None,
            "reason":    "ok" if user else "verify_failed",
        })
    except Exception as e:
        return jsonify({"ok": False, "reason": str(e)})


@app.route("/debug/shot", methods=["GET", "POST"])
def debug_shot():
    """Dev-only screenshot paste page. Resizes to max 800px and saves to /tmp/shot.png."""
    if request.method == "POST":
        import base64, struct, zlib
        data = request.json.get("img", "")
        if data.startswith("data:image"):
            data = data.split(",", 1)[1]
        raw = base64.b64decode(data)
        # Resize via PIL if available, else save raw
        try:
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(raw))
            img.thumbnail((800, 1600), Image.LANCZOS)
            out = io.BytesIO()
            img.save(out, format="PNG", optimize=True)
            raw = out.getvalue()
        except ImportError:
            pass
        with open("/tmp/shot.png", "wb") as f:
            f.write(raw)
        kb = len(raw) // 1024
        logger.info("debug_shot saved %d KB → /tmp/shot.png", kb)
        return jsonify({"ok": True, "kb": kb})
    return """<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paste Screenshot</title>
<style>body{background:#1a1a2e;color:#eee;font-family:sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;margin:0;gap:20px}
#drop{width:90vw;max-width:500px;height:220px;border:3px dashed #555;border-radius:16px;display:flex;align-items:center;justify-content:center;font-size:1.1rem;color:#aaa;cursor:pointer;transition:border-color .2s}
#drop.over{border-color:#e74c3c}#status{font-size:0.9rem;color:#aaa;min-height:24px}
#preview{max-width:90vw;max-height:300px;border-radius:8px;display:none}</style></head>
<body>
<h2 style="margin:0">📸 Paste Screenshot</h2>
<div id="drop">Paste (Ctrl+V) or drag &amp; drop a screenshot here</div>
<div id="status">Waiting…</div>
<img id="preview">
<script>
const status = document.getElementById('status');
const preview = document.getElementById('preview');
const drop = document.getElementById('drop');

function send(file) {
  const r = new FileReader();
  r.onload = async e => {
    preview.src = e.target.result; preview.style.display='block';
    status.textContent = 'Uploading…';
    const res = await fetch('/debug/shot', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({img:e.target.result})});
    const d = await res.json();
    status.textContent = d.ok ? `✓ Saved (${d.kb} KB) — Claude can now read it` : 'Error saving';
  };
  r.readAsDataURL(file);
}

document.addEventListener('paste', e => {
  const item = [...e.clipboardData.items].find(i => i.type.startsWith('image'));
  if (item) send(item.getAsFile());
});
drop.addEventListener('dragover', e => { e.preventDefault(); drop.classList.add('over'); });
drop.addEventListener('dragleave', () => drop.classList.remove('over'));
drop.addEventListener('drop', e => { e.preventDefault(); drop.classList.remove('over'); const f = e.dataTransfer.files[0]; if(f) send(f); });
drop.addEventListener('click', () => { const inp = document.createElement('input'); inp.type='file'; inp.accept='image/*'; inp.onchange=e=>send(e.target.files[0]); inp.click(); });
</script></body></html>"""

@app.route("/debug/logos")
def debug_logos():
    """Dev-only MatTrack logo gallery."""
    import glob as _glob
    logos_dir = "/mnt/c/Users/mtxpert/claude/mattrack-social/logos"
    files = sorted(_glob.glob(f"{logos_dir}/*.png"))
    imgs = "".join(
        f'<div style="margin:30px 0;text-align:center">'
        f'<p style="font-family:monospace;color:#aaa;margin-bottom:8px">{os.path.basename(f)}</p>'
        f'<img src="/debug/logos/img/{os.path.basename(f)}" style="max-width:520px;border-radius:8px;border:1px solid #333"/>'
        f'</div>'
        for f in files
    )
    return Response(
        f'<!DOCTYPE html><html><body style="background:#111;color:#eee;font-family:sans-serif;padding:20px">'
        f'<h1 style="text-align:center">MatTrack Logo Options</h1>{imgs}</body></html>',
        mimetype="text/html",
    )

@app.route("/debug/logos/img/<filename>")
def debug_logos_img(filename):
    logos_dir = "/mnt/c/Users/mtxpert/claude/mattrack-social/logos"
    path = os.path.join(logos_dir, filename)
    if not os.path.exists(path):
        return "not found", 404
    return send_file(path, mimetype="image/png")

@app.route("/api/fighter/<path:name>")
def api_fighter(name):
    """
    Return all recorded results for a fighter by name (case-insensitive).
    Foundation for athlete profiles — aggregates IBJJF, NAGA, etc.
    """
    from results import get_fighter_profile
    results = get_fighter_profile(name.strip())
    return jsonify({"name": name, "results": results})


@app.route("/api/stripe/checkout", methods=["POST"])
@limiter.limit("10 per hour")
def api_stripe_checkout():
    """Create a Stripe Checkout session and return the URL."""
    from auth import get_user_from_token
    from payments import create_checkout_session
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    data      = request.json or {}
    plan      = data.get("plan", "individual")
    if plan not in ("individual", "gym", "affiliate"):
        return jsonify({"error": "Invalid plan"}), 400
    base_url  = os.environ.get("APP_URL", request.host_url.rstrip("/"))
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
        logger.error("checkout failed user=%s: %s", user.get("sub"), e, exc_info=True)
        return jsonify({"error": "Failed to create checkout session"}), 500


@app.route("/api/stripe/webhook", methods=["POST"])
def api_stripe_webhook():
    """Stripe webhook receiver. Must use raw body for signature verification."""
    from payments import handle_webhook
    payload    = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")
    ok, reason = handle_webhook(payload, sig_header)
    if not ok:
        return jsonify({"error": reason}), 400
    return jsonify({"received": True})


@app.route("/api/gym/codes")
@limiter.limit("30 per hour")
def api_gym_codes():
    """List access codes for the authenticated gym pack owner."""
    from auth import get_user_from_token
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    try:
        from supabase import create_client
        sb = create_client(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_KEY", ""),
        )
        packs = sb.table("gym_packs").select("id,school_name,max_codes,sub_status,plan").eq("owner_id", user["sub"]).execute()
        result = []
        for pack in (packs.data or []):
            codes = sb.table("access_codes").select("code,redeemed_by,redeemed_at,created_at").eq("pack_id", pack["id"]).execute()
            result.append({**pack, "codes": codes.data or []})
        return jsonify(result)
    except Exception as e:
        logger.error("gym codes fetch failed user=%s: %s", user.get("sub"), e, exc_info=True)
        return jsonify({"error": "Failed to load codes"}), 500


@app.route("/api/gym/redeem", methods=["POST"])
@limiter.limit("10 per hour")
def api_gym_redeem():
    """Redeem an access code to upgrade the current user to individual plan."""
    from auth import get_user_from_token
    from payments import redeem_access_code
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


@app.route("/api/admin/generate-codes", methods=["POST"])
def api_admin_generate_codes():
    """
    Admin endpoint: create a test gym pack and generate access codes.
    Protected by X-Upload-Key header (same key used for cache uploads).
    Body: { "owner_id": "<user-uuid>", "school_name": "...", "count": 10 }
    """
    expected = os.environ.get("UPLOAD_KEY", "")
    if not expected or request.headers.get("X-Upload-Key") != expected:
        return jsonify({"error": "unauthorized"}), 401
    data = request.json or {}
    owner_id    = data.get("owner_id", "").strip()
    school_name = data.get("school_name", "Test School")
    count       = min(int(data.get("count", 10)), 50)
    if not owner_id:
        return jsonify({"error": "owner_id required"}), 400
    try:
        from supabase import create_client
        from payments import generate_access_codes
        sb = create_client(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_KEY", ""),
        )
        # Create or reuse a gym pack for this owner
        existing = sb.table("gym_packs").select("id").eq("owner_id", owner_id).execute()
        if existing.data:
            pack_id = existing.data[0]["id"]
        else:
            insert = sb.table("gym_packs").insert({
                "owner_id": owner_id,
                "school_name": school_name,
                "plan": "gym",
                "max_codes": count,
                "sub_status": "active",
            }).execute()
            pack_id = insert.data[0]["id"]
        codes = generate_access_codes(pack_id, count)
        # Ensure the owner has gym plan
        sb.table("users").update({"plan": "gym", "sub_status": "active"}).eq("id", owner_id).execute()
        return jsonify({"pack_id": pack_id, "codes": codes})
    except Exception as e:
        logger.error("admin generate-codes failed: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/billing/portal", methods=["POST"])
@limiter.limit("10 per hour")
def api_billing_portal():
    """Return a Stripe customer portal URL for managing subscriptions."""
    from auth import get_user_from_token
    user = get_user_from_token(request)
    if not user:
        return jsonify({"error": "Authentication required"}), 401
    try:
        import stripe
        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
        from supabase import create_client
        sb = create_client(os.environ.get("SUPABASE_URL",""), os.environ.get("SUPABASE_SERVICE_KEY",""))
        row = sb.table("users").select("stripe_customer_id").eq("id", user["sub"]).single().execute()
        customer_id = (row.data or {}).get("stripe_customer_id")
        if not customer_id:
            return jsonify({"error": "No billing account found"}), 404
        base_url = os.environ.get("APP_URL", request.host_url.rstrip("/"))
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=base_url,
        )
        return jsonify({"url": session.url})
    except Exception as e:
        logger.error("billing portal failed user=%s: %s", user.get("sub"), e, exc_info=True)
        return jsonify({"error": "Failed to open billing portal"}), 500


@app.route("/manifest.json")
def serve_manifest():
    return send_file("static/manifest.json", mimetype="application/manifest+json")


@app.route("/sw.js")
def serve_sw():
    """Service worker must be served from root scope."""
    resp = send_file("static/sw.js", mimetype="application/javascript")
    resp.headers["Service-Worker-Allowed"] = "/"
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/push/vapid-key")
def api_push_vapid_key():
    """Return the VAPID public key for subscription setup."""
    return jsonify({"publicKey": os.environ.get("VAPID_PUBLIC_KEY", "")})


@app.route("/api/push/subscribe", methods=["POST"])
@limiter.limit("20 per hour")
def api_push_subscribe():
    """Save a push subscription with the category_ids the user is watching."""
    data = request.json or {}
    endpoint     = data.get("endpoint", "").strip()
    p256dh       = data.get("p256dh", "").strip()
    auth         = data.get("auth", "").strip()
    category_ids = data.get("category_ids", [])
    if not endpoint or not p256dh or not auth:
        return jsonify({"error": "Missing subscription fields"}), 400
    try:
        from supabase import create_client
        sb = create_client(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_KEY", ""),
        )
        sb.table("push_subscriptions").upsert({
            "endpoint":     endpoint,
            "p256dh":       p256dh,
            "auth":         auth,
            "category_ids": category_ids,
            "updated_at":   datetime.utcnow().isoformat(),
        }, on_conflict="endpoint").execute()
        return jsonify({"success": True})
    except Exception as e:
        logger.error("push subscribe failed: %s", e, exc_info=True)
        return jsonify({"error": "Failed to save subscription"}), 500


@app.route("/api/push/unsubscribe", methods=["POST"])
@limiter.limit("20 per hour")
def api_push_unsubscribe():
    """Remove a push subscription by endpoint."""
    endpoint = (request.json or {}).get("endpoint", "").strip()
    if not endpoint:
        return jsonify({"error": "Missing endpoint"}), 400
    try:
        from supabase import create_client
        sb = create_client(
            os.environ.get("SUPABASE_URL", ""),
            os.environ.get("SUPABASE_SERVICE_KEY", ""),
        )
        sb.table("push_subscriptions").delete().eq("endpoint", endpoint).execute()
        return jsonify({"success": True})
    except Exception as e:
        logger.error("push unsubscribe failed: %s", e, exc_info=True)
        return jsonify({"error": "Failed to remove subscription"}), 500


from auth import _prewarm_jwks
_prewarm_jwks()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5950)))
