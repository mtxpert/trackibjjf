"""
IBJJF Tournament Tracker
Scalable architecture:
  - Roster cache: one Playwright scrape per tournament serves all users
  - Client-side filtering: search is a JS .filter(), zero server load
  - Shared bracket state: background poller updates once per interval for all users
  - Server-Sent Events: pushes bracket changes to all connected clients simultaneously
"""

from flask import Flask, render_template, jsonify, request, Response, send_file
import os, threading, time, json, queue, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "ibjjf-tracker-key")

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
    return 'America/New_York'   # IBJJF HQ default

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

# ── Shared bracket watcher ────────────────────────────────────────────────────
# category_id -> {tournament_id, last_fetched, interval_sec}
_watch_registry = {}
_watch_lock     = threading.Lock()

# SSE subscriber queues: tournament_id -> list[queue.Queue]
_sse_clients = {}
_sse_lock    = threading.Lock()


def _sse_push(tournament_id, event_data):
    with _sse_lock:
        qs = list(_sse_clients.get(tournament_id, []))
    for q in qs:
        try:
            q.put_nowait(event_data)
        except queue.Full:
            pass   # slow client — drop event rather than block


def register_watch(tournament_id, category_id, interval_sec=30):
    """Register a category for background polling. Skips if already marked complete."""
    with _watch_lock:
        # Don't re-add brackets we already know are finished
        cached = _brackets.get(category_id)
        if cached and cached.get("results_final"):
            return
        existing = _watch_registry.get(category_id, {})
        _watch_registry[category_id] = {
            "tournament_id": tournament_id,
            "last_fetched":  existing.get("last_fetched", 0),
            "interval_sec":  interval_sec,
        }


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
                with _watch_lock:
                    _watch_registry.pop(cid, None)
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
                # Clear immediately so /api/refresh doesn't show them again
                state["changes"] = []
        except Exception:
            pass


def _background_poller():
    """
    Single long-running daemon thread.
    Fetches all stale brackets concurrently via thread pool (no Playwright),
    pushes changes via SSE, removes completed brackets from the registry.
    ~2s to refresh 166 brackets; polls every 30s.
    """
    from watcher import fetch_brackets_batch

    while True:
        now = time.time()
        to_refresh = []
        tid_by_cid = {}

        with _watch_lock:
            for cid, info in list(_watch_registry.items()):
                if (now - info["last_fetched"]) >= info["interval_sec"]:
                    tid = info["tournament_id"]
                    to_refresh.append((tid, cid, ""))
                    tid_by_cid[cid] = tid

        if to_refresh:
            try:
                batch_results = fetch_brackets_batch(to_refresh, concurrency=20)
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

            for t in tournaments:
                try:
                    if not load_roster_cache(t["id"]):
                        _build_one_tournament(t)
                        _time.sleep(5)   # pause between builds
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
        err = {"error": str(e), "category_id": category_id,
               "bracket_url": f"{BASE_URL}/tournaments/{tournament_id}/categories/{category_id}"}
        _brackets[category_id] = err
        return err


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/tournaments")
def api_tournaments():
    from pathlib import Path as _Path
    import json as _json
    # Try seed cache first (instant, no network)
    for seed in [
        _Path(__file__).parent / "seed_cache" / "tournaments.json",
        _Path("/tmp") / "tournaments.json",
    ]:
        if seed.exists():
            try:
                return app.response_class(
                    response=seed.read_text(),
                    status=200,
                    mimetype='application/json'
                )
            except Exception:
                pass
    # Fallback: live fetch
    try:
        from scraper import get_tournaments
        return jsonify(get_tournaments())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
def api_cache_build(tournament_id):
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


# ── Search (live scrape fallback) ─────────────────────────────────────────────

@app.route("/api/search", methods=["POST"])
def api_search():
    data          = request.json or {}
    tournament_id = data.get("tournament_id", "").strip()
    school_name   = data.get("school_name", "").strip()

    if not tournament_id or not school_name:
        return jsonify({"error": "tournament_id and school_name are required"}), 400

    job_id = f"{tournament_id}_{school_name.replace(' ', '_')}_{int(time.time())}"

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


# ── Debug: synchronous bracket fetch ─────────────────────────────────────────

@app.route("/api/debug/bracket/<tournament_id>/<category_id>")
def api_debug_bracket(tournament_id, category_id):
    """Synchronous bracket fetch for debugging — returns result or error directly."""
    from watcher import fetch_bracket
    state = fetch_bracket(tournament_id, category_id)
    return jsonify(state)


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

    if not tournament_id or not athletes:
        return jsonify({"error": "tournament_id and athletes required"}), 400

    tz_name = _tournament_tz(tournament_name)

    cat_ids = list({a["category_id"] for a in athletes if a.get("category_id")})
    if not cat_ids:
        return jsonify({"updated": athletes, "changes": []})

    # Register for background watching (idempotent)
    for cid in cat_ids:
        register_watch(tournament_id, cid)

    # Check if any categories have never been fetched — kick off an immediate fetch
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

            # Only show fight info for upcoming fights (today/future, not completed)
            if not results_final:
                for fight in state.get("fights", []):
                    if not _fight_is_upcoming(fight):
                        continue
                    for comp in fight.get("competitors", []):
                        if name_lower in comp["name"].lower():
                            a["mat"]           = fight["mat"]
                            a["fight_time"]    = fight["time"]
                            a["fight_time_utc"] = _fight_time_to_utc(fight["time"], tz_name)
                            a["fight_num"]     = fight["fight_num"]
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
    today = date.today()
    if fight_date > today:
        return True
    if fight_date < today:
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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
