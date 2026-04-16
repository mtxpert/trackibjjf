# MatTrack + TrackBJJ — Coding Reference & Unification Plan

## 1. PLATFORM OVERVIEW

Two Flask apps, one Supabase database, one Stripe account.

| | **mattrack.net** | **trackbjj.net** |
|---|---|---|
| File | `app.py` | `app_trackbjj.py` |
| Port | 5950 | 5951 |
| Purpose | Live bracket tracking | Athlete repository / career stats |
| Primary user | Coaches watching tournaments | Athletes checking results & profiles |
| Template | `templates/index.html` (single SPA file) | `trackbjj/templates/trackbjj/*.html` |
| Auth state | ✅ Full (login, plan, billing) | ⚠️ Partial (just added login modal + `/api/auth/me`) |

Both share:
- Same Supabase project (`kzqvfuqxtbrhlgphyntb.supabase.co`)
- Same `public.users` table → same plan for both sites
- Same `auth.py` / `payments.py` modules
- Same design tokens (colors, fonts, spacing)

---

## 2. DATABASE SCHEMA

### Auth & Billing

```sql
public.users
  id UUID PK          -- mirrors auth.users.id (Supabase auth)
  email TEXT
  plan TEXT           -- 'free' | 'individual' | 'gym' | 'affiliate'
  stripe_customer_id TEXT
  stripe_sub_id TEXT
  sub_status TEXT     -- 'active' | 'canceled' | 'past_due' | 'trialing'
  sub_expires_at TIMESTAMPTZ

public.gym_packs
  id UUID PK
  owner_id UUID FK users
  plan TEXT           -- 'gym' | 'affiliate'
  school_name TEXT
  school_slug TEXT UNIQUE
  max_codes INT
  stripe_sub_id TEXT
  sub_status TEXT

public.access_codes
  id UUID PK
  code TEXT UNIQUE    -- 'TRACK-XXXX'
  pack_id UUID FK gym_packs
  redeemed_by UUID FK users   -- NULL = unclaimed
  redeemed_at TIMESTAMPTZ
```

### Live Brackets (mattrack.net writes, shared read)

```sql
public.bracket_finals
  category_id TEXT PK         -- bjjcompsystem category ID
  tournament_id TEXT
  tournament_name TEXT
  division TEXT
  source TEXT                 -- 'ibjjf' | 'naga' | 'compnet'
  ranking JSONB               -- [{pos, name}]
  state_json JSONB            -- full bracket state
  event_date TEXT             -- YYYY-MM-DD

public.fighter_results
  id UUID PK
  athlete_name TEXT           -- lowercase
  athlete_display TEXT        -- original casing
  team TEXT
  tournament_id TEXT
  tournament_name TEXT
  category_id TEXT
  division TEXT
  source TEXT
  placement TEXT|INT          -- 1/2/3 or 'eliminated'
  event_date TEXT
  UNIQUE (athlete_name, category_id)
```

### Match History (shared core table)

```sql
public.tournament_results
  source TEXT                 -- 'ibjjf' | 'smoothcomp'
  event_id TEXT               -- tournament ID
  event_title TEXT
  event_date TEXT
  division TEXT
  placement INT|NULL          -- 1/2/3 or NULL (eliminated)
  athlete_name TEXT           -- lowercase
  athlete_display TEXT
  team TEXT
  athlete_id INT              -- Smoothcomp user_id
  ibjjf_athlete_id INT
  status TEXT                 -- 'registered' | NULL
  UNIQUE (source, event_id, division, placement, athlete_name)
```

### Athlete Profiles (trackbjj.net)

```sql
public.ibjjf_athletes
  ibjjf_id INT PK
  name TEXT
  name_lower TEXT INDEX
  slug TEXT
  belt TEXT
  academy TEXT
  gender TEXT
  points FLOAT
  ranking_category TEXT       -- 'adult' | 'm1' .. 'm7'
  age_division TEXT
  gi_nogi TEXT
  ranking_year INT

public.sc_ibjjf_verified      -- manual IBJJF claim
  sc_uid INT PK               -- Smoothcomp user_id
  ibjjf_athlete_id INT
  ibjjf_name TEXT
  belt TEXT
  academy TEXT
  photo_url TEXT              -- ⚠️ column needs: ALTER TABLE sc_ibjjf_verified ADD COLUMN IF NOT EXISTS photo_url text;

public.sc_smoothcomp_verified  -- SC ownership verification
  sc_uid INT PK
  sc_email TEXT
  sc_user_id TEXT
  sc_name TEXT

public.athlete_social_links
  sc_uid INT PK
  instagram_handle TEXT
  instagram_user_id TEXT
  instagram_token TEXT
  instagram_token_expires TIMESTAMPTZ
  facebook_url TEXT
  youtube_url TEXT
```

### Notifications (mattrack.net only)

```sql
public.push_subscriptions
  endpoint TEXT PK
  p256dh TEXT
  auth TEXT
  category_ids JSONB         -- watched bracket IDs
  updated_at TIMESTAMPTZ
```

---

## 3. ENVIRONMENT VARIABLES

```bash
# Supabase (both apps)
SUPABASE_URL=https://kzqvfuqxtbrhlgphyntb.supabase.co
SUPABASE_ANON_KEY=eyJ...          # Public key (safe in JS)
SUPABASE_SERVICE_KEY=eyJ...       # Admin key (backend only, never expose)
SUPABASE_JWT_SECRET=...           # HS256 fallback for JWT verify

# Flask
SECRET_KEY=...
PORT=5950                          # mattrack
TRACKBJJ_PORT=5951

# Stripe (both apps share)
STRIPE_SECRET_KEY=sk_live_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_INDIVIDUAL=price_...
STRIPE_PRICE_GYM=price_...
STRIPE_PRICE_AFFILIATE=price_...

# Meta / Instagram (trackbjj.net)
META_APP_ID=2117140528845445
META_APP_SECRET=50515783c9b95bc307a0efa4b915f735

# Web Push VAPID (mattrack.net)
VAPID_PUBLIC_KEY=...
VAPID_PRIVATE_KEY=...
VAPID_EMAIL=mailto:info@mattrack.net

# Admin
UPLOAD_KEY=...                     # Protects /api/cache/* build endpoints
DEV_BYPASS_AUTH=1                  # Skips plan check in local dev
APP_URL=https://mattrack.net
```

**Supabase JS (hardcoded in templates — anon key is public-safe):**
```js
const SUPABASE_URL  = 'https://kzqvfuqxtbrhlgphyntb.supabase.co';
const SUPABASE_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...';
```

---

## 4. SHARED MODULES

### `auth.py`
```python
get_user_from_token(request) → dict|None
  # Reads Authorization: Bearer <token>
  # Verifies ES256 via JWKS (24h cache) OR HS256 via JWT_SECRET
  # Returns {sub, email, ...} or None on failure — NEVER raises

get_user_plan(user_id: str) → str
  # Queries public.users.plan
  # Returns 'free' on any error

is_plan_active(user_id: str) → bool
  # plan != 'free' AND sub_status == 'active'

_prewarm_jwks()
  # Background thread — fetches JWKS at startup to avoid slow first auth
```

Both apps import from `auth.py`. The same function, same Supabase table.

### `payments.py`
```python
create_checkout_session(user_id, email, plan, success_url, cancel_url) → str|None
handle_webhook(payload, sig_header) → dict
generate_access_codes(pack_id, count) → list[str]   # Format: TRACK-XXXX
redeem_access_code(code, user_id) → dict             # {success, plan}
```

Stripe webhook events handled:
- `checkout.session.completed` → sets plan, sub_status='active'
- `customer.subscription.updated` → syncs sub_status
- `customer.subscription.deleted` → plan='free', sub_status='canceled'
- `invoice.payment_failed` → sub_status='past_due' (access retained)

### `results.py`
```python
save_bracket_final(category_id, tournament_id, tournament_name, division,
                   source, ranking, state, event_date) → None
  # Writes to: bracket_finals, fighter_results, tournament_results
  # Looks up ibjjf_athlete_id by name (cached in memory)
  # Normalises source: 'naga'/'compnet' → 'smoothcomp' for tournament_results
  # FIRE-AND-FORGET — never raises

load_bracket_finals() → dict[category_id, state]
  # Called at startup to warm _brackets from DB
```

### `ibjjf_api.py`
```python
login(email, password) → (athlete_id, jwt_token)
get_athlete_profile(token) → {athlete_id, name, belt, academy, photo_url, birth_date, gender}
get_active_registrations(athlete_id, token) → list[dict]
```

### `ibjjf_rankings.py`
```python
get_rankings(sb, slug, belt, gender, ranking_category, age_division, weight) → dict
weight_slug_from_division(division) → str
```

### `meta_api.py`
```python
get_oauth_url(sc_uid, redirect_uri) → str
exchange_code(code, redirect_uri) → dict
get_long_lived_token(short_token) → dict
get_instagram_user_id(token) → {ig_user_id, ig_username}
get_recent_posts(ig_user_id, token, limit=9) → list[dict]
```

---

## 5. MATTRACK.NET — HOW IT WORKS

### Live Bracket Architecture

```
User loads page
  └─ /api/tournaments → merged IBJJF + NAGA + CompNet events

User selects tournament, athletes
  └─ /api/roster/<tid> → download full athlete JSON (cached 1h, filtered in JS)
  └─ /api/refresh (POST) → register categories for watching, return current status

Background poller (every 10s):
  └─ Groups _watch_registry by source (ibjjf / naga)
  └─ fetch_brackets_batch(20 concurrent workers) → ~2s for 166 brackets
  └─ diff_states(old, new) → detect changes
  └─ _sse_clients[tournament_id].put(changes) → push to all watchers
  └─ save completed brackets → results.save_bracket_final()

User's SSE client (/api/events/<tid>):
  └─ Receives changes → updates fight status, placement in UI
```

### Key In-Memory State (app.py)
```python
_brackets: dict          # category_id → bracket state dict
_watch_registry: dict    # category_id → {tournament_id, source, interval_sec, ...}
_sse_clients: dict       # tournament_id → [Queue, Queue, ...]
_build_jobs: dict        # job_id → {status, progress, current_cat, athletes}
```

### Auth + Plan Enforcement (mattrack.net)
```
/api/auth/me → {authenticated, plan, active, email}
/api/refresh → checks: free plan + >1 athlete → 402
/api/stripe/checkout → POST → Stripe URL
/api/stripe/webhook → processes Stripe events
/api/gym/codes → list codes (gym/affiliate owners)
/api/gym/redeem → redeem code → individual plan
/api/billing/portal → Stripe portal URL
```

### Supabase JS Auth (frontend, templates/index.html)
```js
_sb = supabase.createClient(URL, ANON, { auth: { persistSession, detectSessionInUrl } })
// On load:
getSession() → if expired, refreshSession() (clock-skew fix)
fetchUserPlan(token) → fetch('/api/auth/me') → _userPlan
onAuthStateChange() → update UI pill badge
signInWithGoogle() → signInWithOAuth({ provider:'google', redirectTo: origin })
sendMagicLink() → signInWithOtp({ email, emailRedirectTo: origin })
```

### UI Pill (mattrack.net header)
```html
<div id="user-pill" class="user-pill" onclick="_onPillClick()">
  <span id="pill-label">Sign In</span>
  <span class="pill-plan free" id="pill-plan-badge" style="display:none">FREE</span>
</div>
```
CSS: `background: rgba(255,255,255,0.18); padding: 5px 11px; border-radius: 20px`

When signed in: label = email prefix, badge shows plan (FREE/INDIVIDUAL/GYM)
When signed out: label = "Sign In", badge hidden

---

## 6. TRACKBJJ.NET — HOW IT WORKS

### Athlete Profile Flow
```
/athlete/<sc_uid>
  1. Fetch all tournament_results WHERE source='smoothcomp' AND athlete_id=sc_uid
  2. Extract display_name, last_name from rows
  3. Fingerprint: parse division strings → {belt, age, gender}
  4. Check sc_ibjjf_verified → ibjjf_athlete_id
  5. If not verified: fuzzy match ibjjf_athletes by name + belt (score >= 0.70)
  6. Fetch IBJJF results via ibjjf_athlete_id or name match
  7. Fetch upcoming registrations (status='registered')
  8. RPC get_match_history_ibjjf + get_match_history_sc → deduplicate
  9. Fetch IBJJF rankings via ibjjf_rankings.get_rankings()
  10. Fetch Instagram posts via meta_api (if token stored)
  11. Render athlete.html
```

### Match History SQL (Supabase RPCs)
```sql
-- get_match_history_ibjjf(p_ibjjf_athlete_id)
-- get_match_history_sc(p_sc_uid)
-- Adjacent-placement inference:
SELECT ...
FROM tournament_results m
JOIN tournament_results o
  ON m.event_id = o.event_id
  AND m.division = o.division
  AND m.source = o.source
  AND ABS(m.placement::int - o.placement::int) = 1
  AND (o.ibjjf_athlete_id IS NULL OR o.ibjjf_athlete_id != p_ibjjf_athlete_id)
-- ABS(place1 - place2) = 1 → they fought each other
-- 1 vs 2 = final, 2 vs 3 = bronze match, 3 vs 4 = semi-loser
```

### Current Auth State (trackbjj.net)
- ✅ Supabase JS loaded in base.html
- ✅ User pill in header (matching mattrack style — partially implemented)
- ✅ Login modal (Google OAuth + magic link)
- ✅ Account modal
- ✅ `/api/auth/me` endpoint
- ⚠️ Pill CSS not yet matching mattrack exactly (interrupted mid-edit)
- ❌ No `/api/stripe/checkout` route
- ❌ No `/api/billing/portal` route
- ❌ No `/api/gym/redeem` route
- ❌ Account modal missing billing/codes buttons
- ❌ No plan enforcement on any routes yet

---

## 7. DESIGN TOKENS (SHARED)

Both sites use identical CSS variables. This is the source of truth:

```css
:root {
  --bg:     #0f0f1a;   /* Page background */
  --card:   #1e1e30;   /* Card / panel background */
  --gray:   #2d2d44;   /* Secondary background, hover */
  --text:   #f0f0f0;   /* Primary text */
  --muted:  #777799;   /* Secondary text, labels */
  --border: #333355;   /* Borders, dividers */
  --accent: #c0392b;   /* Primary accent (red) — both sites */
  --gold:   #ffd700;
  --silver: #c0c0c0;
  --bronze: #cd7f32;
}
```

**Note**: mattrack uses `--red: #c0392b` as the token name; trackbjj uses `--accent: #c0392b`. These are the same color — keep `--accent` everywhere going forward.

### Header (both sites)
```css
header {
  background: var(--accent);   /* Red bar */
  padding: 14px 16px 12px;
  display: flex; align-items: center; gap: 12px;
  position: sticky; top: 0; z-index: 50;
  box-shadow: 0 2px 10px rgba(0,0,0,.5);
}
```

### User Pill (mattrack — this is the canonical version)
```css
.user-pill {
  margin-left: auto;
  display: flex; align-items: center; gap: 6px;
  font-size: 0.75rem;
  background: rgba(255,255,255,0.18);
  padding: 5px 11px; border-radius: 20px;
  cursor: pointer; white-space: nowrap;
}
.pill-plan { font-size:.65rem; background:#27ae60; color:white; padding:1px 6px; border-radius:8px; font-weight:700; text-transform:uppercase; }
.pill-plan.free { background: var(--muted); }
```

```html
<!-- Canonical pill HTML (both sites must use this) -->
<div id="user-pill" class="user-pill" onclick="authPillClick()">
  <span id="pill-label">Sign In</span>
  <span class="pill-plan free" id="pill-plan-badge" style="display:none">FREE</span>
</div>
```

### Modals (both sites)
```css
.modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:200; align-items:center; justify-content:center; }
.modal-overlay.open { display:flex; }
.modal-sheet { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:24px; width:100%; max-width:360px; margin:16px; position:relative; }
.btn-modal-close { position:absolute; top:16px; right:16px; background:none; border:none; color:var(--muted); font-size:1.4rem; cursor:pointer; }
.btn-google { width:100%; padding:14px; border-radius:12px; background:white; color:#222; border:none; font-size:.95rem; font-weight:600; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:10px; }
```

### Buttons (both sites)
```css
.btn-primary { background: var(--accent); color: white; border: none; border-radius: 10px; padding: 12px; font-weight: 600; cursor: pointer; width: 100%; }
.btn-secondary { background: var(--gray); color: var(--text); border: 1px solid var(--border); border-radius: 10px; padding: 12px; cursor: pointer; width: 100%; }
```

---

## 8. AUTH FLOW (CANONICAL — BOTH SITES)

### Frontend (JS)
```js
// 1. Init client (same credentials on both sites)
const SUPABASE_URL  = 'https://kzqvfuqxtbrhlgphyntb.supabase.co';
const SUPABASE_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...';
_sb = supabase.createClient(URL, ANON, {
  auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true }
});

// 2. Restore session
getSession() → session
// Clock-skew fix: if null + token in localStorage → refreshSession()

// 3. Show user immediately from JWT
_userEmail = session.user.email;
updateAuthUI();  // Don't wait for plan fetch

// 4. Fetch plan async
fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
  → { authenticated, plan, active, email }
  → if not authenticated → refreshSession() → retry

// 5. Listen for changes
onAuthStateChange((event, session) => { ... })

// 6. Google OAuth
signInWithOAuth({ provider: 'google', options: { redirectTo: location.origin + '/' } })
```

### Supabase redirect URLs (already configured)
```
https://www.mattrack.net/**
https://www.trackbjj.net/**
http://localhost:5950/**
http://localhost:5951/**
```

### Backend (Python)
```python
# Both apps expose this route identically:
@app.route("/api/auth/me")
def api_auth_me():
    from auth import get_user_from_token, get_user_plan, is_plan_active
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
```

### Cross-Site Session Persistence
Supabase stores the JWT in `localStorage` under key `sb-kzqvfuqxtbrhlgphyntb-auth-token`.  
localStorage is **origin-scoped** (mattrack.net ≠ trackbjj.net).  
Both sites use the same Supabase client → both call `getSession()` independently → both read their own localStorage → both authenticate the **same user** because the JWT is issued by the same Supabase project.

**Result**: Sign in on mattrack.net → visit trackbjj.net → trackbjj independently reads its own localStorage → if already visited trackbjj while signed in, it has the token. If not, user needs to sign in once on each domain. This is expected behavior for cross-origin localStorage.

**To make it feel seamless**: If user signs in on mattrack, clicking "↗ TrackBJJ" opens a new tab. Since trackbjj.net will not have the token yet, it shows "Sign In". User clicks Sign In → "Continue with Google" → Supabase detects their existing Google session → instant auth, no password prompt. This is the correct cross-domain auth UX.

---

## 9. PLAN TO UNIFY BOTH SITES

### Gap Analysis

| Feature | mattrack.net | trackbjj.net | Status |
|---|---|---|---|
| Supabase JS loaded | ✅ | ✅ | Done |
| User pill in header | ✅ `.user-pill` | ⚠️ Partial | **Fix needed** |
| Login modal (Google + magic link) | ✅ | ✅ | Done |
| Account modal | ✅ Full | ⚠️ Minimal | **Expand needed** |
| `/api/auth/me` | ✅ | ✅ | Done |
| `/api/stripe/checkout` | ✅ | ❌ | **Add to trackbjj** |
| `/api/billing/portal` | ✅ | ❌ | **Add to trackbjj** |
| `/api/gym/redeem` | ✅ | ❌ | **Add to trackbjj** |
| `/api/gym/codes` | ✅ | ❌ | **Add to trackbjj** |
| Stripe webhook | ✅ | ❌ (not needed — same Supabase) | Skip |
| Plan enforcement | ✅ | ❌ | **Define + add** |
| Plan badge in pill | ✅ | ⚠️ Partial | **Fix** |
| Upgrade CTA | ✅ | ⚠️ Points to mattrack | Acceptable |
| Access code redemption UI | ✅ | ❌ | **Add** |
| Sign out | ✅ | ✅ | Done |
| Manage billing button | ✅ | ❌ | **Add to account modal** |
| Manage codes button (gym) | ✅ | ❌ | **Add to account modal** |
| Profile photo (IBJJF) | N/A | ⚠️ Built, needs DB column | **Run SQL** |
| Premium profile section | N/A | ✅ | Done |

### Implementation Steps (in order)

#### Step 1 — Fix trackbjj user pill (BLOCKED mid-edit)
Replace the current `<button class="btn-auth">` in `base.html` with the canonical pill:
```html
<div id="user-pill" class="user-pill" onclick="tbAuthButtonClick()">
  <span id="pill-label">Sign In</span>
  <span class="pill-plan free" id="pill-plan-badge" style="display:none">FREE</span>
</div>
```
Update `tbUpdateUI()` in base.html JS to set `pill-label` and `pill-plan-badge` identically to mattrack's `updateAuthUI()`.

#### Step 2 — Expand trackbjj account modal
Add to `modal-account` in base.html:
- Upgrade Plan button (→ `/api/stripe/checkout`)
- Manage Billing button (→ `/api/billing/portal`)
- Redeem Code input + button
- Manage Team Codes button (gym/affiliate plan only)
- Sign Out button

#### Step 3 — Add payment routes to app_trackbjj.py
```python
@app.route("/api/stripe/checkout", methods=["POST"])
@app.route("/api/billing/portal", methods=["POST"])
@app.route("/api/gym/redeem", methods=["POST"])
@app.route("/api/gym/codes", methods=["GET"])
```
These can import and call `payments.py` functions identically to app.py.

#### Step 4 — Run required SQL in Supabase
```sql
-- Enable IBJJF photo storage
ALTER TABLE sc_ibjjf_verified ADD COLUMN IF NOT EXISTS photo_url text;
```

#### Step 5 — Define trackbjj premium features
What's gated behind `individual` plan on trackbjj:
- Edit social links (currently always available — gate it)
- Claim / link IBJJF profile (gate it)
- Link / verify Smoothcomp (gate it)
- Full match history (show last 5 free, all for premium)
- Head-to-head stats
- Profile photo from IBJJF

Free features (always available):
- View any athlete profile
- Competition results table
- IBJJF rankings strip
- Search

#### Step 6 — Unify JS auth functions
Extract shared auth JS into a canonical block. Both sites use same function names:
- `initAuth()` — mattrack; `tbInitAuth()` — trackbjj → rename both to `initAuth()`
- `signInWithGoogle()` — same name both sites
- `sendMagicLink()` — same name both sites
- `signOut()` — same name both sites
- `updateAuthUI()` — same name both sites, same pill IDs

This makes copy-paste between sites trivial.

---

## 10. EXTERNAL SERVICES

### Supabase RPCs (custom SQL functions in DB)
```sql
get_match_history_ibjjf(p_ibjjf_athlete_id TEXT) → table(...)
get_match_history_sc(p_sc_uid TEXT) → table(...)
search_athletes(q TEXT) → table(sc_uid, name, team, ...)
get_athlete_stats() → {smoothcomp_athletes, ibjjf_athletes, ...}
```

### Smoothcomp (130+ orgs)
Base URL pattern: `https://{subdomain}.smoothcomp.com`
- Known subdomains: naga, compnet, adcc, fuji, tco, grapplingx, newbreed, rollalot, nfc, pbjjf, united, goodfight, subchallenge, gi (more via scraper_smoothcomp.py auto-discovery)
- Bracket endpoint: `/en/event/{event_id}/bracket/{bracket_id}/getRenderData`
- Placement endpoint: `/en/event/{event_id}/bracket/{bracket_id}/getPlacementTableData`

### IBJJF
- Tournament list: `https://www.bjjcompsystem.com/tournaments`
- Roster: `https://www.bjjcompsystem.com/tournaments/{id}/categories`
- Bracket: `https://www.bjjcompsystem.com/tournaments/{id}/categories/{cat_id}`
- Schedule API: `https://ibjjf.com/api/v1/events/upcomings.json`
- Rankings: `https://ibjjf.com/{year}-athletes-ranking`

---

## 11. FILE RESPONSIBILITIES (QUICK REFERENCE)

| File | What it does |
|---|---|
| `app.py` | MatTrack: tournaments, brackets, SSE, roster cache, all mattrack routes |
| `app_trackbjj.py` | TrackBJJ: athlete profiles, search, social links, Instagram OAuth |
| `auth.py` | JWT verify + plan lookup — used by both apps |
| `payments.py` | Stripe checkout, webhooks, access codes — used by both apps |
| `results.py` | Save bracket finals → fighter_results + tournament_results |
| `watcher.py` | Fetch + parse IBJJF brackets (no Playwright) |
| `scraper.py` | IBJJF tournament list + roster building |
| `scraper_naga.py` | NAGA/Smoothcomp event list + bracket fetching |
| `scraper_smoothcomp.py` | Unified event discovery across 130+ Smoothcomp orgs |
| `ibjjf_api.py` | Authenticated IBJJF API (login, profile, registrations) |
| `ibjjf_rankings.py` | IBJJF ranking page scraper |
| `meta_api.py` | Instagram OAuth + post fetching |
| `templates/index.html` | Full mattrack SPA (~3500 lines: CSS + HTML + JS) |
| `trackbjj/templates/trackbjj/base.html` | Shared header + auth modals + auth JS |
| `trackbjj/templates/trackbjj/athlete.html` | Athlete profile page |
| `trackbjj/templates/trackbjj/claim.html` | IBJJF claim form |
| `trackbjj/templates/trackbjj/verify_sc.html` | Smoothcomp verify form |
| `trackbjj/templates/trackbjj/social*.html` | Social links editor |

---

## 12. PATTERNS & CONVENTIONS

### Error Handling
- All external API calls wrapped in `try/except` — log warning, return safe default
- `save_bracket_final()` never raises — always fire-and-forget
- `get_user_from_token()` returns None on any failure — caller must handle

### Supabase Client
```python
# Both apps: single global client, service key, stateless HTTP
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
# Safe to reuse globally (supabase-py v2 is stateless)
```

### Background Threads
```python
# Pattern used throughout:
t = threading.Thread(target=fn, daemon=True)
t.start()
# daemon=True → thread dies with main process
```

### Division Parsing
```python
# Belt: dict lookup (supports Portuguese)
# Age: dict lookup with range matching (±1 rank)
# Gender: regex r"\bfem|\bwom|\bf\b"
# All parsing returns None on no match (never raises)
```

### Roster Caching
```
Seed (shipped JSON) → Runtime disk cache (/tmp) → Supabase
Priority: Supabase > disk > seed
TTL: Roster 1h, JWKS 24h, Events 30min
```

### Rate Limiting (app.py, Flask-Limiter)
```python
# Per-endpoint via @limiter.limit("N/period")
# Storage: in-memory (resets on restart)
# Key: remote IP address
```
