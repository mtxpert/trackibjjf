# How MatTrack Works

A living blueprint for **mattrack.net** (live tournament tracking) and **trackbjj.net** (athlete career repository). Single source of truth — when behavior changes, this doc changes.

---

## 1. The Two Apps

| | mattrack.net | trackbjj.net |
|---|---|---|
| Built for | Coaches watching live brackets | Athletes checking results & history |
| Code | `app.py` + `templates/index.html` (SPA) | `app_trackbjj.py` + `trackbjj/templates/` |
| Reads from | bracket_finals, tournament_events | tournament_results, ibjjf_athletes, sc_ibjjf_verified |
| Writes to | bracket_finals, fighter_results, tournament_results | sc_ibjjf_verified, findme_reports, team_profiles* |

Both sites share one Supabase project, one Stripe account, and the same `public.users` table — so a paid plan unlocks both.

---

## 2. Where the Data Comes From

```
        ┌─────────────────────────┐
        │  External tournament    │
        │      websites           │
        └────────────┬────────────┘
                     │ scrapers (Render crons)
                     ▼
        ┌─────────────────────────┐
        │   Supabase (Postgres)   │
        │   ──────────────────    │
        │   tournament_events     │ ← discovery
        │   bracket_finals        │ ← live brackets
        │   tournament_results    │ ← flat history
        │   ibjjf_athletes        │ ← directory
        │   sc_ibjjf_verified     │ ← claims
        └────────────┬────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   mattrack.net           trackbjj.net
```

### Tracked sources

| Org | Format | Scraper | Coverage |
|---|---|---|---|
| **IBJJF** | bjjcompsystem.com brackets + ibjjfdb.com results | `auto_watch.py`, `scrape_ibjjf_historical.py` | Modern + historical (1994-present) |
| **Smoothcomp** | smoothcomp.com / *.smoothcomp.com brackets | `scrape_sc_brackets.py`, `scrape_smoothcomp_historical.py` | Live brackets + post-event |
| **NAGA** | nagafighter.com | `scraper_naga.py` | Live |
| **ADCC** | adcombat.com | `scrape_adcc_historical.py` | Historical |
| **AJP** | ajptour.com | `scrape_ajp_historical.py` | Historical |
| **SJJIF** | sjjifworldleague.com | `scrape_sjjif_historical.py` | Historical |
| **UAEJJF** | uaejjf.com | `scrape_uaejjf_historical.py` | Historical |
| **Compnet** | compnet.com.au | `scraper_compnet.py` | Live |

---

## 3. Scraper Schedule (Render Crons)

| Cron | Schedule (UTC) | What it does |
|---|---|---|
| `scrape-tournament-list-nightly` | `0 7 * * *` (3am ET) | Pulls tournament index from each org → `tournament_events` |
| `scrape-ibjjf-registrations-nightly` | `30 7 * * *` (3:30am ET) | Pulls IBJJF athlete registrations → `tournament_results` |
| `scrape-sc-registrations-nightly` | `0 8 * * *` (4am ET) | Pulls Smoothcomp registrations → `tournament_results` |
| `live-bracket-scraper` | `*/30 12-23,0-3 * * *` (every 30m, 8am-11pm ET) | Sweeps active bracket pages → `bracket_finals` + `tournament_results` |

**⚠️ Known gap:** `live-bracket-scraper` currently runs `auto_watch.py --sweep-only` which only re-checks tournaments **already** in `bracket_finals`. Brand-new tournaments don't get auto-discovered until a manual `--tid X` push. Fix queued: drop the `--sweep-only` flag so the cron does full discovery every run.

---

## 4. Tables (the ones that matter for behavior)

### `tournament_events`
Master list of every event we know about. Source of truth for the homepage list and the upcoming/past filters.
Key columns: `source`, `event_id`, `name`, `start_date`, `end_date`, `location`, `has_brackets`, `is_past`.

### `bracket_finals`
One row per division (category) with the **full bracket state** in `state_json` — fights, competitors, ranking, results_final flag. This is what powers the live "Watch" screen and the medal podium.
Key columns: `category_id` (PK), `tournament_id`, `division`, `state_json`, `ranking`, `event_date`.

### `tournament_results`
Flat athlete-result history across **all** sources. One row per (athlete × event × division × placement). This is what powers head-to-head queries, athlete profile pages, and search.
Key columns: `source`, `event_id`, `athlete_name`, `athlete_display`, `division`, `placement`, `team`, `event_date`, `ibjjf_athlete_id`.

### `ibjjf_athletes`
IBJJF's official athlete directory (scraped from ibjjf.com profiles). Used as the search target for IBJJF-only athletes who haven't yet linked a Smoothcomp account.
Key columns: `ibjjf_id`, `name`, `name_lower`, `academy`, `belt`, `country`.

### `sc_ibjjf_verified`
The **claim ledger**. One row per verified link between an IBJJF athlete and a Smoothcomp account (or either one alone).
Key columns: `sc_uid`, `ibjjf_athlete_id`, `email`, `verified_at`. Unique constraint on `ibjjf_athlete_id` blocks claim-jacking.

### `findme_reports`
Captures every search that didn't find a clean match — feeds the daily Claude scheduled task that triages unresolved athletes.
Key columns: `submitted_name`, `email`, `status` (pending/resolved/unresolvable), `resolution_notes`.

---

## 5. Key Routes

### mattrack.net
| Route | Purpose |
|---|---|
| `/` | Home — tournament list + search |
| `/org/<source>/<event_id>` | Per-event watch screen with live bracket data |
| `/api/tournaments` | Tournament list (reads `tournament_events`) |
| `/api/roster/<event_id>` | Athletes registered for an event |
| `/api/pick-statuses` | Live status of selected athletes (medal/next-fight/eliminated) |
| `/api/refresh` | Force re-scrape an event's brackets |
| `/api/search` | Athlete search across the active event |

### trackbjj.net
| Route | Purpose |
|---|---|
| `/` | Athlete search |
| `/athlete/<sc_uid>` | Smoothcomp-keyed athlete profile |
| `/ibjjf-athlete/<ibjjf_id>` | IBJJF-only athlete profile (redirects to `/athlete/<sc_uid>` once claimed) |
| `/findme` | Self-claim entry point — IBJJF or SC auth |
| `/claim-me?sc_uid=X&ibjjf_id=Y` | Unified claim flow with intent-matching to block claim-jackers |

---

## 6. Auth & Profile Claims

```
   User searches "tyler michael walker"
            │
            ▼
   Search returns IBJJF-only card
            │
            ▼
   "This is me — Claim" button
            │
            ▼
   /claim-me?ibjjf_id=675335
            │
            ▼
   Login with IBJJF credentials → ibjjf_api.login(email, pwd)
            │
            ▼
   Validate auth'd ibjjf_id matches intent (else: reject, flash error)
            │
            ▼
   Upsert sc_ibjjf_verified row (unique on ibjjf_athlete_id — blocks claim-jacking)
            │
            ▼
   Mark findme_report resolved → redirect to /athlete/<sc_uid>
```

Owner emails (`mbambic@gmail.com`, `chrisbambic@gmail.com`, `tbambic@gmail.com`) bypass the paywall via `OWNER_EMAILS` short-circuit in `auth.py`.

---

## 7. Plans & Billing

| Plan | What it unlocks | Stripe link |
|---|---|---|
| **free** | Search-only, limited data | — |
| **individual** | Full search + watch + push notifications | `STRIPE_PRICE_INDIVIDUAL` |
| **gym** | Up to N member codes for one school | `STRIPE_PRICE_GYM` |
| **affiliate** | Multi-school packs (BJJ Globetrotters etc) | `STRIPE_PRICE_AFFILIATE` |

Billing is fully Stripe-managed via `payments.py`. `/api/billing/portal` returns a Stripe Customer Portal link — no plan-change UI built into the app.

---

## 8. Push & Service Worker

- VAPID keys live in env (`VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`)
- `/sw.js` registers as service worker — handles bracket-update notifications
- `/api/push/subscribe` stores subscriptions in `push_subscriptions` table
- Notifications fire when a watched athlete's status changes (next-fight assigned, won/lost, medaled)

---

## 9. Live Status

_(populated by the route at runtime — not from this doc)_
