# MatTrack — Feature Backlog

## 1. Social Media Results Sharing
**Status:** Basic "📋 Copy" button shipped (copies plain-text podium to clipboard).

**Enhancements wanted:**
- Richer formatting: team name, belt/age division, tournament city
- "Share selected athletes only" vs "share all placers"
- Image card generation (e.g. canvas-rendered medal card) for Instagram/stories
- Per-athlete share button on each card in addition to the bulk copy

---

## 2. Per-Tournament Threaded Watchers
**Status:** Single background poller thread shared across all brackets.

**Goal:** One watcher thread per active tournament, auto-spun up when the first
user views that tournament and torn down when all watched brackets are final AND
no users are actively viewing it.

**Design sketch:**
- Track active viewers per tournament_id (increment on SSE connect, decrement on disconnect)
- Spin up a `TournamentWatcher(tournament_id)` thread when viewer_count goes 0→1
- Thread polls only the brackets for that tournament (not the global registry)
- Shutdown condition: `results_final=True` for all watched brackets in that tournament
  AND viewer_count == 0 (nobody watching anymore)
- Shared `_tournament_watchers: dict[tournament_id, WatcherThread]` with a lock
- Gracefully handle the case where the same bracket is watched across multiple
  tournaments (shouldn't happen but worth guarding)

---

## 3. Athlete / Fighter Profiles
**Status:** `bracket_finals` + `fighter_results` tables in Supabase, `/api/fighter/<name>` endpoint wired. Data populates as brackets complete going forward.

**Goal:** Cross-platform career record — IBJJF + NAGA + future orgs.

**Remaining work:**
- Backfill existing Atlanta NAGA results into `fighter_results` (run a one-time script
  against the cached bracket states in seed_cache)
- Athlete profile screen in the UI: search by name → show career record table
  (date | tournament | division | placement | org)
- Win/loss summary stats (gold/silver/bronze/eliminated counts)
- Name disambiguation: same athlete, slightly different spellings across orgs
  (consider fuzzy match or a manual alias table)
- Optional: link fighter profile to a MatTrack user account so athletes can
  claim their own record

---

## 4. Supabase Tables Needed (run in SQL Editor)
The following were added to `schema.sql` but must be created manually in the
Supabase dashboard SQL Editor if not yet applied:

```sql
-- See full SQL in schema.sql, sections:
--   public.bracket_finals
--   public.fighter_results
```
