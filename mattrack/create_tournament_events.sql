-- =============================================================================
-- tournament_events — one row per tournament/event, populated by
-- scrape_tournament_list.py (nightly cron). The client UI reads ONLY
-- from this table; no more live scrapers on the request path.
--
-- Run this in the Supabase SQL editor:
--   Dashboard → SQL Editor → New query → paste → Run
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.tournament_events (
    source            TEXT NOT NULL,      -- 'ibjjf', 'naga', 'compnet', 'adcc', 'gi', etc.
    event_id          TEXT NOT NULL,      -- source's native event ID
    name              TEXT NOT NULL,
    start_date        DATE,
    end_date          DATE,
    location          TEXT,
    city              TEXT,
    country           TEXT,
    country_code      TEXT,
    lat               DOUBLE PRECISION,
    lng               DOUBLE PRECISION,
    url               TEXT,
    cover_image       TEXT,
    has_brackets      BOOLEAN DEFAULT FALSE,
    is_past           BOOLEAN DEFAULT FALSE,
    registered_count  INT DEFAULT 0,
    last_scraped      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, event_id)
);

CREATE INDEX IF NOT EXISTS idx_te_start_date ON public.tournament_events (start_date);
CREATE INDEX IF NOT EXISTS idx_te_source     ON public.tournament_events (source);
CREATE INDEX IF NOT EXISTS idx_te_is_past    ON public.tournament_events (is_past);
