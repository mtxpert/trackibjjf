-- =============================================================================
-- Schools (trackopenmat.net) + Seminars (trackbjjseminars.net)
-- =============================================================================

-- schools ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schools (
  id                          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  team_slug                   text        UNIQUE NOT NULL,                 -- joins to tournament_results.team_slug
  display_name                text        NOT NULL,
  short_name                  text,
  description                 text,
  address_line1               text,
  address_line2               text,
  city                        text,
  state_province              text,
  country                     text,
  postal_code                 text,
  latitude                    numeric(10,7),
  longitude                   numeric(10,7),
  phone                       text,
  email                       text,
  website                     text,
  instagram_handle            text,
  facebook_handle             text,
  youtube_url                 text,
  founded_year                int,
  head_instructor_name        text,
  head_instructor_athlete_id  text,
  school_type                 text        DEFAULT 'school',                -- school|club|training_center|mat_space
  accepts_dropins             boolean     DEFAULT false,
  dropin_fee_usd              numeric(8,2),
  etiquette_md                text,
  claimed                     boolean     DEFAULT false,
  claimed_at                  timestamptz,
  created_by_user_id          uuid,
  created_at                  timestamptz NOT NULL DEFAULT now(),
  updated_at                  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_schools_city  ON schools (lower(city));
CREATE INDEX IF NOT EXISTS idx_schools_state ON schools (state_province);
CREATE INDEX IF NOT EXISTS idx_schools_geo   ON schools (latitude, longitude)
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- school_schedules -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS school_schedules (
  id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  school_id    uuid        NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
  day_of_week  int         NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),     -- 0=Sun..6=Sat
  start_time   time        NOT NULL,
  end_time     time,
  class_type   text        NOT NULL,                                         -- Gi|No-Gi|Open Mat|Kids|Beginner|Competition|Wrestling|MMA
  level        text        DEFAULT 'All Levels',
  coach_name   text,
  notes        text,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sched_school_day ON school_schedules (school_id, day_of_week);
CREATE INDEX IF NOT EXISTS idx_sched_open_mat   ON school_schedules (school_id) WHERE class_type='Open Mat';

-- school_claims --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS school_claims (
  id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  school_id            uuid        NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
  user_id              uuid        NOT NULL,
  role                 text        NOT NULL DEFAULT 'owner',                 -- owner|head_instructor|instructor|manager
  verified             boolean     DEFAULT false,
  verification_method  text,
  verification_token   text,
  verified_at          timestamptz,
  created_at           timestamptz NOT NULL DEFAULT now(),
  UNIQUE (school_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_school_claims_user ON school_claims (user_id);

-- seminars -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS seminars (
  id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  slug                     text        UNIQUE NOT NULL,
  title                    text        NOT NULL,
  instructor_name          text        NOT NULL,
  instructor_athlete_id    text,
  co_instructors           jsonb       DEFAULT '[]'::jsonb,
  host_school_id           uuid        REFERENCES schools(id) ON DELETE SET NULL,
  host_venue_name          text,
  host_address             text,
  host_city                text,
  host_state               text,
  host_country             text,
  start_datetime           timestamptz NOT NULL,
  end_datetime             timestamptz,
  timezone                 text        DEFAULT 'America/New_York',
  cost_usd                 numeric(8,2),
  registration_url         text,
  registration_deadline    timestamptz,
  max_attendees            int,
  level                    text        DEFAULT 'All Levels',
  gi_required              boolean,
  topic                    text,
  description_md           text,
  flyer_url                text,
  video_promo_url          text,
  approved                 boolean     DEFAULT true,
  created_by_user_id       uuid,
  created_at               timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_seminars_start    ON seminars (start_datetime);
CREATE INDEX IF NOT EXISTS idx_seminars_city     ON seminars (lower(host_city));
CREATE INDEX IF NOT EXISTS idx_seminars_instr    ON seminars (instructor_athlete_id) WHERE instructor_athlete_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_seminars_host     ON seminars (host_school_id) WHERE host_school_id IS NOT NULL;

-- updated_at trigger ---------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS schools_updated_at ON schools;
CREATE TRIGGER schools_updated_at  BEFORE UPDATE ON schools
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS seminars_updated_at ON seminars;
CREATE TRIGGER seminars_updated_at BEFORE UPDATE ON seminars
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =============================================================================
-- Bootstrap schools from existing tournament_results.team data
-- (one row per distinct team_slug; display_name = most common spelling)
-- =============================================================================
INSERT INTO schools (team_slug, display_name)
SELECT
  lower(regexp_replace(team, '[^a-zA-Z0-9]+', '-', 'g')) AS team_slug,
  (array_agg(team ORDER BY n DESC))[1]                  AS display_name
FROM (
  SELECT team, COUNT(*) AS n
    FROM tournament_results
   WHERE team IS NOT NULL AND length(team) > 1
   GROUP BY team
) t
GROUP BY 1
HAVING (array_agg(team ORDER BY n DESC))[1] IS NOT NULL
ON CONFLICT (team_slug) DO NOTHING;
