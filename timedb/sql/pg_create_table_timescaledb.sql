-- TimeDB Schema - Part 1: Tables and Indexes
-- This part runs inside a transaction.

BEGIN;

-- Ensure the extension is available
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 1) METADATA TABLES
-- ============================================================================

-- A. BATCHES
CREATE TABLE IF NOT EXISTS batches_table (
  batch_id          bigserial PRIMARY KEY,
  workflow_id       text,
  batch_start_time  timestamptz,
  batch_finish_time timestamptz,
  known_time        timestamptz NOT NULL DEFAULT now(),
  batch_params      jsonb,
  inserted_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT batch_params_is_object
    CHECK (batch_params IS NULL OR jsonb_typeof(batch_params) = 'object')
);

-- B. SERIES
CREATE TABLE IF NOT EXISTS series_table (
  series_id     bigserial PRIMARY KEY,
  name          text NOT NULL,
  unit          text NOT NULL,
  labels        jsonb NOT NULL DEFAULT '{}',
  description   text,
  overlapping   boolean NOT NULL DEFAULT false,
  retention     text NOT NULL DEFAULT 'medium',
  inserted_at   timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT series_identity_uniq UNIQUE (name, labels),
  CONSTRAINT series_name_not_empty CHECK (length(btrim(name)) > 0),
  CONSTRAINT valid_retention CHECK (retention IN ('short', 'medium', 'long'))
);
CREATE INDEX IF NOT EXISTS series_labels_gin_idx ON series_table USING GIN (labels);


-- ============================================================================
-- 2) FLAT (Fact Table)
-- ============================================================================

CREATE TABLE IF NOT EXISTS flat (
  flat_id         bigserial,
  series_id       bigint NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),
  inserted_at     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_flat
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  CONSTRAINT annotation_not_empty_flat
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array_flat
    CHECK (tags IS NULL OR cardinality(tags) > 0),
  UNIQUE (series_id, valid_time)
);


-- ============================================================================
-- 3) OVERLAPPING HYPERTABLES (Independent Tables)
-- ============================================================================
-- Each tier is an independent table that will be converted to a hypertable
-- in Part 2. Application-level routing directs inserts to the correct table
-- based on series_table.retention.

-- TIER 1: SHORT (6 Months retention)
CREATE TABLE IF NOT EXISTS overlapping_short (
  overlapping_id  bigserial,
  batch_id        bigint NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  series_id       bigint NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  known_time      timestamptz NOT NULL,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_short
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  UNIQUE (series_id, valid_time, known_time)
);

-- TIER 2: MEDIUM (3 Years retention)
CREATE TABLE IF NOT EXISTS overlapping_medium (
  overlapping_id  bigserial,
  batch_id        bigint NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  series_id       bigint NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  known_time      timestamptz NOT NULL,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_medium
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  UNIQUE (series_id, valid_time, known_time)
);

-- TIER 3: LONG (5 Years retention)
CREATE TABLE IF NOT EXISTS overlapping_long (
  overlapping_id  bigserial,
  batch_id        bigint NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  series_id       bigint NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  known_time      timestamptz NOT NULL,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_long
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  UNIQUE (series_id, valid_time, known_time)
);


-- ============================================================================
-- 4) INDEXES ON OVERLAPPING TABLES
-- ============================================================================

CREATE INDEX IF NOT EXISTS short_lookup_idx  ON overlapping_short  (series_id, valid_time, known_time DESC);
CREATE INDEX IF NOT EXISTS medium_lookup_idx ON overlapping_medium (series_id, valid_time, known_time DESC);
CREATE INDEX IF NOT EXISTS long_lookup_idx   ON overlapping_long   (series_id, valid_time, known_time DESC);


-- ============================================================================
-- 5) UNIFIED OVERLAPPING VIEW
-- ============================================================================

CREATE OR REPLACE VIEW all_overlapping_raw AS
SELECT *, 'short'  as retention FROM overlapping_short
UNION ALL
SELECT *, 'medium' as retention FROM overlapping_medium
UNION ALL
SELECT *, 'long'   as retention FROM overlapping_long;

COMMIT;
