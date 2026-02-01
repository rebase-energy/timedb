BEGIN;

-- ============================================================================
-- 1) BATCHES_TABLE
-- Tracks metadata for every ingestion event (manual or automated).
-- ============================================================================

CREATE TABLE IF NOT EXISTS batches_table (
  batch_id           uuid PRIMARY KEY,
  tenant_id          uuid NOT NULL,
  workflow_id        text,
  batch_start_time   timestamptz,
  batch_finish_time  timestamptz,
  -- "Time of Knowledge": used to track when forecasts were available
  known_time         timestamptz NOT NULL DEFAULT now(),
  batch_params       jsonb,
  inserted_at        timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT batch_params_is_object
    CHECK (batch_params IS NULL OR jsonb_typeof(batch_params) = 'object')
);

-- Accelerates batch discovery by tenant, sorted by most recent knowledge
CREATE INDEX IF NOT EXISTS batches_tenant_known_idx
  ON batches_table (tenant_id, known_time DESC);

-- Supports audit trails and performance tracking for specific workflow pipelines
CREATE INDEX IF NOT EXISTS batches_workflow_start_idx
  ON batches_table (tenant_id, workflow_id, batch_start_time DESC);


-- ============================================================================
-- 2) SERIES_TABLE
-- Defines unique time-series identities (e.g., Wind Speed at Site A).
-- ============================================================================

CREATE TABLE IF NOT EXISTS series_table (
  series_id        uuid PRIMARY KEY,
  name             text NOT NULL,
  unit             text NOT NULL,
  -- Differentiates series via arbitrary key-value pairs
  labels           jsonb NOT NULL DEFAULT '{}',
  description      text,
  inserted_at      timestamptz NOT NULL DEFAULT now(),

  -- Ensures name + labels form a unique business identity
  CONSTRAINT series_identity_uniq UNIQUE (name, labels),
  CONSTRAINT series_name_not_empty CHECK (length(btrim(name)) > 0),
  CONSTRAINT series_unit_not_empty CHECK (length(btrim(unit)) > 0)
);

-- Enables fast searching for series containing specific label subsets
CREATE INDEX IF NOT EXISTS series_labels_gin_idx ON series_table USING GIN (labels);
-- Basic B-Tree for direct name-based filtering
CREATE INDEX IF NOT EXISTS series_name_idx ON series_table (name);


-- ============================================================================
-- 3) VALUES_TABLE
-- The fact table storing versioned data points and forecasts.
-- ============================================================================

CREATE TABLE IF NOT EXISTS values_table (
  value_id         bigserial PRIMARY KEY,
  batch_id         uuid NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  tenant_id        uuid NOT NULL,
  series_id        uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time       timestamptz NOT NULL,
  valid_time_end   timestamptz,
  value            double precision,
  annotation       text,
  metadata         jsonb,
  tags             text[],   -- Sparse labels for data quality flags
  changed_by       text,
  change_time      timestamptz NOT NULL DEFAULT now(),
  -- Flag to quickly toggle the latest state vs. historical revisions
  is_current       boolean NOT NULL DEFAULT true,

  CONSTRAINT valid_time_interval_check
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  CONSTRAINT annotation_not_empty
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array
    CHECK (tags IS NULL OR cardinality(tags) > 0)
);

-- PRIMARY OPERATIONAL INDEX:
-- 1. Enforces uniqueness for current values (now scoped per batch).
-- 2. Optimized for lookups by tenant/series/time.
CREATE UNIQUE INDEX IF NOT EXISTS values_current_optimized_idx
  ON values_table (
    tenant_id,
    series_id,
    valid_time,
    (COALESCE(valid_time_end, valid_time)),
    batch_id  -- <--- Moved HERE to make it part of the Unique Key
  )
  INCLUDE (value) -- value stays here to enable fast Index-Only Scans
  WHERE is_current;

-- HISTORICAL LOOKUP INDEX:
-- Supports overlapping forecast analysis and backtesting by tenant/series.
CREATE INDEX IF NOT EXISTS values_history_tenant_idx
  ON values_table (tenant_id, series_id, valid_time)
  WHERE NOT is_current;

-- Optimized search for metadata flags and JSON properties
CREATE INDEX IF NOT EXISTS values_tags_gin_idx ON values_table USING GIN (tags);
CREATE INDEX IF NOT EXISTS values_metadata_gin_idx ON values_table USING GIN (metadata);


-- ============================================================================
-- 5) CONVENIENCE VIEWS
-- ============================================================================

-- Simplified access to the most recent version of all time-series data
CREATE OR REPLACE VIEW current_values_table AS
SELECT 
  v.*,
  s.name AS parameter_name,
  s.unit AS parameter_unit,
  s.labels AS series_labels
FROM values_table v
JOIN series_table s ON v.series_id = s.series_id
WHERE v.is_current = true;

COMMIT;