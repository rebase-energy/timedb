-- TimeDB Schema - Part 1: Tables and Indexes
-- This part runs inside a transaction.

-- Ensure the extension is available
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 1) METADATA TABLES
-- ============================================================================

-- A. BATCHES
CREATE TABLE IF NOT EXISTS batches_table (
  batch_id          uuid PRIMARY KEY,
  tenant_id         uuid NOT NULL,
  workflow_id       text,
  batch_start_time  timestamptz,
  batch_finish_time timestamptz,
  known_time        timestamptz NOT NULL DEFAULT now(),
  batch_params      jsonb,
  inserted_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT batch_params_is_object
    CHECK (batch_params IS NULL OR jsonb_typeof(batch_params) = 'object')
);
CREATE INDEX IF NOT EXISTS batches_tenant_known_idx ON batches_table (tenant_id, known_time DESC);

-- B. SERIES
CREATE TABLE IF NOT EXISTS series_table (
  series_id     uuid PRIMARY KEY,
  name          text NOT NULL,
  unit          text NOT NULL,
  labels        jsonb NOT NULL DEFAULT '{}',
  description   text,
  data_class    text NOT NULL DEFAULT 'projection',
  storage_tier  text NOT NULL DEFAULT 'medium',
  inserted_at   timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT series_identity_uniq UNIQUE (name, labels),
  CONSTRAINT series_name_not_empty CHECK (length(btrim(name)) > 0),
  CONSTRAINT valid_data_class CHECK (data_class IN ('actual', 'projection')),
  CONSTRAINT valid_storage_tier CHECK (storage_tier IN ('short', 'medium', 'long'))
);
CREATE INDEX IF NOT EXISTS series_labels_gin_idx ON series_table USING GIN (labels);


-- ============================================================================
-- 2) ACTUALS (Fact Table)
-- ============================================================================

CREATE TABLE IF NOT EXISTS actuals (
  actual_id       bigserial,
  tenant_id       uuid NOT NULL,
  series_id       uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),
  inserted_at     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_actuals
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  CONSTRAINT annotation_not_empty_actuals
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array_actuals
    CHECK (tags IS NULL OR cardinality(tags) > 0),
  UNIQUE (tenant_id, series_id, valid_time)
);


-- ============================================================================
-- 3) PROJECTIONS: LONG (5 Years)
-- ============================================================================

CREATE TABLE IF NOT EXISTS projections_long (
  projection_id   bigserial,
  batch_id        uuid NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  tenant_id       uuid NOT NULL,
  series_id       uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
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
  CONSTRAINT annotation_not_empty_long
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array_long
    CHECK (tags IS NULL OR cardinality(tags) > 0)
);

CREATE INDEX IF NOT EXISTS long_lookup_idx
  ON projections_long (tenant_id, series_id, valid_time, known_time DESC);


-- ============================================================================
-- 4) PROJECTIONS: MEDIUM (3 Years)
-- ============================================================================

CREATE TABLE IF NOT EXISTS projections_medium (
  projection_id   bigserial,
  batch_id        uuid NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  tenant_id       uuid NOT NULL,
  series_id       uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,
  known_time      timestamptz NOT NULL,
  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check_med
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time),
  CONSTRAINT annotation_not_empty_med
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array_med
    CHECK (tags IS NULL OR cardinality(tags) > 0)
);

CREATE INDEX IF NOT EXISTS medium_lookup_idx
  ON projections_medium (tenant_id, series_id, valid_time, known_time DESC);


-- ============================================================================
-- 5) PROJECTIONS: SHORT (6 Months)
-- ============================================================================

CREATE TABLE IF NOT EXISTS projections_short (
  projection_id   bigserial,
  batch_id        uuid NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  tenant_id       uuid NOT NULL,
  series_id       uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
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
  CONSTRAINT annotation_not_empty_short
    CHECK (annotation IS NULL OR length(btrim(annotation)) > 0),
  CONSTRAINT tags_not_empty_array_short
    CHECK (tags IS NULL OR cardinality(tags) > 0)
);

CREATE INDEX IF NOT EXISTS short_lookup_idx
  ON projections_short (tenant_id, series_id, valid_time, known_time DESC);
