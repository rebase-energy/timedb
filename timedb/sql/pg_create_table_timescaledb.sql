-- TimescaleDB Schema for TimeDB - Part 1: Tables and Indexes
-- This part can run in a transaction

-- Ensure the extension is available
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 1) BATCHES_TABLE (Standard Table)
-- ============================================================================
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

-- ============================================================================
-- 2) SERIES_TABLE (Standard Table / Metadata)
-- ============================================================================
CREATE TABLE IF NOT EXISTS series_table (
  series_id   uuid PRIMARY KEY,
  name        text NOT NULL,
  unit        text NOT NULL,
  labels      jsonb NOT NULL DEFAULT '{}',
  description text,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT series_identity_uniq UNIQUE (name, labels),
  CONSTRAINT series_name_not_empty CHECK (length(btrim(name)) > 0),
  CONSTRAINT series_unit_not_empty CHECK (length(btrim(unit)) > 0)
);

CREATE INDEX IF NOT EXISTS series_labels_gin_idx ON series_table USING GIN (labels);

-- ============================================================================
-- 3) VALUES_TABLE (Will become Hypertable)
-- TimescaleDB version: known_time is denormalized from batches_table for
-- efficient time-based queries and compression. No is_current flag - latest
-- values are determined by known_time using queries with ORDER BY known_time DESC.
-- ============================================================================
CREATE TABLE IF NOT EXISTS values_table (
  value_id        bigserial,
  batch_id        uuid NOT NULL REFERENCES batches_table(batch_id) ON DELETE CASCADE,
  tenant_id       uuid NOT NULL,
  series_id       uuid NOT NULL REFERENCES series_table(series_id) ON DELETE CASCADE,
  valid_time      timestamptz NOT NULL,
  valid_time_end  timestamptz,
  value           double precision,

  -- Denormalized from batches_table for efficient queries and compression
  known_time      timestamptz NOT NULL,

  annotation      text,
  metadata        jsonb,
  tags            text[],
  changed_by      text,
  change_time     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT valid_time_interval_check
    CHECK (valid_time_end IS NULL OR valid_time_end > valid_time)
);

-- Primary operational index: (tenant_id, series_id, valid_time, known_time DESC)
-- Supports fast lookups and finding latest values by known_time
CREATE INDEX IF NOT EXISTS values_lookup_idx
  ON values_table (tenant_id, series_id, valid_time, known_time DESC);

-- Index for known_time-based queries (e.g., "what was known at time X")
CREATE INDEX IF NOT EXISTS values_known_time_idx
  ON values_table (tenant_id, known_time DESC, valid_time);

-- Unique constraint: one value per (tenant, series, valid_time, batch)
CREATE UNIQUE INDEX IF NOT EXISTS values_unique_idx
  ON values_table (tenant_id, series_id, valid_time, COALESCE(valid_time_end, valid_time), batch_id);
