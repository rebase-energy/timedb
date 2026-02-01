-- TimescaleDB Schema for TimeDB - Part 2: Hypertable and Continuous Aggregates
-- This part MUST run with autocommit (outside a transaction)

-- ============================================================================
-- 1) Convert values_table to Hypertable
-- ============================================================================
SELECT create_hypertable('values_table', 'valid_time', migrate_data => TRUE, if_not_exists => TRUE);

-- ============================================================================
-- 2) COMPRESSION SETTINGS (Raw Data)
-- ============================================================================
ALTER TABLE values_table SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'series_id, tenant_id',
  timescaledb.compress_orderby = 'valid_time DESC, known_time DESC'
);

SELECT add_compression_policy('values_table', INTERVAL '14 days', if_not_exists => TRUE);

-- ============================================================================
-- 3) CONTINUOUS AGGREGATE (The "Latest Forecast" View)
-- This materialized view automatically resolves overlapping batches
-- to show only the latest value for every time bucket based on known_time.
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_values
WITH (timescaledb.continuous) AS
SELECT
  tenant_id,
  series_id,
  time_bucket('1 minute', valid_time) as bucket_time,
  last(value, known_time) as value,
  last(batch_id, known_time) as source_batch_id,
  max(known_time) as latest_known_time
FROM values_table
GROUP BY tenant_id, series_id, bucket_time;

-- 3a) Policy: Automatically refresh the view every 30 minutes
-- (Must be set up before compression policy)
SELECT add_continuous_aggregate_policy('latest_values',
  start_offset => NULL,
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '30 minutes',
  if_not_exists => TRUE
);

-- 3b) Compress the View itself for fast queries
ALTER MATERIALIZED VIEW latest_values SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'series_id, tenant_id'
);

-- 3c) Policy: Compress data in the view older than 7 days
SELECT add_compression_policy('latest_values', INTERVAL '7 days', if_not_exists => TRUE);

-- ============================================================================
-- 4) CONVENIENCE VIEW: Current values with series metadata
-- Uses the continuous aggregate for fast access to latest values
-- ============================================================================
CREATE OR REPLACE VIEW current_values_view AS
SELECT
  lv.tenant_id,
  lv.series_id,
  lv.bucket_time as valid_time,
  lv.value,
  lv.source_batch_id as batch_id,
  lv.latest_known_time as known_time,
  s.name AS parameter_name,
  s.unit AS parameter_unit,
  s.labels AS series_labels
FROM latest_values lv
JOIN series_table s ON lv.series_id = s.series_id;
