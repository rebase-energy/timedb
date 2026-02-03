-- TimeDB Schema - Part 2: Hypertables, Compression, Continuous Aggregates, Views, Policies
-- This part MUST run with autocommit (outside a transaction block).

-- ============================================================================
-- 1) HYPERTABLES
-- ============================================================================

SELECT create_hypertable('actuals', 'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('projections_long', 'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('projections_medium', 'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('projections_short', 'valid_time', if_not_exists => TRUE);


-- ============================================================================
-- 2) COMPRESSION SETTINGS
-- ============================================================================

ALTER TABLE actuals SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'series_id, tenant_id',
  timescaledb.compress_orderby = 'valid_time DESC'
);
SELECT add_compression_policy('actuals', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE projections_long SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'series_id, tenant_id',
  timescaledb.compress_orderby = 'valid_time DESC'
);
SELECT add_compression_policy('projections_long', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE projections_medium SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'series_id, tenant_id',
  timescaledb.compress_orderby = 'valid_time DESC'
);
SELECT add_compression_policy('projections_medium', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE projections_short SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'series_id, tenant_id',
  timescaledb.compress_orderby = 'valid_time DESC'
);
SELECT add_compression_policy('projections_short', INTERVAL '14 days', if_not_exists => TRUE);


-- ============================================================================
-- 3) RETENTION POLICIES
-- ============================================================================

SELECT add_retention_policy('projections_long', INTERVAL '5 years', if_not_exists => TRUE);
SELECT add_retention_policy('projections_medium', INTERVAL '3 years', if_not_exists => TRUE);
SELECT add_retention_policy('projections_short', INTERVAL '6 months', if_not_exists => TRUE);


-- ============================================================================
-- 4) UNIFIED RAW VIEW
-- ============================================================================

CREATE OR REPLACE VIEW all_projections_raw AS
SELECT *, 'long'   as tier FROM projections_long
UNION ALL
SELECT *, 'medium' as tier FROM projections_medium
UNION ALL
SELECT *, 'short'  as tier FROM projections_short;


-- ============================================================================
-- 5) CONTINUOUS AGGREGATES (Updated for Real-Time Access)
-- ============================================================================

-- Goal: Set materialized_only = false so new data is visible before the refresh policy runs.

-- Long Term
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_projections_long
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
  tenant_id, series_id,
  time_bucket('1 minute', valid_time) as bucket_time,
  last(value, known_time) as value,
  last(batch_id, known_time) as source_batch_id,
  max(known_time) as generated_at
FROM projections_long
GROUP BY tenant_id, series_id, bucket_time;

-- Medium Term
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_projections_medium
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
  tenant_id, series_id,
  time_bucket('1 minute', valid_time) as bucket_time,
  last(value, known_time) as value,
  last(batch_id, known_time) as source_batch_id,
  max(known_time) as generated_at
FROM projections_medium
GROUP BY tenant_id, series_id, bucket_time;

-- Short Term
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_projections_short
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
  tenant_id, series_id,
  time_bucket('1 minute', valid_time) as bucket_time,
  last(value, known_time) as value,
  last(batch_id, known_time) as source_batch_id,
  max(known_time) as generated_at
FROM projections_short
GROUP BY tenant_id, series_id, bucket_time;


-- ============================================================================
-- 6) FINAL API VIEW
-- ============================================================================

CREATE OR REPLACE VIEW latest_projection_curve AS
SELECT *, 'long'   as tier FROM latest_projections_long
UNION ALL
SELECT *, 'medium' as tier FROM latest_projections_medium
UNION ALL
SELECT *, 'short'  as tier FROM latest_projections_short;


-- ============================================================================
-- 7) REFRESH POLICIES
-- ============================================================================

SELECT add_continuous_aggregate_policy('latest_projections_short',
  start_offset => INTERVAL '3 days',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('latest_projections_medium',
  start_offset => INTERVAL '7 days',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('latest_projections_long',
  start_offset => INTERVAL '1 month',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '2 hours');