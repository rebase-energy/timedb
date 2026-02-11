-- TimeDB Schema - Part 2: Hypertables, Compression, Retention
-- This part MUST run with autocommit (outside a transaction block).

-- ============================================================================
-- 1) HYPERTABLES
-- ============================================================================
-- Flat is a standalone hypertable.
-- Overlapping children are partition children AND hypertables.

SELECT create_hypertable('flat', 'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('overlapping_short',  'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('overlapping_medium', 'valid_time', if_not_exists => TRUE);
SELECT create_hypertable('overlapping_long',   'valid_time', if_not_exists => TRUE);


-- ============================================================================
-- 2) RETENTION POLICIES (Variable Retention per Tier)
-- ============================================================================

SELECT add_retention_policy('overlapping_short',  INTERVAL '{retention_short}', if_not_exists => TRUE);
SELECT add_retention_policy('overlapping_medium', INTERVAL '{retention_medium}', if_not_exists => TRUE);
SELECT add_retention_policy('overlapping_long',   INTERVAL '{retention_long}', if_not_exists => TRUE);


-- ============================================================================
-- 3) COMPRESSION SETTINGS
-- ============================================================================

-- Flat
ALTER TABLE flat SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'valid_time DESC');
SELECT add_compression_policy('flat', INTERVAL '14 days', if_not_exists => TRUE);

-- Overlapping Short
ALTER TABLE overlapping_short SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'valid_time DESC');
SELECT add_compression_policy('overlapping_short', INTERVAL '14 days', if_not_exists => TRUE);

-- Overlapping Medium
ALTER TABLE overlapping_medium SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'valid_time DESC');
SELECT add_compression_policy('overlapping_medium', INTERVAL '14 days', if_not_exists => TRUE);

-- Overlapping Long
ALTER TABLE overlapping_long SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'valid_time DESC');
SELECT add_compression_policy('overlapping_long', INTERVAL '14 days', if_not_exists => TRUE);
