-- TimeDB ClickHouse Schema
-- One unified events table, one MV that collapses corrections, one tiny
-- run → series mapping. Run metadata lives in energydb.runs (PostgreSQL).

-- ============================================================================
-- 1) EVENTS — unified time-series store
-- ============================================================================
-- Append-only. Reads go through events_by_kt (MV) for aggregation -- the raw
-- events table is only needed for correction-chain audits.
--
-- Partition key: (retention, toYYYYMM(valid_time)) — keeps tiers physically
-- separate so TTL drops whole partitions, and retention-filtered reads prune
-- down to one tier.
--
-- Sort key: (series_id, valid_time, knowledge_time, change_time) — works for
-- both flat (one kt per vt, collapses naturally) and overlapping series.
--
-- TTL: computed per row by retention. Still drops whole partitions because
-- (retention, toYYYYMM(valid_time)) aligns TTL boundaries with partitions.

CREATE TABLE IF NOT EXISTS events (
    series_id      UInt64                                CODEC(Delta, ZSTD(1)),
    valid_time     DateTime64(6, 'UTC')                  CODEC(DoubleDelta, ZSTD(1)),
    knowledge_time DateTime64(6, 'UTC')                  CODEC(Delta, ZSTD(1)),
    change_time    DateTime64(6, 'UTC')                  CODEC(DoubleDelta, ZSTD(1)),
    value          Float64 DEFAULT nan                   CODEC(Gorilla, ZSTD(1)),
    valid_time_end DateTime64(6, 'UTC') DEFAULT toDateTime64('2200-01-01 00:00:00', 6, 'UTC')
                                                         CODEC(DoubleDelta, ZSTD(1)),
    run_id         UInt64                                CODEC(ZSTD(1)),
    changed_by     LowCardinality(String),
    annotation     String                                CODEC(ZSTD(3)),
    retention      LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY (retention, toYYYYMM(valid_time))
ORDER BY (series_id, valid_time, knowledge_time, change_time)
TTL toDate(valid_time) + toIntervalDay(
      multiIf(retention = 'short', 180,
              retention = 'medium', 1095,
              1825))
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 2) RUN_SERIES — which runs touched which series
-- ============================================================================
-- Tiny mapping table. Replaces the FINAL + DISTINCT join pattern that was used
-- against the data table for "runs for this series" lookups.

CREATE TABLE IF NOT EXISTS run_series (
    series_id   UInt64,
    run_id      UInt64,
    first_seen  DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(first_seen)
ORDER BY (series_id, run_id)
SETTINGS index_granularity = 8192;
