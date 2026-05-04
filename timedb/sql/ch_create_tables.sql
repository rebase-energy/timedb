-- TimeDB ClickHouse Schema
-- One unified series_values table plus a tiny run → series mapping. Run
-- metadata lives in energydb.runs (PostgreSQL).

-- ============================================================================
-- 1) SERIES_VALUES — unified time-series store
-- ============================================================================
-- Append-only. Reads aggregate over correction chains directly against this
-- table -- no MV is needed.
--
-- Partition key: (retention, toYYYYMM(valid_time)) — keeps tiers physically
-- separate so TTL drops whole partitions, and retention-filtered reads prune
-- down to one tier.
--
-- Sort key: (series_id, valid_time, knowledge_time, change_time) — works for
-- both flat (one kt per vt, collapses naturally) and overlapping series.
--
-- TTL: computed per row by retention, plus a `DELETE WHERE` that excludes the
-- 'forever' tier from TTL evaluation entirely. Still drops whole partitions
-- because (retention, toYYYYMM(valid_time)) aligns TTL boundaries with
-- partitions, and the `retention != 'forever'` predicate prunes 'forever'
-- partitions from the merge work cheaply. The trailing 1825 in the multiIf
-- is a type-stabilizer -- it is unreachable because both _VALID_RETENTIONS
-- (timedb.write) and the PG `valid_retention` CHECK constraint reject any
-- value outside {short, medium, long, forever}.

CREATE TABLE IF NOT EXISTS series_values (
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
      multiIf(retention = 'short',  180,
              retention = 'medium', 1095,
              retention = 'long',   1825,
              1825))
    DELETE WHERE retention != 'forever'
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
