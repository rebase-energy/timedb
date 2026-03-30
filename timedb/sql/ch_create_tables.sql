-- TimeDB ClickHouse Schema
-- All values tables and run metadata live here.
-- series_table stays in PostgreSQL.

-- ============================================================================
-- 1) RUNS TABLE
-- ============================================================================
-- Append-only run metadata. ReplacingMergeTree deduplicates retried inserts.

CREATE TABLE IF NOT EXISTS runs_table (
    run_id            String,
    workflow_id       Nullable(String),
    run_start_time    Nullable(DateTime64(6, 'UTC')),
    run_finish_time   Nullable(DateTime64(6, 'UTC')),
    run_params        String DEFAULT '{}',
    inserted_at       DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY run_id
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 2) FLAT (Fact Table)
-- ============================================================================
-- Append-only immutable facts. Use argMax(value, change_time) to read latest.
-- change_time in ORDER BY preserves full correction history.

CREATE TABLE IF NOT EXISTS flat (
    series_id      Int64                          CODEC(Delta, ZSTD(3)),
    valid_time     DateTime64(6, 'UTC')           CODEC(DoubleDelta, ZSTD(3)),
    valid_time_end Nullable(DateTime64(6, 'UTC')) CODEC(DoubleDelta, ZSTD(3)),
    value          Float64 DEFAULT nan            CODEC(Gorilla, ZSTD(3)),
    knowledge_time DateTime64(6, 'UTC')           CODEC(DoubleDelta, ZSTD(3)),
    change_time    DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(DoubleDelta, ZSTD(3)),
    run_id         String                         CODEC(ZSTD(3)),
    changed_by     Nullable(String)               CODEC(ZSTD(3)),
    annotation     Nullable(String)               CODEC(ZSTD(3)),
    metadata       String DEFAULT '{}'            CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_time)
ORDER BY (series_id, valid_time, change_time)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 3) OVERLAPPING HYPERTABLES (3 Retention Tiers)
-- ============================================================================
-- Pure-append forecast versioning. Use argMax(value, (knowledge_time, change_time))
-- to read the latest forecast. TTL drops old data by valid_time.

CREATE TABLE IF NOT EXISTS overlapping_short (
    series_id      Int64                          CODEC(Delta, ZSTD(3)),
    valid_time     DateTime64(6, 'UTC')           CODEC(DoubleDelta, ZSTD(3)),
    valid_time_end Nullable(DateTime64(6, 'UTC')) CODEC(DoubleDelta, ZSTD(3)),
    value          Float64 DEFAULT nan            CODEC(Gorilla, ZSTD(3)),
    knowledge_time DateTime64(6, 'UTC')           CODEC(Delta, ZSTD(3)),
    change_time    DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(DoubleDelta, ZSTD(3)),
    run_id         String                         CODEC(ZSTD(3)),
    changed_by     Nullable(String)               CODEC(ZSTD(3)),
    annotation     Nullable(String)               CODEC(ZSTD(3)),
    metadata       String DEFAULT '{}'            CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_time)
ORDER BY (series_id, valid_time, knowledge_time, change_time)
TTL toDate(valid_time) + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS overlapping_medium (
    series_id      Int64                          CODEC(Delta, ZSTD(3)),
    valid_time     DateTime64(6, 'UTC')           CODEC(DoubleDelta, ZSTD(3)),
    valid_time_end Nullable(DateTime64(6, 'UTC')) CODEC(DoubleDelta, ZSTD(3)),
    value          Float64 DEFAULT nan            CODEC(Gorilla, ZSTD(3)),
    knowledge_time DateTime64(6, 'UTC')           CODEC(Delta, ZSTD(3)),
    change_time    DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(DoubleDelta, ZSTD(3)),
    run_id         String                         CODEC(ZSTD(3)),
    changed_by     Nullable(String)               CODEC(ZSTD(3)),
    annotation     Nullable(String)               CODEC(ZSTD(3)),
    metadata       String DEFAULT '{}'            CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_time)
ORDER BY (series_id, valid_time, knowledge_time, change_time)
TTL toDate(valid_time) + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS overlapping_long (
    series_id      Int64                          CODEC(Delta, ZSTD(3)),
    valid_time     DateTime64(6, 'UTC')           CODEC(DoubleDelta, ZSTD(3)),
    valid_time_end Nullable(DateTime64(6, 'UTC')) CODEC(DoubleDelta, ZSTD(3)),
    value          Float64 DEFAULT nan            CODEC(Gorilla, ZSTD(3)),
    knowledge_time DateTime64(6, 'UTC')           CODEC(Delta, ZSTD(3)),
    change_time    DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(DoubleDelta, ZSTD(3)),
    run_id         String                         CODEC(ZSTD(3)),
    changed_by     Nullable(String)               CODEC(ZSTD(3)),
    annotation     Nullable(String)               CODEC(ZSTD(3)),
    metadata       String DEFAULT '{}'            CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_time)
ORDER BY (series_id, valid_time, knowledge_time, change_time)
TTL toDate(valid_time) + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;
