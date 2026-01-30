-- 1. Refresh statistics so Row Counts are accurate
ANALYZE batches_table;
ANALYZE series_table;
ANALYZE values_table;

-- 2. Comprehensive Table Storage Summary
WITH table_stats AS (
    SELECT
        c.oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.reltuples AS row_count,
        pg_table_size(c.oid) AS data_bytes,
        pg_indexes_size(c.oid) AS index_bytes,
        pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' 
      AND c.relkind = 'r'
      AND c.relname IN ('batches_table', 'series_table', 'values_table')
)
SELECT
    table_name AS "Table",
    row_count::bigint AS "Total Rows",
    pg_size_pretty(data_bytes) AS "Raw Data",
    pg_size_pretty(index_bytes) AS "All Indices",
    pg_size_pretty(total_bytes) AS "Total Size",
    ROUND(100.0 * index_bytes / NULLIF(total_bytes, 0), 2) AS "Index %"
FROM table_stats
ORDER BY total_bytes DESC;

-- 3. Individual Index Breakdown (Deep Dive)
SELECT
    t.relname AS "Parent Table",
    i.relname AS "Index Name",
    am.amname AS "Type", -- btree, gin, etc.
    pg_size_pretty(pg_relation_size(i.oid)) AS "Index Size",
    CASE 
        WHEN idx.indisunique THEN 'Yes' 
        ELSE 'No' 
    END AS "Unique",
    -- Calculate what percentage of the table's total size this specific index takes
    ROUND(100.0 * pg_relation_size(i.oid) / NULLIF(pg_total_relation_size(t.oid), 0), 2) AS "Table %"
FROM pg_class t
JOIN pg_index idx ON t.oid = idx.indrelid
JOIN pg_class i ON i.oid = idx.indexrelid
JOIN pg_am am ON i.relam = am.oid
WHERE t.relname IN ('batches_table', 'series_table', 'values_table')
ORDER BY t.relname, pg_relation_size(i.oid) DESC;