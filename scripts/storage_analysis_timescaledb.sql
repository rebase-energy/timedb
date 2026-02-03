SELECT
    row_number() OVER (ORDER BY total_bytes DESC) AS "#",
    table_name AS "Table",
    row_est AS "Total Rows",
    pg_size_pretty(data_bytes) AS "Raw Data",
    pg_size_pretty(index_bytes) AS "All Indices",
    pg_size_pretty(total_bytes) AS "Total Size",
    ROUND((index_bytes::numeric / NULLIF(total_bytes, 0)::numeric) * 100, 2) AS "Index %",
    comp_ratio AS "Comp Ratio"
FROM (
    -- 1. Analyze the Hypertable (values_table) with Compression Stats
    SELECT
        'values_table' AS table_name,
        approximate_row_count('values_table') AS row_est,
        (d.table_bytes + COALESCE(d.toast_bytes, 0)) AS data_bytes,
        d.index_bytes AS index_bytes,
        d.total_bytes AS total_bytes,
        -- Calculate Ratio: Total Uncompressed / Total Compressed
        CASE 
            WHEN c.after_compression_total_bytes > 0 THEN 
                ROUND((c.before_compression_total_bytes::numeric / c.after_compression_total_bytes::numeric), 2)
            ELSE 1.00 
        END AS comp_ratio
    FROM hypertable_detailed_size('values_table') d
    -- Join with compression stats to get the before/after comparison
    LEFT JOIN hypertable_compression_stats('values_table') c ON true

    UNION ALL

    -- 2. Analyze Standard Table (batches_table)
    SELECT
        'batches_table',
        (SELECT count(*) FROM batches_table),
        (pg_total_relation_size('batches_table') - pg_indexes_size('batches_table')),
        pg_indexes_size('batches_table'),
        pg_total_relation_size('batches_table'),
        1.00 -- Standard tables are uncompressed

    UNION ALL

    -- 3. Analyze Standard Table (series_table)
    SELECT
        'series_table',
        (SELECT count(*) FROM series_table),
        (pg_total_relation_size('series_table') - pg_indexes_size('series_table')),
        pg_indexes_size('series_table'),
        pg_total_relation_size('series_table'),
        1.00 -- Standard tables are uncompressed
) sub_stats
ORDER BY total_bytes DESC;