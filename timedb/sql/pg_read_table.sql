SELECT
  r.run_start_time AS run_time,
  v.valid_time,
  v.run_id,
  v.tenant_id,
  v.series_id,
  s.series_key,
  s.series_unit,
  v.value,
  v.annotation,
  v.tags,
  v.changed_by,
  v.change_time
FROM runs_table r
JOIN values_table v
  ON v.run_id = r.run_id AND v.tenant_id = r.tenant_id
JOIN series_table s
  ON v.series_id = s.series_id
WHERE v.is_current = true
  AND (%(tenant_id)s   IS NULL OR v.tenant_id = %(tenant_id)s)
  AND (%(start_valid)s IS NULL OR v.valid_time >= %(start_valid)s)
  AND (%(end_valid)s   IS NULL OR v.valid_time <  %(end_valid)s)
  AND (%(start_run)s   IS NULL OR r.run_start_time >= %(start_run)s)
  AND (%(end_run)s     IS NULL OR r.run_start_time <  %(end_run)s)
ORDER BY r.run_start_time, v.valid_time;
