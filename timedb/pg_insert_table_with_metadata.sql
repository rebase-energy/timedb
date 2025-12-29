INSERT INTO metadata_table (
    run_id, valid_time, metadata_key,
    value_number, value_string, value_bool, value_time, value_json,
    inserted_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (run_id, valid_time, metadata_key) DO UPDATE
    SET value_number = EXCLUDED.value_number,
        value_string = EXCLUDED.value_string,
        value_bool   = EXCLUDED.value_bool,
        value_time   = EXCLUDED.value_time,
        value_json   = EXCLUDED.value_json,
        inserted_at  = now();

