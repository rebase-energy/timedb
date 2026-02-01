import time
import uuid
import numpy as np
import pandas as pd
import argparse
from datetime import datetime, timedelta, timezone
from timedb import TimeDataClient

def run_heavy_batch_ingest(mode='ingest_and_update', time_offset_weeks=0):
    td = TimeDataClient()

    print(f"Running in '{mode}' mode")

    # Configuration
    NUM_SERIES = 1
    TOTAL_BATCHES = 3000
    BATCHES_PER_SERIES = TOTAL_BATCHES // NUM_SERIES
    ROWS_PER_BATCH = 480
    SERIES_NAME = "high_freq_sensor"

    # Skip ingestion if in update_only mode
    if mode == 'update_only':
    if mode == 'update_only':
        print("Skipping ingestion and database recreation (update_only mode)")
        overall_count = 0  # Will be calculated from existing data
        base_time = None  # Will be calculated from existing data
    else:
        # Clean start
        td.delete()
        td.create()

        total_points = NUM_SERIES * BATCHES_PER_SERIES * ROWS_PER_BATCH
        print(f"Target: {NUM_SERIES} series x {BATCHES_PER_SERIES} batches/series = {TOTAL_BATCHES} total batches")
        print(f"Total data points: {total_points:,}")

        # 1. Pre-create series identities
        print("Pre-creating series identities...")
        series_ids = [f"sensor_{i:03d}" for i in range(NUM_SERIES)]
        for s_id in series_ids:
            td.create_series(name=SERIES_NAME, unit="unit", labels={"id": s_id})

        # 2. Main Ingestion Loop
        start_bench = time.perf_counter()
        overall_count = 0
        base_time = datetime.now(timezone.utc) + timedelta(weeks=time_offset_weeks)

        if time_offset_weeks != 0:
            print(f"Adjusting valid times by {time_offset_weeks} weeks. New base_time: {base_time}")

        for s_id in series_ids:
            coll = td.series(SERIES_NAME).where(id=s_id)

            # Pre-generate all timestamps for this series to save time inside the batch loop
            # Total rows for this series = BATCHES_PER_SERIES * ROWS_PER_BATCH
            series_timestamps = pd.date_range(
                start=base_time,
                periods=BATCHES_PER_SERIES * ROWS_PER_BATCH,
                freq='S'
            )

            for b_idx in range(BATCHES_PER_SERIES):
                # Slice the pre-generated timestamps for this batch
                start_idx = b_idx * ROWS_PER_BATCH
                end_idx = start_idx + ROWS_PER_BATCH
                batch_ts = series_timestamps[start_idx:end_idx]

                # Fast value generation
                batch_values = np.random.normal(50, 5, ROWS_PER_BATCH)

                df_batch = pd.DataFrame({
                    "valid_time": batch_ts,
                    SERIES_NAME: batch_values
                })

                print(f"Finished {b_idx + 1}/{BATCHES_PER_SERIES} batches")

                # Execute the insert
                coll.insert_batch(
                    df=df_batch,
                    batch_id=uuid.uuid4(),
                    batch_start_time=datetime.now(timezone.utc)
                )

                overall_count += ROWS_PER_BATCH

            print(f"Finished {s_id} | Total Rows so far: {overall_count:,}")

        total_time = time.perf_counter() - start_bench

        print("\n" + "="*30)
        print(f"FINAL STATS")
        print(f"Total Rows: {overall_count:,}")
        print(f"Total Batches: {TOTAL_BATCHES}")
        print(f"Total Time: {total_time:.2f}s")
        print(f"Throughput: {overall_count / total_time:.2f} rows/sec")
        print(f"Batch Latency: {(total_time / TOTAL_BATCHES) * 1000:.2f} ms/batch")
        print("="*30)

    # --- Start Update Logic ---
    if mode == 'ingest_only':
        print("Skipping update logic (ingest_only mode)")
        return

    print("--- Starting Update Logic ---")

    # 1. Read all inserted data to get value_ids
    all_series_collection = td.series(SERIES_NAME)

    # For update_only mode, read all data without time constraints
    if mode == 'update_only':
        print(f"Reading all existing values to retrieve value_ids...")
        read_start_time = time.perf_counter()
        df_all_values = all_series_collection.read()
        overall_count = len(df_all_values)
        print(f"Found {overall_count:,} existing records")
    else:
        # base_time should be set from ingestion phase
        assert base_time is not None, "base_time should be set in ingest mode"
        print(f"Reading all {overall_count} inserted values to retrieve value_ids...")
        read_start_time = time.perf_counter()
        max_valid_time = base_time + timedelta(seconds=(BATCHES_PER_SERIES * ROWS_PER_BATCH) - 1)
        df_all_values = all_series_collection.read(
            start_valid=base_time,
            end_valid=max_valid_time + timedelta(seconds=1)
        )
    
    read_end_time = time.perf_counter()
    print(f"Finished reading in {read_end_time - read_start_time:.2f}s. Total rows read: {len(df_all_values):,}")
    
    if 'value_id' not in df_all_values.columns:
        print("Error: 'value_id' column not found in the DataFrame. Cannot proceed with update by value_id.")
        print("Available columns:", df_all_values.columns.tolist())
        return # Exit if value_id is not found.
    
    # 2. Select 10% of value_ids for update
    all_value_ids = df_all_values['value_id'].tolist()
    num_to_update = int(len(all_value_ids) * 0.1)
    if num_to_update == 0 and len(all_value_ids) > 0: 
        num_to_update = 1
    
    if len(all_value_ids) == 0:
        print("No values found to update.")
        return

    value_ids_to_update = np.random.choice(all_value_ids, num_to_update, replace=False)
    
    print(f"Selected {len(value_ids_to_update):,} records ({num_to_update / len(all_value_ids) * 100:.2f}%) for update.")

    # 3. Construct the update payload
    updates_payload = []
    for value_id in value_ids_to_update:
        updates_payload.append({
            "value_id": value_id,
            "tags": ['batch_corrected', 'reviewed'],
            "annotation": 'Batch correction: adjusted by processor', 
            "changed_by": 'batch_processor@example.com'
        })

    # 4. Call td.update_records
    update_start_time = time.perf_counter()
    update_result = td.update_records(updates_payload)
    update_end_time = time.perf_counter()
    
    print(f"Update operation finished in {update_end_time - update_start_time:.2f}s.")
    print(f"Records successfully updated: {len(update_result.get('updated', [])):,}")
    print(f"Records skipped (no ops): {len(update_result.get('skipped_no_ops', [])):,}")
    print("--- End Update Logic ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Bulk insert and optionally update time-series data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Ingest only, no updates
  python bulk_insert.py --mode ingest_only

  # Ingest and update (default)
  python bulk_insert.py --mode ingest_and_update

  # Update only, skip ingestion
  python bulk_insert.py --mode update_only

  # Shift valid times 4 weeks earlier
  python bulk_insert.py --time-offset -4

  # Ingest only with 2 weeks earlier timestamps
  python bulk_insert.py --mode ingest_only --time-offset -2
        '''
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['ingest_only', 'ingest_and_update', 'update_only'],
        default='ingest_and_update',
        help='Operation mode: ingest_only (skip updates), ingest_and_update (default), or update_only (skip ingestion)'
    )

    parser.add_argument(
        '--time-offset',
        type=int,
        default=0,
        metavar='WEEKS',
        help='Time offset in weeks (negative for earlier, positive for later). Example: -4 shifts all valid times 4 weeks earlier'
    )

    args = parser.parse_args()

    run_heavy_batch_ingest(mode=args.mode, time_offset_weeks=args.time_offset)

# New table, with timescaledb

# before compression
# #	Table	Total Rows	Raw Data	All Indices	Total Size	Index %	Comp Ratio
# 1	values_table	1440000	173 MB	334 MB	507 MB	65.82	1.00
# 2	batches_table	3000	344 kB	392 kB	736 kB	53.26	1.00
# 3	series_table	1	16 kB	56 kB	72 kB	77.78	1.00

# after compression
# #	Table	Total Rows	Raw Data	All Indices	Total Size	Index %	Comp Ratio
# 1	values_table	1441000	12 MB	240 kB	12 MB	1.89	41.25
# 2	batches_table	3000	344 kB	392 kB	736 kB	53.26	1.00
# 3	series_table	1	16 kB	56 kB	72 kB	77.78	1.00



# #	Table	Total Rows	Raw Data	All Indices	Total Size	Index %
# 1	values_table	1440000	173 MB	174 MB	347 MB	50.08
# 2	batches_table	3000	344 kB	720 kB	1064 kB	67.67
# 3	series_table	1	16 kB	72 kB	88 kB	81.82
#	Parent Table	Index Name	Type	Index Size	Unique	Table %
# 1	batches_table	batches_workflow_start_idx	btree	328 kB	No	30.83
# 2	batches_table	batches_tenant_known_idx	btree	232 kB	No	21.80
# 3	batches_table	batches_table_pkey	btree	160 kB	Yes	15.04
# 4	series_table	series_labels_gin_idx	gin	24 kB	No	27.27
# 5	series_table	series_name_idx	btree	16 kB	No	18.18
# 6	series_table	series_table_pkey	btree	16 kB	Yes	18.18
# 7	series_table	series_identity_uniq	btree	16 kB	Yes	18.18
# 8	values_table	values_current_optimized_idx	btree	132 MB	Yes	37.96
# 9	values_table	values_table_pkey	btree	31 MB	Yes	8.90
# 10	values_table	values_tags_gin_idx	gin	5688 kB	No	1.60
# 11	values_table	values_metadata_gin_idx	gin	5688 kB	No	1.60
# 12	values_table	values_history_tenant_idx	btree	8192 bytes	No	0.00

# #	Parent Table	Index Name	Type	Index Size	Unique	Table %
# 1	batches_table	batches_workflow_start_idx	btree	192 kB	No	38.71
# 2	batches_table	batches_table_pkey	btree	88 kB	Yes	17.74
# 3	series_table	series_labels_gin_idx	gin	24 kB	No	27.27
# 4	series_table	series_identity_uniq	btree	16 kB	Yes	18.18
# 5	series_table	series_table_pkey	btree	16 kB	Yes	18.18
# 6	series_table	series_name_idx	btree	16 kB	No	18.18
# 7	values_table	values_batch_tenant_series_time_idx	btree	121 MB	No	25.16
# 8	values_table	values_one_current_idx	btree	121 MB	Yes	25.11
# 9	values_table	values_batch_time_idx	btree	94 MB	No	19.52
# 10	values_table	values_table_pkey	btree	18 MB	Yes	3.65
# 11	values_table	values_valid_time_idx	btree	18 MB	No	3.65
# 12	values_table	values_series_id_idx	btree	5192 kB	No	1.06
# 13	values_table	values_tags_gin_idx	gin	3400 kB	No	0.69
# 14	values_table	values_metadata_gin_idx	gin	3400 kB	No	0.69

# NUM_SERIES = 1
# TOTAL_BATCHES = 3000
# BATCHES_PER_SERIES = TOTAL_BATCHES // NUM_SERIES
# ROWS_PER_BATCH = 480
# SERIES_NAME = "high_freq_sensor"
# #	Table	Total Rows	Raw Data	All Indices	Total Size	Index %
# 1	values_table	816960	98 MB	382 MB	480 MB	79.54
# 2	batches_table	1702	216 kB	280 kB	496 kB	56.45
# 3	series_table	1	16 kB	72 kB	88 kB	81.82

# NUM_SERIES = 3000
# TOTAL_BATCHES = 1
# BATCHES_PER_SERIES = TOTAL_BATCHES // NUM_SERIES
# ROWS_PER_BATCH = 480
# SERIES_NAME = "high_freq_sensor"
# #	Table	Rows (Est)	Raw Data	Indices	Total	Index %
# 1	series_table	3000	344 kB	808 kB	1152 kB	70.14
# 2	values_table	0	8192 bytes	80 kB	88 kB	90.91
# 3	batches_table	0	8192 bytes	16 kB	24 kB	66.67

# # Configuration
# NUM_SERIES = 30
# TOTAL_BATCHES = 300
# BATCHES_PER_SERIES = TOTAL_BATCHES // NUM_SERIES  # 100 batches per series
# ROWS_PER_BATCH = 480
# explanation: 	Table	Rows (Est)	Raw Data	Indices	Total
# values_table:	144000	17 MB	67 MB	84 MB	
# batches_table:	300	64 kB	80 kB	144 kB	
# series_table:	30	16 kB	72 kB	88 kB


# NUM_SERIES = 1
# TOTAL_BATCHES = 3000
# BATCHES_PER_SERIES = TOTAL_BATCHES // NUM_SERIES
# ROWS_PER_BATCH = 480
#	Table	Rows (Est)	Raw Data	Indices	Total	Status
# 1	values_table	816960	98 MB	382 MB	480 MB	
# 2	batches_table	1702	216 kB	280 kB	496 kB	
# 3	series_table	1	16 kB	72 kB	88 kB