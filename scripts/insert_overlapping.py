import time
import uuid
import numpy as np
import pandas as pd
import argparse
from datetime import datetime, timedelta, timezone
from timedb import TimeDataClient

def run_overlapping_insert(time_offset_weeks=0, batch_delay_seconds=2):
    """
    Insert overlapping batches to test data versioning and updates.

    Configuration:
    - 1 series
    - 4 batches
    - 72 hourly values per batch (3 days of data)
    - Batches inserted with specified delay between them (default: 2 seconds)
    """
    td = TimeDataClient()

    # Clean start
    td.delete()
    td.create()

    # Configuration
    NUM_SERIES = 1
    TOTAL_BATCHES = 4
    ROWS_PER_BATCH = 72  # 72 hourly values = 3 days
    SERIES_NAME = "overlapping_sensor"
    BATCH_DELAY_SECONDS = batch_delay_seconds  # Seconds between batch insertions

    total_points = NUM_SERIES * TOTAL_BATCHES * ROWS_PER_BATCH
    print(f"Configuration:")
    print(f"  Series: {NUM_SERIES}")
    print(f"  Batches: {TOTAL_BATCHES}")
    print(f"  Values per batch: {ROWS_PER_BATCH} (hourly)")
    print(f"  Batch delay: {BATCH_DELAY_SECONDS} seconds")
    print(f"  Total data points: {total_points:,}")
    print()

    # 1. Pre-create series identity
    print("Creating series...")
    series_id = "sensor_001"
    td.create_series(name=SERIES_NAME, unit="kW", labels={"id": series_id})

    # 2. Main Ingestion Loop
    start_bench = time.perf_counter()
    overall_count = 0
    base_valid_time = datetime.now(timezone.utc) + timedelta(weeks=time_offset_weeks)

    if time_offset_weeks != 0:
        print(f"Adjusting valid times by {time_offset_weeks} weeks. Base valid time: {base_valid_time}")

    print(f"\nStarting batch insertions at {datetime.now(timezone.utc).isoformat()}")
    print("="*60)

    coll = td.series(SERIES_NAME).where(id=series_id)

    for batch_idx in range(TOTAL_BATCHES):
        # Each batch uses the current time as batch_start_time
        # This ensures proper temporal versioning where later batches supersede earlier ones
        batch_start_time = datetime.now(timezone.utc)

        # Generate timestamps for this batch (72 hourly values)
        batch_timestamps = pd.date_range(
            start=base_valid_time,
            periods=ROWS_PER_BATCH,
            freq='h'  # Hourly frequency
        )

        # Generate random values with slight variation per batch
        batch_values = np.random.normal(50 + batch_idx * 2, 5, ROWS_PER_BATCH)

        df_batch = pd.DataFrame({
            "valid_time": batch_timestamps,
            SERIES_NAME: batch_values
        })

        batch_uuid = uuid.uuid4()

        print(f"\nBatch {batch_idx + 1}/{TOTAL_BATCHES}:")
        print(f"  UUID: {batch_uuid}")
        print(f"  Valid time range: {batch_timestamps[0]} to {batch_timestamps[-1]}")
        print(f"  Batch start time: {batch_start_time.isoformat()}")
        print(f"  Mean value: {batch_values.mean():.2f}")

        # Execute the insert
        coll.insert_batch(
            df=df_batch,
            batch_id=batch_uuid,
            batch_start_time=batch_start_time
        )

        overall_count += ROWS_PER_BATCH
        print(f"  ✓ Inserted {ROWS_PER_BATCH} values")

        # Wait for the specified delay before the next batch to ensure temporal separation
        if batch_idx < TOTAL_BATCHES - 1:  # Don't wait after the last batch
            print(f"  ⏳ Waiting {BATCH_DELAY_SECONDS} seconds before next batch...")
            time.sleep(BATCH_DELAY_SECONDS)

    total_time = time.perf_counter() - start_bench

    print("\n" + "="*60)
    print(f"INSERTION COMPLETE")
    print(f"Total values inserted: {overall_count:,}")
    print(f"Total batches: {TOTAL_BATCHES}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Throughput: {overall_count / total_time:.2f} values/sec")
    print("="*60)

    # Read back the data to see what's current
    print("\nReading back current data...")
    result = coll.read(
        start_valid=base_valid_time,
        end_valid=base_valid_time + timedelta(hours=ROWS_PER_BATCH)
    )

    # Handle tuple return (df, batch_mapping) or just df
    if isinstance(result, tuple):
        df_current, batch_mapping = result
        print(f"Batch mapping: {len(batch_mapping)} batches")
    else:
        df_current = result

    print(f"Current records retrieved: {len(df_current):,}")
    if len(df_current) > 0:
        print(f"Columns: {df_current.columns.tolist()}")
        print(f"\nFirst few records:")
        print(df_current.head(10))

        if 'batch_id' in df_current.columns:
            batch_distribution = df_current['batch_id'].value_counts()
            print(f"\nDistribution of current values by batch:")
            for batch_id, count in batch_distribution.items():
                print(f"  {batch_id}: {count} values")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Insert overlapping batches to test data versioning',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run with default settings (2 seconds between batches)
  python isnert_overlapping.py

  # Insert batches 5 seconds apart
  python isnert_overlapping.py --batch-delay 5

  # Shift valid times 4 weeks earlier
  python isnert_overlapping.py --time-offset -4

  # Combine options: 10 second delay, 2 weeks earlier
  python isnert_overlapping.py --time-offset -2 --batch-delay 10
        '''
    )

    parser.add_argument(
        '--time-offset',
        type=int,
        default=0,
        metavar='WEEKS',
        help='Time offset in weeks for valid times (negative for earlier). Default: 0'
    )

    parser.add_argument(
        '--batch-delay',
        type=int,
        default=2,
        metavar='SECONDS',
        help='Seconds between batch insertions. Default: 2'
    )

    args = parser.parse_args()

    run_overlapping_insert(
        time_offset_weeks=args.time_offset,
        batch_delay_seconds=args.batch_delay
    )
