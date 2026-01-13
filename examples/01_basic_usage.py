"""
Example 1: Basic Usage - Creating schema and inserting time series data

This example demonstrates:
- Creating the database schema
- Inserting a run with time series values
- Reading the data back
"""
import os
import uuid
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from timedb.db import create, insert, read

load_dotenv()


def main():
    # Get database connection string from environment
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: Set TIMEDB_DSN or DATABASE_URL environment variable")
        return
    
    print("=" * 60)
    print("Example 1: Basic Usage")
    print("=" * 60)
    
    # Step 1: Create the database schema
    print("\n1. Creating database schema...")
    create.create_schema(conninfo)
    print("   ✓ Schema created successfully")
    
    # Step 2: Create a run with time series values
    print("\n2. Inserting a forecast run with values...")
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()  # In production, this would come from context
    series_id = uuid.uuid4()  # In production, this would come from context
    workflow_id = "example-forecast"
    run_start_time = datetime.now(timezone.utc)
    
    # Create some sample time series data
    base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    value_rows = [
        (tenant_id, base_time + timedelta(hours=i), series_id, 100.0 + i * 0.5)
        for i in range(24)  # 24 hours of data
    ]
    
    insert.insert_run_with_values(
        conninfo,
        run_id=run_id,
        tenant_id=tenant_id,
        workflow_id=workflow_id,
        run_start_time=run_start_time,
        run_finish_time=None,
        value_rows=value_rows,
    )
    print(f"   ✓ Inserted run {run_id}")
    print(f"   ✓ Inserted {len(value_rows)} time series values")
    
    # Step 3: Read the data back
    print("\n3. Reading data back...")
    df = read.read_values_between(
        conninfo,
        tenant_id=tenant_id,
        start_valid=base_time,
        end_valid=base_time + timedelta(hours=24),
        mode="flat",
    )
    
    print(f"   ✓ Retrieved {len(df)} values")
    print("\n   First 5 values:")
    print(df.head())
    
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

