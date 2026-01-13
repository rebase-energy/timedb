import timedb as td
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

# Delete database schema
td.delete()

# Create database schema
td.create()

# Create first time series: Temperature data (first 24 hours: 0-23)
base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
dates1 = [base_time + timedelta(hours=i) for i in range(24)]
df1 = pd.DataFrame({
    'valid_time': dates1,
    'value': [20.0 + i * 0.3 for i in range(24)]  # Temperature rising
})

# Insert first time series (series_id will be auto-generated)
result1 = td.insert_run(df=df1)
print(result1)
print(f"✓ Inserted first time series with series_id: {result1.series_ids}")
print(f"  Time range: {dates1[0]} to {dates1[-1]}")

# Create second time series: Humidity data (next 24 hours: 24-47, following the first)
dates2 = [base_time + timedelta(hours=i) for i in range(24, 48)]  # Hours 24-47
df2 = pd.DataFrame({
    'valid_time': dates2,
    'value': [30.0 - (i-24) * 0.5 for i in range(24, 48)]  # Humidity decreasing
})

# Insert second time series (series_id will be auto-generated, different from first)
result2 = td.insert_run(df=df2)
print(f"✓ Inserted second time series with series_id: {result2.series_ids}")
print(f"  Time range: {dates2[0]} to {dates2[-1]}")

