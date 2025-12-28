import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timezone
from typing import Optional

load_dotenv()

def read_values_between(
    conninfo: str,
    *,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_run: Optional[datetime] = None,
    end_run: Optional[datetime] = None,
) -> pd.DataFrame:
    sql = """
    SELECT
      r.run_start_time AS run_time,
      v.valid_time,
      v.run_id,
      v.series_key,
      v.value,
      v.comment,
      v.tags,
      v.changed_by,
      v.change_time
    FROM runs_table r
    JOIN values_table v
      ON v.run_id = r.run_id
    WHERE v.is_current = true
      AND (%(start_valid)s IS NULL OR v.valid_time >= %(start_valid)s)
      AND (%(end_valid)s   IS NULL OR v.valid_time <  %(end_valid)s)
      AND (%(start_run)s   IS NULL OR r.run_start_time >= %(start_run)s)
      AND (%(end_run)s     IS NULL OR r.run_start_time <  %(end_run)s)
    ORDER BY r.run_start_time, v.valid_time;
    """

    params = {
        "start_valid": start_valid,
        "end_valid": end_valid,
        "start_run": start_run,
        "end_run": end_run,
    }

    engine = create_engine(conninfo)
    df = pd.read_sql_query(sql, engine, params=params)

    # Ensure timezone-aware pandas datetimes
    df["run_time"] = pd.to_datetime(df["run_time"], utc=True)
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)

    df = df.set_index(["run_time", "valid_time"]).sort_index()
    return df


if __name__ == "__main__":
    conninfo = os.environ["NEON_PG_URL"]

    # Example 1: filter only on valid_time (same as your current behavior)
    df1 = read_values(
        conninfo,
        start_valid=datetime(2025, 12, 25, 0, 0, tzinfo=timezone.utc),
        end_valid=datetime(2025, 12, 28, 0, 0, tzinfo=timezone.utc),
    )

    # Example 2: filter only on run_time
    df2 = read_values(
        conninfo,
        start_run=datetime(2025, 12, 26, 0, 0, tzinfo=timezone.utc),
        end_run=datetime(2025, 12, 27, 0, 0, tzinfo=timezone.utc),
    )

    # Example 3: filter on both
    df3 = read_values(
        conninfo,
        start_valid=datetime(2025, 12, 25, 0, 0, tzinfo=timezone.utc),
        end_valid=datetime(2025, 12, 28, 0, 0, tzinfo=timezone.utc),
        start_run=datetime(2025, 12, 26, 0, 0, tzinfo=timezone.utc),
        end_run=datetime(2025, 12, 27, 0, 0, tzinfo=timezone.utc),
    )

    print(df1.head())
