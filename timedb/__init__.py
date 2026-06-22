"""timedb — minimal stateless ClickHouse library for 3-dimensional time series.

Usage:

    from timedb import TimeDBClient
    td = TimeDBClient()           # reads TIMEDB_CH_URL
    td.create()
    td.write(df, retention="medium")
    td.read(series_ids=[1, 2], retention="medium")
"""

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from . import profiling  # noqa: E402
from .client import TimeDBClient  # noqa: E402
from .write import RETENTION_TIERS, UnchangedScope, WriteResult  # noqa: E402

__all__ = [
    "RETENTION_TIERS",
    "TimeDBClient",
    "UnchangedScope",
    "WriteResult",
    "profiling",
]
