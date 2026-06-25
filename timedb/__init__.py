"""timedb — minimal stateless ClickHouse library for 3-dimensional time series.

Usage:

    from timedb import TimeDBClient
    td = TimeDBClient()           # reads TIMEDB_CH_URL
    td.create()
    td.write(df, retention="medium")
    td.read(series_ids=[1, 2], retention="medium")
"""

from dotenv import load_dotenv

# Load only the .env in the current working directory (no upward tree-walk).
# override=False so an already-set env var (Docker, CI, uv run --env-file, a
# caller that loaded its own .env first) always wins over this dev fallback.
load_dotenv(".env")

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
