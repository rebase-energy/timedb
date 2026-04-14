"""
TimeDB - A time series database backed by PostgreSQL + ClickHouse.

Quick usage:
    import timedb as td

    td.create()
    td.create_series('wind_power', unit='MW',
                     labels={'site': 'offshore_1'}, overlapping=True)

    td.get_series('wind_power').where(site='offshore_1').insert(df=df, knowledge_time=knowledge_time)
    df = td.get_series('wind_power').where(site='offshore_1').read()

Explicit client usage (for custom connection settings):
    from timedb import TimeDataClient

    td = TimeDataClient(pg_conninfo='postgresql://...', ch_url='clickhouse://...')
    td.get_series('wind_power').read()

Environment variables:
    TIMEDB_PG_DSN  - PostgreSQL connection string (series)
    TIMEDB_CH_URL  - ClickHouse DSN (runs + all values tables)
"""

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from .sdk import (
    TimeDataClient,
    SeriesCollection,
    InsertResult,
    IncompatibleUnitError,
)

from timedatamodel import TimeSeries, DataShape, DataType, TimeSeriesType, GeoLocation

from . import profiling

# ---------------------------------------------------------------------------
# Lazy default client & module-level convenience functions
# ---------------------------------------------------------------------------

_default_client = None


def _get_default_client():
    """Get or create the default TimeDataClient (lazy singleton)."""
    global _default_client
    if _default_client is None:
        _default_client = TimeDataClient()
    return _default_client


def create():
    """Create database schema. See :meth:`TimeDataClient.create`."""
    return _get_default_client().create()


def delete():
    """Delete database schema. See :meth:`TimeDataClient.delete`."""
    return _get_default_client().delete()


def create_series(name, unit="dimensionless", labels=None, description=None,
                  overlapping=False, retention="medium"):
    """Get-or-create a single series. See :meth:`TimeDataClient.create_series`."""
    return _get_default_client().create_series(
        name, unit=unit, labels=labels, description=description,
        overlapping=overlapping, retention=retention,
    )


def create_series_many(series):
    """Bulk get-or-create multiple series. See :meth:`TimeDataClient.create_series_many`."""
    return _get_default_client().create_series_many(series)


def get_series(name=None, unit=None, series_id=None):
    """Start building a series collection. See :meth:`TimeDataClient.get_series`."""
    return _get_default_client().get_series(name, unit=unit, series_id=series_id)


def write(df, name_col="name", label_cols=None, run_cols=None, *, knowledge_time=None,
          unit=None, workflow_id=None, run_start_time=None, run_finish_time=None,
          run_params=None):
    """Insert multi-series long-format data. See :meth:`TimeDataClient.write`."""
    return _get_default_client().write(
        df, name_col, label_cols, run_cols,
        knowledge_time=knowledge_time, unit=unit, workflow_id=workflow_id,
        run_start_time=run_start_time, run_finish_time=run_finish_time,
        run_params=run_params,
    )


def read(manifest, name_col=None, label_cols=None, series_col=None, **kwargs):
    """Read multi-series data via manifest. See :meth:`TimeDataClient.read`."""
    return _get_default_client().read(
        manifest, name_col=name_col, label_cols=label_cols,
        series_col=series_col, **kwargs,
    )


def read_relative(manifest, name_col=None, label_cols=None, series_col=None, **kwargs):
    """Read multi-series relative data via manifest. See :meth:`TimeDataClient.read_relative`."""
    return _get_default_client().read_relative(
        manifest, name_col=name_col, label_cols=label_cols,
        series_col=series_col, **kwargs,
    )


__all__ = [
    # SDK
    'TimeDataClient',
    'SeriesCollection',
    'InsertResult',
    'IncompatibleUnitError',
    'create',
    'delete',
    'create_series',
    'create_series_many',
    'get_series',
    'write',
    'read',
    'read_relative',
    # Profiling
    'profiling',
    # TimeSeries container
    'TimeSeries',
    'DataShape',
    'DataType',
    'TimeSeriesType',
    'GeoLocation',
]
