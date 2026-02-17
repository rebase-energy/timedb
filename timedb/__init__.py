"""
TimeDB - A time series database for PostgreSQL with TimescaleDB.

Quick usage:
    import timedb as tb

    tb.create()
    tb.create_series(name='wind_power', unit='MW',
                     labels={'site': 'offshore_1'}, overlapping=True)

    tb.series('wind_power').where(site='offshore_1').insert(df=df, known_time=known_time)
    df = tb.series('wind_power').where(site='offshore_1').read()

Explicit client usage (for custom connection settings):
    from timedb import TimeDataClient

    td = TimeDataClient(conninfo='postgresql://...', min_size=4, max_size=20)
    td.series('wind_power').read()
"""

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from .sdk import (
    TimeDataClient,
    SeriesCollection,
    InsertResult,
    IncompatibleUnitError,
)

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


def create(retention=None, *, retention_short="6 months",
           retention_medium="3 years", retention_long="5 years"):
    """Create database schema. See :meth:`TimeDataClient.create`."""
    return _get_default_client().create(
        retention, retention_short=retention_short,
        retention_medium=retention_medium, retention_long=retention_long,
    )


def delete():
    """Delete database schema. See :meth:`TimeDataClient.delete`."""
    return _get_default_client().delete()


def create_series(name, unit="dimensionless", labels=None, description=None,
                  overlapping=False, retention="medium"):
    """Create a new time series. See :meth:`TimeDataClient.create_series`."""
    return _get_default_client().create_series(
        name, unit=unit, labels=labels, description=description,
        overlapping=overlapping, retention=retention,
    )


def series(name=None, unit=None, series_id=None):
    """Start building a series collection. See :meth:`TimeDataClient.series`."""
    return _get_default_client().series(name, unit=unit, series_id=series_id)


__all__ = [
    'TimeDataClient',
    'SeriesCollection',
    'InsertResult',
    'IncompatibleUnitError',
    'create',
    'delete',
    'create_series',
    'series',
]
