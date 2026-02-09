"""
TimeDB - A time series database for PostgreSQL with TimescaleDB.

High-level SDK usage:
    from timedb import TimeDataClient
    import pandas as pd
    from datetime import datetime, timezone, timedelta

    # Create client and schema
    td = TimeDataClient()
    td.create()

    # Create a series
    series_id = td.create_series(
        name='wind_power',
        unit='MW',
        labels={'site': 'offshore_1', 'type': 'forecast'}
    )

    # Insert data using the fluent API
    df = pd.DataFrame({
        'valid_time': times,
        'wind_power': values
    })
    result = td.series('wind_power').where(site='offshore_1', type='forecast').insert(
        df=df,
        known_time=datetime.now(timezone.utc)
    )

    # Read latest values
    df_latest = td.series('wind_power').where(site='offshore_1').read()

    # Read all forecast revisions
    df_all = td.series('wind_power').where(site='offshore_1').read(versions=True)
"""

from .sdk import (
    TimeDataClient,
    SeriesCollection,
    InsertResult,
)
from .units import IncompatibleUnitError

__all__ = [
    'TimeDataClient',
    'SeriesCollection',
    'InsertResult',
    'IncompatibleUnitError',
]
