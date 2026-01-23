"""
TimeDB - A time series database for PostgreSQL.

High-level SDK usage:
    import timedb as td
    import pint
    ureg = pint.UnitRegistry()
    
    # Create schema
    td.create()
    
    # Insert run with DataFrame containing Pint Quantity columns
    # Each column (except time columns) becomes a separate series
    df = pd.DataFrame({
        "valid_time": times,
        "power": power_vals_kW * ureg.kW,              # Series with kW unit
        "wind_speed": wind_vals_m_s * (ureg.meter / ureg.second),  # Series with m/s unit
        "temperature": temp_vals_C * ureg.degC          # Series with degC unit
    })
    
    result = td.insert_run(df=df)
    # result.series_ids = {
    #     'power': <uuid>,
    #     'wind_speed': <uuid>,
    #     'temperature': <uuid>
    # }
    
    # With custom series keys
    result = td.insert_run(
        df=df,
        series_key_overrides={
            'power': 'wind_power_forecast',
            'wind_speed': 'wind_speed_measured'
        }
    )
"""

from .sdk import create, delete, start_api, start_api_background, check_api, insert_run, read, read_values_flat, read_values_overlapping, update_records, create_series, get_or_create_series, get_mapping, InsertResult, DEFAULT_TENANT_ID
from .units import IncompatibleUnitError

__all__ = ['create', 'delete', 'start_api', 'start_api_background', 'check_api', 'insert_run', 'read', 'read_values_flat', 'read_values_overlapping', 'update_records', 'create_series', 'get_or_create_series', 'get_mapping', 'InsertResult', 'DEFAULT_TENANT_ID', 'IncompatibleUnitError']

