"""Unit tests for the TimeSeries class (no DB required)."""
import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
import pyarrow as pa

from timedb import TimeSeries, DataShape

_TS_TYPE = pa.timestamp("us", tz="UTC")

_UTC = timezone.utc
_T0 = datetime(2025, 1, 1, tzinfo=_UTC)


# =============================================================================
# from_pandas shape restriction
# =============================================================================

def test_from_pandas_simple():
    """from_pandas accepts a plain valid_time+value DataFrame (SIMPLE)."""
    df = pd.DataFrame({
        "valid_time": [_T0, _T0 + timedelta(hours=1)],
        "value": [1.0, 2.0],
    })
    ts = TimeSeries.from_pandas(df)
    assert ts.shape == DataShape.SIMPLE


def test_from_pandas_versioned():
    """from_pandas accepts a DataFrame with knowledge_time (VERSIONED)."""
    df = pd.DataFrame({
        "knowledge_time": [_T0],
        "valid_time": [_T0 + timedelta(hours=1)],
        "value": [1.0],
    })
    ts = TimeSeries.from_pandas(df)
    assert ts.shape == DataShape.VERSIONED


def test_from_pandas_rejects_audit():
    """from_pandas raises ValueError when change_time is present (AUDIT shape)."""
    df = pd.DataFrame({
        "knowledge_time": [_T0],
        "change_time":    [_T0],
        "valid_time":     [_T0 + timedelta(hours=1)],
        "value":          [1.0],
    })
    with pytest.raises(ValueError, match="AUDIT"):
        TimeSeries.from_pandas(df)


def test_from_pandas_rejects_corrected():
    """from_pandas raises ValueError when change_time but no knowledge_time (CORRECTED shape)."""
    df = pd.DataFrame({
        "valid_time":  [_T0],
        "change_time": [_T0],
        "value":       [1.0],
    })
    with pytest.raises(ValueError, match="CORRECTED"):
        TimeSeries.from_pandas(df)


def test_from_pandas_error_mentions_from_arrow():
    """The ValueError message directs users to from_arrow."""
    df = pd.DataFrame({
        "valid_time":  [_T0],
        "change_time": [_T0],
        "value":       [1.0],
    })
    with pytest.raises(ValueError, match="from_arrow"):
        TimeSeries.from_pandas(df)


def test_from_arrow_still_accepts_audit():
    """from_arrow remains unrestricted and can wrap AUDIT read results."""
    table = pa.table({
        "knowledge_time": pa.array([_T0], type=_TS_TYPE),
        "change_time":    pa.array([_T0], type=_TS_TYPE),
        "valid_time":     pa.array([_T0 + timedelta(hours=1)], type=_TS_TYPE),
        "value":          pa.array([1.0], type=pa.float64()),
    })
    ts = TimeSeries.from_arrow(table)
    assert ts.shape == DataShape.AUDIT
