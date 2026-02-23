"""Tests for pint unit support: insert conversion and read as_pint."""
import pytest
import pandas as pd
import pint
import pint_pandas
from datetime import datetime, timezone, timedelta

from timedb.sdk import IncompatibleUnitError, _resolve_pint_values


# =============================================================================
# Unit tests for _resolve_pint_values (no DB needed)
# =============================================================================

class TestResolvePintValues:
    """Test the _resolve_pint_values function in isolation."""

    def test_plain_float_passthrough(self):
        """Plain float64 Series passes through unchanged."""
        s = pd.Series([1.0, 2.0, 3.0], name="value")
        result = _resolve_pint_values(s, "MW")
        pd.testing.assert_series_equal(result, s)

    def test_same_unit_strips_pint(self):
        """Pint array with same unit as series is stripped to plain float64."""
        s = pd.Series(pd.array([1.0, 2.0, 3.0], dtype="pint[MW]"), name="value")
        result = _resolve_pint_values(s, "MW")
        assert not hasattr(result.dtype, 'units')
        assert list(result) == [1.0, 2.0, 3.0]

    def test_compatible_unit_converts(self):
        """Pint array with compatible unit is converted (kW -> MW)."""
        s = pd.Series(pd.array([500.0, 1000.0], dtype="pint[kW]"), name="value")
        result = _resolve_pint_values(s, "MW")
        assert not hasattr(result.dtype, 'units')
        assert result.iloc[0] == pytest.approx(0.5)
        assert result.iloc[1] == pytest.approx(1.0)

    def test_dimensionless_treated_as_series_unit(self):
        """Pint dimensionless is treated as series unit, stripped to float64."""
        s = pd.Series(pd.array([42.0], dtype="pint[dimensionless]"), name="value")
        result = _resolve_pint_values(s, "MW")
        assert not hasattr(result.dtype, 'units')
        assert result.iloc[0] == 42.0

    def test_incompatible_unit_raises(self):
        """Pint array with incompatible unit raises IncompatibleUnitError."""
        s = pd.Series(pd.array([10.0], dtype="pint[kg]"), name="value")
        with pytest.raises(IncompatibleUnitError, match="Cannot convert"):
            _resolve_pint_values(s, "MW")

    def test_preserves_index(self):
        """Conversion preserves the original Series index."""
        idx = pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC")
        s = pd.Series(pd.array([500.0, 1000.0, 1500.0], dtype="pint[kW]"), index=idx, name="value")
        result = _resolve_pint_values(s, "MW")
        assert list(result.index) == list(idx)


# =============================================================================
# Integration tests (require DB)
# =============================================================================

def test_insert_pint_same_unit(td, sample_datetime):
    """Insert pint array with same unit as series — stored as plain float."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": pd.array([1.5, 2.5], dtype="pint[MW]"),
    })
    td.get_series("power").insert(df)

    df_read = td.get_series("power").read()
    assert df_read["value"].iloc[0] == pytest.approx(1.5)
    assert df_read["value"].iloc[1] == pytest.approx(2.5)


def test_insert_pint_converts_kw_to_mw(td, sample_datetime):
    """Insert kW into MW series — auto-converted."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": pd.array([500.0], dtype="pint[kW]"),
    })
    td.get_series("power").insert(df)

    df_read = td.get_series("power").read()
    assert df_read["value"].iloc[0] == pytest.approx(0.5)


def test_insert_pint_incompatible_raises(td, sample_datetime):
    """Insert incompatible unit raises IncompatibleUnitError."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": pd.array([10.0], dtype="pint[kg]"),
    })

    with pytest.raises(IncompatibleUnitError):
        td.get_series("power").insert(df)


def test_insert_plain_float_no_unit_check(td, sample_datetime):
    """Insert plain float64 — no unit check, backward compatible."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [999.0],
    })
    td.get_series("power").insert(df)

    df_read = td.get_series("power").read()
    assert df_read["value"].iloc[0] == pytest.approx(999.0)


def test_read_as_pint(td, sample_datetime):
    """Read with as_pint=True returns pint dtype column."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [1.5, 2.5],
    })
    td.get_series("power").insert(df)

    df_pint = td.get_series("power").read(as_pint=True)
    assert hasattr(df_pint["value"].dtype, 'units')
    assert str(df_pint["value"].dtype.units) == "megawatt"
    assert df_pint["value"].values.quantity.magnitude[0] == pytest.approx(1.5)


def test_read_as_pint_false_default(td, sample_datetime):
    """Read without as_pint returns plain float64 (default)."""
    td.create_series(name="power", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [1.5],
    })
    td.get_series("power").insert(df)

    df_read = td.get_series("power").read()
    assert not hasattr(df_read["value"].dtype, 'units')
    assert df_read["value"].dtype == "float64"
