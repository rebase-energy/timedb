"""Tests for pipeline_utils — shared pure utilities."""
import warnings

import polars as pl
import pyarrow as pa
import pytest

from timedb.pipeline_utils import _compute_unit_factor, _resolve_polars_units, _to_arrow
from timedb.types import IncompatibleUnitError


# ---------------------------------------------------------------------------
# _compute_unit_factor
# ---------------------------------------------------------------------------

def test_compute_unit_factor_same_unit():
    assert _compute_unit_factor("MW", "MW") is None


def test_compute_unit_factor_dimensionless():
    assert _compute_unit_factor("dimensionless", "MW") is None


def test_compute_unit_factor_compatible():
    factor = _compute_unit_factor("kW", "MW")
    assert factor == pytest.approx(0.001)


def test_compute_unit_factor_compatible_reverse():
    factor = _compute_unit_factor("MW", "kW")
    assert factor == pytest.approx(1000.0)


def test_compute_unit_factor_incompatible():
    with pytest.raises(IncompatibleUnitError, match="Cannot convert"):
        _compute_unit_factor("kg", "MW")


# ---------------------------------------------------------------------------
# _resolve_polars_units
# ---------------------------------------------------------------------------

def test_resolve_polars_units_same():
    df = pl.DataFrame({"value": [1.0, 2.0, 3.0]})
    result = _resolve_polars_units(df, "MW", "MW")
    assert result["value"].to_list() == [1.0, 2.0, 3.0]


def test_resolve_polars_units_dimensionless_warning():
    df = pl.DataFrame({"value": [1.0, 2.0]})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = _resolve_polars_units(df, "dimensionless", "MW")
        assert len(w) == 1
        assert "dimensionless" in str(w[0].message)
    assert result["value"].to_list() == [1.0, 2.0]


def test_resolve_polars_units_dimensionless_to_dimensionless_no_warning():
    df = pl.DataFrame({"value": [1.0]})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = _resolve_polars_units(df, "dimensionless", "dimensionless")
        assert len(w) == 0
    assert result["value"].to_list() == [1.0]


def test_resolve_polars_units_conversion():
    df = pl.DataFrame({"value": [1000.0, 2000.0]})
    result = _resolve_polars_units(df, "kW", "MW")
    assert result["value"].to_list() == pytest.approx([1.0, 2.0])


# ---------------------------------------------------------------------------
# _to_arrow
# ---------------------------------------------------------------------------

def test_to_arrow_selects_columns():
    df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
    table = _to_arrow(df, ["a", "c"])
    assert table.column_names == ["a", "c"]
    assert table.num_rows == 1


def test_to_arrow_combine_chunks():
    df = pl.DataFrame({"x": [1, 2, 3]})
    table = _to_arrow(df, ["x"])
    # combine_chunks ensures single chunk per column
    assert table.column("x").num_chunks == 1
