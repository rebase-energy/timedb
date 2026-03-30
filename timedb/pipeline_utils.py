"""
Shared utilities for insert, write, and read pipelines.

Pure data-transformation helpers — no database calls, no SDK-level concerns.
"""
import warnings
from functools import lru_cache
from typing import List, Optional

import polars as pl
import pyarrow as pa

from .types import IncompatibleUnitError


# Optional columns that are passed through to the DB if present in the input.
OPTIONAL_PASSTHROUGH = ["valid_time_end", "change_time", "changed_by", "annotation"]


@lru_cache(maxsize=256)
def _compute_unit_factor(ts_unit: str, series_unit: str) -> Optional[float]:
    """Return the pint conversion factor from *ts_unit* to *series_unit*, or ``None``.

    Returns ``None`` when no multiplication is needed:
    - Same unit → no-op.
    - ``ts_unit == "dimensionless"`` → no-op (caller handles the warning).

    Raises :class:`IncompatibleUnitError` if the units are not dimensionally compatible.
    Raises :class:`ImportError` if pint is not installed.
    """
    if ts_unit == series_unit or ts_unit == "dimensionless":
        return None
    try:
        import pint
    except ImportError:
        raise ImportError(
            "pint is required for automatic unit conversion. "
            "Install with: pip install pint"
        )
    ureg = pint.application_registry.get()
    try:
        return float(ureg.Quantity(1, ts_unit).to(series_unit).magnitude)
    except pint.DimensionalityError:
        raise IncompatibleUnitError(
            f"Cannot convert '{ts_unit}' to '{series_unit}'. "
            f"Units are not dimensionally compatible."
        )


def _resolve_polars_units(df: pl.DataFrame, ts_unit: str, series_unit: str) -> pl.DataFrame:
    """Scale the ``value`` column when *ts_unit* and *series_unit* differ.

    Rules:
    - Same unit → return unchanged.
    - ``ts_unit == "dimensionless"`` → return unchanged, warn if *series_unit* is not dimensionless.
    - Compatible units → multiply ``value`` by the pint conversion factor.
    - Incompatible units → raise :class:`IncompatibleUnitError`.
    """
    if ts_unit == series_unit:
        return df
    if ts_unit == "dimensionless":
        if series_unit != "dimensionless":
            warnings.warn(
                f"Inserting a dimensionless TimeSeries into series with unit "
                f"'{series_unit}'. Values will be stored as-is without conversion. "
                f"Set the unit on your TimeSeries to enable automatic conversion.",
                UserWarning,
                stacklevel=4,
            )
        return df
    factor = _compute_unit_factor(ts_unit, series_unit)
    return df.with_columns(pl.col("value") * factor)


def _to_arrow(df: pl.DataFrame, final_cols: List[str]) -> pa.Table:
    """Select final columns and convert to a chunked Arrow table."""
    return df.select(final_cols).to_arrow().combine_chunks()
