"""
Insert pipeline: single-series pure data-transformation engine.

Converts user-facing input (pd.DataFrame, pl.DataFrame, or TimeSeries) into a
fully-decorated ``pa.Table`` ready for the DB layer.  No database calls, no
SDK-level concerns.

All column decoration (run_id, series_id, knowledge_time) happens in a single
Polars ``with_columns()`` Rust pass before ``to_arrow()``.  Zero PyArrow column
appends.
"""
from datetime import datetime, timezone
from typing import Optional, Union

import pandas as pd
import polars as pl
import pyarrow as pa

from timedatamodel import TimeSeries
from .pipeline_utils import _resolve_polars_units, _to_arrow


# ── Validation helpers ───────────────────────────────────────────────────────

def _validate_df_columns(df: pd.DataFrame) -> None:
    """Validate DataFrame column structure.

    Accepted column sets:

    - ``[valid_time, value]``
    - ``[valid_time, valid_time_end, value]``
    - ``[knowledge_time, valid_time, value]``
    - ``[knowledge_time, valid_time, valid_time_end, value]``
    """
    cols = list(df.columns)
    valid_sets = [
        ['valid_time', 'value'],
        ['valid_time', 'valid_time_end', 'value'],
        ['knowledge_time', 'valid_time', 'value'],
        ['knowledge_time', 'valid_time', 'valid_time_end', 'value'],
    ]
    if cols not in valid_sets:
        raise ValueError(
            f"DataFrame columns must be one of: {valid_sets}. "
            f"Found {len(cols)} columns: {cols}"
        )


# ── Core pipeline ────────────────────────────────────────────────────────────

def _input_to_polars(
    data: Union[pd.DataFrame, pl.DataFrame, TimeSeries],
    series_unit: str,
    data_unit: Optional[str],
) -> pl.DataFrame:
    """Convert any insert input to a plain Polars DataFrame with unit conversion applied.

    - :class:`~timedatamodel.TimeSeries`: validated via ``validate_for_insert()``,
      unit converted via pint.
    - ``pd.DataFrame`` or ``pl.DataFrame``: column format validated, converted to Polars
      (pandas only), unit converted if *data_unit* is provided.

    Returns a Polars DataFrame containing only the data columns
    (``valid_time``, optionally ``valid_time_end`` / ``knowledge_time``, ``value``).
    No metadata columns (``run_id``, ``series_id``) are added here.
    """
    if isinstance(data, TimeSeries):
        pl_df, _ = data.validate_for_insert()
        return _resolve_polars_units(pl_df, ts_unit=data.unit, series_unit=series_unit)

    # Convert to Polars once, then do all validation on the uniform type
    if isinstance(data, pd.DataFrame):
        pl_df = pl.from_pandas(data)
    elif isinstance(data, pl.DataFrame):
        pl_df = data
    else:
        raise TypeError(
            f"Expected pd.DataFrame, pl.DataFrame, or TimeSeries, got {type(data).__name__}"
        )

    _validate_df_columns(pl_df)  # list(df.columns) works on both pandas and Polars
    for col in ["valid_time", "valid_time_end", "knowledge_time"]:
        if col not in pl_df.columns:
            continue
        dtype = pl_df.schema[col]
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
            raise ValueError(f"'{col}' must be timezone-aware. Found timezone-naive datetime.")
    if data_unit is not None:
        pl_df = _resolve_polars_units(pl_df, ts_unit=data_unit, series_unit=series_unit)
    return pl_df


def normalize_insert_input(
    data: Union[pd.DataFrame, pl.DataFrame, TimeSeries],
    series_unit: str,
    *,
    series_id: int,
    run_id,
    knowledge_time: Optional[datetime] = None,
    data_unit: Optional[str] = None,
) -> pa.Table:
    """Convert DataFrame or TimeSeries insert input to a fully-decorated ``pa.Table``.

    All column decoration (``run_id``, ``series_id``, ``knowledge_time``) is
    performed in a single Polars ``with_columns()`` Rust pass before
    ``to_arrow()``.  Zero PyArrow column appends.

    Args:
        data: Input data — ``pd.DataFrame`` or :class:`~timedatamodel.TimeSeries`.
        series_unit: Canonical unit of the target series.
        series_id: Integer series ID to stamp into every row.
        run_id: UUID run identifier to stamp into every row.
        knowledge_time: Broadcast knowledge_time for all rows.  Mutually
            exclusive with a ``knowledge_time`` column already present in
            *data*.  Defaults to ``now(UTC)`` when neither is provided.
        data_unit: Unit of the incoming ``pd.DataFrame`` values.  When
            provided, values are converted from *data_unit* to *series_unit*
            before insert.  Only applies to the DataFrame path; ignored for
            :class:`~timedatamodel.TimeSeries` (which carries its own unit).

    Returns:
        ``pa.Table`` with columns ``[run_id, series_id, valid_time,
        (valid_time_end,) value, knowledge_time]`` ready for insert into ClickHouse.

    Raises:
        ValueError: If *knowledge_time* kwarg and a ``knowledge_time`` column
            are both present in *data*.
        IncompatibleUnitError: If *data_unit* is not convertible to *series_unit*.
    """
    # 1. Normalise any input type to a plain Polars DataFrame
    pl_df = _input_to_polars(data, series_unit, data_unit)

    # 2. Inspect columns now that we have a uniform Polars df
    source_has_kt = "knowledge_time" in pl_df.columns
    source_has_vte = "valid_time_end" in pl_df.columns
    if source_has_kt and knowledge_time is not None:
        raise ValueError(
            "Ambiguous knowledge_time: the data already contains a 'knowledge_time' column "
            "and knowledge_time was also passed as a keyword argument. "
            "Remove the kwarg or the column."
        )

    final_cols = ["run_id", "series_id", "valid_time", "value", "knowledge_time"]
    if source_has_vte:
        final_cols.insert(3, "valid_time_end")

    # 3. Prepare the knowledge_time literal (if needed) — set dtype at construction
    kt_col = None
    if not source_has_kt:
        kt = knowledge_time if knowledge_time is not None else datetime.now(timezone.utc)
        kt_col = pl.lit(kt, dtype=pl.Datetime("us", "UTC")).alias("knowledge_time")

    # 4. Single Rust pass: stamp run_id, series_id, fill NaN sentinels, (knowledge_time)
    cols_to_add = [
        pl.lit(str(run_id)).alias("run_id"),
        pl.lit(series_id, dtype=pl.Int64).alias("series_id"),
        pl.col("value").fill_null(float("nan")),
    ]
    if kt_col is not None:
        cols_to_add.append(kt_col)
    pl_df = pl_df.with_columns(cols_to_add)

    # 5. Reject duplicate valid_times for flat (non-versioned) series
    if not source_has_kt and pl_df.select(pl.col("valid_time").is_duplicated().any()).item():
        raise ValueError(
            "Cannot insert duplicate valid_time(s) into a flat series. "
            "Deduplicate your data before calling insert()."
        )

    # 6. Convert to Arrow
    return _to_arrow(pl_df, final_cols)
