"""
Insert pipeline: pure data-transformation engine.

Converts user-facing input (pd.DataFrame or TimeSeries) into a fully-decorated
``pa.Table`` ready for the DB layer.  No database calls, no SDK-level concerns.

All column decoration (batch_id, series_id, knowledge_time) happens in a single
Polars ``with_columns()`` Rust pass before ``to_arrow()``.  Zero PyArrow column
appends.
"""
import warnings
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa

from timedatamodel import TimeSeries, DataShape
from .types import IncompatibleUnitError


# ── Validation helpers (operate on pd.DataFrame) ─────────────────────────────

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


def _check_df_timezone(df: pd.DataFrame) -> None:
    """Raise ValueError if any timestamp column contains timezone-naive datetimes."""
    for col in ['knowledge_time', 'valid_time', 'valid_time_end']:
        if col not in df.columns:
            continue
        s = df[col]
        if hasattr(s.dtype, 'tz'):
            if s.dtype.tz is None:
                raise ValueError(
                    f"{col} must be timezone-aware. Found timezone-naive datetime."
                )
        elif len(s) > 0:
            first = s.iloc[0]
            if isinstance(first, (pd.Timestamp, datetime)) and first.tzinfo is None:
                raise ValueError(
                    f"{col} must be timezone-aware. Found timezone-naive datetime."
                )


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


# ── Multi-series pipeline ─────────────────────────────────────────────────────

# Optional columns that are passed through to the DB if present in the input.
# All other columns are dropped after routing columns are removed.
_OPTIONAL_PASSTHROUGH = ["valid_time_end", "change_time", "changed_by", "annotation"]


def normalize_write_input(
    pl_df: pl.DataFrame,
    name_col: str,
    label_cols: List[str],
    mapping_df: pl.DataFrame,
    *,
    batch_id: Optional[str] = None,
    batch_mapping: Optional[pl.DataFrame] = None,
    batch_cols: Optional[List[str]] = None,
    knowledge_time: Optional[datetime] = None,
) -> Dict[str, Tuple[pa.Table, Dict[str, Any]]]:
    """Convert a multi-series Polars DataFrame to partitioned Arrow tables.

    *pl_df* must already be a Polars DataFrame (caller converts from Pandas if
    needed).  *mapping_df* must be a flat Polars DataFrame with columns
    ``[name_col, *label_cols, _series_id, _unit, _factor, _target_table,
    _overlapping, _retention]`` — one row per unique series identity.

    Unit conversion is applied vectorized via a join.  All
    :class:`IncompatibleUnitError` errors are raised **before** any DB
    interaction.

    Optional audit/interval columns (``valid_time_end``, ``change_time``,
    ``changed_by``, ``annotation``) are passed through if present in *pl_df*,
    enabling historical data migration without loss of audit trail.

    Args:
        pl_df: Input data as a Polars DataFrame.  Must contain ``valid_time``
            and ``value`` columns plus *name_col* and *label_cols*.
        name_col: Column in *pl_df* that maps to ``series_table.name``.
        label_cols: Columns in *pl_df* that map to ``series_table.labels``.
        mapping_df: Flat routing table with columns
            ``[name_col, *label_cols, _series_id, _unit, _factor,
            _target_table, _overlapping, _retention]``.
        batch_id: UUID batch identifier stamped into every row (single-batch
            path, mutually exclusive with *batch_mapping*).
        batch_mapping: Polars DataFrame with columns ``[*batch_cols, "_batch_id"]``
            for per-row batch stamping (multi-batch path, mutually exclusive
            with *batch_id*).
        batch_cols: Columns in *pl_df* used as the join key for *batch_mapping*.
            Required when *batch_mapping* is provided.
        knowledge_time: Broadcast knowledge_time for all rows (mutually
            exclusive with a ``knowledge_time`` column in *pl_df*).

    Returns:
        ``{target_table: (arrow_table, routing_dict)}`` — one entry per unique
        routing destination.

    Raises:
        ValueError: If ``valid_time`` or ``value`` are missing.
        ValueError: If both a ``knowledge_time`` column and *knowledge_time*
            kwarg are provided.
        ValueError: If any datetime column is timezone-naive.
    """
    routing_cols = {name_col} | set(label_cols)
    data_col_names = [c for c in pl_df.columns if c not in routing_cols]

    # ── Validate required columns ─────────────────────────────────────────────
    if "valid_time" not in data_col_names:
        raise ValueError("DataFrame must contain a 'valid_time' column.")
    if "value" not in data_col_names:
        raise ValueError("DataFrame must contain a 'value' column.")

    source_has_kt = "knowledge_time" in data_col_names
    if source_has_kt and knowledge_time is not None:
        raise ValueError(
            "Ambiguous knowledge_time: the data already contains a 'knowledge_time' column "
            "and knowledge_time was also passed as a keyword argument. "
            "Remove the kwarg or the column."
        )

    # ── Polars-native timezone validation ─────────────────────────────────────
    for col in ["valid_time", "valid_time_end", "knowledge_time"]:
        if col not in pl_df.columns:
            continue
        dtype = pl_df.schema[col]
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
            raise ValueError(
                f"'{col}' must be timezone-aware. Found timezone-naive datetime."
            )

    # ── Type-safe join: cast mapping_df routing cols to match pl_df schema ────
    # Include "unit" in the join key when it's present — this supports per-row
    # unit conversion where mapping_df has one row per (series, unit) combo.
    join_on = [name_col] + label_cols
    if "unit" in pl_df.columns:
        join_on = join_on + ["unit"]
    cast_schema = {col: pl_df.schema[col] for col in join_on if col in pl_df.schema}
    safe_mapping = mapping_df.cast(cast_schema)

    # Vectorized join: stamp _series_id, _factor, _target_table, routing meta
    joined = pl_df.join(safe_mapping, on=join_on, how="left")

    # ── Multi-batch path: join batch_mapping to stamp per-row batch_id ────────
    if batch_mapping is not None and batch_cols:
        joined = joined.join(batch_mapping, on=batch_cols, how="left")

    # ── Prepare knowledge_time literal if not already in data ─────────────────
    kt_col = None
    if not source_has_kt:
        kt = knowledge_time if knowledge_time is not None else datetime.now(timezone.utc)
        kt_col = pl.lit(kt, dtype=pl.Datetime("us", "UTC")).alias("knowledge_time")

    # ── Single Rust pass: batch_id, series_id, knowledge_time, value * factor ─
    if batch_mapping is not None and batch_cols:
        # Multi-batch: per-row batch_id from joined _batch_id column
        batch_id_expr = pl.col("_batch_id").alias("batch_id")
    else:
        # Single-batch: broadcast scalar literal
        batch_id_expr = pl.lit(str(batch_id)).alias("batch_id")

    decoration: List[pl.Expr] = [
        batch_id_expr,
        pl.col("_series_id").cast(pl.Int64).alias("series_id"),
        pl.col("value") * pl.col("_factor"),
    ]
    if kt_col is not None:
        decoration.append(kt_col)
    joined = joined.with_columns(decoration)

    # ── Build final column list with dynamic passthrough ──────────────────────
    final_cols = ["batch_id", "series_id", "valid_time", "value", "knowledge_time"]
    for col in _OPTIONAL_PASSTHROUGH:
        if col in pl_df.columns:
            final_cols.append(col)

    # ── Build reverse routing lookup: target_table → routing dict ─────────────
    table_to_routing: Dict[str, Dict[str, Any]] = {}
    for row in safe_mapping.iter_rows(named=True):
        t = row["_target_table"]
        if t not in table_to_routing:
            table_to_routing[t] = {
                "overlapping": row["_overlapping"],
                "retention": row["_retention"],
                "table": t,
            }

    # ── Partition by target table, convert each partition to Arrow ─────────────
    result: Dict[str, Tuple[pa.Table, Dict[str, Any]]] = {}
    for group_df in joined.partition_by("_target_table"):
        target_table = group_df["_target_table"][0]
        routing = table_to_routing[target_table]
        arrow_table = group_df.select(final_cols).to_arrow()
        result[target_table] = (arrow_table, routing)

    return result


# ── Core pipeline ─────────────────────────────────────────────────────────────

def normalize_insert_input(
    data: Union[pd.DataFrame, TimeSeries],
    series_unit: str,
    *,
    series_id: int,
    batch_id,
    knowledge_time: Optional[datetime] = None,
    data_unit: Optional[str] = None,
) -> Tuple[pa.Table, DataShape]:
    """Convert DataFrame or TimeSeries insert input to a fully-decorated ``pa.Table``.

    All column decoration (``batch_id``, ``series_id``, ``knowledge_time``) is
    performed in a single Polars ``with_columns()`` Rust pass before
    ``to_arrow()``.  Zero PyArrow column appends.

    Args:
        data: Input data — ``pd.DataFrame`` or :class:`~timedatamodel.TimeSeries`.
        series_unit: Canonical unit of the target series.
        series_id: Integer series ID to stamp into every row.
        batch_id: UUID batch identifier to stamp into every row.
        knowledge_time: Broadcast knowledge_time for all rows.  Mutually
            exclusive with a ``knowledge_time`` column already present in
            *data*.  Defaults to ``now(UTC)`` when neither is provided.
        data_unit: Unit of the incoming ``pd.DataFrame`` values.  When
            provided, values are converted from *data_unit* to *series_unit*
            before insert.  Only applies to the DataFrame path; ignored for
            :class:`~timedatamodel.TimeSeries` (which carries its own unit).

    Returns:
        ``(pa.Table, DataShape)`` — complete Arrow table with columns
        ``[batch_id, series_id, valid_time, (valid_time_end,) value,
        knowledge_time]`` ready to COPY into the database.

    Raises:
        ValueError: If *knowledge_time* kwarg and a ``knowledge_time`` column
            are both present in *data*.
        IncompatibleUnitError: If *data_unit* is not convertible to *series_unit*.
    """
    # 1. Inspect SOURCE data columns (before any conversion)
    source_has_kt = (
        "knowledge_time" in data.columns
        if isinstance(data, pd.DataFrame)
        else "knowledge_time" in data.df.columns
    )
    source_has_vte = (
        "valid_time_end" in data.columns
        if isinstance(data, pd.DataFrame)
        else "valid_time_end" in data.df.columns
    )
    # Canonical column order for the output Arrow table
    final_cols = (
        ["batch_id", "series_id", "valid_time", "valid_time_end", "value", "knowledge_time"]
        if source_has_vte
        else ["batch_id", "series_id", "valid_time", "value", "knowledge_time"]
    )
    if source_has_kt and knowledge_time is not None:
        raise ValueError(
            "Ambiguous knowledge_time: the data already contains a 'knowledge_time' column "
            "and knowledge_time was also passed as a keyword argument. "
            "Remove the kwarg or the column."
        )

    # 2. Prepare the knowledge_time literal (if needed) — set dtype at construction
    kt_col = None
    if not source_has_kt:
        kt = knowledge_time if knowledge_time is not None else datetime.now(timezone.utc)
        kt_col = pl.lit(kt, dtype=pl.Datetime("us", "UTC")).alias("knowledge_time")

    # 3. Constant decoration columns (always added)
    cols_to_add = [
        pl.lit(str(batch_id)).alias("batch_id"),
        pl.lit(series_id, dtype=pl.Int64).alias("series_id"),
    ]
    if kt_col is not None:
        cols_to_add.append(kt_col)

    # 4a. TimeSeries path — extract inner Polars df, apply unit conversion, decorate
    if isinstance(data, TimeSeries):
        df, shape = data.validate_for_insert()
        df = _resolve_polars_units(df, ts_unit=data.unit, series_unit=series_unit)
        df = df.with_columns(cols_to_add)   # single Rust pass: batch_id, series_id, (kt)
        df = df.select(final_cols)          # reorder to canonical layout (zero-copy)
        table = df.to_arrow()               # complete, final table

    # 4b. pd.DataFrame path — validate, convert to Polars, apply unit conversion, decorate
    else:
        _validate_df_columns(data)
        _check_df_timezone(data)

        # _check_df_timezone() already guarantees tz-awareness;
        # pl.from_pandas() preserves timezone information automatically.
        pl_df = pl.from_pandas(data)
        if data_unit is not None:
            pl_df = _resolve_polars_units(pl_df, ts_unit=data_unit, series_unit=series_unit)
        shape = DataShape.VERSIONED if source_has_kt else DataShape.SIMPLE

        pl_df = pl_df.with_columns(cols_to_add)  # single Rust pass: batch_id, series_id, (kt)
        pl_df = pl_df.select(final_cols)          # reorder to canonical layout (zero-copy)
        table = pl_df.to_arrow()                  # complete, final table

    return table, shape
