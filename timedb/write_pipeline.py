"""
Write pipeline: multi-series write orchestration.

Resolves series identities, builds vectorized Polars mappings, applies unit
conversion, partitions data by target table, and inserts all partitions
atomically.  Delegates pure data transformation to :mod:`pipeline_utils` and
DB operations to :mod:`db`.
"""
import uuid
import warnings
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa

from . import db, profiling
from .connection import _get_connection
from .db.series import SeriesRegistry
from .pipeline_utils import _compute_unit_factor, _to_arrow, OPTIONAL_PASSTHROUGH
from .types import RunContext, InsertResult

try:
    from uuid import uuid7
except ImportError:
    from uuid6 import uuid7

from psycopg_pool import ConnectionPool


# ── Reserved column sets (shared with sdk.py for validation) ─────────────────

_WRITE_RUN_RESERVED_COLS = frozenset({
    "workflow_id", "run_start_time", "run_finish_time",
})


# ── Multi-series data transformation ─────────────────────────────────────────

def normalize_write_input(
    pl_df: pl.DataFrame,
    name_col: Optional[str],
    label_cols: List[str],
    mapping_df: pl.DataFrame,
    *,
    series_col: Optional[str] = None,
    run_id: Optional[str] = None,
    run_mapping: Optional[pl.DataFrame] = None,
    run_cols: Optional[List[str]] = None,
    knowledge_time: Optional[datetime] = None,
) -> Dict[str, Tuple[pa.Table, Dict[str, Any]]]:
    """Convert a multi-series Polars DataFrame to partitioned Arrow tables.

    *pl_df* must already be a Polars DataFrame (caller converts from Pandas if
    needed).  *mapping_df* must be a flat Polars DataFrame keyed on routing
    columns (either ``[name_col, *label_cols]`` or ``[series_col]``) with
    metadata columns ``[_series_id, _unit, _factor, _target_table,
    _overlapping, _retention]`` — one row per unique series identity.

    Unit conversion is applied vectorized via a join.  All
    :class:`IncompatibleUnitError` errors are raised **before** any DB
    interaction.

    Optional audit/interval columns (``valid_time_end``, ``change_time``,
    ``changed_by``, ``annotation``) are passed through if present in *pl_df*,
    enabling historical data migration without loss of audit trail.

    Args:
        pl_df: Input data as a Polars DataFrame.  Must contain ``valid_time``
            and ``value`` columns plus routing columns.
        name_col: Column in *pl_df* that maps to ``series_table.name``.
            ``None`` when *series_col* is used.
        label_cols: Columns in *pl_df* that map to ``series_table.labels``.
        mapping_df: Flat routing table keyed on routing columns with
            ``[_series_id, _unit, _factor, _target_table, _overlapping,
            _retention]`` metadata columns.
        series_col: Column whose values are integer series IDs.  When set,
            the join uses *series_col* instead of *name_col*/*label_cols*.
        run_id: UUID run identifier stamped into every row (single-run
            path, mutually exclusive with *run_mapping*).
        run_mapping: Polars DataFrame with columns ``[*run_cols, "_run_id"]``
            for per-row run stamping (multi-run path, mutually exclusive
            with *run_id*).
        run_cols: Columns in *pl_df* used as the join key for *run_mapping*.
            Required when *run_mapping* is provided.
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
    if series_col is not None:
        routing_cols = {series_col}
    else:
        routing_cols = {name_col} | set(label_cols)
    data_col_names = [c for c in pl_df.columns if c not in routing_cols]

    # ── Validate required columns ─────────────────────────────────────────────
    if "valid_time" not in data_col_names:
        raise ValueError("DataFrame must contain a 'valid_time' column.")
    if "value" not in data_col_names:
        raise ValueError("DataFrame must contain a 'value' column.")

    if run_mapping is not None and not run_cols:
        raise ValueError("run_cols is required when run_mapping is provided.")

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
    if series_col is not None:
        join_on: list[str] = [series_col]
    else:
        assert name_col is not None
        join_on = [name_col] + label_cols
    if "unit" in pl_df.columns:
        join_on = join_on + ["unit"]
    cast_schema = {col: pl_df.schema[col] for col in join_on if col in pl_df.schema}
    safe_mapping = mapping_df.cast(cast_schema)

    # Vectorized join: stamp _series_id, _factor, _target_table, routing meta
    joined = pl_df.join(safe_mapping, on=join_on, how="left")

    # Validate all rows matched — null _series_id means unmatched routing keys
    null_count = joined["_series_id"].null_count()
    if null_count > 0:
        unmatched = joined.filter(pl.col("_series_id").is_null()).select(join_on).unique().head(10).to_dicts()
        raise ValueError(
            f"Found {null_count} row(s) with unmapped routing keys after joining "
            f"with series mapping. Unmatched combinations: {unmatched}"
        )

    # ── Multi-run path: join run_mapping to stamp per-row run_id ────────────
    if run_mapping is not None and run_cols:
        joined = joined.join(run_mapping, on=run_cols, how="left")

    # ── Prepare knowledge_time literal if not already in data ─────────────────
    kt_col = None
    if not source_has_kt:
        kt = knowledge_time if knowledge_time is not None else datetime.now(timezone.utc)
        kt_col = pl.lit(kt, dtype=pl.Datetime("us", "UTC")).alias("knowledge_time")

    # ── Single Rust pass: run_id, series_id, knowledge_time, value * factor ──
    if run_mapping is not None and run_cols:
        run_id_expr = pl.col("_run_id").alias("run_id")
    else:
        run_id_expr = pl.lit(str(run_id)).alias("run_id")

    decoration: List[pl.Expr] = [
        run_id_expr,
        pl.col("_series_id").cast(pl.Int64).alias("series_id"),
        (pl.col("value") * pl.col("_factor")).fill_null(float("nan")),
    ]
    if kt_col is not None:
        decoration.append(kt_col)
    joined = joined.with_columns(decoration)

    # ── Build final column list with dynamic passthrough ──────────────────────
    final_cols = ["run_id", "series_id", "valid_time", "value", "knowledge_time"]
    for col in OPTIONAL_PASSTHROUGH:
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
        if not routing["overlapping"]:
            if group_df.select(["series_id", "valid_time"]).is_duplicated().any():
                raise ValueError(
                    "Cannot insert duplicate (series_id, valid_time) pair(s) into a "
                    "flat series. Deduplicate your data before calling write()."
                )
        result[target_table] = (_to_arrow(group_df, final_cols), routing)

    return result


# ── Run context management ───────────────────────────────────────────────────

def _build_multi_run_contexts(
    pl_df: pl.DataFrame,
    run_cols: List[str],
    *,
    global_workflow_id: Optional[str],
    global_run_start_time: Optional[datetime],
    global_run_finish_time: Optional[datetime],
    global_run_params: Optional[dict],
) -> Tuple[Dict[str, RunContext], pl.DataFrame]:
    run_ctx_map: Dict[str, RunContext] = {}
    run_rows: List[dict] = []
    for row in pl_df.select(run_cols).unique().rows(named=True):
        rid = str(uuid7())
        r_start = row.get("run_start_time", global_run_start_time) or datetime.now(timezone.utc)
        if r_start.tzinfo is None:
            raise ValueError("run_start_time must be timezone-aware")
        unreserved = {k: v for k, v in row.items() if k not in _WRITE_RUN_RESERVED_COLS}
        run_ctx_map[rid] = RunContext(
            run_id=rid,
            workflow_id=row.get("workflow_id", global_workflow_id) or "sdk-workflow",
            run_start_time=r_start,
            run_finish_time=row.get("run_finish_time", global_run_finish_time),
            run_params={**(global_run_params or {}), **unreserved} or None,
        )
        run_rows.append({**row, "_run_id": rid})
    schema = {**{col: pl_df.schema[col] for col in run_cols}, "_run_id": pl.String}
    return run_ctx_map, pl.DataFrame(run_rows, schema=schema)


# ── Core multi-series write ──────────────────────────────────────────────────

def _write(
    df: Union[pd.DataFrame, pl.DataFrame],
    name_col: Optional[str],
    label_cols: List[str],
    run_cols: Optional[List[str]] = None,
    series_col: Optional[str] = None,
    *,
    knowledge_time: Optional[datetime] = None,
    data_unit: Optional[str] = None,
    workflow_id: Optional[str] = None,
    run_start_time: Optional[datetime] = None,
    run_finish_time: Optional[datetime] = None,
    run_params: Optional[dict] = None,
    make_run_context=None,
    ch_client=None,
    _pool: Optional[ConnectionPool] = None,
) -> List[InsertResult]:
    """
    Core multi-series write implementation.

    Accepts long-format data (all routing info in columns), resolves series
    identities, builds a vectorized Polars mapping, and inserts all partitions
    in one atomic transaction.
    """

    t_write_start = perf_counter()

    if run_start_time is not None and run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")

    # ── Normalize to Polars immediately ──────────────────────────────────────
    pl_df = pl.from_pandas(df) if isinstance(df, pd.DataFrame) else df

    # ── Detect per-row unit column ────────────────────────────────────────────
    has_unit_col = "unit" in pl_df.columns
    if has_unit_col and pl_df["unit"].null_count() > 0:
        raise ValueError(
            "The 'unit' column contains null values. "
            "Every row must specify a unit. Use 'dimensionless' for unit-free values."
        )

    # ── Resolve series and build mapping DataFrame ─────────────────────────────
    t_resolve_start = perf_counter()
    registry = SeriesRegistry()

    if series_col is not None:
        # ── Fast path: series_col provides IDs directly ──────────────────
        pl_df = pl_df.with_columns(pl.col(series_col).cast(pl.Int64))
        identity_cols = [series_col]
        mapping_keys_cols = [series_col] + (["unit"] if has_unit_col else [])
        unique_df = pl_df.select(mapping_keys_cols).unique()
        mapping_combos = unique_df.to_dicts()
        unique_sids = pl_df[series_col].unique().to_list()

        with _get_connection(_pool) as conn:
            found_ids = db.series.resolve_series_by_ids(conn, unique_sids, registry)
        profiling._record(profiling.PHASE_WRITE_SERIES_RESOLVE, perf_counter() - t_resolve_start)

        if missing := [sid for sid in unique_sids if sid not in found_ids]:
            raise ValueError(
                f"No series found for {len(missing)} series_id(s): {missing}"
            )

        mapping_rows: Dict[str, list] = {
            series_col: [],
            "_series_id": [], "_unit": [], "_factor": [],
            "_target_table": [], "_overlapping": [], "_retention": [],
        }
        if has_unit_col:
            mapping_rows["unit"] = []

        for row in mapping_combos:
            sid = row[series_col]
            meta = registry.get_cached(sid)
            series_unit = meta["unit"]
            incoming_unit = row["unit"] if has_unit_col else (data_unit or "dimensionless")

            factor = _compute_unit_factor(incoming_unit, series_unit)
            if factor is None and incoming_unit == "dimensionless" and series_unit != "dimensionless":
                warnings.warn(
                    f"Inserting dimensionless values into series with unit '{series_unit}' "
                    f"(series_id={sid}). Values stored as-is without conversion.",
                    UserWarning,
                    stacklevel=3,
                )

            mapping_rows[series_col].append(sid)
            if has_unit_col:
                mapping_rows["unit"].append(row["unit"])
            mapping_rows["_series_id"].append(sid)
            mapping_rows["_unit"].append(series_unit)
            mapping_rows["_factor"].append(factor if factor is not None else 1.0)
            mapping_rows["_target_table"].append(meta["table"])
            mapping_rows["_overlapping"].append(meta["overlapping"])
            mapping_rows["_retention"].append(meta["retention"])

    else:
        # ── Standard path: resolve by name + labels ──────────────────────
        identity_cols = [name_col] + label_cols
        mapping_keys_cols = identity_cols + (["unit"] if has_unit_col else [])
        unique_df = pl_df.select(mapping_keys_cols).unique()
        mapping_combos = unique_df.to_dicts()
        identity_df = unique_df.select(identity_cols).unique() if has_unit_col else unique_df
        identities = [(row[0], dict(zip(label_cols, row[1:]))) for row in identity_df.iter_rows()]

        with _get_connection(_pool) as conn:
            found = db.series.resolve_series(conn, identities, registry)
        profiling._record(profiling.PHASE_WRITE_SERIES_RESOLVE, perf_counter() - t_resolve_start)

        if missing := [
            f"name={n!r}, labels={l!r}"
            for n, l in identities
            if db.series._make_series_key(n.strip(), l) not in found
        ]:
            raise ValueError(
                f"No series found for {len(missing)} identity combination(s):\n"
                + "\n".join(f"  {m}" for m in missing)
            )

        mapping_rows: Dict[str, list] = {
            name_col: [], **{lc: [] for lc in label_cols},
            "_series_id": [], "_unit": [], "_factor": [],
            "_target_table": [], "_overlapping": [], "_retention": [],
        }
        if has_unit_col:
            mapping_rows["unit"] = []

        for row in mapping_combos:
            name = row[name_col]
            labels = {k: row[k] for k in label_cols}
            sid = found[db.series._make_series_key(name.strip(), labels)]
            meta = registry.get_cached(sid)
            series_unit = meta["unit"]
            incoming_unit = row["unit"] if has_unit_col else (data_unit or "dimensionless")

            factor = _compute_unit_factor(incoming_unit, series_unit)
            if factor is None and incoming_unit == "dimensionless" and series_unit != "dimensionless":
                warnings.warn(
                    f"Inserting dimensionless values into series with unit '{series_unit}' "
                    f"(name={name!r}, labels={labels!r}). Values stored as-is without conversion.",
                    UserWarning,
                    stacklevel=3,
                )

            mapping_rows[name_col].append(name)
            for lc in label_cols:
                mapping_rows[lc].append(labels.get(lc))
            if has_unit_col:
                mapping_rows["unit"].append(row["unit"])
            mapping_rows["_series_id"].append(sid)
            mapping_rows["_unit"].append(series_unit)
            mapping_rows["_factor"].append(factor if factor is not None else 1.0)
            mapping_rows["_target_table"].append(meta["table"])
            mapping_rows["_overlapping"].append(meta["overlapping"])
            mapping_rows["_retention"].append(meta["retention"])

    # Derive routing col types from pl_df.schema (labels can be any type).
    _mapping_schema = {col: pl_df.schema[col] for col in mapping_keys_cols}
    _mapping_schema.update({
        "_series_id": pl.Int64,
        "_unit": pl.String,
        "_factor": pl.Float64,
        "_target_table": pl.String,
        "_overlapping": pl.Boolean,
        "_retention": pl.String,
    })
    mapping_df = pl.DataFrame(mapping_rows, schema=_mapping_schema)

    # ── Build run context(s) and normalize/partition ─────────────────────────
    t_normalize_start = perf_counter()
    if run_cols:
        run_ctx_map, run_mapping_df = _build_multi_run_contexts(
            pl_df=pl_df,
            run_cols=run_cols,
            global_workflow_id=workflow_id,
            global_run_start_time=run_start_time,
            global_run_finish_time=run_finish_time,
            global_run_params=run_params,
        )
        partitioned = normalize_write_input(
            pl_df,
            name_col=name_col,
            label_cols=label_cols,
            series_col=series_col,
            mapping_df=mapping_df,
            run_mapping=run_mapping_df,
            run_cols=run_cols,
            knowledge_time=knowledge_time,
        )
        result_df = (
            pl_df.select(identity_cols + run_cols).unique()
            .join(mapping_df.select(identity_cols + ["_series_id"]), on=identity_cols)
            .join(run_mapping_df.select(run_cols + ["_run_id"]), on=run_cols)
            .select(["_series_id", "_run_id"])
            .unique()
        )
    else:
        single_run_ctx = make_run_context(workflow_id, run_start_time, run_finish_time, run_params)
        run_ctx_map = {single_run_ctx.run_id: single_run_ctx}
        partitioned = normalize_write_input(
            pl_df,
            name_col=name_col,
            label_cols=label_cols,
            series_col=series_col,
            mapping_df=mapping_df,
            run_id=single_run_ctx.run_id,
            knowledge_time=knowledge_time,
        )
        result_df = mapping_df.select(["_series_id", pl.lit(single_run_ctx.run_id).alias("_run_id")]).unique()
    profiling._record(profiling.PHASE_WRITE_NORMALIZE, perf_counter() - t_normalize_start)

    # ── Insert all partitions ─────────────────────────────────────────────────
    db.insert.insert_tables(ch_client, partitioned=partitioned, run_contexts=run_ctx_map)

    # ── Build results: one InsertResult per (series_id, run_id) pair ──────────
    results = [
        InsertResult(
            series_id=sid,
            run_id=uuid.UUID(rid),
            workflow_id=run_ctx_map[rid].workflow_id,
        )
        for sid, rid in result_df.iter_rows()
    ]
    profiling._record(profiling.PHASE_WRITE_TOTAL, perf_counter() - t_write_start)
    return results
