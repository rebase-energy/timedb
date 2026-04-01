"""
Read pipeline: multi-series read orchestration.

Resolves series from a manifest DataFrame, routes queries by ClickHouse table,
batches SQL execution, and assembles the final result by joining data with
series metadata.
"""
from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa

from . import db
from .connection import _get_connection
from .db.series import SeriesRegistry

from psycopg_pool import ConnectionPool


# Columns that should not be auto-inferred as label dimensions in read manifest
_READ_RESERVED_COLS = frozenset({
    "series_id", "unit", "name",
})


# ---------------------------------------------------------------------------
# Multi-series read orchestrators
# ---------------------------------------------------------------------------

def _read_multi(
    ch_client,
    series_metas: Dict[int, Dict],
    *,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    overlapping: bool = False,
    include_updates: bool = False,
) -> pa.Table:
    """Read data for multiple series, batched by ClickHouse table.

    Groups series by target table (``flat`` or ``overlapping_*``), runs one SQL
    query per table group, and concatenates the results.  The same read function
    is applied to all tables — the ``overlapping`` and ``include_updates`` flags
    determine the SQL shape, not the series type.

    Returns:
        ``pa.Table`` with ``series_id`` as the first column, followed by data
        columns whose shape depends on *overlapping* and *include_updates*.
    """
    # Group series_ids by their target table
    groups: Dict[str, List[int]] = {}  # table_name -> [series_ids]
    for sid, meta in series_metas.items():
        tbl = meta["table"]
        groups.setdefault(tbl, []).append(sid)

    time_filters = dict(start_valid=start_valid, end_valid=end_valid,
                        start_known=start_known, end_known=end_known)

    # Route each table group to the correct read function
    tables: List[pa.Table] = []
    for tbl, sids in groups.items():
        is_flat = (tbl == "flat")
        if is_flat:
            if overlapping and include_updates:
                fn = db.read.read_flat_history_updates_multi
            elif overlapping:
                fn = db.read.read_flat_history_multi
            elif include_updates:
                fn = db.read.read_flat_updates_multi
            else:
                fn = db.read.read_flat_multi
        else:
            if overlapping and include_updates:
                fn = db.read.read_overlapping_history_updates_multi
            elif overlapping:
                fn = db.read.read_overlapping_history_multi
            elif include_updates:
                fn = db.read.read_overlapping_updates_multi
            else:
                fn = db.read.read_overlapping_multi
        tables.append(fn(ch_client, series_ids=sids, table=tbl, **time_filters))

    if not tables:
        if overlapping and include_updates:
            cols = ["series_id", "knowledge_time", "valid_time", "change_time",
                    "value", "changed_by", "annotation"]
        elif overlapping:
            cols = ["series_id", "knowledge_time", "valid_time", "value"]
        elif include_updates:
            cols = ["series_id", "valid_time", "change_time", "value",
                    "changed_by", "annotation"]
        else:
            cols = ["series_id", "valid_time", "value"]
        return db.read._empty_table(cols)

    if len(tables) == 1:
        return tables[0]

    return pa.concat_tables(tables, promote_options="permissive")


def _read_relative_multi(
    ch_client,
    series_metas: Dict[int, Dict],
    *,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pa.Table:
    """Read relative data for multiple overlapping series, batched by table.

    Returns:
        ``pa.Table`` with columns ``(series_id, valid_time, value)``.
    """
    groups: Dict[str, List[int]] = {}
    for sid, meta in series_metas.items():
        groups.setdefault(meta["table"], []).append(sid)

    tables: List[pa.Table] = []
    for tbl, sids in groups.items():
        t = db.read.read_relative_multi(
            ch_client, series_ids=sids, table=tbl,
            window_length=window_length, issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid, end_valid=end_valid,
        )
        tables.append(t)

    if not tables:
        return db.read._empty_table(["series_id", "valid_time", "value"])

    return pa.concat_tables(tables)


# ---------------------------------------------------------------------------
# Manifest resolution
# ---------------------------------------------------------------------------

def _resolve_manifest(
    pl_manifest: pl.DataFrame,
    name_col: Optional[str],
    label_cols: Optional[List[str]],
    series_col: Optional[str],
    _pool: Optional[ConnectionPool],
) -> Tuple[pl.DataFrame, Dict[int, Dict]]:
    """Resolve a manifest DataFrame to series metadata.

    Returns:
        Tuple of (mapping_df, series_metas) where mapping_df has the identity
        columns plus ``_series_id``, ``_unit``, ``_table``, ``_overlapping``,
        and series_metas maps series_id -> cached metadata dict.
    """
    all_cols = list(pl_manifest.columns)
    registry = SeriesRegistry()

    if series_col is not None:
        # Fast path: series IDs
        pl_manifest = pl_manifest.with_columns(pl.col(series_col).cast(pl.Int64))
        unique_sids = pl_manifest[series_col].unique().to_list()
        identity_cols = [series_col]

        with _get_connection(_pool) as conn:
            found_ids = db.series.resolve_series_by_ids(conn, unique_sids, registry)

        if missing := [sid for sid in unique_sids if sid not in found_ids]:
            raise ValueError(
                f"No series found for {len(missing)} series_id(s): {missing}"
            )

        mapping_rows: Dict[str, list] = {
            series_col: [],
            "_series_id": [], "_unit": [], "_table": [], "_overlapping": [],
        }
        for sid in unique_sids:
            meta = registry.get_cached(sid)
            mapping_rows[series_col].append(sid)
            mapping_rows["_series_id"].append(sid)
            mapping_rows["_unit"].append(meta["unit"])
            mapping_rows["_table"].append(meta["table"])
            mapping_rows["_overlapping"].append(meta["overlapping"])

    else:
        # Standard path: name + labels
        name_col = name_col or "name"
        if name_col not in all_cols:
            raise ValueError(
                f"Name column {name_col!r} not found in manifest. "
                f"Rename your column to 'name' or pass name_col='your_column'."
            )

        if label_cols is None:
            exclude = _READ_RESERVED_COLS | {name_col}
            label_cols = [c for c in all_cols if c not in exclude]

        identity_cols = [name_col] + label_cols
        unique_df = pl_manifest.select(identity_cols).unique()
        identities = [(row[0], dict(zip(label_cols, row[1:]))) for row in unique_df.iter_rows()]

        with _get_connection(_pool) as conn:
            found = db.series.resolve_series(conn, identities, registry)

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
            "_series_id": [], "_unit": [], "_table": [], "_overlapping": [],
        }
        for row in unique_df.iter_rows():
            name = row[0]
            labels = dict(zip(label_cols, row[1:]))
            sid = found[db.series._make_series_key(name.strip(), labels)]
            meta = registry.get_cached(sid)

            mapping_rows[name_col].append(name)
            for lc in label_cols:
                mapping_rows[lc].append(labels.get(lc))
            mapping_rows["_series_id"].append(sid)
            mapping_rows["_unit"].append(meta["unit"])
            mapping_rows["_table"].append(meta["table"])
            mapping_rows["_overlapping"].append(meta["overlapping"])

    # Build mapping DataFrame with correct types
    _mapping_schema = {col: pl_manifest.schema[col] for col in identity_cols}
    _mapping_schema.update({
        "_series_id": pl.Int64,
        "_unit": pl.String,
        "_table": pl.String,
        "_overlapping": pl.Boolean,
    })
    mapping_df = pl.DataFrame(mapping_rows, schema=_mapping_schema)
    series_metas = {sid: registry.get_cached(sid) for sid in mapping_df["_series_id"].unique().to_list()}

    return mapping_df, series_metas


# ---------------------------------------------------------------------------
# Result assembly
# ---------------------------------------------------------------------------

def _build_read_result(
    ch_data: pa.Table,
    mapping_df: pl.DataFrame,
    name_col: Optional[str],
    label_cols: List[str],
    series_col: Optional[str],
) -> pl.DataFrame:
    """Join CH data with metadata and order columns for the final result."""
    ch_df = pl.from_arrow(ch_data)

    # Build column order (same logic for empty and non-empty paths)
    if series_col:
        id_cols = [series_col]
    else:
        id_cols = [name_col] + label_cols
    meta_cols_list = ["unit", "series_id"]
    data_cols = [c for c in ch_df.columns if c not in id_cols and c not in meta_cols_list]
    col_order = id_cols + meta_cols_list + data_cols

    if ch_df.is_empty():
        meta_for_schema = mapping_df.select(
            ["_series_id", "_unit"] +
            ([series_col] if series_col else [name_col] + label_cols)
        ).rename({"_series_id": "series_id", "_unit": "unit"})
        schema = {}
        for c in col_order:
            if c in meta_for_schema.columns:
                schema[c] = meta_for_schema.schema[c]
            elif c in ch_df.columns:
                schema[c] = ch_df.schema[c]
        return pl.DataFrame(schema=schema).head(0)

    # Left join: CH data on left, mapping on right (many-to-one, preserves all data rows)
    meta_for_join = mapping_df.select(
        ["_series_id", "_unit"] +
        ([series_col] if series_col else [name_col] + label_cols)
    ).unique().rename({"_series_id": "series_id", "_unit": "unit"})

    result = ch_df.join(meta_for_join, on="series_id", how="left")

    return result.select(col_order)


# ---------------------------------------------------------------------------
# Top-level orchestrators
# ---------------------------------------------------------------------------

def _read(
    manifest: Union[pd.DataFrame, pl.DataFrame],
    name_col: Optional[str],
    label_cols: Optional[List[str]],
    series_col: Optional[str],
    *,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    overlapping: bool = False,
    include_updates: bool = False,
    ch_client=None,
    _pool: Optional[ConnectionPool] = None,
) -> pl.DataFrame:
    """Core multi-series read implementation."""
    # Validate mutual exclusivity
    if series_col is not None:
        if name_col is not None:
            raise ValueError(
                "series_col and name_col are mutually exclusive. "
                "Use series_col to route by series ID, or name_col/label_cols to route by name and labels."
            )
        if label_cols is not None:
            raise ValueError(
                "series_col and label_cols are mutually exclusive. "
                "Use series_col to route by series ID, or name_col/label_cols to route by name and labels."
            )

    if series_col is None:
        name_col = name_col or "name"

    pl_manifest = pl.from_pandas(manifest) if isinstance(manifest, pd.DataFrame) else manifest

    if series_col is not None and series_col not in pl_manifest.columns:
        raise ValueError(
            f"Series column {series_col!r} not found in manifest. "
            f"Available columns: {list(pl_manifest.columns)}"
        )

    mapping_df, series_metas = _resolve_manifest(
        pl_manifest, name_col=name_col, label_cols=label_cols,
        series_col=series_col, _pool=_pool,
    )

    if not series_metas:
        raise ValueError("No series matched the manifest.")

    ch_data = _read_multi(
        ch_client,
        series_metas,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        overlapping=overlapping,
        include_updates=include_updates,
    )

    # Determine effective label_cols for output
    if series_col is None and label_cols is None:
        exclude = _READ_RESERVED_COLS | {name_col}
        label_cols = [c for c in pl_manifest.columns if c not in exclude]

    return _build_read_result(
        ch_data,
        mapping_df,
        name_col=name_col if series_col is None else None,
        label_cols=label_cols if series_col is None else [],
        series_col=series_col,
    )


def _read_relative(
    manifest: Union[pd.DataFrame, pl.DataFrame],
    name_col: Optional[str],
    label_cols: Optional[List[str]],
    series_col: Optional[str],
    *,
    window_length: Optional[timedelta] = None,
    issue_offset: Optional[timedelta] = None,
    start_window: Optional[datetime] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    days_ahead: Optional[int] = None,
    time_of_day: Optional[dt_time] = None,
    ch_client=None,
    _pool: Optional[ConnectionPool] = None,
) -> pl.DataFrame:
    """Core multi-series relative read implementation."""
    # Validate mode
    using_daily = days_ahead is not None or time_of_day is not None
    using_explicit = window_length is not None or issue_offset is not None

    if using_daily and using_explicit:
        raise ValueError(
            "Cannot mix (days_ahead, time_of_day) with (window_length, issue_offset). Use one set."
        )

    if using_daily:
        if days_ahead is None or time_of_day is None:
            raise ValueError("Both days_ahead and time_of_day must be provided together.")
        if start_valid is None:
            raise ValueError("start_valid is required when using days_ahead/time_of_day.")
        window_length = timedelta(days=1)
        issue_offset = (
            timedelta(
                hours=time_of_day.hour,
                minutes=time_of_day.minute,
                seconds=time_of_day.second,
                microseconds=time_of_day.microsecond,
            )
            - timedelta(days=days_ahead)
        )
        start_window = start_valid.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    else:
        if window_length is None or issue_offset is None:
            raise ValueError("Both window_length and issue_offset are required.")
        start_window = start_window if start_window is not None else start_valid
        if start_window is None:
            raise ValueError(
                "start_window is required when start_valid is not provided. "
                "Pass start_window to set the window alignment origin."
            )

    # Validate mutual exclusivity and normalize name_col
    if series_col is not None:
        if name_col is not None:
            raise ValueError("series_col and name_col are mutually exclusive.")
        if label_cols is not None:
            raise ValueError("series_col and label_cols are mutually exclusive.")
    else:
        name_col = name_col or "name"

    pl_manifest = pl.from_pandas(manifest) if isinstance(manifest, pd.DataFrame) else manifest

    if series_col is not None and series_col not in pl_manifest.columns:
        raise ValueError(
            f"Series column {series_col!r} not found in manifest. "
            f"Available columns: {list(pl_manifest.columns)}"
        )

    mapping_df, series_metas = _resolve_manifest(
        pl_manifest, name_col=name_col, label_cols=label_cols,
        series_col=series_col, _pool=_pool,
    )

    if not series_metas:
        raise ValueError("No series matched the manifest.")

    # Validate all series are overlapping
    flat_series = [
        series_metas[sid]["name"]
        for sid in series_metas
        if not series_metas[sid]["overlapping"]
    ]
    if flat_series:
        raise ValueError(
            f"read_relative() is only supported for overlapping (versioned) series. "
            f"The following flat series were matched: {flat_series}"
        )

    ch_data = _read_relative_multi(
        ch_client,
        series_metas,
        window_length=window_length,
        issue_offset=issue_offset,
        start_window=start_window,
        start_valid=start_valid,
        end_valid=end_valid,
    )

    if series_col is None and label_cols is None:
        exclude = _READ_RESERVED_COLS | {name_col}
        label_cols = [c for c in pl_manifest.columns if c not in exclude]

    return _build_read_result(
        ch_data,
        mapping_df,
        name_col=name_col if series_col is None else None,
        label_cols=label_cols if series_col is None else [],
        series_col=series_col,
    )
