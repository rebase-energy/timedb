"""TimeDBClient — the ClickHouse-only public facade.

Pure time-series I/O. No metadata, no runs table, no shape dispatch. Callers
(energydb) supply ``series_id``, ``run_id``, and ``retention`` as context;
``timedb`` just stores and retrieves.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import datetime, timedelta
from datetime import time as dt_time
from importlib import resources

import clickhouse_connect
import pandas as pd
import polars as pl

from . import read as _read
from . import write as _write


def _get_ch_url() -> str:
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not ch_url:
        raise ValueError("ClickHouse connection not configured. Pass ch_url or set TIMEDB_CH_URL.")
    return ch_url


_DDL = resources.files("timedb").joinpath("sql", "ch_create_tables.sql").read_text(encoding="utf-8")

_CH_TABLES = ["events", "run_series"]


class TimeDBClient:
    def __init__(self, ch_url: str | None = None):
        self._ch_url = ch_url or _get_ch_url()
        self._ch = clickhouse_connect.get_client(dsn=self._ch_url)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def create(self) -> None:
        """Create the events table and run_series mapping."""
        for statement in _DDL.split(";"):
            s = statement.strip()
            if not s:
                continue
            non_comment = [ln for ln in s.splitlines() if ln.strip() and not ln.strip().startswith("--")]
            if not non_comment:
                continue
            self._ch.command(s)

    def delete(self) -> None:
        """Drop both CH tables."""
        for name in _CH_TABLES:
            self._ch.command(f"DROP TABLE IF EXISTS {name}")

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    def write(
        self,
        df: pd.DataFrame | pl.DataFrame,
        *,
        retention: str | None = None,
        knowledge_time: datetime | None = None,
    ) -> None:
        return _write.write(
            self._ch,
            df,
            retention=retention,
            knowledge_time=knowledge_time,
        )

    def read(
        self,
        *,
        series_ids: Sequence[int],
        retention: str | Sequence[str] | None = None,
        start_valid: datetime | None = None,
        end_valid: datetime | None = None,
        start_known: datetime | None = None,
        end_known: datetime | None = None,
        include_updates: bool = False,
        include_knowledge_time: bool = False,
    ) -> pl.DataFrame:
        return _read.read(
            self._ch,
            series_ids=series_ids,
            retention=retention,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
        )

    def read_relative(
        self,
        *,
        series_ids: Sequence[int],
        retention: str | Sequence[str] | None = None,
        window_length: timedelta | None = None,
        issue_offset: timedelta | None = None,
        start_window: datetime | None = None,
        start_valid: datetime | None = None,
        end_valid: datetime | None = None,
        days_ahead: int | None = None,
        time_of_day: dt_time | None = None,
    ) -> pl.DataFrame:
        return _read.read_relative(
            self._ch,
            series_ids=series_ids,
            retention=retention,
            window_length=window_length,
            issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid,
            end_valid=end_valid,
            days_ahead=days_ahead,
            time_of_day=time_of_day,
        )

    def read_run_series(
        self,
        *,
        series_id: int,
    ) -> list[int]:
        """Return run_ids that touched a given series_id, latest first.

        Data only — the ``energydb.runs`` PG table hydrates the metadata.
        """
        sql = """
        SELECT run_id
        FROM run_series FINAL
        WHERE series_id = {series_id:UInt64}
        ORDER BY first_seen DESC
        """
        result = self._ch.query(sql, parameters={"series_id": series_id})
        return [int(row[0]) for row in result.result_rows]
