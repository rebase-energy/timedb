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
import clickhouse_connect.common
import pandas as pd
import polars as pl

from . import read as _read
from . import write as _write

# ClickHouse Cloud marks some insert settings (e.g. ``max_partitions_per_insert_block``)
# readonly for the connecting role. clickhouse-connect's default action is to raise
# on any unknown/readonly setting; ``drop`` makes it skip them (with a logged warning)
# instead, so the same insert settings work on Cloud and self-hosted alike. Dropped
# guards like the partition-block limit just fall back to the server default, which is
# safe — and on Cloud the limit is readonly so it could not be raised regardless.
clickhouse_connect.common.set_setting("invalid_setting_action", "drop")


def _get_ch_url() -> str:
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not ch_url:
        raise ValueError("ClickHouse connection not configured. Pass ch_url or set TIMEDB_CH_URL.")
    return ch_url


# CH client HTTP timeouts (seconds), both env-tunable without a code change.
#
# ``send_receive_timeout`` (TIMEDB_CH_TIMEOUT) is the response *read* timeout.
#
# ``connect_timeout`` (TIMEDB_CH_CONNECT_TIMEOUT) is the TCP/TLS connect timeout
# AND — crucially — the socket timeout urllib3 uses while *sending the request
# body*. It only switches to the read timeout once the body is fully sent. So a
# large bulk insert over a slow or distant link (e.g. a laptop → ClickHouse
# Cloud in another region) is bounded by ``connect_timeout``, not the read one:
# clickhouse-connect's default of 10s kills the upload mid-flight while it is
# still legitimately streaming. Raise it; for a very slow uplink, raise it more.
_DEFAULT_CH_TIMEOUT_S = 900
_DEFAULT_CH_CONNECT_TIMEOUT_S = 60


def _get_ch_timeout() -> int:
    raw = os.environ.get("TIMEDB_CH_TIMEOUT")
    return int(raw) if raw else _DEFAULT_CH_TIMEOUT_S


def _get_ch_connect_timeout() -> int:
    raw = os.environ.get("TIMEDB_CH_CONNECT_TIMEOUT")
    return int(raw) if raw else _DEFAULT_CH_CONNECT_TIMEOUT_S


_DDL = resources.files("timedb").joinpath("sql", "ch_create_tables.sql").read_text(encoding="utf-8")

_CH_TABLES = ["series_values", "run_series"]


class TimeDBClient:
    # Auxiliary CH clients for parallel inserts. clickhouse-connect rejects
    # concurrent calls on a single client ("Attempt to execute concurrent
    # queries within the same session"), so the write path keeps a small
    # sidecar pool: one client so the tiny ``run_series`` insert can overlap
    # the ``series_values`` insert (each insert pays a fixed per-insert commit
    # latency — ~135 ms on ClickHouse Cloud — so serializing them doubles it),
    # plus a second one when a large values batch is split. Two-way is the
    # split sweet spot: measured 556 ms → 349 ms (1.59×) on the 1.7 M-row
    # insert; 4-way is no better because the CH-side write pipeline
    # (parsing + merge tree insert) saturates first.

    def __init__(self, ch_url: str | None = None):
        self._ch_url = ch_url or _get_ch_url()
        self._ch_timeout = _get_ch_timeout()
        self._ch_connect_timeout = _get_ch_connect_timeout()
        self._ch = self._new_client()
        self._aux_clients: list = []

    def _new_client(self):
        return clickhouse_connect.get_client(
            dsn=self._ch_url,
            connect_timeout=self._ch_connect_timeout,
            send_receive_timeout=self._ch_timeout,
        )

    def _ensure_aux_clients(self, n: int) -> list:
        """Return at least ``n`` sidecar insert clients, grown lazily.

        A small-write-only process never builds more than one (the
        ``run_series`` lane); the second appears on the first split insert.
        """
        while len(self._aux_clients) < n:
            self._aux_clients.append(self._new_client())
        return self._aux_clients

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def create(self) -> None:
        """Create the series_values table and run_series mapping."""
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
        skip_unchanged: bool = False,
        unchanged_scope: _write.UnchangedScope = "valid_time",
    ) -> _write.WriteResult:
        return _write.write(
            self._ch,
            df,
            retention=retention,
            knowledge_time=knowledge_time,
            skip_unchanged=skip_unchanged,
            unchanged_scope=unchanged_scope,
            aux_clients=self._ensure_aux_clients,
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
        meta_source: _read.PgEngineMeta | None = None,
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
            meta_source=meta_source,
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
        meta_source: _read.PgEngineMeta | None = None,
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
            meta_source=meta_source,
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
