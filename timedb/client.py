"""TimeDBClient: the ClickHouse-only public facade.

Pure time-series I/O. No metadata, no runs table, no shape dispatch. Callers
(energydb) supply ``series_id``, ``run_id``, and ``retention`` as context;
``timedb`` just stores and retrieves.
"""

from __future__ import annotations

import os
from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from datetime import time as dt_time
from importlib import resources

import clickhouse_connect
import clickhouse_connect.common
import pandas as pd
import polars as pl

from . import read as _read
from . import write as _write

# ClickHouse Cloud marks some insert settings (e.g.
# max_partitions_per_insert_block) readonly for the connecting role.
# clickhouse-connect's default is to raise on any unknown/readonly setting;
# drop makes it skip them (with a logged warning) instead, so the same insert
# settings work on Cloud and self-hosted alike. Dropped
# guards like the partition-block limit just fall back to the server default,
# which is safe, and on Cloud the limit is readonly so it could not be raised
# regardless.
clickhouse_connect.common.set_setting("invalid_setting_action", "drop")


def _get_ch_url() -> str:
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not ch_url:
        raise ValueError("ClickHouse connection not configured. Pass ch_url or set TIMEDB_CH_URL.")
    return ch_url


# CH client HTTP timeouts (seconds), both env-tunable without a code change.
#
# send_receive_timeout (TIMEDB_CH_TIMEOUT) is the response read timeout.
#
# connect_timeout (TIMEDB_CH_CONNECT_TIMEOUT) is the TCP/TLS connect timeout
# AND, crucially, the socket timeout urllib3 uses while *sending the request
# body*. It only switches to the read timeout once the body is fully sent. So a
# large bulk insert over a slow or distant link (e.g. a laptop → ClickHouse
# Cloud in another region) is bounded by connect_timeout, not the read one:
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
    # One CH client. The write path overlaps its series_values and
    # run_series inserts (and, for a large values batch, two split halves)
    # concurrently on this single client: it is sessionless (see
    # _new_client), so its queries run in parallel over the client's HTTP
    # connection pool rather than contending for one session. This overlap saves
    # the fixed per-insert commit latency (~135 ms on ClickHouse Cloud) that
    # serializing the lanes would pay twice; measured 556 ms → 349 ms (1.59×) on
    # the 1.7 M-row split insert.

    def __init__(self, ch_url: str | None = None):
        self._ch_url = ch_url or _get_ch_url()
        self._ch_timeout = _get_ch_timeout()
        self._ch_connect_timeout = _get_ch_connect_timeout()
        self._ch = self._new_client()

    def _new_client(self):
        # Sessionless on purpose: a ClickHouse session serializes queries (the
        # driver raises "concurrent queries within the same session"), while
        # sessionless queries on one client may overlap freely; that is what
        # lets callers fan out reads with asyncio.gather / threads on a single
        # TimeDBClient. Nothing here needs session state (settings ride inline
        # per query/insert; no temp tables, no SET). Trade-off: no sticky
        # replica routing on ClickHouse Cloud, so read-after-write across
        # replicas is eventually consistent (typically sub-second).
        return clickhouse_connect.get_client(
            dsn=self._ch_url,
            connect_timeout=self._ch_connect_timeout,
            send_receive_timeout=self._ch_timeout,
            autogenerate_session_id=False,
        )

    # -----------------------------------------------------------------------
    # Schema
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # I/O
    # -----------------------------------------------------------------------

    def write(
        self,
        df: pd.DataFrame | pl.DataFrame,
        *,
        retention: str | None = None,
        knowledge_time: datetime | None = None,
        skip_unchanged: bool = False,
        unchanged_scope: _write.UnchangedScope = "valid_time",
        knowledge_time_scoped_series: Collection[int] | None = None,
    ) -> _write.WriteResult:
        """Write time-series rows into ``series_values`` and their
        ``run_series`` mapping.

        ``df`` may be a Pandas or Polars frame. Required columns:
        ``series_id``, ``valid_time``, ``value``. Optional columns get a
        per-batch default when absent: ``knowledge_time`` (this kwarg, else
        ``datetime.now(UTC)``), ``change_time`` (``now(UTC)``), ``run_id``
        (one client-generated UUID7 truncated to 63 bits), ``valid_time_end``
        (the ``2200-01-01`` sentinel), and ``changed_by`` / ``annotation``
        (empty strings). Every timestamp column must be timezone-aware; naive
        values raise ``ValueError``.

        ``retention`` and ``knowledge_time`` may be given as a kwarg *or* a
        column, never both. ``retention`` defaults to ``"forever"`` (no TTL);
        see :data:`~timedb.RETENTION_TIERS` for the valid tiers.

        With ``skip_unchanged=True``, rows whose latest stored
        ``(value, annotation, changed_by)`` already matches are dropped
        before the insert, at the cost of one bounded read-back.
        ``unchanged_scope`` picks the comparison key: ``"valid_time"``
        (default), ``"knowledge_time"``, or ``"auto"``, which applies the
        knowledge-time key to the ids in ``knowledge_time_scoped_series``
        and the valid-time key to every other series in the frame.
        Supplying ``knowledge_time_scoped_series`` with any other scope
        raises.

        Returns a :class:`~timedb.WriteResult`: a
        ``NamedTuple(written, skipped)`` of row counts.
        """
        return _write.write(
            self._ch,
            df,
            retention=retention,
            knowledge_time=knowledge_time,
            skip_unchanged=skip_unchanged,
            unchanged_scope=unchanged_scope,
            knowledge_time_scoped_series=knowledge_time_scoped_series,
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
        """Read values for ``series_ids``, returning a Polars DataFrame.

        By default this collapses to the latest value per ``valid_time``,
        the row with the largest ``(knowledge_time, change_time)``, and
        returns ``series_id, valid_time, value``. Two flags widen it:

        * ``include_knowledge_time=True``: one row per
          ``(knowledge_time, valid_time)``, i.e. every forecast run
          side-by-side, adding ``knowledge_time``.
        * ``include_updates=True``: the full correction chain on the
          winning run, adding ``change_time``, ``changed_by`` and
          ``annotation``.

        Setting both returns the complete 3-dimensional audit log.

        ``retention`` accepts one tier or a sequence of tiers and prunes
        whole partitions. ``start_valid`` / ``end_valid`` bound
        ``valid_time``; ``start_known`` / ``end_known`` bound
        ``knowledge_time``. All datetimes must be timezone-aware.

        ``meta_source`` is an advanced hook: pass a
        :class:`~timedb.PgEngineMeta` to have ClickHouse resolve the series
        set itself through a PostgreSQL engine table instead of sending an
        explicit id array (energydb's concurrent read path uses it).
        """
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
        """Per-window cutoff read: for each window, the latest forecast
        issued at or before that window's cutoff.

        This is what backtests and day-ahead simulations need, "what
        forecast was available at decision time", and it returns
        ``series_id, valid_time, value``.

        Two mutually exclusive parameter sets address the windows; mixing
        them raises ``ValueError``:

        * **Low-level**: ``window_length`` plus ``issue_offset`` (relative
          to each window start) and ``start_window``.
        * **Daily shorthand**: ``days_ahead`` plus ``time_of_day``, giving
          fixed 1-day windows with a human-friendly cutoff.

        ``start_valid`` / ``end_valid`` bound the returned range,
        ``retention`` prunes partitions, and ``meta_source`` behaves as in
        :meth:`read`. All datetimes must be timezone-aware.
        """
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

        Data only: the ``energydb.runs`` PG table hydrates the metadata.
        """
        sql = """
        SELECT run_id
        FROM run_series FINAL
        WHERE series_id = {series_id:UInt64}
        ORDER BY first_seen DESC
        """
        result = self._ch.query(sql, parameters={"series_id": series_id})
        return [int(row[0]) for row in result.result_rows]
