"""Test that TimeDataClient forwards its pg_conninfo/ch_url to module-level helpers.

This verifies that when a TimeDataClient is initialized with custom connection
parameters, the create(), delete(), and create_series() methods use those
parameters instead of reading from environment variables.
"""

from unittest.mock import patch, MagicMock
from timedb.sdk import TimeDataClient


CUSTOM_PG_DSN = "postgresql://user:pass@localhost:5432/testdb"
CUSTOM_CH_URL = "clickhouse://user:pass@localhost:8123/timedb"


@patch("timedb.sdk.clickhouse_connect.get_client")
@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._create")
def test_create_forwards_conninfo(mock_create, mock_pool, mock_ch):
    """TimeDataClient.create() should pass pg_conninfo and ch_url to _create()."""
    mock_ch.return_value = MagicMock()
    client = TimeDataClient(pg_conninfo=CUSTOM_PG_DSN, ch_url=CUSTOM_CH_URL)
    client.create()
    mock_create.assert_called_once_with(pg_conninfo=CUSTOM_PG_DSN, ch_url=CUSTOM_CH_URL)
    client.close()


@patch("timedb.sdk.clickhouse_connect.get_client")
@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._delete")
def test_delete_forwards_conninfo(mock_delete, mock_pool, mock_ch):
    """TimeDataClient.delete() should pass pg_conninfo and ch_url to _delete()."""
    mock_ch.return_value = MagicMock()
    client = TimeDataClient(pg_conninfo=CUSTOM_PG_DSN, ch_url=CUSTOM_CH_URL)
    client.delete()
    mock_delete.assert_called_once_with(pg_conninfo=CUSTOM_PG_DSN, ch_url=CUSTOM_CH_URL)
    client.close()


@patch("timedb.sdk.clickhouse_connect.get_client")
@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._create_series")
def test_create_series_uses_pool_connection(mock_create_series, mock_pool, mock_ch):
    """TimeDataClient.create_series() should use a pool connection, not conninfo."""
    mock_ch.return_value = MagicMock()
    mock_create_series.return_value = 1
    pool_conn = MagicMock()
    mock_pool.return_value.connection.return_value.__enter__ = MagicMock(return_value=pool_conn)
    mock_pool.return_value.connection.return_value.__exit__ = MagicMock(return_value=False)
    client = TimeDataClient(pg_conninfo=CUSTOM_PG_DSN, ch_url=CUSTOM_CH_URL)
    client.create_series("test_series", unit="kWh")
    mock_create_series.assert_called_once()
    assert mock_create_series.call_args.kwargs["conn"] is pool_conn
    assert "conninfo" not in mock_create_series.call_args.kwargs
    client.close()


@patch("timedb.sdk.clickhouse_connect.get_client")
@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk.db.create.create_schema")
def test_create_without_conninfo_falls_back_to_env(mock_create_schema, mock_pool, mock_ch, monkeypatch):
    """_create() without args should fall back to env vars."""
    mock_ch.return_value = MagicMock()
    monkeypatch.setenv("TIMEDB_PG_DSN", CUSTOM_PG_DSN)
    monkeypatch.setenv("TIMEDB_CH_URL", CUSTOM_CH_URL)
    from timedb.sdk import _create
    _create()
    mock_create_schema.assert_called_once()
    assert mock_create_schema.call_args[0][0] == CUSTOM_PG_DSN
    assert mock_create_schema.call_args[0][1] == CUSTOM_CH_URL
