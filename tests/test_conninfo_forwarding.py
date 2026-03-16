"""Test that TimeDataClient forwards its conninfo to module-level helpers.

This verifies that when a TimeDataClient is initialized with a custom conninfo
(e.g., containing search_path for non-public schemas), the create(), delete(),
and create_series() methods use that conninfo instead of reading from
environment variables.
"""

from unittest.mock import patch
from timedb.sdk import TimeDataClient


CUSTOM_CONNINFO = "postgresql://user:pass@localhost:5432/testdb?options=-csearch_path%3Dcustom_schema%2Cpublic"


@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._create")
def test_create_forwards_conninfo(mock_create, mock_pool):
    """TimeDataClient.create() should pass self._conninfo to _create()."""
    client = TimeDataClient(conninfo=CUSTOM_CONNINFO)
    client.create()
    mock_create.assert_called_once()
    assert mock_create.call_args.kwargs["conninfo"] == CUSTOM_CONNINFO
    client.close()


@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._delete")
def test_delete_forwards_conninfo(mock_delete, mock_pool):
    """TimeDataClient.delete() should pass self._conninfo to _delete()."""
    client = TimeDataClient(conninfo=CUSTOM_CONNINFO)
    client.delete()
    mock_delete.assert_called_once_with(conninfo=CUSTOM_CONNINFO)
    client.close()


@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk._create_series")
def test_create_series_forwards_conninfo(mock_create_series, mock_pool):
    """TimeDataClient.create_series() should pass self._conninfo to _create_series()."""
    mock_create_series.return_value = 1
    client = TimeDataClient(conninfo=CUSTOM_CONNINFO)
    client.create_series(name="test_series", unit="kWh")
    mock_create_series.assert_called_once()
    assert mock_create_series.call_args.kwargs["conninfo"] == CUSTOM_CONNINFO
    client.close()


@patch("timedb.sdk.ConnectionPool")
@patch("timedb.sdk.db.create.create_schema")
def test_create_without_conninfo_falls_back_to_env(mock_create_schema, mock_pool, monkeypatch):
    """_create() without conninfo should fall back to env vars."""
    monkeypatch.setenv("TIMEDB_DSN", "postgresql://fallback@localhost/db")
    from timedb.sdk import _create
    _create()
    mock_create_schema.assert_called_once()
    assert mock_create_schema.call_args[0][0] == "postgresql://fallback@localhost/db"
