"""Tests for connection.py — environment variable resolution."""
import pytest

from timedb.connection import _get_pg_conninfo, _get_ch_url


def test_get_pg_conninfo_from_env(monkeypatch):
    monkeypatch.setenv("TIMEDB_PG_DSN", "postgresql://test:pass@localhost/db")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert _get_pg_conninfo() == "postgresql://test:pass@localhost/db"


def test_get_pg_conninfo_fallback_database_url(monkeypatch):
    monkeypatch.delenv("TIMEDB_PG_DSN", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://fallback@localhost/db")
    assert _get_pg_conninfo() == "postgresql://fallback@localhost/db"


def test_get_pg_conninfo_prefers_timedb_dsn(monkeypatch):
    monkeypatch.setenv("TIMEDB_PG_DSN", "postgresql://primary@localhost/db")
    monkeypatch.setenv("DATABASE_URL", "postgresql://fallback@localhost/db")
    assert _get_pg_conninfo() == "postgresql://primary@localhost/db"


def test_get_pg_conninfo_missing_raises(monkeypatch):
    monkeypatch.delenv("TIMEDB_PG_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError, match="TIMEDB_PG_DSN"):
        _get_pg_conninfo()


def test_get_ch_url_from_env(monkeypatch):
    monkeypatch.setenv("TIMEDB_CH_URL", "http://localhost:8123/default")
    assert _get_ch_url() == "http://localhost:8123/default"


def test_get_ch_url_missing_raises(monkeypatch):
    monkeypatch.delenv("TIMEDB_CH_URL", raising=False)
    with pytest.raises(ValueError, match="TIMEDB_CH_URL"):
        _get_ch_url()
