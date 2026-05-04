"""Smoke tests for the public surface and dependency hygiene."""

import sys


def test_public_exports():
    import timedb

    assert hasattr(timedb, "TimeDBClient")
    assert hasattr(timedb, "profiling")


def test_no_psycopg_pulled_in():
    """timedb is a pure CH library — it must not pull in psycopg or pint."""
    for mod in list(sys.modules):
        if mod.startswith(("timedb", "psycopg", "pint")):
            del sys.modules[mod]
    import timedb  # noqa: F401

    assert "psycopg" not in sys.modules, "timedb should not import psycopg"
    assert "psycopg_pool" not in sys.modules, "timedb should not import psycopg_pool"
    assert "pint" not in sys.modules, "timedb should not import pint"


def test_no_shape_or_tables_exports():
    """The Shape enum and TABLES catalog were removed in the unified ``series_values`` redesign."""
    import timedb

    assert not hasattr(timedb, "TABLES")
    assert not hasattr(timedb, "Shape")
    assert not hasattr(timedb, "shape_of")
    assert not hasattr(timedb, "InsertResult")
