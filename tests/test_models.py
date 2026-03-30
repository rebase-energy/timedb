"""Verify that the SQLAlchemy model matches the expected series_table schema."""

from timedb.models import Base, SeriesTable
import pytest

models = pytest.importorskip("timedb.models")

def test_series_table_columns():
    table = SeriesTable.__table__
    assert table.name == "series_table"
    assert {c.name for c in table.columns} == {
        "series_id", "name", "unit", "labels",
        "description", "overlapping", "retention", "inserted_at",
    }


def test_series_table_constraints():
    table = SeriesTable.__table__
    constraint_names = {c.name for c in table.constraints if c.name}
    assert "series_identity_uniq" in constraint_names
    assert "series_name_not_empty" in constraint_names
    assert "valid_retention" in constraint_names


def test_base_metadata_contains_series_table():
    assert "series_table" in Base.metadata.tables
