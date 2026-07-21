"""Unit tests for ``timedb.read._meta_cte`` — the scalar engine-meta subquery.

Pure (no DB): asserts each addressing field renders the expected pushdown
conditions and binds the expected params.
"""

from __future__ import annotations

from timedb.read import PgEngineMeta, _meta_cte

_TABLE = "energydb_series_meta_pg"


def test_meta_cte_edge_triples_renders_three_single_column_ins():
    """A set of (from_path, to_path, edge_type) identities pushes down as three
    single-column INs over the unique per-column values (cartesian superset)."""
    ms = PgEngineMeta(
        table=_TABLE,
        edge_triples=(("Grid/A", "Grid/B", "Line"), ("Grid/A", "Grid/C", "Line")),
        data_type=("actual",),
        name=("flow",),
    )
    cte, params = _meta_cte(ms)

    assert "from_path IN {ms_from:Array(String)}" in cte
    assert "to_path IN {ms_to:Array(String)}" in cte
    assert "edge_type IN {ms_etype:Array(String)}" in cte
    assert set(params["ms_from"]) == {"Grid/A"}
    assert set(params["ms_to"]) == {"Grid/B", "Grid/C"}
    assert set(params["ms_etype"]) == {"Line"}
    # data_type / name still narrow via IN (set-valued).
    assert params["ms_dt"] == ["actual"]
    assert params["ms_name"] == ["flow"]


def test_meta_cte_single_edge_triple_still_scalar():
    """The scalar edge_triple form (single edge scope) is unchanged."""
    ms = PgEngineMeta(table=_TABLE, edge_triple=("Grid/A", "Grid/B", "Line"))
    cte, params = _meta_cte(ms)

    assert "from_path = {ms_from:String}" in cte
    assert params == {"ms_from": "Grid/A", "ms_to": "Grid/B", "ms_etype": "Line"}


def test_meta_cte_requires_an_addressing_field():
    import pytest

    with pytest.raises(ValueError, match="edge_triple / edge_triples"):
        _meta_cte(PgEngineMeta(table=_TABLE))
