"""Opt-in per-phase timing collector for TimeDB internal operations.

Disabled by default — zero overhead when disabled (no ``perf_counter`` calls,
no function calls in the hot path). Benchmark scripts activate it per-trial
to collect phase-level timing breakdowns.

Not thread-safe; designed for single-threaded benchmark use.

Usage:

    from timedb import profiling

    profiling.enable()
    profiling.reset()
    # ... run operation ...
    phases = profiling.collect()   # dict of phase -> elapsed seconds
    profiling.disable()

    # Or, for hot-path instrumentation:
    with profiling._phase(profiling.PHASE_EDB_RESOLVE):
        ...
"""

import time as _time
from contextlib import contextmanager

# ── Write phase constants ─────────────────────────────────────────────────────

PHASE_WRITE_NORMALIZE = "write.normalize"  # Polars prep: cast, fill_null, lit stamps
PHASE_WRITE_SERIES_VALUES_INSERT = "write.series_values_insert"  # CH insert_arrow into series_values
PHASE_WRITE_RUN_SERIES_INSERT = "write.run_series_insert"  # CH insert_arrow into run_series
PHASE_WRITE_TOTAL = "write.total"  # Full td.write() wall time


# ── Read phase constants ──────────────────────────────────────────────────────

PHASE_READ_SQL_EXEC = "read.sql_exec"  # ch_client.query_arrow() — CH query + Arrow transfer
PHASE_READ_BUILD_ARROW = "read.build_arrow"  # result.select + NaN-masking for null handling
PHASE_READ_TO_POLARS = "read.to_polars"  # pl.from_arrow() conversion
PHASE_READ_TOTAL = "read.total"  # Full td.read() wall time


# ── EnergyDB phase constants (used from energydb scope/_io/_join/client) ──────

PHASE_EDB_CONN_ACQUIRE = "edb.conn_acquire"  # pool.connection() checkout (per acquire)
PHASE_EDB_RESOLVE_SUBTREE = "edb.resolve_subtree"  # recursive subtree CTE in _resolve_target_node_uuids
PHASE_EDB_RESOLVE = "edb.resolve"  # resolve_for_read/resolve_for_write + resolve_manifest
PHASE_EDB_MANIFEST_BUILD = "edb.manifest_build"  # polars stitching in _build_read_manifest / _attach_routing
PHASE_EDB_RUNS_UPSERT = "edb.runs_upsert"  # runs_mod.upsert_run + the trailing PG commit
PHASE_EDB_UNIT_CONVERT = "edb.unit_convert"  # apply_manifest_unit_conversion / apply_per_series_unit
PHASE_EDB_HIERARCHY_JOIN = "edb.hierarchy_join"  # join_hierarchy / join_edge_hierarchy post-read
PHASE_EDB_OUTPUT_CONVERT = "edb.output_convert"  # to_polars(input) + to_output(result) boundary conversions


# ── Internal state ────────────────────────────────────────────────────────────

_enabled: bool = False
_timings: dict[str, float] = {}


# ── Public API ────────────────────────────────────────────────────────────────


def enable() -> None:
    """Enable profiling collection. Call before each trial."""
    global _enabled, _timings
    _enabled = True
    _timings = {}


def disable() -> None:
    """Disable profiling and clear timings."""
    global _enabled, _timings
    _enabled = False
    _timings = {}


def reset() -> None:
    """Clear accumulated timings while keeping profiling enabled."""
    global _timings
    _timings = {}


def is_enabled() -> bool:
    """Return True if profiling is currently active."""
    return _enabled


def collect() -> dict[str, float]:
    """Return a copy of accumulated timings (in seconds)."""
    return dict(_timings)


# ── Internal recording (called by DB layer) ───────────────────────────────────


def _record(phase: str, elapsed_s: float) -> None:
    """Accumulate elapsed time for a named phase.

    No-op when profiling is disabled. Adds to existing value if the same phase
    is recorded multiple times (e.g. multiple SQL calls within a single read).
    """
    if not _enabled:
        return
    _timings[phase] = _timings.get(phase, 0.0) + elapsed_s


@contextmanager
def _phase(phase: str):
    """Context manager that records ``phase`` only when profiling is enabled.

    Zero overhead when disabled: no ``perf_counter`` calls, the body still
    runs unchanged. Use at call-site instead of hand-rolling
    ``_t = perf_counter(); ...; _record(phase, perf_counter() - _t)``.
    """
    if not _enabled:
        yield
        return
    _t = _time.perf_counter()
    try:
        yield
    finally:
        _record(phase, _time.perf_counter() - _t)
