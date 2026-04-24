"""
Opt-in per-phase timing collector for TimeDB internal operations.

Disabled by default — zero overhead when disabled (no perf_counter calls,
no function calls). Benchmark scripts activate it per-trial to collect
phase-level timing breakdowns.

Not thread-safe; designed for single-threaded benchmark use.

Usage:
    from timedb import profiling

    profiling.enable()
    profiling.reset()
    # ... run operation ...
    phases = profiling.collect()   # dict of phase -> elapsed seconds
    profiling.disable()
"""


# ── Insert phase constants ────────────────────────────────────────────────────

PHASE_INSERT_NORMALIZE = "insert.normalize"  # normalize_insert_input() — Polars preparation in _insert()
PHASE_INSERT_RUN_METADATA = "insert.run_metadata"  # _insert_run_metadata() — CH insert for run records
PHASE_INSERT_ARROW = "insert.arrow"  # ch_client.insert_arrow() — bulk Arrow transfer to ClickHouse
PHASE_INSERT_TOTAL = "insert.total"  # Full insert_tables() wall time

# ── Write phase constants ─────────────────────────────────────────────────────

PHASE_WRITE_SERIES_RESOLVE = "write.series_resolve"  # resolve_series() DB call (one round-trip for all series)
PHASE_WRITE_NORMALIZE = "write.normalize"  # normalize_write_input() Polars join + decorate pass
PHASE_WRITE_TOTAL = "write.total"  # Full _write() wall time (excludes SDK overhead above _write)

# ── Read phase constants ──────────────────────────────────────────────────────

PHASE_READ_SERIES_RESOLVE = "read.series_resolve"  # _resolve_manifest() — series lookup + registry build
PHASE_READ_SQL_EXEC = "read.sql_exec"  # ch_client.query_arrow() — execution + Arrow transfer in one call
PHASE_READ_BUILD_ARROW = "read.build_arrow"  # result.select(columns) — Arrow column selection
PHASE_READ_TO_POLARS = "read.to_polars"  # pl.from_arrow() at SDK boundary
PHASE_READ_BUILD_RESULT = "read.build_result"  # _build_read_result() — join CH data with metadata
PHASE_READ_TOTAL = "read.total"  # Full _fetch_ch_arrow() wall time

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


# ── Internal recording (called by DB/SDK layer) ───────────────────────────────


def _record(phase: str, elapsed_s: float) -> None:
    """
    Accumulate elapsed time for a named phase.
    No-op when profiling is disabled.
    Adds to existing value if the same phase is recorded multiple times.
    """
    if not _enabled:
        return
    _timings[phase] = _timings.get(phase, 0.0) + elapsed_s
