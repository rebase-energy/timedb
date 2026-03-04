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

from typing import Dict

# ── Insert phase constants ────────────────────────────────────────────────────

PHASE_INSERT_CSV_SERIALIZE = "insert.csv_serialize"  # Build Arrow staging table + pa_csv.write_csv
PHASE_INSERT_STAGE_DDL     = "insert.stage_ddl"      # CREATE TEMP TABLE DDL
PHASE_INSERT_COPY          = "insert.copy"            # COPY FROM STDIN write
PHASE_INSERT_UPSERT        = "insert.upsert"          # INSERT...ON CONFLICT (flat) or CTE (overlapping, incl. batch creation)
PHASE_INSERT_TOTAL         = "insert.total"           # Full insert_table() wall time

# ── Read phase constants ──────────────────────────────────────────────────────

PHASE_READ_SQL_EXEC         = "read.sql_exec"          # cursor.execute() — SQL planning + execution in Postgres
PHASE_READ_FETCH_ROWS       = "read.fetch_rows"        # cursor.fetchall() — transfer rows to Python
PHASE_READ_BUILD_ARROW      = "read.build_arrow"       # Build pa.Table from fetched rows
PHASE_READ_BUILD_TIMESERIES = "read.build_timeseries"  # TimeSeries.__init__: shape inference + validation + metadata
PHASE_READ_TO_PANDAS        = "read.to_pandas"         # TimeSeries.to_pandas(): pa.Table.to_pandas() + set_index()
PHASE_READ_TOTAL            = "read.total"             # Full _fetch_arrow() wall time (DB portion only)

# ── Internal state ────────────────────────────────────────────────────────────

_enabled: bool = False
_timings: Dict[str, float] = {}


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


def collect() -> Dict[str, float]:
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
