"""
Opt-in per-phase timing collector for TimeDB internal operations.

Disabled by default — zero overhead in production. Benchmark scripts
activate it per-trial to collect phase-level timing breakdowns.

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

# ── Phase name constants ──────────────────────────────────────────────────────

# Insert phases
PHASE_INSERT_NORMALIZE    = "insert.normalize"     # DataFrame → row tuples
PHASE_INSERT_STAGE_CREATE = "insert.stage_create"  # CREATE TEMP TABLE (large batches only)
PHASE_INSERT_COPY         = "insert.copy"           # COPY protocol write
PHASE_INSERT_UPSERT       = "insert.upsert"         # INSERT...ON CONFLICT (or executemany)
PHASE_INSERT_BATCH_META   = "insert.batch_meta"     # INSERT into batches_table (overlapping only)
PHASE_INSERT_TOTAL        = "insert.total"          # full insert_values() wall time

# Read phases
PHASE_READ_SQL_EXEC   = "read.sql_exec"    # cursor.execute()
PHASE_READ_FETCH      = "read.fetch"       # cursor.fetchall()
PHASE_READ_DATAFRAME  = "read.dataframe"   # pd.DataFrame construction + dtype conversion
PHASE_READ_TOTAL      = "read.total"       # full read function wall time

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


# ── Internal recording (called by DB layer) ───────────────────────────────────

def _record(phase: str, elapsed_s: float) -> None:
    """
    Accumulate elapsed time for a named phase.
    No-op when profiling is disabled.
    Adds to existing value if the same phase is recorded multiple times.
    """
    if not _enabled:
        return
    _timings[phase] = _timings.get(phase, 0.0) + elapsed_s
