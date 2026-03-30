"""Shared data types for the TimeDB insert pipeline."""
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, NamedTuple


class IncompatibleUnitError(ValueError):
    """Raised when units cannot be converted to each other."""
    pass


class InsertResult(NamedTuple):
    """Result from insert — returned to the user."""
    run_id: uuid.UUID
    workflow_id: Optional[str]
    series_id: int


@dataclass
class RunContext:
    """Internal container for run metadata passed to the DB layer.

    Holds everything needed for INSERT INTO runs_table.
    run_id is stored as str (the form PostgreSQL and Arrow expect).
    run_params is the raw dict; callers serialize to JSON at the point of use.
    """
    run_id: str
    workflow_id: Optional[str]
    run_start_time: Optional[datetime]
    run_finish_time: Optional[datetime]
    run_params: Optional[Dict]
