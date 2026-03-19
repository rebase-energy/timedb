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
    batch_id: uuid.UUID
    workflow_id: Optional[str]
    series_id: int


@dataclass
class BatchContext:
    """Internal container for batch metadata passed to the DB layer.

    Holds everything needed for INSERT INTO batches_table.
    batch_id is stored as str (the form PostgreSQL and Arrow expect).
    batch_params is the raw dict; callers serialize to JSON at the point of use.
    """
    batch_id: str
    workflow_id: Optional[str]
    batch_start_time: Optional[datetime]
    batch_finish_time: Optional[datetime]
    batch_params: Optional[Dict]
