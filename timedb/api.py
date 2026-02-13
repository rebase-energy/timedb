"""
FastAPI application for timedb - REST API

Provides REST API endpoints for time series database operations.

The API exposes endpoints for:
- Creating and managing time series (POST /series, GET /series)
- Inserting time series data (POST /values)
- Reading/querying time series data (GET /values)
- Updating existing records (PUT /values)
- Discovering labels and counts (GET /series/labels, GET /series/count)

Interactive documentation:
    - Swagger UI: /docs
    - ReDoc: /redoc

Environment:
    Requires TIMEDB_DSN or DATABASE_URL environment variable
    for database connection.
"""
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import psycopg
from psycopg_pool import ConnectionPool
import pandas as pd

from . import db

# Load .env file but DO NOT override existing environment variables
load_dotenv(override=False)


# Database connection string from environment
def get_dsn() -> str:
    """Get database connection string from environment variables."""
    dsn = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return dsn


# =============================================================================
# Lifespan: connection pool
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ConnectionPool(get_dsn(), min_size=2, max_size=10, open=True)
    yield
    app.state.pool.close()


def _get_conn(request: Request):
    """Get a connection from the pool (use as context manager)."""
    return request.app.state.pool.connection()


# =============================================================================
# Shared helpers
# =============================================================================

def _ensure_tz(dt_val: Optional[datetime]) -> Optional[datetime]:
    """Ensure a datetime is timezone-aware (default to UTC if naive)."""
    if dt_val is not None and dt_val.tzinfo is None:
        return dt_val.replace(tzinfo=timezone.utc)
    return dt_val


def _parse_labels(labels_json: Optional[str]) -> Optional[Dict[str, str]]:
    """Parse a JSON string into a labels dict."""
    if labels_json is None:
        return None
    try:
        parsed = json.loads(labels_json)
        if not isinstance(parsed, dict):
            raise ValueError
        return parsed
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=400, detail="labels must be a valid JSON object (e.g. '{\"site\":\"Gotland\"}')")


def _resolve_series(
    conn,
    name: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    series_id: Optional[int] = None,
    unit: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Resolve series by name+labels, series_id, or unit.

    Returns list of dicts with keys: series_id, name, unit, labels, overlapping, retention, description.
    Mirrors the SDK's _resolve_ids() pattern.
    """
    query = "SELECT series_id, name, description, unit, labels, overlapping, retention FROM series_table"
    clauses: list = []
    params: list = []

    if series_id is not None:
        clauses.append("series_id = %s")
        params.append(series_id)
    if name is not None:
        clauses.append("name = %s")
        params.append(name)
    if unit is not None:
        clauses.append("unit = %s")
        params.append(unit)
    if labels:
        clauses.append("labels @> %s::jsonb")
        params.append(json.dumps(labels))

    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY name, unit, series_id"

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        {
            "series_id": row[0],
            "name": row[1],
            "description": row[2],
            "unit": row[3],
            "labels": row[4] or {},
            "overlapping": row[5],
            "retention": row[6],
        }
        for row in rows
    ]


def _serialize_datetime(value):
    """Convert datetime/Timestamp to ISO string."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    elif isinstance(value, datetime):
        return value.isoformat()
    return value


# =============================================================================
# Pydantic models
# =============================================================================

class DataPoint(BaseModel):
    """A single data point for insertion."""
    valid_time: datetime
    value: Optional[float] = None
    valid_time_end: Optional[datetime] = None


class InsertRequest(BaseModel):
    """Request to insert time series data.

    Specify the target series by name+labels OR by series_id.
    The series must already exist (use POST /series to create it first).

    Attributes:
        name: Series name (used with labels to resolve series_id)
        labels: Labels for series resolution (e.g., {"site": "Gotland"})
        series_id: Direct series_id (alternative to name+labels)
        workflow_id: Workflow identifier (defaults to 'api-workflow')
        known_time: Time of knowledge (defaults to now(), important for overlapping series)
        batch_params: Custom parameters to store with the batch
        data: Array of data points to insert
    """
    name: Optional[str] = Field(None, description="Series name (resolve by name+labels)")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels for series resolution")
    series_id: Optional[int] = Field(None, description="Direct series_id (alternative to name+labels)")
    workflow_id: str = Field(default="api-workflow", description="Workflow identifier")
    known_time: Optional[datetime] = Field(None, description="Time of knowledge (defaults to now())")
    batch_params: Optional[Dict[str, Any]] = Field(None, description="Custom batch parameters")
    data: List[DataPoint] = Field(default_factory=list, description="Data points to insert")


class InsertResponse(BaseModel):
    """Response after inserting data."""
    batch_id: Optional[int] = Field(None, description="Batch ID (None for flat series)")
    series_id: int
    rows_inserted: int


class RecordUpdateRequest(BaseModel):
    """Request to update a record.

    Supports both flat and overlapping series with different update semantics:

    **Flat series**: In-place update by (series_id, valid_time).
    **Overlapping series**: Creates new version with known_time=now().

    Identify the series by series_id OR by name(+labels).

    For overlapping series, three lookup methods (all optional):
    - batch_id + valid_time: latest version in that batch
    - known_time + valid_time: exact version lookup
    - just valid_time: latest version overall

    For value, annotation, tags, and changed_by:
    - Omit the field to leave it unchanged
    - Set to null to explicitly clear it
    - Set to a value to update it
    """
    valid_time: datetime
    # Series identification: provide series_id OR name(+labels)
    series_id: Optional[int] = Field(default=None, description="Series ID (alternative to name+labels)")
    name: Optional[str] = Field(default=None, description="Series name (alternative to series_id)")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels for series resolution")
    # Overlapping version lookup (all optional)
    batch_id: Optional[int] = Field(default=None, description="For overlapping: target specific batch")
    known_time: Optional[datetime] = Field(default=None, description="For overlapping: target specific version")
    # Tri-state update fields
    value: Optional[float] = Field(default=None, description="Omit to leave unchanged, null to clear")
    annotation: Optional[str] = Field(default=None, description="Omit to leave unchanged, null to clear")
    tags: Optional[List[str]] = Field(default=None, description="Omit to leave unchanged, null or [] to clear")
    changed_by: Optional[str] = Field(default=None, description="Who made the change")


class UpdateRecordsRequest(BaseModel):
    """Request to update multiple records."""
    updates: List[RecordUpdateRequest]


class UpdateRecordsResponse(BaseModel):
    """Response after updating records."""
    updated: List[Dict[str, Any]]
    skipped_no_ops: List[Dict[str, Any]]


class CreateSeriesRequest(BaseModel):
    """Request to create a new time series.

    Series identity is determined by (name, labels). Two series with the same name
    but different labels are different series.
    """
    name: str = Field(..., description="Series name (e.g., 'wind_power_forecast')")
    description: Optional[str] = Field(None, description="Description of the series")
    unit: str = Field(default="dimensionless", description="Canonical unit (e.g., 'MW', 'kW', 'MWh')")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels (e.g., {'site': 'Gotland'})")
    overlapping: bool = Field(default=False, description="True for versioned/revised data")
    retention: str = Field(default="medium", description="'short', 'medium', or 'long' retention")


class CreateSeriesResponse(BaseModel):
    """Response after creating a series."""
    series_id: int
    message: str


class SeriesInfo(BaseModel):
    """Information about a time series."""
    series_id: int
    name: str
    description: Optional[str] = None
    unit: str
    labels: Dict[str, str] = Field(default_factory=dict)
    overlapping: bool = False
    retention: str = "medium"


# =============================================================================
# FastAPI app
# =============================================================================

app = FastAPI(
    title="TimeDB API",
    description="REST API for time series database operations",
    version="0.2.0",
    lifespan=lifespan,
)


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/")
async def root():
    """Root endpoint with API information."""
    data = {
        "name": "TimeDB API",
        "version": "0.2.0",
        "description": "REST API for reading and writing time series data",
        "endpoints": {
            "insert_values": "POST /values - Insert time series data",
            "read_values": "GET /values - Read time series values",
            "update_records": "PUT /values - Update existing records",
            "create_series": "POST /series - Create a new time series",
            "list_series": "GET /series - List/filter time series",
            "series_labels": "GET /series/labels - List unique label values",
            "series_count": "GET /series/count - Count matching series",
        },
        "admin_note": "Schema creation/deletion must be done through CLI or SDK, not through the API.",
    }
    json_str = json.dumps(data)
    return Response(content=json_str.encode("utf-8"), media_type="application/json")


@app.post("/values", response_model=InsertResponse)
async def insert_values(request_body: InsertRequest, request: Request):
    """
    Insert time series data.

    Specify the target series by name+labels OR by series_id.
    The series must already exist (use POST /series to create it first).

    Routing is automatic:
    - Flat series: inserted directly (no batch created, batch_id=null in response)
    - Overlapping series: a batch is created with known_time tracking
    """
    try:
        if request_body.series_id is None and request_body.name is None:
            raise HTTPException(
                status_code=400,
                detail="Provide either 'series_id' or 'name' (+labels) to identify the target series.",
            )

        if not request_body.data:
            raise HTTPException(status_code=400, detail="'data' must contain at least one data point.")

        known_time = _ensure_tz(request_body.known_time)

        with _get_conn(request) as conn:
            # Resolve series
            if request_body.series_id is not None:
                series_list = _resolve_series(conn, series_id=request_body.series_id)
            else:
                series_list = _resolve_series(conn, name=request_body.name, labels=request_body.labels)

            if len(series_list) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No series found. Create it first with POST /series.",
                )
            if len(series_list) > 1:
                raise HTTPException(
                    status_code=400,
                    detail=f"Multiple series ({len(series_list)}) match the filters. Use more specific labels or series_id.",
                )

            series = series_list[0]
            sid = series["series_id"]

            # Build value_rows
            value_rows = []
            for dp in request_body.data:
                vt = _ensure_tz(dp.valid_time)
                if dp.valid_time_end is not None:
                    vte = _ensure_tz(dp.valid_time_end)
                    value_rows.append((vt, vte, sid, dp.value))
                else:
                    value_rows.append((vt, sid, dp.value))

            # Build routing
            series_routing = {
                sid: {"overlapping": series["overlapping"], "retention": series["retention"]}
            }

            batch_id = db.insert.insert_values(
                conninfo=conn,
                workflow_id=request_body.workflow_id,
                batch_start_time=datetime.now(timezone.utc),
                value_rows=value_rows,
                known_time=known_time,
                batch_params=request_body.batch_params,
                series_routing=series_routing,
            )

        return InsertResponse(
            batch_id=batch_id,
            series_id=sid,
            rows_inserted=len(value_rows),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting values: {str(e)}")


@app.get("/values", response_model=Dict[str, Any])
async def read_values(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by series name"),
    labels: Optional[str] = Query(None, description="Filter by labels (JSON, e.g. '{\"site\":\"Gotland\"}')"),
    series_id: Optional[int] = Query(None, description="Filter by series_id"),
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_known: Optional[datetime] = Query(None, description="Start of known_time range (ISO format)"),
    end_known: Optional[datetime] = Query(None, description="End of known_time range (ISO format)"),
    versions: bool = Query(False, description="If true, return all overlapping revisions (for backtesting)"),
):
    """
    Read time series values.

    Filter by series name, labels, and/or series_id.
    Time range filtering via start_valid/end_valid and start_known/end_known.

    By default returns the latest value per (valid_time, series_id).
    Set versions=true to return all forecast revisions with their known_time.
    """
    try:
        label_filters = _parse_labels(labels)
        start_valid = _ensure_tz(start_valid)
        end_valid = _ensure_tz(end_valid)
        start_known = _ensure_tz(start_known)
        end_known = _ensure_tz(end_known)

        with _get_conn(request) as conn:
            # Resolve series to get IDs and routing info
            series_list = _resolve_series(conn, name=name, labels=label_filters, series_id=series_id)

            if not series_list:
                return {"count": 0, "data": []}

            series_ids = [s["series_id"] for s in series_list]
            overlapping_set = {s["overlapping"] for s in series_list}

            # Route reads based on series type (mirrors SDK logic)
            dfs = []

            flat_ids = [s["series_id"] for s in series_list if not s["overlapping"]]
            overlapping_ids = [s["series_id"] for s in series_list if s["overlapping"]]

            if flat_ids and not versions:
                # Flat read (skip if filtering by known_time since flat don't have it)
                if start_known is None and end_known is None:
                    df = db.read.read_flat(conn, series_ids=flat_ids, start_valid=start_valid, end_valid=end_valid)
                    if len(df) > 0:
                        dfs.append(df)
            elif flat_ids and versions:
                # Flat in versions mode — include inserted_at as known_time
                if start_known is None and end_known is None:
                    from .db.read import _build_where_clause, _ensure_conn
                    where_clause, params = _build_where_clause(
                        series_ids=flat_ids, start_valid=start_valid, end_valid=end_valid,
                        time_col="v.valid_time", known_time_col="v.inserted_at",
                    )
                    sql = f"""
                    SELECT v.inserted_at as known_time, v.valid_time, v.value, v.series_id,
                           s.name, s.unit, s.labels
                    FROM flat v JOIN series_table s ON v.series_id = s.series_id
                    {where_clause}
                    ORDER BY v.inserted_at, v.valid_time, v.series_id;
                    """
                    import warnings
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
                        df = pd.read_sql(sql, conn, params=params)
                    if len(df) > 0:
                        df["known_time"] = pd.to_datetime(df["known_time"], utc=True)
                        df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
                        df = df.set_index(["known_time", "valid_time", "series_id"]).sort_index()
                        dfs.append(df)

            if overlapping_ids:
                if versions:
                    df = db.read.read_overlapping_all(
                        conn, series_ids=overlapping_ids,
                        start_valid=start_valid, end_valid=end_valid,
                        start_known=start_known, end_known=end_known,
                    )
                else:
                    df = db.read.read_overlapping_latest(
                        conn, series_ids=overlapping_ids,
                        start_valid=start_valid, end_valid=end_valid,
                        start_known=start_known, end_known=end_known,
                    )
                if len(df) > 0:
                    dfs.append(df)

            if not dfs:
                return {"count": 0, "data": []}

            combined = pd.concat(dfs).sort_index()

        # Convert DataFrame to JSON-serializable records
        df_reset = combined.reset_index()
        records = df_reset.to_dict(orient="records")

        for record in records:
            for key, value in record.items():
                if isinstance(value, (pd.Timestamp, datetime)):
                    record[key] = value.isoformat()
                elif pd.isna(value):
                    record[key] = None

        return {"count": len(records), "data": records}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading values: {str(e)}")


@app.put("/values", response_model=UpdateRecordsResponse)
async def update_records(request_body: UpdateRecordsRequest, request: Request):
    """
    Update one or more records (flat or overlapping series).

    Identify the series by series_id OR by name(+labels).

    **Flat series**: In-place update by (series_id, valid_time).
    **Overlapping series**: Creates new version. Three lookup methods:
    - batch_id + valid_time: latest version in that batch
    - known_time + valid_time: exact version lookup
    - just valid_time: latest version overall

    Tri-state updates:
    - Omit a field to leave it unchanged
    - Set to None to explicitly clear the field
    - Set to a value to update it
    """
    try:
        with _get_conn(request) as conn:
            # Build update dicts, resolving name+labels → series_id
            updates = []
            # Cache name+labels → series_id to avoid repeated lookups
            _name_labels_cache: Dict[tuple, int] = {}

            for req_update in request_body.updates:
                valid_time = _ensure_tz(req_update.valid_time)
                known_time = _ensure_tz(req_update.known_time)
                provided_fields = req_update.model_dump(exclude_unset=True)

                # Resolve series_id from series_id or name+labels
                if req_update.series_id is not None:
                    resolved_id = req_update.series_id
                elif req_update.name is not None:
                    cache_key = (req_update.name, tuple(sorted(req_update.labels.items())))
                    if cache_key in _name_labels_cache:
                        resolved_id = _name_labels_cache[cache_key]
                    else:
                        series_list = _resolve_series(
                            conn, name=req_update.name,
                            labels=req_update.labels if req_update.labels else None,
                        )
                        if len(series_list) == 0:
                            raise HTTPException(
                                status_code=404,
                                detail=f"No series found for name='{req_update.name}', labels={req_update.labels}",
                            )
                        if len(series_list) > 1:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Multiple series ({len(series_list)}) match name='{req_update.name}', labels={req_update.labels}. Use series_id or more specific labels.",
                            )
                        resolved_id = series_list[0]["series_id"]
                        _name_labels_cache[cache_key] = resolved_id
                else:
                    raise HTTPException(
                        status_code=400,
                        detail="Each update must include 'series_id' or 'name' (+labels) to identify the series.",
                    )

                update_dict = {
                    "valid_time": valid_time,
                    "series_id": resolved_id,
                }

                if req_update.batch_id is not None:
                    update_dict["batch_id"] = req_update.batch_id
                if known_time is not None:
                    update_dict["known_time"] = known_time

                if "value" in provided_fields:
                    update_dict["value"] = req_update.value
                if "annotation" in provided_fields:
                    update_dict["annotation"] = req_update.annotation
                if "tags" in provided_fields:
                    update_dict["tags"] = req_update.tags
                if "changed_by" in provided_fields:
                    update_dict["changed_by"] = req_update.changed_by

                updates.append(update_dict)

            outcome = db.update.update_records(conn, updates=updates)

        updated = []
        for r in outcome["updated"]:
            item = {}
            for k, v in r.items():
                item[k] = v.isoformat() if isinstance(v, datetime) else v
            updated.append(item)

        skipped = []
        for r in outcome["skipped_no_ops"]:
            item = {}
            for k, v in r.items():
                item[k] = v.isoformat() if isinstance(v, datetime) else v
            skipped.append(item)

        return UpdateRecordsResponse(updated=updated, skipped_no_ops=skipped)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating records: {str(e)}")


@app.post("/series", response_model=CreateSeriesResponse)
async def create_series(request_body: CreateSeriesRequest, request: Request):
    """
    Create a new time series.

    Series identity is determined by (name, labels). Two series with the same name
    but different labels are different series.
    """
    try:
        with _get_conn(request) as conn:
            series_id = db.series.create_series(
                conn,
                name=request_body.name,
                description=request_body.description,
                unit=request_body.unit,
                labels=request_body.labels,
                overlapping=request_body.overlapping,
                retention=request_body.retention,
            )

        return CreateSeriesResponse(series_id=series_id, message="Series created successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating series: {str(e)}")


@app.get("/series", response_model=List[SeriesInfo])
async def list_series(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by series name"),
    labels: Optional[str] = Query(None, description="Filter by labels (JSON, e.g. '{\"site\":\"Gotland\"}')"),
    unit: Optional[str] = Query(None, description="Filter by unit"),
    series_id: Optional[int] = Query(None, description="Filter by series_id"),
):
    """
    List time series, optionally filtered by name, labels, unit, or series_id.

    Returns a list of series with their metadata.
    """
    try:
        label_filters = _parse_labels(labels)

        with _get_conn(request) as conn:
            series_list = _resolve_series(conn, name=name, labels=label_filters, series_id=series_id, unit=unit)

        return [
            SeriesInfo(
                series_id=s["series_id"],
                name=s["name"],
                description=s["description"],
                unit=s["unit"],
                labels=s["labels"],
                overlapping=s["overlapping"],
                retention=s["retention"],
            )
            for s in series_list
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing series: {str(e)}")


@app.get("/series/labels")
async def list_labels(
    request: Request,
    label_key: str = Query(..., description="The label key to get unique values for"),
    name: Optional[str] = Query(None, description="Filter by series name"),
    labels: Optional[str] = Query(None, description="Filter by labels (JSON)"),
):
    """
    List unique values for a specific label key across matching series.

    Example: GET /series/labels?label_key=site&name=wind_power
    Returns: {"label_key": "site", "values": ["Gotland", "Aland"]}
    """
    try:
        label_filters = _parse_labels(labels)

        with _get_conn(request) as conn:
            series_list = _resolve_series(conn, name=name, labels=label_filters)

        values = set()
        for s in series_list:
            lbl = s["labels"] or {}
            if label_key in lbl:
                values.add(lbl[label_key])

        return {"label_key": label_key, "values": sorted(values)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing labels: {str(e)}")


@app.get("/series/count")
async def count_series(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by series name"),
    labels: Optional[str] = Query(None, description="Filter by labels (JSON)"),
    unit: Optional[str] = Query(None, description="Filter by unit"),
):
    """
    Count time series matching the filters.

    Returns: {"count": 42}
    """
    try:
        label_filters = _parse_labels(labels)

        with _get_conn(request) as conn:
            series_list = _resolve_series(conn, name=name, labels=label_filters, unit=unit)

        return {"count": len(series_list)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error counting series: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
