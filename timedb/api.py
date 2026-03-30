"""
FastAPI application for timedb - REST API

Provides REST API endpoints for time series database operations.

The API exposes endpoints for:
- Creating and managing time series (POST /series, GET /series)
- Inserting time series data (POST /values)
- Reading/querying time series data (GET /values)
- Discovering labels and counts (GET /series/labels, GET /series/count)

Interactive documentation:
    - Swagger UI: /docs
    - ReDoc: /redoc

Environment:
    Requires TIMEDB_PG_DSN and TIMEDB_CH_URL environment variables.
"""
import io
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field, ValidationError
import polars as pl

from .sdk import TimeDataClient


ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


def _to_arrow_response(table: pa.Table) -> Response:
    """Serialize a PyArrow table as an Arrow IPC stream response."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return Response(content=sink.getvalue(), media_type=ARROW_CONTENT_TYPE)


def _parse_arrow_body(body: bytes) -> pa.Table:
    """Parse an Arrow IPC stream body.

    NOTE: The client must use the IPC *stream* format:
      - Polars:  df.write_ipc_stream(buf)
      - PyArrow: pa.ipc.new_stream(sink, schema)
    The Arrow *file* format (write_ipc / Feather) is NOT compatible.
    """
    return pa.ipc.open_stream(io.BytesIO(body)).read_all()


def _get_pg_conninfo() -> str:
    conninfo = os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        raise RuntimeError("Set TIMEDB_PG_DSN environment variable.")
    return conninfo


def _get_ch_url() -> str:
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not ch_url:
        raise RuntimeError("Set TIMEDB_CH_URL environment variable.")
    return ch_url


# =============================================================================
# Lifespan: SDK client initialization
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize TimeDataClient with connection pool for the API's lifetime."""
    app.state.td = TimeDataClient(
        pg_conninfo=_get_pg_conninfo(),
        ch_url=_get_ch_url(),
    )
    yield
    app.state.td.close()


def _get_client(request: Request) -> TimeDataClient:
    """Get the SDK client from app state."""
    return request.app.state.td


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
        knowledge_time: Time of knowledge (defaults to now(), important for overlapping series)
        run_params: Custom parameters to store with the run
        data: Array of data points to insert
    """
    name: Optional[str] = Field(None, description="Series name (resolve by name+labels)")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels for series resolution")
    series_id: Optional[int] = Field(None, description="Direct series_id (alternative to name+labels)")
    workflow_id: str = Field(default="api-workflow", description="Workflow identifier")
    knowledge_time: Optional[datetime] = Field(None, description="Time of knowledge (defaults to now())")
    run_params: Optional[Dict[str, Any]] = Field(None, description="Custom run parameters")
    data: List[DataPoint] = Field(default_factory=list, description="Data points to insert")


class InsertResponse(BaseModel):
    """Response after inserting data."""
    run_id: Optional[uuid.UUID] = Field(None, description="Run UUID")
    series_id: int
    rows_inserted: int


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


class CreateSeriesManyRequest(BaseModel):
    """Request to bulk-create multiple time series."""
    series: List[CreateSeriesRequest]


class CreateSeriesManyResponse(BaseModel):
    """Response after bulk-creating series. series_ids are in the same order as the input."""
    series_ids: List[int]


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
    version="0.1.4",
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
        "version": "0.1.4",
        "description": "REST API for reading and writing time series data",
        "endpoints": {
            "insert_values": "POST /values - Insert single-series data (JSON or Arrow IPC stream)",
            "write_values": "POST /write - Insert multi-series data in long format (JSON or Arrow IPC stream)",
            "read_values": "GET /values - Read single-series values (JSON or Arrow IPC stream)",
            "read_multi": "POST /read - Read multi-series data via manifest (JSON or Arrow IPC stream)",
            "create_series": "POST /series - Create a new time series",
            "create_series_many": "POST /series/many - Bulk create multiple series",
            "list_series": "GET /series - List/filter time series",
            "series_labels": "GET /series/labels - List unique label values",
            "series_count": "GET /series/count - Count matching series",
        },
        "admin_note": "Schema creation/deletion must be done through CLI or SDK, not through the API.",
    }
    json_str = json.dumps(data)
    return Response(content=json_str.encode("utf-8"), media_type="application/json")


@app.post(
    "/values",
    response_model=InsertResponse,
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {"schema": InsertRequest.model_json_schema()},
                "application/vnd.apache.arrow.stream": {
                    "schema": {
                        "type": "string",
                        "format": "binary",
                        "description": (
                            "Arrow IPC stream bytes. "
                            "Required columns: valid_time (timestamp[us,UTC]), value (float64). "
                            "Optional: valid_time_end, knowledge_time. "
                            "Client: df.write_ipc_stream(buf) in Polars or pa.ipc.new_stream() in PyArrow."
                        ),
                    }
                },
            }
        }
    },
)
async def insert_values(
    request: Request,
    # Arrow-path parameters (query params; ignored when Content-Type is JSON)
    series_id: Optional[int] = Query(None, description="[Arrow] Direct series_id"),
    name: Optional[str] = Query(None, description="[Arrow] Series name"),
    labels: Optional[str] = Query(None, description="[Arrow] Labels JSON, e.g. '{\"site\":\"Gotland\"}'"),
    workflow_id: str = Query("api-workflow", description="[Arrow] Workflow identifier"),
    knowledge_time: Optional[datetime] = Query(None, description="[Arrow] Knowledge time (defaults to now)"),
    run_params: Optional[str] = Query(None, description="[Arrow] Run params JSON"),
):
    """
    Insert time series data for a single series.

    **JSON** (`Content-Type: application/json`): Send an `InsertRequest` body.
    Series is identified by `name`+`labels` or `series_id` in the body.

    **Arrow IPC stream** (`Content-Type: application/vnd.apache.arrow.stream`):
    Send a raw Arrow IPC stream as the body. Identify the series and provide
    run metadata via query parameters. The Arrow table must have at least
    `valid_time` (timestamp[us,UTC]) and `value` (float64) columns.
    Optional columns: `valid_time_end`, `knowledge_time`.

    Routing is automatic:
    - Flat series: no run created (run_id=null in response)
    - Overlapping series: run created with knowledge_time tracking
    """
    try:
        td = _get_client(request)
        content_type = request.headers.get("content-type", "application/json")

        if ARROW_CONTENT_TYPE in content_type:
            # --- Arrow IPC path ---
            if series_id is None and name is None:
                raise HTTPException(
                    status_code=400,
                    detail="[Arrow] Provide 'series_id' or 'name' query param to identify the target series.",
                )
            label_filters = _parse_labels(labels)
            kt = _ensure_tz(knowledge_time)
            bp = json.loads(run_params) if run_params else None

            body = await request.body()
            arrow_table = _parse_arrow_body(body)
            df = pl.from_arrow(arrow_table)

            if series_id is not None:
                collection = td.get_series(series_id=series_id)
            else:
                collection = td.get_series(name=name)
                if label_filters:
                    collection = collection.where(**label_filters)

            result = collection.insert(df, knowledge_time=kt, workflow_id=workflow_id, run_params=bp)
            return InsertResponse(run_id=result.run_id, series_id=result.series_id, rows_inserted=len(df))

        else:
            # --- JSON path ---
            try:
                request_body = InsertRequest.model_validate(await request.json())
            except ValidationError as e:
                raise HTTPException(status_code=422, detail=e.errors())

            if request_body.series_id is None and request_body.name is None:
                raise HTTPException(
                    status_code=400,
                    detail="Provide either 'series_id' or 'name' (+labels) to identify the target series.",
                )
            if not request_body.data:
                raise HTTPException(status_code=400, detail="'data' must contain at least one data point.")

            kt = _ensure_tz(request_body.knowledge_time)

            if request_body.series_id is not None:
                collection = td.get_series(series_id=request_body.series_id)
            else:
                collection = td.get_series(name=request_body.name)
                if request_body.labels:
                    collection = collection.where(**request_body.labels)

            rows: List[Dict[str, Any]] = []
            for dp in request_body.data:
                valid_time = _ensure_tz(dp.valid_time)
                if dp.valid_time_end is not None:
                    rows.append({
                        "valid_time": valid_time,
                        "valid_time_end": _ensure_tz(dp.valid_time_end),
                        "value": dp.value,
                    })
                else:
                    rows.append({"valid_time": valid_time, "value": dp.value})

            has_end = any("valid_time_end" in r for r in rows)
            schema: Dict[str, Any] = {"valid_time": pl.Datetime("us", "UTC")}
            if has_end:
                schema["valid_time_end"] = pl.Datetime("us", "UTC")
            schema["value"] = pl.Float64
            df = pl.DataFrame(rows, schema=schema)

            result = collection.insert(
                df,
                knowledge_time=kt,
                workflow_id=request_body.workflow_id,
                run_params=request_body.run_params,
            )
            return InsertResponse(
                run_id=result.run_id,
                series_id=result.series_id,
                rows_inserted=len(request_body.data),
            )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting values: {str(e)}")


@app.get("/values")
async def read_values(
    request: Request,
    name: Optional[str] = Query(None, description="Filter by series name"),
    labels: Optional[str] = Query(None, description="Filter by labels (JSON, e.g. '{\"site\":\"Gotland\"}')"),
    series_id: Optional[int] = Query(None, description="Filter by series_id"),
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_known: Optional[datetime] = Query(None, description="Start of knowledge_time range (ISO format)"),
    end_known: Optional[datetime] = Query(None, description="End of knowledge_time range (ISO format)"),
    overlapping: bool = Query(False, description="Return one row per (knowledge_time, valid_time). Raises an error for flat series."),
    include_updates: bool = Query(False, description="Expose the correction chain with change_time, changed_by, and annotation."),
):
    """
    Read time series values.

    Filter by series name, labels, and/or series_id.
    Time range filtering via start_valid/end_valid and start_known/end_known.

    overlapping=false, include_updates=false (default): latest value per valid_time.
    overlapping=false, include_updates=true: correction chain for the winning forecast run.
    overlapping=true, include_updates=false: one row per (knowledge_time, valid_time).
    overlapping=true, include_updates=true: full correction chain across all forecast runs.

    Set `Accept: application/vnd.apache.arrow.stream` to receive an Arrow IPC stream
    instead of JSON. Client: `pl.read_ipc_stream(response.content)` (Polars) or
    `pa.ipc.open_stream(io.BytesIO(response.content))` (PyArrow).
    """
    try:
        label_filters = _parse_labels(labels)
        start_valid = _ensure_tz(start_valid)
        end_valid = _ensure_tz(end_valid)
        start_known = _ensure_tz(start_known)
        end_known = _ensure_tz(end_known)

        # Build SeriesCollection via SDK
        td = _get_client(request)
        collection = td.get_series(name=name, series_id=series_id)
        if label_filters:
            collection = collection.where(**label_filters)

        # Read via SDK (handles all routing logic internally)
        ts = collection.read(
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            overlapping=overlapping,
            include_updates=include_updates,
        )

        # Arrow IPC response
        accept = request.headers.get("accept", "application/json")
        if ARROW_CONTENT_TYPE in accept:
            return _to_arrow_response(ts.to_pyarrow())

        # JSON response
        if ts.num_rows == 0:
            return {"count": 0, "data": []}

        df_pl = ts.to_polars()
        dt_cols = [c for c, t in zip(df_pl.columns, df_pl.dtypes) if isinstance(t, pl.Datetime)]
        df_pl = df_pl.with_columns([pl.col(c).dt.to_string("%Y-%m-%dT%H:%M:%S%z") for c in dt_cols])
        records = df_pl.to_dicts()

        return {"count": len(records), "data": records}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading values: {str(e)}")


@app.post("/read")
async def read_multi(
    request: Request,
    name_col: Optional[str] = Query(None, description="Column whose values map to series name (defaults to 'name', mutually exclusive with series_col)"),
    label_cols: Optional[str] = Query(
        None,
        description="Comma-separated label column names. Omit to auto-infer (mutually exclusive with series_col).",
    ),
    series_col: Optional[str] = Query(None, description="Column whose values are integer series IDs (bypasses name/label resolution, mutually exclusive with name_col/label_cols)"),
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_known: Optional[datetime] = Query(None, description="Start of knowledge_time range (ISO format)"),
    end_known: Optional[datetime] = Query(None, description="End of knowledge_time range (ISO format)"),
    overlapping: bool = Query(False, description="Return all forecast versions for overlapping series. Flat series are unaffected."),
    include_updates: bool = Query(False, description="Expose the correction chain with change_time, changed_by, and annotation."),
):
    """
    Read multi-series data using a manifest DataFrame.

    The manifest specifies which series to read using the same routing columns
    as ``POST /write``: either *name_col*/*label_cols* (resolve by name and
    labels) or *series_col* (route by integer series ID).

    **Arrow IPC stream** (`Content-Type: application/vnd.apache.arrow.stream`):
    Send a manifest as Arrow IPC stream body. The manifest should contain only
    routing columns (name + labels, or series_id).
    Client: ``manifest.write_ipc_stream(buf)`` (Polars).

    **JSON** (`Content-Type: application/json`):
    Send a list of row dicts, e.g.:
    ```json
    [
      {"metric": "wind_power", "site": "Gotland"},
      {"metric": "wind_power", "site": "Oslo"}
    ]
    ```

    Set ``Accept: application/vnd.apache.arrow.stream`` to receive an Arrow IPC
    stream instead of JSON.

    Returns a long-format DataFrame with columns:
    ``[name_col, *label_cols, unit, series_id, <data_columns>]``.
    """
    try:
        td = _get_client(request)
        content_type = request.headers.get("content-type", "application/json")

        parsed_label_cols = (
            [c.strip() for c in label_cols.split(",") if c.strip()] if label_cols is not None else None
        )

        sv = _ensure_tz(start_valid)
        ev = _ensure_tz(end_valid)
        sk = _ensure_tz(start_known)
        ek = _ensure_tz(end_known)

        if ARROW_CONTENT_TYPE in content_type:
            body = await request.body()
            arrow_table = _parse_arrow_body(body)
            manifest = pl.from_arrow(arrow_table)
        else:
            json_data = await request.json()
            if not isinstance(json_data, list):
                raise HTTPException(status_code=422, detail="JSON body must be a list of row dicts.")
            manifest = pl.DataFrame(json_data)

        result_df = td.read(
            manifest,
            name_col=name_col,
            label_cols=parsed_label_cols,
            series_col=series_col,
            start_valid=sv,
            end_valid=ev,
            start_known=sk,
            end_known=ek,
            overlapping=overlapping,
            include_updates=include_updates,
        )

        accept = request.headers.get("accept", "application/json")
        if ARROW_CONTENT_TYPE in accept:
            return _to_arrow_response(result_df.to_arrow())

        if result_df.is_empty():
            return {"count": 0, "data": []}

        dt_cols = [c for c, t in zip(result_df.columns, result_df.dtypes) if isinstance(t, pl.Datetime)]
        result_df = result_df.with_columns([pl.col(c).dt.to_string("%Y-%m-%dT%H:%M:%S%z") for c in dt_cols])
        records = result_df.to_dicts()

        return {"count": len(records), "data": records}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading values: {str(e)}")


@app.post("/write")
async def write_values(
    request: Request,
    name_col: Optional[str] = Query(None, description="Column whose values map to series name (defaults to 'name', mutually exclusive with series_col)"),
    label_cols: Optional[str] = Query(
        None,
        description="Comma-separated label column names. Omit to auto-infer (mutually exclusive with series_col).",
    ),
    series_col: Optional[str] = Query(None, description="Column whose values are integer series IDs (bypasses name/label resolution, mutually exclusive with name_col/label_cols)"),
    run_cols: Optional[str] = Query(None, description="Comma-separated run column names"),
    knowledge_time: Optional[datetime] = Query(None, description="Broadcast knowledge_time for all rows"),
    unit: Optional[str] = Query(None, description="Unit of incoming values (auto-converts to canonical unit)"),
    workflow_id: Optional[str] = Query(None, description="Workflow identifier"),
    run_start_time: Optional[datetime] = Query(None, description="Run start time"),
    run_finish_time: Optional[datetime] = Query(None, description="Run finish time"),
    run_params: Optional[str] = Query(None, description="Run params JSON string"),
):
    """
    Insert multi-series data in long/tidy format.

    All series must already exist. Route by name/labels (default) or by series ID
    (pass ``series_col``). The two modes are mutually exclusive.

    **Arrow IPC stream** (`Content-Type: application/vnd.apache.arrow.stream`):
    Arrow table with `valid_time`, `value`, and routing columns (name_col/label_cols or series_col).
    Client: `df.write_ipc_stream(buf)` (Polars) or `pa.ipc.new_stream()` (PyArrow).

    **JSON** (`Content-Type: application/json`):
    List of row dicts, e.g.:
    ```json
    [
      {"name": "wind_power", "site": "Gotland", "valid_time": "2024-01-01T00:00:00Z", "value": 100.0},
      {"name": "wind_power", "site": "Aland",   "valid_time": "2024-01-01T00:00:00Z", "value": 95.0}
    ]
    ```

    Returns a list of insert results, one per unique (series_id, run_id) combination.
    """
    try:
        td = _get_client(request)
        content_type = request.headers.get("content-type", "application/json")

        # Parse column config
        parsed_label_cols = (
            [c.strip() for c in label_cols.split(",") if c.strip()] if label_cols is not None else None
        )
        parsed_run_cols = (
            [c.strip() for c in run_cols.split(",") if c.strip()] if run_cols is not None else None
        )
        bp = json.loads(run_params) if run_params else None
        kt = _ensure_tz(knowledge_time)
        bst = _ensure_tz(run_start_time)
        bft = _ensure_tz(run_finish_time)

        if ARROW_CONTENT_TYPE in content_type:
            body = await request.body()
            arrow_table = _parse_arrow_body(body)
            df = pl.from_arrow(arrow_table)
        else:
            json_data = await request.json()
            if not isinstance(json_data, list):
                raise HTTPException(status_code=422, detail="JSON body must be a list of row dicts.")
            df = pl.DataFrame(json_data)
            # Cast ISO datetime strings to proper timezone-aware timestamps
            for dt_col in ["valid_time", "valid_time_end", "knowledge_time"]:
                if dt_col in df.columns and df[dt_col].dtype == pl.Utf8:
                    df = df.with_columns(
                        pl.col(dt_col).str.to_datetime(time_unit="us", time_zone="UTC")
                    )

        results = td.write(
            df,
            name_col=name_col,
            label_cols=parsed_label_cols,
            series_col=series_col,
            run_cols=parsed_run_cols,
            knowledge_time=kt,
            unit=unit,
            workflow_id=workflow_id,
            run_start_time=bst,
            run_finish_time=bft,
            run_params=bp,
        )

        return [
            {
                "run_id": str(r.run_id) if r.run_id else None,
                "series_id": r.series_id,
                "workflow_id": r.workflow_id,
            }
            for r in results
        ]

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing values: {str(e)}")


@app.post("/series", response_model=CreateSeriesResponse)
async def create_series(request_body: CreateSeriesRequest, request: Request):
    """
    Create a new time series.

    Series identity is determined by (name, labels). Two series with the same name
    but different labels are different series.
    """
    try:
        td = _get_client(request)
        series_id = td.create_series(
            request_body.name,
            description=request_body.description,
            unit=request_body.unit,
            labels=request_body.labels,
            overlapping=request_body.overlapping,
            retention=request_body.retention,
        )

        return CreateSeriesResponse(series_id=series_id, message="Series created successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating series: {str(e)}")


@app.post("/series/many", response_model=CreateSeriesManyResponse)
async def create_series_many(request_body: CreateSeriesManyRequest, request: Request):
    """
    Bulk get-or-create multiple time series in a single round-trip.

    Returns series_ids in the same order as the input list.
    """
    try:
        td = _get_client(request)
        ids = td.create_series_many([s.model_dump() for s in request_body.series])
        return CreateSeriesManyResponse(series_ids=ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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

        td = _get_client(request)
        collection = td.get_series(name=name, unit=unit, series_id=series_id)
        if label_filters:
            collection = collection.where(**label_filters)

        series_list = collection.list_series()

        return [
            SeriesInfo(
                series_id=s["series_id"],
                name=s["name"],
                description=s.get("description"),
                unit=s["unit"],
                labels=s.get("labels", {}),
                overlapping=s["overlapping"],
                retention=s["retention"],
            )
            for s in series_list
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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

        td = _get_client(request)
        collection = td.get_series(name=name)
        if label_filters:
            collection = collection.where(**label_filters)

        values = collection.list_labels(label_key)

        return {"label_key": label_key, "values": sorted(values)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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

        td = _get_client(request)
        collection = td.get_series(name=name, unit=unit)
        if label_filters:
            collection = collection.where(**label_filters)

        count = collection.count()

        return {"count": count}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error counting series: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
