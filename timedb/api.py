"""
FastAPI application for timedb - REST API

Provides REST API endpoints for time series database operations.

The API exposes endpoints for:
- Creating and managing time series
- Reading/querying time series data
- Uploading batches of data
- Updating existing records

Interactive documentation:
    - Swagger UI: /docs
    - ReDoc: /redoc

Environment:
    Requires TIMEDB_DSN or DATABASE_URL environment variable
    for database connection.
"""
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
import psycopg
import pandas as pd

from . import db

# Load .env file but DO NOT override existing environment variables
# This allows users to set TIMEDB_DSN/DATABASE_URL before importing this module
load_dotenv(override=False)

# Database connection string from environment
def get_dsn() -> str:
    """Get database connection string from environment variables."""
    dsn = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise HTTPException(
            status_code=500,
            detail="Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return dsn


# Pydantic models for request/response
class ValueRow(BaseModel):
    """A single value row for insertion.

    Represents one data point to insert into the database. Supports both
    point-in-time values (just valid_time) and interval values
    (valid_time and valid_time_end).

    Attributes:
        valid_time: The time this value is valid for
        value_key: Identifier for this value within the batch
        value: The numeric value (optional, can be null)
        valid_time_end: End of validity interval for interval-based data
        series_id: Optional UUID of existing series. If not provided, a new series
                  will be created based on value_key.
    """
    valid_time: datetime
    value_key: str
    value: Optional[float] = None
    valid_time_end: Optional[datetime] = None  # For interval values
    series_id: Optional[str] = None  # Optional series_id. If not provided, a new series will be created.

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateBatchRequest(BaseModel):
    """Request to create a batch with values.

    A batch represents a logical group of data points, typically all inserted
    at the same time or as part of the same workflow. For overlapping series,
    the known_time tracks when this data became available.

    Attributes:
        workflow_id: Workflow identifier (defaults to 'api-workflow')
        batch_start_time: Start time of the batch (when data generation started)
        batch_finish_time: End time of the batch (optional)
        known_time: Time of knowledge - when the data became available.
                   Defaults to insertion time if not provided.
                   Important for overlapping series to track forecast updates.
        batch_params: Custom parameters to store with the batch
        value_rows: Array of value rows to insert
        series_descriptions: Optional dict mapping value_key to description.
                           Used when creating new series automatically.
    """
    workflow_id: Optional[str] = Field(default="api-workflow", description="Workflow identifier (defaults to 'api-workflow')")
    batch_start_time: datetime
    batch_finish_time: Optional[datetime] = None
    known_time: Optional[datetime] = None  # Time of knowledge - defaults to inserted_at (now()) if not provided
    batch_params: Optional[Dict[str, Any]] = None
    value_rows: List[ValueRow] = Field(default_factory=list)
    series_descriptions: Optional[Dict[str, str]] = Field(None, description="Optional dict mapping value_key to description. Used when creating new series.")

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateBatchResponse(BaseModel):
    """Response after creating a batch."""
    batch_id: str
    message: str
    series_ids: Dict[str, str]  # Maps name to series_id (UUID as string)


class RecordUpdateRequest(BaseModel):
    """Request to update a record.

    Supports both flat and overlapping series with different update semantics:

    **Flat series**: In-place update by (series_id, valid_time).
    - No batch_id or known_time needed
    - Updates the value, annotation, or tags at that timestamp

    **Overlapping series**: Creates new version with known_time=now().
    Lookup priority (use what you have available):
    - known_time + valid_time: Exact version lookup
    - batch_id + valid_time: Latest version in that batch
    - Just valid_time: Latest version overall

    For value, annotation, and tags:
    - Omit the field to leave it unchanged
    - Set to null to explicitly clear it
    - Set to a value to update it

    Attributes:
        valid_time: The valid time of the record to update
        series_id: UUID of the series to update
        batch_id: For overlapping: target specific batch
        known_time: For overlapping: target specific version
        value: New value (omit to leave unchanged, null to clear, or provide a value)
        annotation: Text annotation (omit to leave unchanged, null to clear)
        tags: List of tags (omit to leave unchanged, null or [] to clear)
    """
    valid_time: datetime
    series_id: str
    batch_id: Optional[str] = Field(default=None, description="For overlapping: target specific batch")
    known_time: Optional[datetime] = Field(default=None, description="For overlapping: target specific version")
    value: Optional[float] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    annotation: Optional[str] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    tags: Optional[List[str]] = Field(default=None, description="Omit to leave unchanged, null or [] to clear, or provide tags")

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class UpdateRecordsRequest(BaseModel):
    """Request to update multiple records."""
    updates: List[RecordUpdateRequest]


class UpdateRecordsResponse(BaseModel):
    """Response after updating records."""
    updated: List[Dict[str, Any]]
    skipped_no_ops: List[Dict[str, Any]]


class ErrorResponse(BaseModel):
    """Error response model."""
    detail: str


class CreateSeriesRequest(BaseModel):
    """Request to create a new time series.

    Attributes:
        name: Human-readable identifier for the series (e.g., 'wind_power_forecast')
        description: Optional description of the series
        unit: Canonical unit for the series (e.g., 'MW', 'kW', 'MWh', 'dimensionless')
        labels: Labels to differentiate series with the same name
               (e.g., {'site': 'Gotland', 'turbine': 'T01'})
        data_class: 'flat' for immutable facts or 'overlapping' for versioned forecasts
        retention: 'short' (6mo), 'medium' (3yr), or 'long' (5yr). Only relevant for overlapping.
    """
    name: str = Field(..., description="Human-readable identifier for the series (e.g., 'wind_power_forecast')")
    description: Optional[str] = Field(None, description="Optional description of the series")
    unit: str = Field(default="dimensionless", description="Canonical unit for the series (e.g., 'MW', 'kW', 'MWh', 'dimensionless')")
    labels: Dict[str, str] = Field(default_factory=dict, description="Labels to differentiate series with the same name (e.g., {'site': 'Gotland', 'turbine': 'T01'})")
    data_class: str = Field(default="flat", description="'flat' for immutable facts or 'overlapping' for versioned forecasts")
    retention: str = Field(default="medium", description="'short', 'medium', or 'long' retention (only relevant for overlapping)")


class CreateSeriesResponse(BaseModel):
    """Response after creating a series.

    Attributes:
        series_id: UUID of the newly created series
        message: Status message
    """
    series_id: str
    message: str


class SeriesInfo(BaseModel):
    """Information about a time series."""
    name: str
    description: Optional[str] = None
    unit: str
    labels: Dict[str, str] = Field(default_factory=dict)
    data_class: str = "flat"
    retention: str = "medium"


# FastAPI app
app = FastAPI(
    title="TimeDB API",
    description="REST API for time series database operations",
    version="0.1.1"
)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    import json
    data = {
        "name": "TimeDB API",
        "version": "0.1.1",
        "description": "REST API for reading and writing time series data",
        "endpoints": {
            "read_values": "GET /values - Read time series values",
            "upload_timeseries": "POST /upload - Upload time series data (create a new run with values)",
            "create_series": "POST /series - Create a new time series",
            "list_timeseries": "GET /list_timeseries - List all time series (series_id -> series_key mapping)",
            "update_records": "PUT /values - Update existing time series records",
        },
        "admin_note": "Schema creation/deletion must be done through CLI or SDK, not through the API."
    }
    # Manually serialize to avoid FastAPI's jsonable_encoder recursion issue
    json_str = json.dumps(data)
    return Response(content=json_str.encode('utf-8'), media_type="application/json")


@app.get("/values", response_model=Dict[str, Any])
async def read_values(
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_known: Optional[datetime] = Query(None, description="Start of known_time range (ISO format)"),
    end_known: Optional[datetime] = Query(None, description="End of known_time range (ISO format)"),
    mode: str = Query("flat", description="Query mode: 'flat' or 'overlapping'"),
):
    """
    Read time series values from the database.

    Returns values filtered by valid_time and/or known_time ranges.
    All datetime parameters should be in ISO format (e.g., 2025-01-01T00:00:00Z).

    Modes:
    - "flat": Returns latest value per (valid_time, series_id), determined by most recent known_time
    - "overlapping": Returns all forecast revisions with their known_time, useful for backtesting
    """
    try:
        dsn = get_dsn()

        # Validate mode parameter
        if mode not in ["flat", "overlapping"]:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")

        # Convert timezone-naive datetimes to UTC if needed
        if start_valid and start_valid.tzinfo is None:
            start_valid = start_valid.replace(tzinfo=timezone.utc)
        if end_valid and end_valid.tzinfo is None:
            end_valid = end_valid.replace(tzinfo=timezone.utc)
        if start_known and start_known.tzinfo is None:
            start_known = start_known.replace(tzinfo=timezone.utc)
        if end_known and end_known.tzinfo is None:
            end_known = end_known.replace(tzinfo=timezone.utc)

        if mode == "flat":
            df = db.read.read_values_flat(
                dsn,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
            )
        elif mode == "overlapping":
            df = db.read.read_values_overlapping(
                dsn,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
            )
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")
        
        # Convert DataFrame to JSON-serializable format
        df_reset = df.reset_index()
        records = df_reset.to_dict(orient="records")
        
        # Convert datetime objects to ISO format strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
                elif isinstance(value, datetime):
                    record[key] = value.isoformat()
                elif pd.isna(value):
                    record[key] = None
        
        return {
            "count": len(records),
            "data": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading values: {str(e)}")


@app.post("/upload", response_model=CreateBatchResponse)
async def upload_timeseries(
    request: CreateBatchRequest,
):
    """
    Upload time series data (create a new batch with associated values).
    
    This endpoint creates a batch entry and inserts value rows. It returns
    the series_ids for all series that were uploaded, which can be used
    in subsequent API calls to query and update the data.
    """
    try:
        dsn = get_dsn()
        batch_id = uuid.uuid4()
        
        # Ensure timezone-aware datetimes
        if request.batch_start_time.tzinfo is None:
            batch_start_time = request.batch_start_time.replace(tzinfo=timezone.utc)
        else:
            batch_start_time = request.batch_start_time
            
        if request.batch_finish_time is not None:
            if request.batch_finish_time.tzinfo is None:
                batch_finish_time = request.batch_finish_time.replace(tzinfo=timezone.utc)
            else:
                batch_finish_time = request.batch_finish_time
        else:
            batch_finish_time = None
        
        if request.known_time is not None:
            if request.known_time.tzinfo is None:
                known_time = request.known_time.replace(tzinfo=timezone.utc)
            else:
                known_time = request.known_time
        else:
            known_time = None  # Will default to now() in insert_batch
        
        # Single-tenant: hardcoded default tenant
        tenant_id = uuid.UUID("00000000-0000-0000-0000-000000000000")

        # workflow_id defaults to "api-workflow" via Field(default="api-workflow")
        workflow_id = request.workflow_id or "api-workflow"
        
        # Get or create series for each unique value_key
        # Convert value_rows to the format expected by insert_run_with_values:
        # - (tenant_id, valid_time, series_id, value) for point-in-time
        # - (tenant_id, valid_time, valid_time_end, series_id, value) for interval
        import psycopg
        with psycopg.connect(dsn) as conn:
            series_mapping = {}  # Maps value_key to series_id (UUID)
            series_ids_dict = {}  # Maps value_key to series_id (string) for response
            series_descriptions = request.series_descriptions or {}
            
            for row in request.value_rows:
                if row.value_key not in series_mapping:
                    # Check if series_id is provided in the row
                    if row.series_id:
                        try:
                            provided_series_id = uuid.UUID(row.series_id)
                            # Verify the series exists - just fetch series info
                            series_info = db.series.get_series_info(conn, provided_series_id)
                            series_id = provided_series_id
                        except ValueError as e:
                            raise HTTPException(status_code=400, detail=f"Invalid series_id format: {e}")
                    else:
                        # Create a new series using create_series
                        description = series_descriptions.get(row.value_key)
                        series_id = db.series.create_series(
                            conn,
                            name=row.value_key,
                            description=description,
                            unit="dimensionless",  # API doesn't support units yet
                        )
                    series_mapping[row.value_key] = series_id
                    series_ids_dict[row.value_key] = str(series_id)
        
        # Convert value_rows to the correct format
        value_rows = []
        for row in request.value_rows:
            series_id = series_mapping[row.value_key]
            if row.valid_time_end is not None:
                # Interval value: (tenant_id, valid_time, valid_time_end, series_id, value)
                value_rows.append((tenant_id, row.valid_time, row.valid_time_end, series_id, row.value))
            else:
                # Point-in-time value: (tenant_id, valid_time, series_id, value)
                value_rows.append((tenant_id, row.valid_time, series_id, row.value))
        
        db.insert.insert_batch_with_values(
            conninfo=dsn,
            batch_id=batch_id,
            tenant_id=tenant_id,
            workflow_id=workflow_id,
            batch_start_time=batch_start_time,
            batch_finish_time=batch_finish_time,
            known_time=known_time,
            value_rows=value_rows,
            batch_params=request.batch_params,
            changed_by=None,
        )
        
        return CreateBatchResponse(
            batch_id=str(batch_id),
            message="Batch created successfully",
            series_ids=series_ids_dict
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating batch: {str(e)}")


@app.put("/values", response_model=UpdateRecordsResponse)
async def update_records(
    request: UpdateRecordsRequest,
):
    """
    Update one or more records (flat or overlapping series).

    **Flat series**: In-place update by (series_id, valid_time).
    **Overlapping series**: Creates new version. Lookup by known_time, batch_id, or latest.

    This endpoint supports tri-state updates:
    - Omit a field to leave it unchanged
    - Set to None to explicitly clear the field
    - Set to a value to update it
    """
    try:
        dsn = get_dsn()

        # Convert request updates to update dicts
        updates = []
        for req_update in request.updates:
            # Parse UUIDs
            try:
                series_id_uuid = uuid.UUID(req_update.series_id)
                batch_id_uuid = uuid.UUID(req_update.batch_id) if req_update.batch_id else None
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format: {e}")

            # Ensure timezone-aware datetime
            valid_time = req_update.valid_time
            if valid_time.tzinfo is None:
                valid_time = valid_time.replace(tzinfo=timezone.utc)

            known_time = req_update.known_time
            if known_time and known_time.tzinfo is None:
                known_time = known_time.replace(tzinfo=timezone.utc)

            # Use model_dump to check which fields were actually provided
            provided_fields = req_update.model_dump(exclude_unset=True)

            # Build update dict - required fields
            update_dict = {
                "valid_time": valid_time,
                "series_id": series_id_uuid,
            }

            # Add optional lookup fields if provided
            if batch_id_uuid is not None:
                update_dict["batch_id"] = batch_id_uuid
            if known_time is not None:
                update_dict["known_time"] = known_time

            # Add optional update fields only if provided
            if "value" in provided_fields:
                update_dict["value"] = req_update.value

            if "annotation" in provided_fields:
                update_dict["annotation"] = req_update.annotation

            if "tags" in provided_fields:
                update_dict["tags"] = req_update.tags

            updates.append(update_dict)

        # Execute updates
        with psycopg.connect(dsn) as conn:
            outcome = db.update.update_records(conn, updates=updates)

        # Convert response to JSON-serializable format
        updated = []
        for r in outcome["updated"]:
            item = {
                "valid_time": r["valid_time"].isoformat(),
                "series_id": str(r["series_id"]),
            }
            # Include optional fields if present
            if "batch_id" in r and r["batch_id"] is not None:
                item["batch_id"] = str(r["batch_id"])
            if "overlapping_id" in r:
                item["overlapping_id"] = r["overlapping_id"]
            if "flat_id" in r:
                item["flat_id"] = r["flat_id"]
            updated.append(item)

        skipped = [
            {
                k: str(v) if isinstance(v, uuid.UUID) else v.isoformat() if isinstance(v, datetime) else v
                for k, v in item.items()
            }
            for item in outcome["skipped_no_ops"]
        ]

        return UpdateRecordsResponse(
            updated=updated,
            skipped_no_ops=skipped
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating records: {str(e)}")


@app.post("/series", response_model=CreateSeriesResponse)
async def create_series(
    request: CreateSeriesRequest,
):
    """
    Create a new time series.

    This endpoint creates a new series with the specified name, description, unit, and labels.
    A new series_id is generated and returned, which can be used in subsequent API calls.

    Note: Series identity is determined by (name, labels). Two series with the same name
    but different labels are different series.
    """
    try:
        dsn = get_dsn()
        import psycopg

        with psycopg.connect(dsn) as conn:
            series_id = db.series.create_series(
                conn,
                name=request.name,
                description=request.description,
                unit=request.unit,
                labels=request.labels,
                data_class=request.data_class,
                retention=request.retention,
            )

        return CreateSeriesResponse(
            series_id=str(series_id),
            message="Series created successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating series: {str(e)}")


@app.get("/list_timeseries", response_model=Dict[str, SeriesInfo])
async def list_timeseries():
    """
    List all time series available in the database.

    Returns a dictionary mapping series_id (as string) to series information
    including name, description, unit, and labels.
    This can be used in subsequent API calls to query and update data.
    """
    try:
        dsn = get_dsn()
        import psycopg

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT series_id, name, description, unit, labels, data_class, retention
                    FROM series_table
                    ORDER BY name
                    """
                )
                rows = cur.fetchall()

            # Build dictionary: series_id (string) -> SeriesInfo
            result = {
                str(series_id): SeriesInfo(
                    name=name,
                    description=description,
                    unit=unit,
                    labels=labels or {},
                    data_class=data_class,
                    retention=retention,
                )
                for series_id, name, description, unit, labels, data_class, retention in rows
            }
            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing time series: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

