"""
FastAPI application for timedb - MVP version
Provides REST API endpoints for time series database operations.
"""
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import Response
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
import psycopg
from psycopg import errors as psycopg_errors
from psycopg.rows import dict_row
import pandas as pd

from . import db
from .auth import get_current_user, CurrentUser

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
    """A single value row for insertion."""
    valid_time: datetime
    value_key: str
    value: Optional[float] = None
    valid_time_end: Optional[datetime] = None  # For interval values
    series_id: Optional[str] = None  # Optional series_id. If not provided, a new series will be created.

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunRequest(BaseModel):
    """Request to create a batch with values."""
    workflow_id: Optional[str] = Field(default="api-workflow", description="Workflow identifier (defaults to 'api-workflow')")
    batch_start_time: datetime
    batch_finish_time: Optional[datetime] = None
    known_time: Optional[datetime] = None  # Time of knowledge - defaults to inserted_at (now()) if not provided
    tenant_id: Optional[str] = None  # Tenant UUID (defaults to zeros UUID for single-tenant)
    batch_params: Optional[Dict[str, Any]] = None
    value_rows: List[ValueRow] = Field(default_factory=list)
    series_descriptions: Optional[Dict[str, str]] = Field(None, description="Optional dict mapping value_key to description. Used when creating new series.")

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunResponse(BaseModel):
    """Response after creating a batch."""
    batch_id: str
    message: str
    series_ids: Dict[str, str]  # Maps name to series_id (UUID as string)


class RecordUpdateRequest(BaseModel):
    """Request to update a record.
    
    For value, annotation, and tags:
    - Omit the field to leave it unchanged
    - Set to null to explicitly clear it
    - Set to a value to update it
    
    Note: changed_by is automatically set from the authenticated user's email.
    """
    batch_id: str
    tenant_id: str
    valid_time: datetime
    series_id: str
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
    """Request to create a new time series."""
    name: str = Field(..., description="Human-readable identifier for the series (e.g., 'wind_power_forecast')")
    description: Optional[str] = Field(None, description="Optional description of the series")
    unit: str = Field(default="dimensionless", description="Canonical unit for the series (e.g., 'MW', 'kW', 'MWh', 'dimensionless')")


class CreateSeriesResponse(BaseModel):
    """Response after creating a series."""
    series_id: str
    message: str


class SeriesInfo(BaseModel):
    """Information about a time series."""
    series_key: str
    description: Optional[str] = None
    unit: str


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
        "authentication": {
            "method": "API Key",
            "header": "X-API-Key",
            "note": "Authentication is optional. If users_table exists, endpoints require authentication. Users can only access data for their own tenant_id."
        },
        "admin_note": "Schema creation/deletion and user management must be done through CLI or SDK, not through the API."
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
    all_versions: bool = Query(False, description="Include all versions (not just current)"),
    current_user: Optional[CurrentUser] = Depends(get_current_user),
):
    """
    Read time series values from the database.
    
    Returns values filtered by valid_time and/or known_time ranges.
    All datetime parameters should be in ISO format (e.g., 2025-01-01T00:00:00Z).
    
    Modes:
    - "flat": Returns (valid_time, value_key, value) with latest known_time per valid_time
    - "overlapping": Returns (known_time, valid_time, value_key, value) - all rows with known_time
    
    all_versions: If True, includes all versions (both is_current=true and false). If False, only current values.
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
        
        # Use tenant_id from authenticated user if available
        tenant_id = None
        if current_user:
            tenant_id = uuid.UUID(current_user.tenant_id)
        
        if mode == "flat":
            df = db.read.read_values_flat(
                dsn,
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                all_versions=all_versions,
            )
        elif mode == "overlapping":
            df = db.read.read_values_overlapping(
                dsn,
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                all_versions=all_versions,
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


@app.post("/upload", response_model=CreateRunResponse)
async def upload_timeseries(
    request: CreateRunRequest,
    current_user: Optional[CurrentUser] = Depends(get_current_user),
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
        
        # Use tenant_id from authenticated user, or from request if no authentication
        if current_user:
            # User is authenticated - use their tenant_id (cannot override)
            tenant_id = uuid.UUID(current_user.tenant_id)
        else:
            # No authentication - use tenant_id from request or default
            if request.tenant_id:
                try:
                    tenant_id = uuid.UUID(request.tenant_id)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid tenant_id format: {request.tenant_id}")
            else:
                # Default to zeros UUID for single-tenant installations
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
                            # Verify the series exists and use it
                            series_id = db.series.get_or_create_series(
                                conn,
                                series_key=row.value_key,
                                series_unit="dimensionless",  # API doesn't support units yet
                                series_id=provided_series_id,  # Verify this series_id exists
                            )
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
            changed_by=current_user.email if current_user else None,
        )
        
        return CreateRunResponse(
            batch_id=str(batch_id),
            message="Batch created successfully",
            series_ids=series_ids_dict
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating batch: {str(e)}")


@app.put("/values", response_model=UpdateRecordsResponse)
async def update_records(
    request: UpdateRecordsRequest,
    current_user: Optional[CurrentUser] = Depends(get_current_user),
):
    """
    Update one or more records in the values table.
    
    This endpoint supports tri-state updates:
    - Omit a field to leave it unchanged
    - Set to None to explicitly clear the field
    - Set to a value to update it
    """
    try:
        dsn = get_dsn()
        
        # Convert request updates to RecordUpdate objects
        updates = []
        for req_update in request.updates:
            # Parse UUIDs
            try:
                batch_id_uuid = uuid.UUID(req_update.batch_id)
                tenant_id_uuid = uuid.UUID(req_update.tenant_id)
                series_id_uuid = uuid.UUID(req_update.series_id)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format: {e}")
            
            # Ensure timezone-aware datetime
            valid_time = req_update.valid_time
            if valid_time.tzinfo is None:
                valid_time = valid_time.replace(tzinfo=timezone.utc)
            
            # Use model_dump to check which fields were actually provided
            # exclude_unset=True gives us only fields that were explicitly set
            provided_fields = req_update.model_dump(exclude_unset=True)
            
            # Build update dict - only include fields that were provided
            update_dict = {
                "batch_id": batch_id_uuid,
                "tenant_id": tenant_id_uuid,
                "valid_time": valid_time,
                "series_id": series_id_uuid,
            }
            
            # Add optional fields only if provided
            if "value" in provided_fields:
                update_dict["value"] = req_update.value  # Can be None (explicit clear) or a float
                
            if "annotation" in provided_fields:
                update_dict["annotation"] = req_update.annotation  # Can be None (explicit clear) or a string
                
            if "tags" in provided_fields:
                update_dict["tags"] = req_update.tags  # Can be None or [] (explicit clear) or a list
            
            # Automatically set changed_by from authenticated user's email if available
            # User cannot override this for security/audit purposes
            if current_user:
                update_dict["changed_by"] = current_user.email
                # Ensure user can only update records for their own tenant_id
                if tenant_id_uuid != uuid.UUID(current_user.tenant_id):
                    raise HTTPException(
                        status_code=403,
                        detail=f"Cannot update records for tenant_id {req_update.tenant_id}. You can only update records for your own tenant_id ({current_user.tenant_id})."
                    )
            
            updates.append(update_dict)
        
        # Execute updates
        with psycopg.connect(dsn) as conn:
            outcome = db.update.update_records(conn, updates=updates)
        
        # Convert response to JSON-serializable format
        updated = [
            {
                "batch_id": str(r["batch_id"]),
                "tenant_id": str(r["tenant_id"]),
                "valid_time": r["valid_time"].isoformat(),
                "series_id": str(r["series_id"]),
                "value_id": r["value_id"]
            }
            for r in outcome["updated"]
        ]
        
        skipped = [
            {
                "batch_id": str(k["batch_id"]),
                "tenant_id": str(k["tenant_id"]),
                "valid_time": k["valid_time"].isoformat(),
                "series_id": str(k["series_id"])
            }
            for k in outcome["skipped_no_ops"]
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
    current_user: Optional[CurrentUser] = Depends(get_current_user),
):
    """
    Create a new time series.
    
    This endpoint creates a new series with the specified name, description, and unit.
    A new series_id is generated and returned, which can be used in subsequent API calls.
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
            )
        
        return CreateSeriesResponse(
            series_id=str(series_id),
            message="Series created successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating series: {str(e)}")


@app.get("/list_timeseries", response_model=Dict[str, SeriesInfo])
async def list_timeseries(
    current_user: Optional[CurrentUser] = Depends(get_current_user),
):
    """
    List all time series available in the database.
    
    Returns a dictionary mapping series_id (as string) to series information
    including series_key, description, and unit.
    This can be used in subsequent API calls to query and update data.
    
    If authentication is enabled, only returns series for the user's tenant_id.
    """
    try:
        dsn = get_dsn()
        import psycopg
        
        with psycopg.connect(dsn) as conn:
            # Filter by tenant_id if user is authenticated
            if current_user:
                tenant_id = uuid.UUID(current_user.tenant_id)
                # Get series_ids that have values for this tenant
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT v.series_id, s.series_key, s.description, s.series_unit
                        FROM values_table v
                        JOIN series_table s ON v.series_id = s.series_id
                        WHERE v.tenant_id = %s
                        ORDER BY s.series_key
                        """,
                        (tenant_id,)
                    )
                    rows = cur.fetchall()
            else:
                # No authentication - return all series
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT series_id, series_key, description, series_unit
                        FROM series_table
                        ORDER BY series_key
                        """
                    )
                    rows = cur.fetchall()
            
            # Build dictionary: series_id (string) -> SeriesInfo
            result = {
                str(series_id): SeriesInfo(
                    series_key=series_key,
                    description=description,
                    unit=series_unit
                )
                for series_id, series_key, description, series_unit in rows
            }
            return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing time series: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

