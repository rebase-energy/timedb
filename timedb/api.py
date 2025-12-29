"""
FastAPI application for timedb - MVP version
Provides REST API endpoints for time series database operations.
"""
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv, find_dotenv
import psycopg
import pandas as pd

from . import db

load_dotenv(find_dotenv())

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

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunRequest(BaseModel):
    """Request to create a run with values."""
    workflow_id: str
    run_start_time: datetime
    run_finish_time: Optional[datetime] = None
    run_params: Optional[Dict[str, Any]] = None
    value_rows: List[ValueRow] = Field(default_factory=list)

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunResponse(BaseModel):
    """Response after creating a run."""
    run_id: str
    message: str


class RecordUpdateRequest(BaseModel):
    """Request to update a record.
    
    For value, comment, and tags:
    - Omit the field to leave it unchanged
    - Set to null to explicitly clear it
    - Set to a value to update it
    """
    run_id: str
    valid_time: datetime
    series_key: str
    value: Optional[float] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    comment: Optional[str] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    tags: Optional[List[str]] = Field(default=None, description="Omit to leave unchanged, null or [] to clear, or provide tags")
    changed_by: Optional[str] = None

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class UpdateRecordsRequest(BaseModel):
    """Request to update multiple records."""
    updates: List[RecordUpdateRequest]


class UpdateRecordsResponse(BaseModel):
    """Response after updating records."""
    updated: List[Dict[str, Any]]
    skipped_no_ops: List[Dict[str, Any]]


class CreateSchemaRequest(BaseModel):
    """Request to create schema."""
    schema: Optional[str] = None
    with_metadata: bool = False


class ErrorResponse(BaseModel):
    """Error response model."""
    detail: str


# FastAPI app
app = FastAPI(
    title="TimeDB API",
    description="REST API for time series database operations",
    version="0.1.1"
)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "TimeDB API",
        "version": "0.1.1",
        "endpoints": {
            "read_values": "GET /values",
            "create_run": "POST /runs",
            "update_records": "PUT /values",
            "create_schema": "POST /schema/create",
            "delete_schema": "DELETE /schema/delete",
        }
    }


@app.get("/values", response_model=Dict[str, Any])
async def read_values(
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_run: Optional[datetime] = Query(None, description="Start of run time range (ISO format)"),
    end_run: Optional[datetime] = Query(None, description="End of run time range (ISO format)"),
):
    """
    Read time series values from the database.
    
    Returns values filtered by valid_time and/or run_time ranges.
    All datetime parameters should be in ISO format (e.g., 2025-01-01T00:00:00Z).
    """
    try:
        dsn = get_dsn()
        
        # Convert timezone-naive datetimes to UTC if needed
        if start_valid and start_valid.tzinfo is None:
            start_valid = start_valid.replace(tzinfo=timezone.utc)
        if end_valid and end_valid.tzinfo is None:
            end_valid = end_valid.replace(tzinfo=timezone.utc)
        if start_run and start_run.tzinfo is None:
            start_run = start_run.replace(tzinfo=timezone.utc)
        if end_run and end_run.tzinfo is None:
            end_run = end_run.replace(tzinfo=timezone.utc)
        
        df = db.read.read_values_between(
            dsn,
            start_valid=start_valid,
            end_valid=end_valid,
            start_run=start_run,
            end_run=end_run,
        )
        
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


@app.post("/runs", response_model=CreateRunResponse)
async def create_run(request: CreateRunRequest):
    """
    Create a new run with associated values.
    
    This endpoint creates a run entry and optionally inserts value rows.
    """
    try:
        dsn = get_dsn()
        run_id = uuid.uuid4()
        
        # Convert value_rows to the format expected by insert_run_with_values
        value_rows = []
        for row in request.value_rows:
            if row.valid_time_end is not None:
                # Interval value: (valid_time, valid_time_end, value_key, value)
                value_rows.append((row.valid_time, row.valid_time_end, row.value_key, row.value))
            else:
                # Point-in-time value: (valid_time, value_key, value)
                value_rows.append((row.valid_time, row.value_key, row.value))
        
        # Ensure timezone-aware datetimes
        if request.run_start_time.tzinfo is None:
            run_start_time = request.run_start_time.replace(tzinfo=timezone.utc)
        else:
            run_start_time = request.run_start_time
            
        if request.run_finish_time is not None:
            if request.run_finish_time.tzinfo is None:
                run_finish_time = request.run_finish_time.replace(tzinfo=timezone.utc)
            else:
                run_finish_time = request.run_finish_time
        else:
            run_finish_time = None
        
        db.insert.insert_run_with_values(
            conninfo=dsn,
            run_id=run_id,
            workflow_id=request.workflow_id,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            value_rows=value_rows,
            run_params=request.run_params,
        )
        
        return CreateRunResponse(
            run_id=str(run_id),
            message="Run created successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating run: {str(e)}")


@app.put("/values", response_model=UpdateRecordsResponse)
async def update_records(request: UpdateRecordsRequest):
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
            # Parse run_id
            try:
                run_id_uuid = uuid.UUID(req_update.run_id)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid run_id format: {req_update.run_id}")
            
            # Ensure timezone-aware datetime
            valid_time = req_update.valid_time
            if valid_time.tzinfo is None:
                valid_time = valid_time.replace(tzinfo=timezone.utc)
            
            # Use model_dump to check which fields were actually provided
            # exclude_unset=True gives us only fields that were explicitly set
            provided_fields = req_update.model_dump(exclude_unset=True)
            
            # Build update dict with _UNSET for fields not provided
            update_dict = {
                "run_id": run_id_uuid,
                "valid_time": valid_time,
                "series_key": req_update.series_key,
                "changed_by": req_update.changed_by,
            }
            
            # Check if each field was provided in the request
            if "value" in provided_fields:
                update_dict["value"] = req_update.value  # Can be None (explicit clear) or a float
            else:
                update_dict["value"] = db.update._UNSET  # Not provided, leave unchanged
                
            if "comment" in provided_fields:
                update_dict["comment"] = req_update.comment  # Can be None (explicit clear) or a string
            else:
                update_dict["comment"] = db.update._UNSET  # Not provided, leave unchanged
                
            if "tags" in provided_fields:
                update_dict["tags"] = req_update.tags  # Can be None or [] (explicit clear) or a list
            else:
                update_dict["tags"] = db.update._UNSET  # Not provided, leave unchanged
            
            update = db.update.RecordUpdate(**update_dict)
            updates.append(update)
        
        # Execute updates
        with psycopg.connect(dsn) as conn:
            outcome = db.update.update_records(conn, updates)
        
        # Convert response to JSON-serializable format
        updated = [
            {
                "run_id": str(r.key.run_id),
                "valid_time": r.key.valid_time.isoformat(),
                "series_key": r.key.series_key,
                "version_id": r.version_id
            }
            for r in outcome.updated
        ]
        
        skipped = [
            {
                "run_id": str(k.run_id),
                "valid_time": k.valid_time.isoformat(),
                "series_key": k.series_key
            }
            for k in outcome.skipped_no_ops
        ]
        
        return UpdateRecordsResponse(
            updated=updated,
            skipped_no_ops=skipped
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating records: {str(e)}")


@app.post("/schema/create")
async def create_schema(request: CreateSchemaRequest):
    """
    Create or update the database schema.
    
    This endpoint creates the timedb tables. It's safe to run multiple times.
    """
    try:
        dsn = get_dsn()
        
        # Set schema/search_path if provided
        old_pgoptions = os.environ.get("PGOPTIONS")
        if request.schema:
            os.environ["PGOPTIONS"] = f"-c search_path={request.schema}"
        
        try:
            db.create.create_schema(dsn)
            message = "Base timedb tables created/updated successfully."
            
            if request.with_metadata:
                db.create_with_metadata.create_schema_metadata(dsn)
                message += " Optional metadata schema created/updated successfully."
            
            return {"message": message}
        finally:
            # Restore PGOPTIONS
            if request.schema:
                if old_pgoptions is None:
                    os.environ.pop("PGOPTIONS", None)
                else:
                    os.environ["PGOPTIONS"] = old_pgoptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating schema: {str(e)}")


@app.delete("/schema/delete")
async def delete_schema(schema: Optional[str] = Query(None, description="Schema name")):
    """
    Delete all timedb tables and views.
    
    WARNING: This will delete all data! Use with caution.
    """
    try:
        dsn = get_dsn()
        
        # Set schema/search_path if provided
        old_pgoptions = os.environ.get("PGOPTIONS")
        if schema:
            os.environ["PGOPTIONS"] = f"-c search_path={schema}"
        
        try:
            db.delete.delete_schema(dsn)
            return {"message": "All timedb tables (including metadata) deleted successfully."}
        finally:
            # Restore PGOPTIONS
            if schema:
                if old_pgoptions is None:
                    os.environ.pop("PGOPTIONS", None)
                else:
                    os.environ["PGOPTIONS"] = old_pgoptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting schema: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

