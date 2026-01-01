"""
Authentication middleware for FastAPI using API keys.
"""
from typing import Optional
from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import psycopg

from . import db

# API key header name
API_KEY_HEADER = "X-API-Key"

# Create API key header security scheme
api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


class CurrentUser(BaseModel):
    """Current authenticated user model."""
    user_id: str
    tenant_id: str
    email: str
    api_key: str


def get_dsn() -> str:
    """Get database connection string from environment variables."""
    import os
    dsn = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise HTTPException(
            status_code=500,
            detail="Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return dsn


def users_table_exists(conn: psycopg.Connection) -> bool:
    """Check if users_table exists in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'users_table'
            )
        """)
        return cur.fetchone()[0]


async def get_current_user(
    api_key: Optional[str] = Security(api_key_header),
) -> Optional[CurrentUser]:
    """
    Dependency to get the current authenticated user from API key.
    
    If users_table doesn't exist, authentication is optional and returns None.
    If users_table exists, authentication is required.
    
    Args:
        api_key: API key from X-API-Key header
    
    Returns:
        CurrentUser object with user information, or None if users_table doesn't exist
    
    Raises:
        HTTPException: If API key is missing or invalid (when users_table exists)
    """
    dsn = get_dsn()
    
    try:
        with psycopg.connect(dsn) as conn:
            # Check if users_table exists
            if not users_table_exists(conn):
                # Users table doesn't exist - authentication is optional
                return None
            
            # Users table exists - authentication is required
            if not api_key:
                raise HTTPException(
                    status_code=401,
                    detail="API key required. Please provide X-API-Key header.",
                    headers={"WWW-Authenticate": "ApiKey"},
                )
            
            user = db.users.get_user_by_api_key(conn, api_key=api_key)
            
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid or inactive API key.",
                    headers={"WWW-Authenticate": "ApiKey"},
                )
            
            return CurrentUser(
                user_id=str(user["user_id"]),
                tenant_id=str(user["tenant_id"]),
                email=user["email"],
                api_key=user["api_key"],
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Authentication error: {str(e)}"
        )

