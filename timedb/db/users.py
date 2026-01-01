"""
User management functions for timedb.
Handles creation, listing, and management of users with API keys.
"""
import uuid
import secrets
from typing import Optional, List, Dict, Any
from datetime import datetime
import psycopg
from psycopg.rows import dict_row


def create_user(
    conn: psycopg.Connection,
    *,
    tenant_id: uuid.UUID,
    email: str,
    api_key: Optional[str] = None,
    is_active: bool = True,
) -> Dict[str, Any]:
    """
    Create a new user in the users_table.
    
    Args:
        conn: Database connection
        tenant_id: Tenant UUID that the user belongs to
        email: User email address (must be unique within tenant)
        api_key: API key for authentication. If None, generates a secure random key.
        is_active: Whether the user account is active (default: True)
    
    Returns:
        Dictionary with user information including user_id, tenant_id, email, api_key, is_active, created_at
    
    Raises:
        psycopg.errors.UniqueViolation: If email+tenant_id combination or api_key already exists
    """
    if api_key is None:
        # Generate a secure random API key (32 bytes = 64 hex characters)
        api_key = secrets.token_urlsafe(32)
    
    email = email.strip()
    if not email:
        raise ValueError("Email cannot be empty")
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            INSERT INTO users_table (tenant_id, email, api_key, is_active)
            VALUES (%s, %s, %s, %s)
            RETURNING user_id, tenant_id, email, api_key, is_active, created_at, updated_at
            """,
            (tenant_id, email, api_key, is_active),
        )
        user = cur.fetchone()
        return dict(user)


def get_user_by_api_key(
    conn: psycopg.Connection,
    *,
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """
    Get user by API key (only returns active users).
    
    Args:
        conn: Database connection
        api_key: API key to look up
    
    Returns:
        Dictionary with user information if found and active, None otherwise
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
            FROM users_table
            WHERE api_key = %s AND is_active = true
            """,
            (api_key,),
        )
        user = cur.fetchone()
        return dict(user) if user else None


def get_user_by_email(
    conn: psycopg.Connection,
    *,
    email: str,
) -> Optional[Dict[str, Any]]:
    """
    Get user by email address.
    
    Args:
        conn: Database connection
        email: Email address to look up
    
    Returns:
        Dictionary with user information if found, None otherwise
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
            FROM users_table
            WHERE email = %s
            """,
            (email.strip(),),
        )
        user = cur.fetchone()
        return dict(user) if user else None


def list_users(
    conn: psycopg.Connection,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    include_inactive: bool = False,
) -> List[Dict[str, Any]]:
    """
    List all users, optionally filtered by tenant_id.
    
    Args:
        conn: Database connection
        tenant_id: Optional tenant UUID to filter users by tenant
        include_inactive: If True, includes inactive users (default: False)
    
    Returns:
        List of dictionaries with user information
    """
    with conn.cursor(row_factory=dict_row) as cur:
        if tenant_id is not None:
            if include_inactive:
                cur.execute(
                    """
                    SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
                    FROM users_table
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC
                    """,
                    (tenant_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
                    FROM users_table
                    WHERE tenant_id = %s AND is_active = true
                    ORDER BY created_at DESC
                    """,
                    (tenant_id,),
                )
        else:
            if include_inactive:
                cur.execute(
                    """
                    SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
                    FROM users_table
                    ORDER BY created_at DESC
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT user_id, tenant_id, email, api_key, is_active, created_at, updated_at
                    FROM users_table
                    WHERE is_active = true
                    ORDER BY created_at DESC
                    """
                )
        users = cur.fetchall()
        return [dict(user) for user in users]


def deactivate_user(
    conn: psycopg.Connection,
    *,
    user_id: Optional[uuid.UUID] = None,
    email: Optional[str] = None,
) -> bool:
    """
    Deactivate a user account (sets is_active = false).
    
    Args:
        conn: Database connection
        user_id: User ID to deactivate (either user_id or email must be provided)
        email: Email address to deactivate (either user_id or email must be provided)
    
    Returns:
        True if user was found and deactivated, False if user was not found
    
    Raises:
        ValueError: If neither user_id nor email is provided
    """
    if user_id is None and email is None:
        raise ValueError("Either user_id or email must be provided")
    
    with conn.cursor() as cur:
        if user_id is not None:
            cur.execute(
                """
                UPDATE users_table
                SET is_active = false
                WHERE user_id = %s
                RETURNING user_id
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                UPDATE users_table
                SET is_active = false
                WHERE email = %s
                RETURNING user_id
                """,
                (email.strip(),),
            )
        result = cur.fetchone()
        return result is not None


def activate_user(
    conn: psycopg.Connection,
    *,
    user_id: Optional[uuid.UUID] = None,
    email: Optional[str] = None,
) -> bool:
    """
    Activate a user account (sets is_active = true).
    
    Args:
        conn: Database connection
        user_id: User ID to activate (either user_id or email must be provided)
        email: Email address to activate (either user_id or email must be provided)
    
    Returns:
        True if user was found and activated, False if user was not found
    
    Raises:
        ValueError: If neither user_id nor email is provided
    """
    if user_id is None and email is None:
        raise ValueError("Either user_id or email must be provided")
    
    with conn.cursor() as cur:
        if user_id is not None:
            cur.execute(
                """
                UPDATE users_table
                SET is_active = true
                WHERE user_id = %s
                RETURNING user_id
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                UPDATE users_table
                SET is_active = true
                WHERE email = %s
                RETURNING user_id
                """,
                (email.strip(),),
            )
        result = cur.fetchone()
        return result is not None


def regenerate_api_key(
    conn: psycopg.Connection,
    *,
    user_id: Optional[uuid.UUID] = None,
    email: Optional[str] = None,
) -> Optional[str]:
    """
    Regenerate API key for a user.
    
    Args:
        conn: Database connection
        user_id: User ID to regenerate key for (either user_id or email must be provided)
        email: Email address to regenerate key for (either user_id or email must be provided)
    
    Returns:
        New API key if user was found, None otherwise
    
    Raises:
        ValueError: If neither user_id nor email is provided
    """
    if user_id is None and email is None:
        raise ValueError("Either user_id or email must be provided")
    
    # Generate a new secure random API key
    new_api_key = secrets.token_urlsafe(32)
    
    with conn.cursor() as cur:
        if user_id is not None:
            cur.execute(
                """
                UPDATE users_table
                SET api_key = %s
                WHERE user_id = %s
                RETURNING api_key
                """,
                (new_api_key, user_id),
            )
        else:
            cur.execute(
                """
                UPDATE users_table
                SET api_key = %s
                WHERE email = %s
                RETURNING api_key
                """,
                (new_api_key, email.strip()),
            )
        result = cur.fetchone()
        return new_api_key if result else None

