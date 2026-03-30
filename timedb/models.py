"""
SQLAlchemy declarative models for TimeDB PostgreSQL tables.

Only series_table lives in PostgreSQL. All values tables live in ClickHouse
and are NOT modeled here.

Usage in an external Alembic-managed monorepo::

    # monorepo/alembic/env.py
    from timedb.models import Base as TimeDBBase
    from myapp.models import Base as AppBase

    target_metadata = [AppBase.metadata, TimeDBBase.metadata]

Requires: pip install timedb[sqlalchemy]
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class SeriesTable(Base):
    __tablename__ = "series_table"

    series_id = sa.Column(
        sa.BigInteger,
        sa.Identity(always=False),
        primary_key=True,
    )
    name = sa.Column(sa.Text, nullable=False)
    unit = sa.Column(sa.Text, nullable=False)
    labels = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    description = sa.Column(sa.Text, nullable=True)
    overlapping = sa.Column(sa.Boolean, nullable=False, server_default=sa.text("false"))
    retention = sa.Column(sa.Text, nullable=False, server_default=sa.text("'medium'"))
    inserted_at = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    )

    __table_args__ = (
        sa.UniqueConstraint("name", "labels", name="series_identity_uniq"),
        sa.CheckConstraint(
            "length(btrim(name)) > 0",
            name="series_name_not_empty",
        ),
        sa.CheckConstraint(
            "retention IN ('short', 'medium', 'long')",
            name="valid_retention",
        ),
        sa.Index(
            "series_labels_gin_idx",
            "labels",
            postgresql_using="gin",
        ),
    )
