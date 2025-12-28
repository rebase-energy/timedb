import os
import psycopg
from dotenv import load_dotenv

from pg_create_table import create_schema

load_dotenv()

# -----------------------------------------------------------------------------
# Optional schema addon: metadata_table
#
# - Anchored to (run_id, valid_time)
# - Long-form metadata (metadata_key + typed value columns)
# - NOT versioned for now (one row per (run_id, valid_time, metadata_key))
# - Idempotent / safe to run multiple times
# -----------------------------------------------------------------------------

DDL = """
BEGIN;

-- ============================================================================
-- metadata_table
-- ----------------------------------------------------------------------------
-- Stores metadata for each (run_id, valid_time) context.
-- NOT VERSIONED (for now):
--   - One row per (run_id, valid_time, metadata_key)
--   - Updates overwrite the row via UPSERT in application code
-- Long-form (EAV-ish) with typed columns.
-- ============================================================================

CREATE TABLE IF NOT EXISTS metadata_table (
  metadata_id   bigserial PRIMARY KEY,

  -- Context key (same as values_table context)
  run_id        uuid NOT NULL REFERENCES runs_table(run_id) ON DELETE CASCADE,
  valid_time    timestamptz NOT NULL,

  -- Name of the metadata field, e.g. 'contractId', 'deliveryStart'
  metadata_key  text NOT NULL,

  -- Typed payload slots (exactly one should be non-null)
  value_number  double precision,
  value_string  text,
  value_bool    boolean,
  value_time    timestamptz,
  value_json    jsonb,

  inserted_at   timestamptz NOT NULL DEFAULT now(),

  -- Enforce exactly one typed field is set
  CONSTRAINT metadata_one_value_set CHECK (
    (value_number IS NOT NULL)::int +
    (value_string IS NOT NULL)::int +
    (value_bool   IS NOT NULL)::int +
    (value_time   IS NOT NULL)::int +
    (value_json   IS NOT NULL)::int
    = 1
  )
);

-- One metadata field per key per context
CREATE UNIQUE INDEX IF NOT EXISTS metadata_context_key_uniq
  ON metadata_table (run_id, valid_time, metadata_key);

-- Speeds up joining metadata onto values
CREATE INDEX IF NOT EXISTS metadata_context_idx
  ON metadata_table (run_id, valid_time);

-- Speeds up filtering by metadata_key
CREATE INDEX IF NOT EXISTS metadata_key_idx
  ON metadata_table (metadata_key);

-- Optional convenience view (metadata isn't versioned, so this is just an alias)
CREATE OR REPLACE VIEW current_metadata_table AS
SELECT *
FROM metadata_table;

COMMIT;
"""


def create_schema_metadata(conninfo: str) -> None:
    """
    Creates (or updates) the optional metadata schema addon.

    Requirements:
      - runs_table must already exist (so run create_schema(conninfo) first)

    Notes:
      - idempotent
      - safe to run multiple times
    """
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)


if __name__ == "__main__":
    conninfo = os.environ["NEON_PG_URL3"]

    create_schema(conninfo)
    create_schema_metadata(conninfo)

    print("Base schema + optional metadata schema created/updated successfully.")