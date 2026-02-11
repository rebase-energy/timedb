# timedb
## TL;DR
**timedb** is an opinionated schema and SDK built on top of PostgreSQL + TimescaleDB, designed to handle overlapping time series revisions and auditable human-in-the-loop updates.

Most time series systems assume a single immutable value per timestamp. **timedb** is built for domains where data is revised, forecasted, reviewed, and corrected over time.

**timedb** lets you:

- ⏱️ Retain "time-of-knowledge" history through a three-dimensional time series data model;
- ✍️ Make versioned ad-hoc updates to the time series data with annotations and tags; and
- 🔀 Represent both timestamp and time-interval time series simultaneously.

## Why timedb?
Most time series systems assume:
- one value per timestamp;
- immutable historical data; and
- no distinction between when something was true vs when it was known.

This pattern is a major drawback in situations such as:
- forecasting, where multiple forecast revisions predict the same timestamp;
- backtesting, where "time-of-knowledge" history is required by algorithms;
- data communication, where an auditable history of updates is required;
- human review and correction, where values are manually adjusted, annotated, or validated over time;
- late-arriving data and backfills, where new information must be incorporated without rewriting history.

In practice, teams work around these limitations by overwriting data, duplicating tables and columns, or encoding semantics in column names — making systems fragile, opaque, and hard to reason about.

**timedb** addresses this by making revisions, provenance, and temporal semantics explicit in the data model, rather than treating them as edge cases.

## Installation
```bash
pip install timedb
```

## Quick Start

For a hands-on tutorial that gets you up and running, see [QUICKSTART.md](QUICKSTART.md). The quick start guide walks you through:
- Setting up TimescaleDB with Docker for local testing
- Installing timedb
- Running the interactive quickstart notebook
- Creating your first time series

## Basic usage

```python
from timedb import TimeDataClient
import pandas as pd
from datetime import datetime, timezone, timedelta

# Create client (reads TIMEDB_DSN or DATABASE_URL from env)
td = TimeDataClient()

# Create database schema
td.create()

# Create a series
td.create_series(
    name="temperature",
    unit="degC",
    labels={"site": "Gotland"},
    data_class="overlapping",  # or "flat" for non-versioned data
    retention="medium",        # "short", "medium", or "long"
)

# Build time series data
base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
df = pd.DataFrame({
    'valid_time': [base_time + timedelta(hours=i) for i in range(24)],
    'temperature': [20.0 + i * 0.3 for i in range(24)]
})

# Insert data
known_time = datetime.now(timezone.utc)
result = td.series("temperature").where(site="Gotland").insert(
    df=df,
    known_time=known_time,
)
print(f"Inserted batch: {result.batch_id}")

# Read latest values
df_latest = td.series("temperature").where(site="Gotland").read()
print(df_latest)

# Read all forecast revisions (overlapping series only)
df_all = td.series("temperature").where(site="Gotland").read(versions=True)
```

See the [examples/](examples/) folder for interactive Jupyter notebooks demonstrating more advanced usage.

## Data model

### Three-dimensional time series

Most time series databases are two-dimensional: they map a timestamp to a value. **timedb** adds three time dimensions to track both when data was valid and how it evolved.

This allows you to store overlapping forecasts, revisions, and manual corrections while preserving the full history of how values changed. For example:
- A forecast made on Monday for Tuesday can coexist with a forecast made on Tuesday for the same time
- You can query "what did we know about Tuesday on Monday?" vs "what do we know now?"
- You can see when and by whom a value was manually corrected

The three time dimensions are:

- **`valid_time`**: When the value is valid (the timestamp being forecasted/measured)
- **`known_time`**: When this value became known (when the forecast was made, batch was processed, or data arrived)
- **`change_time`**: When this specific row was created/modified (timestamps manual edits and updates). Automatically set to `now()` on insert and update.

Supporting metadata:

- **`batch_id`**: Groups values that were inserted together in one batch
- **`changed_by`**: Who made a manual change (optional, for audit trail)
- **`annotation`**: Human-readable description of changes
- **`tags`**: Quality flags for review/validation status

This design naturally supports:

- **Forecast revisions**: Multiple predictions for the same `valid_time` from different `known_time`s
- **Data corrections**: Manual updates that preserve the original value with full audit trail via `change_time`
- **Backtesting**: Reconstruct what was known at any point in the past
- **Human review**: Track who changed what, when, and why through `changed_by`, `change_time`, `annotation`, and `tags`

### Two data classes

**timedb** supports two data classes for different use cases:

- **`flat`**: For data that doesn't need versioning (e.g., actuals, measurements). Each `(series_id, valid_time)` has exactly one value. Updates overwrite the previous value with a new `change_time` for audit.
- **`overlapping`**: For data with multiple revisions (e.g., forecasts). Each `(series_id, valid_time)` can have multiple values, distinguished by `known_time`. The latest value is determined by `MAX(known_time)`.

### Retention tiers

Overlapping series are stored in one of three retention tiers, configurable at series creation and at schema creation:

| Tier | Default retention | Table |
|------|-------------------|-------|
| `short` | 6 months | `overlapping_short` |
| `medium` | 3 years | `overlapping_medium` |
| `long` | 5 years | `overlapping_long` |

Retention periods are configurable when creating the schema:
```python
td.create(retention="5 years")                  # sets medium tier (default)
td.create(retention_short="1 month",             # set each tier independently
          retention_medium="2 years",
          retention_long="10 years")
```

### Additional attributes

Beyond the core time dimensions, **timedb** includes attributes for human-in-the-loop corrections:
- **`tags`**: Array of strings for quality flags (e.g., `["reviewed", "corrected"]`)
- **`annotation`**: Text descriptions of changes
- **`metadata`**: JSONB field for flexible metadata storage
- **`changed_by`**: Who made the change

Updates create new rows rather than overwriting, so you maintain a complete audit trail of all changes.

## Tables

### batches_table

| Field | Type | Purpose |
|---|---|---|
| `batch_id` **(PK)** | UUID | Unique identifier for the batch |
| `workflow_id` | text | Identifier for the workflow/pipeline (optional) |
| `batch_start_time` | timestamptz | When the batch started (optional) |
| `batch_finish_time` | timestamptz | When the batch finished (optional) |
| `known_time` | timestamptz | When the data was known/available (default: now()) |
| `batch_params` | jsonb | Parameters/config used for this batch (optional) |
| `inserted_at` | timestamptz | When the row was inserted (default: now()) |

---

### series_table

| Field | Type | Purpose |
|---|---|---|
| `series_id` **(PK)** | UUID | Unique identifier for the series |
| `name` | text | Parameter name (e.g., 'wind_power', 'temperature') |
| `unit` | text | Canonical unit for the series (e.g., 'MW', 'degC') |
| `labels` | jsonb | Dimensions differentiating series (e.g., `{"site": "Gotland"}`) |
| `description` | text | Human-readable description (optional) |
| `data_class` | text | `'flat'` or `'overlapping'` (default: `'overlapping'`) |
| `retention` | text | `'short'`, `'medium'`, or `'long'` (default: `'medium'`) |
| `inserted_at` | timestamptz | When the series was created (default: now()) |

**Uniqueness constraint**: `(name, labels)` — A series is uniquely identified by its name and labels.

---

### flat

For `data_class='flat'` series. Each `(series_id, valid_time)` has exactly one row.

| Field | Type | Purpose |
|---|---|---|
| `flat_id` | bigserial | Row identifier |
| `series_id` **(FK)** | UUID | References `series_table` |
| `valid_time` | timestamptz | When the value is valid |
| `valid_time_end` | timestamptz | Interval end time (optional; NULL = point-in-time) |
| `value` | double precision | Numeric value |
| `annotation` | text | Human annotation (optional) |
| `metadata` | jsonb | Flexible metadata (optional) |
| `tags` | text[] | Quality flags (optional) |
| `changed_by` | text | Who made the change (optional) |
| `change_time` | timestamptz | When this row was created (default: now()) |

---

### overlapping_short / overlapping_medium / overlapping_long

For `data_class='overlapping'` series. Each `(series_id, valid_time, known_time)` combination is unique, allowing multiple revisions.

| Field | Type | Purpose |
|---|---|---|
| `overlapping_id` | bigserial | Row identifier |
| `batch_id` **(FK)** | UUID | References `batches_table` |
| `series_id` **(FK)** | UUID | References `series_table` |
| `valid_time` | timestamptz | When the value is valid |
| `valid_time_end` | timestamptz | Interval end time (optional; NULL = point-in-time) |
| `value` | double precision | Numeric value |
| `known_time` | timestamptz | When this value became known |
| `annotation` | text | Human annotation (optional) |
| `metadata` | jsonb | Flexible metadata (optional) |
| `tags` | text[] | Quality flags (optional) |
| `changed_by` | text | Who made the change (optional) |
| `change_time` | timestamptz | When this row was created (default: now()) |

Series are routed to the correct table based on the `retention` field in `series_table`.

## Roadmap
- [x] Decouple the knowledge time from the batch_start_time
- [x] Python SDK that allows time series data manipulations, reads and writes
- [x] RESTful API layer that serves data to users
- [x] Unit handling (e.g. MW, kW)
- [x] Built in data retention with configurable retention periods
- [ ] Handle different time zones in the API layer while always storing in UTC in the database
- [ ] Support for postgres time intervals (tsrange/tstzrange)
- [ ] Support for subscribing to database updates through the API
- [ ] Xarray integration for multidimensional time series
- [ ] Polars integration for lazy computations
- [ ] Parquet file integration
- [ ] Real-Time Subscriptions through websocket subscription
- [ ] Store time series with geographic coordinates. Query by spatial region (e.g., "all temperature sensors in this polygon")
- [ ] Automatic alignment and interpolation of different time series resolutions
- [ ] Symbolic time series + serialization
