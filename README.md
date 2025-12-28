# timedb
## TL;DR
**timedb** is a opinionated schema and API built on top of PostgreSQL design to handle overlapping time series revisions and auditable human-in-the-loop updates. 

Most time series systems assume a single immutable value per timestamp. **timedb** is built for domains where data is revised, forecasted, reviewed, and corrected over time.

**timedb** lets you: 

- ⏱️ Retain "time-of-knowledge" history through a three-dimensional time series data model;
- ✍️ Make versioned ad-hoc updates to the time series data with comments and tags; and 
- 🔀 Represent both timestamp and time-interval time series simultaneously.

## Why timedb? 
Most time series systems assume:
- one value per timestamp;
- immutable historical data; and
- no distinction between when something was true vs when it was known.

This pattern is a major drawback in situations such as: 
- forecasting, where multiple forecast revisions predicts the same timestamp;
- backtesting, where "time-of-knowledge" history is required by algorithms;
- data communication, where and auditable history of updates is required.
- Human review and correction, where values are manually adjusted, annotated, or validated over time
- Late-arriving data and backfills, where new information must be incorporated without rewriting history

In practice, teams work around these limitations by overwriting data, duplicating tables and columns, or encoding semantics in column names — making systems fragile, opaque, and hard to reason about.

**timedb** addresses this by making revisions, provenance, and temporal semantics explicit in the data model, rather than treating them as edge cases.

## Basic usage
TBD

## Core concept
1. Three-dimensional time series data model
Every time series value is described using three independent timelines:
- `knowledge_time`, the time when the value was known
- `valid_time`, the time the value is representing a fact for
- `change_time`, the time the value was changed

2. Additional attributes that

## Installation
```python
pip install timedb
```

## Roadmap
- [ ] Decouple the knowledge time from the run_time
- [ ] RESTful API layer that serves data to users
- [ ] Handle different time zones in the API layer while always storing in UTC in the database. 
- [ ] Support for postgres time intervals (tsrange/tstzrange)
- [ ] Built in data retention, TTL, and archiving
- [ ] Support for subscribing to database updates through the API 
- [ ] Python SDK that allows time series data manipulations, reads and writes
- [ ] Unit handling (e.g. MW, kW)
