<div align="center">
  <h1>⏱️ TimeDB</h1>
  <p><b>A minimal, stateless Python client for 3-dimensional time series on ClickHouse.</b></p>

  <a href="https://pypi.org/project/timedb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/timedb?color=blue&style=flat-square"></a>
  <a href="https://pypi.org/project/timedb/"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/timedb?style=flat-square"></a>
  <a href="https://github.com/rebase-energy/timedb/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"></a>
  <a href="https://www.rebase.energy/join-slack"><img alt="Slack" src="https://img.shields.io/badge/Slack-Join%20Community-4A154B?logo=slack&style=flat-square"></a>
</div>

<br/>

**TimeDB** stores **overlapping forecast revisions**, **auditable corrections**, and **"time-of-knowledge" history** as plain rows in ClickHouse. Every value carries a three-dimensional timestamp so you can replay exactly what was known at any past instant.

Traditional time-series stores assume one immutable value per timestamp. TimeDB is built for the messy reality of forecasts that get revised, observations that get corrected, and backtests that need strict point-in-time audits.

---

## 🧊 The 3D Temporal Data Model

At the heart of TimeDB is its three-dimensional approach to time. We track not just *when* data is valid, but *when it became known* and *when it was altered*.

| Dimension | Description | Real-World Example |
| :---- | :--- | :--- |
| 📅&nbsp;**`valid_time`** | The time the value represents a fact for. | *"Wind speed forecast for Wednesday 12:00"* |
| ⏰&nbsp;**`knowledge_time`** | The time when the value was predicted/known. | *"Generated on Monday 18:00"* |
| ✏️&nbsp;**`change_time`** | The time when the value was written or changed. | *"Manually overridden on Tuesday 09:00"* |

> **Audit & Metadata:** Every row also carries `changed_by` and `annotation` text fields so corrections leave a readable trail instead of silent overwrites.

---

## ✨ Why Choose TimeDB?

- 📊 **Forecast Revisions:** Store overlapping forecasts side-by-side — every `knowledge_time` is preserved.
- 🔄 **Auditable Updates:** Corrections are new rows with a fresh `change_time`; reads collapse them into the latest state, with full history available on demand.
- ⏪ **True Backtesting:** Query historical data as of any point in time (*"What did our model know last Monday?"*), or use `read_relative()` for per-window day-ahead cutoffs.
- 🗂️ **Retention Tiers:** Pick `short` / `medium` / `long` / `forever` per series; ClickHouse drops whole partitions when TTLs expire.
- 🪶 **Stateless & Minimal:** One class, two tables, no catalog. Series identity (`series_id`) is owned by the caller — no naming, units, or labels to keep in sync.

---

## 🚀 Quick Start

### 1. Installation

```bash
pip install timedb
```

Requires Python 3.12+ and a reachable ClickHouse instance. TimeDB reads its connection string from `TIMEDB_CH_URL` (also picked up from a `.env` file).

### 2. Usage Example

```python
import polars as pl
from datetime import datetime, timezone
from timedb import TimeDBClient

td = TimeDBClient()  # reads TIMEDB_CH_URL
td.create()          # creates series_values + run_series tables

# 1. Write a forecast run for series_id=42, issued at 06:00.
kt = datetime(2025, 1, 1, 6, tzinfo=timezone.utc)
df = pl.DataFrame({
    "series_id":  [42] * 24,
    "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
    "value":      [100.0 + i * 2 for i in range(24)],
})
td.write(df, retention="medium", knowledge_time=kt)

# 2. A later forecast revision — same valid_time window, higher knowledge_time.
kt2 = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
td.write(df.with_columns(pl.col("value") + 5), retention="medium", knowledge_time=kt2)

# 3. Latest value per valid_time (the second run wins).
latest = td.read(series_ids=[42])

# 4. Full forecast history — one row per (knowledge_time, valid_time).
history = td.read(series_ids=[42], include_knowledge_time=True)
```

Required write columns are `series_id`, `valid_time`, `value`. Everything else (`change_time`, `run_id`, `changed_by`, `annotation`, `valid_time_end`) is optional and stamped with safe defaults per batch. All timestamp columns must be timezone-aware.

---

## 🤝 The Full Stack — TimeDB + EnergyDB

TimeDB stores rows keyed by integer `series_id` and nothing else. That's enough when you're managing identity in your own application — but for the energy domain, we ship the full stack.

**[EnergyDB](https://github.com/rebase-energy/energydb)** adds, on top of the same ClickHouse store (plus a thin PostgreSQL catalog):

- 🌳 **Typed asset trees**: `Portfolio` → `Site` → `WindTurbine` / `PVArray` / `Battery`, etc. — every asset class from [EnergyDataModel](https://github.com/rebase-energy/energydatamodel).
- 🔗 **Grid edges**: `Line`, transformer, pipe — connect any two nodes, attach their own time series.
- 🧭 **Fluent path scopes**: `client.get_node("my-portfolio", "Offshore-1", "T01").read(name="power", data_type="actual")` resolves to one indexed SQL query.
- ⚖️ **Per-series canonical units**: declare `MW` once; `pint` converts on every read and write via a `unit=` kwarg.
- 🧬 **Run/workflow provenance**: `workflow_id`, `model_name`, `run_start_time`, etc. attached at write time.
- 📋 **Diffable structural changes**: `dry_run=True` previews every rename, move, delete, or insert as a `TreeDiff` before you commit.

Use **TimeDB** for the raw storage primitive. Use **EnergyDB** for an asset-aware catalog of energy portfolios.

---

## 🧪 Try it in Google Colab

Want to try TimeDB without a local setup? Open our Quickstart in Colab — the first cell automatically installs ClickHouse inside the VM.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb)

> **Note:** Data persists only within the active Colab session. Additional notebooks are available in the `examples/` directory.

---

## 📚 Documentation & Resources

- [📖 Official Documentation](https://timedb.readthedocs.io)
- [⚙️ Installation Guide](https://timedb.readthedocs.io/en/latest/installation.html)
- [🐍 SDK Guide](https://timedb.readthedocs.io/en/latest/sdk.html)
- [🌐 Reference](https://timedb.readthedocs.io/en/latest/reference.html)
- [💡 Examples & Notebooks](examples/)

---

## 🤝 Contributing

Contributions are welcome! If you're interested in improving TimeDB, please see our [Development Guide](DEVELOPMENT.md) for local setup instructions.

---

<div align="center">
<p>Licensed under the <a href="LICENSE">Apache-2.0 License</a>.</p>
<p>Find a bug or have a feature request? <a href="https://github.com/rebase-energy/timedb/issues">Open an Issue</a>.</p>
</div>
