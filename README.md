<div align="center">
  <h1>⏱️ TimeDB</h1>
  <p><b>An open-source, opinionated time-series database built on PostgreSQL & ClickHouse.</b></p>

  <a href="https://pypi.org/project/timedb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/timedb?color=blue&style=flat-square"></a>
  <a href="https://pypi.org/project/timedb/"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/timedb?style=flat-square"></a>
  <a href="https://github.com/rebase-energy/timedb/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"></a>
  <a href="https://www.rebase.energy/join-slack"><img alt="Slack" src="https://img.shields.io/badge/Slack-Join%20Community-4A154B?logo=slack&style=flat-square"></a>
</div>

<br/>

**TimeDB** is designed to natively handle **overlapping forecast revisions**, **auditable human-in-the-loop updates**, and **"time-of-knowledge" history**. Using a three-dimensional temporal data model, it provides a seamless workflow through its Python SDK and FastAPI backend.

Traditional time-series databases assume one immutable value per timestamp. TimeDB is built for complex, real-world domains where forecasts evolve, data gets corrected, and historical backtesting requires strict audits.

---

## 🧊 The 3D Temporal Data Model

At the heart of TimeDB is its three-dimensional approach to time. We track not just *when* data is valid, but *when it became known* and *when it was altered*.

| Dimension | Description | Real-World Example |
| :---- | :--- | :--- |
| 📅&nbsp;**`valid_time`** | The time the value represents a fact for. | *"Wind speed forecast for Wednesday 12:00"* |
| ⏰&nbsp;**`knowledge_time`** | The time when the value was predicted/known. | *"Generated on Monday 18:00"* |
| ✏️&nbsp;**`change_time`** | The time when the value was written or changed. | *"Manually overridden on Tuesday 09:00"* |

> **Audit & Metadata:** Every data point also supports `tags`, `annotations`, and `changed_by` to maintain a perfect audit trail of who changed what, when, and why.

---

## ✨ Why Choose TimeDB?

- 📊 **Forecast Revisions:** Store overlapping forecasts with full provenance.
- 🔄 **Auditable Updates:** Manual adjustments generate audit trails, not silent overwrites.
- ⏪ **True Backtesting:** Query historical data as of any point in time (*"What did our model know last Monday?"*).
- 🏷️ **Label-Based Organization:** Easily filter and slice series by meaningful dimensions.

<div align="center">
  <p></p>  <img src="example.gif" alt="TimeDB demo" width="700"/>
</div>

---

## 🚀 Quick Start

### 1. Installation

```bash
pip install timedb
```

Requires Python 3.9+, PostgreSQL (series metadata), and ClickHouse (time-series values).

### 2. Usage Example

```python
import timedb as td
import pandas as pd
from datetime import datetime, timezone
from timedb import TimeSeries

# Create Schema
td.create()

# 1. Create a forecast series
td.create_series(
    name="wind_power",
    unit="MW",
    labels={"site": "offshore_1"},
    overlapping=True
)

# 2. Insert forecast with knowledge_time
knowledge_time = datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc)
ts = TimeSeries.from_pandas(
    pd.DataFrame({
        'valid_time': pd.date_range('2025-01-01', periods=24, freq='h', tz='UTC'),
        'value': [100 + i*2 for i in range(24)]
    }),
    unit='MW',
)

td.get_series("wind_power") \
   .where(site="offshore_1") \
   .insert(ts, knowledge_time=knowledge_time)

# 3. Read latest forecast — returns a TimeSeries
ts_latest = td.get_series("wind_power").where(site="offshore_1").read()
df_latest = ts_latest.to_pandas()  # pd.DataFrame with valid_time index

# 4. Read forecast history (one row per knowledge_time × valid_time)
ts_all = td.get_series("wind_power").where(site="offshore_1").read(overlapping=True)
```

---

## 🧪 Try it in Google Colab

Want to try TimeDB without a local setup? Open our Quickstart in Colab — the first cell automatically installs PostgreSQL + ClickHouse inside the VM.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb)

> **Note:** Data persists only within the active Colab session. Additional notebooks are available in the `examples/` directory.

---

## 📚 Documentation & Resources

- [📖 Official Documentation](https://timedb.readthedocs.io)
- [⚙️ Installation Guide](https://timedb.readthedocs.io/en/latest/installation.html)
- [🐍 Python SDK Documentation](https://timedb.readthedocs.io/en/latest/sdk.html)
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
