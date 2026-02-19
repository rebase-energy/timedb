# TimeDB

**TimeDB** is an **open source, opinionated time series database** built on **PostgreSQL** and **TimescaleDB** designed to natively handle **overlapping forecast revisions**, **auditable human-in-the-loop updates**, and **"time-of-knowledge" history** through a three-dimensional temporal data model. TimeDB provides a seamless workflow through its **Python SDK** and **FastAPI** backend.

## Features

- **Time-of-Knowledge History**: Track not just when data is valid, but when it became known
- **Forecast Revisions**: Store overlapping forecasts with full provenance
- **Auditable Updates**: Every change records who, what, when, and why
- **Backtesting Ready**: Query historical data as of any point in time
- **Label-Based Organization**: Filter series by meaningful dimensions


## Why timedb?

Traditional time series databases assume one immutable value per timestamp. **TimeDB** is built for domains where:

- 📊 **Forecasts evolve**: Multiple predictions for the same timestamp from different times
- 🔄 **Data gets corrected**: Manual adjustments need audit trails, not overwrites
- ⏪ **Backtesting requires history**: "What did we know on Monday?" vs "what do we know now?"
- ✏️ **Humans review data**: Track who changed what, when, and why


## Quick Start

```bash
pip install timedb
```

```python
import timedb as td
import pandas as pd
from datetime import datetime, timezone

td.create()

# Create a forecast series
td.create_series(name="wind_power", unit="MW",
                 labels={"site": "offshore_1"}, overlapping=True)

# Insert forecast with known_time
known_time = datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc)
df = pd.DataFrame({
    'valid_time': pd.date_range('2025-01-01', periods=24, freq='h', tz='UTC'),
    'value': [100 + i*2 for i in range(24)]
})
td.series("wind_power").where(site="offshore_1").insert(df=df, known_time=known_time)

# Read latest forecast
df_latest = td.series("wind_power").where(site="offshore_1").read()

# Read all forecast revisions
df_all = td.series("wind_power").where(site="offshore_1").read(versions=True)
```

> For custom connection settings (host, pool size, etc.), use `TimeDataClient` directly:
> `from timedb import TimeDataClient; td = TimeDataClient(conninfo="postgresql://...")`

## Try in Google Colab

Try the quickstart in Colab — no local setup required. The first cell installs PostgreSQL + TimescaleDB automatically inside the Colab VM (~2 min).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/FreaxMATE/timedb/blob/colab/examples/quickstart.ipynb)

Additional notebooks and Google Colab links are available in the [examples directory](examples/).

> **Note**: The Colab setup cell installs PostgreSQL 14 + TimescaleDB. Data persists only within the active Colab session.

## Documentation

- [Installation Guide](docs/installation.rst)
- [SDK Documentation](docs/sdk.rst)
- [REST API Reference](docs/api_reference.rst)
- [Examples & Notebooks](examples/)
- [Development Guide](DEVELOPMENT.md)

## Data Model

Three time dimensions:

| Dimension | Description | Example |
|-----------|-------------|---------|
| **valid_time** | The time the value represents a fact for | "Wind speed forecast for Wednesday 12:00" |
| **known_time** | The time when the value was known | "Wind speed forecast for Wednesday 12:00 was generated on Monday 18:00" |
| **change_time** | The time when the value was changed | "Wind speed forecast for Wednesday 12:00 was manually changed on Tuesday 9:00" |

Plus metadata: `tags`, `annotation`, `changed_by`, `change_time` for audit trails.

## Requirements

- Python 3.9+
- PostgreSQL 12+ with TimescaleDB
- (Optional) Jupyter for notebooks

## Contributing

Contributions welcome! See [DEVELOPMENT.md](DEVELOPMENT.md) for setup instructions.

## Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## License

Apache-2.0 

## See Also

- [Official Documentation](https://timedb.readthedocs.io/)
- [Examples Repository](examples/)
- [Issue Tracker](https://github.com/rebase-energy/timedb/issues)
