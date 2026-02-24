# 🚀 v0.1.4 — The Initial Release of TimeDB

We are thrilled to announce the official initial release of TimeDB! ⏱️

TimeDB is an open-source, opinionated time-series database built on top of PostgreSQL and TimescaleDB. While traditional databases treat time as a single dimension, TimeDB is built for the messy reality of forecasts, revisions, and "human-in-the-loop" data corrections.

## 🧊 Why TimeDB?

In industries like energy, weather, and finance, data isn't just a point in time—it's an evolving story. TimeDB introduces a 3D Temporal Data Model to ensure you never lose the "time-of-knowledge" history:

- 📅 `valid_time`: When the event actually happens.
- ⏰ `knowledge_time`: When the prediction or observation was first recorded.
- ✏️ `change_time`: When a value was manually adjusted or corrected (full audit trail).

## ✨ Key Features

- **Overlapping Forecasts**: Native support for storing multiple forecast revisions for the same time period without overwriting history.
- **True Backtesting**: Query your data exactly as it was known at any point in the past ("What did our model see last Monday?").
- **Python SDK & FastAPI**: A seamless developer experience designed for data scientists and engineers.
- **Unit Management**: Built-in validation and conversion to ensure data consistency across teams.
- **Label-Based Organization**: Slice and dice your series using meaningful metadata dimensions.
- **Auditable by Default**: Every change tracks `changed_by` and annotations, moving away from silent overwrites.

## 🛠️ Getting Started

Install via `pip`:

```bash
pip install timedb
```

Don't want to set up a database yet? Jump into our Google Colab Quickstart. It spins up a temporary PostgreSQL + TimescaleDB instance for you to play with.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb)

## 📚 Resources

- **Documentation**: <https://timedb.readthedocs.io>
- **Community**: [Join us on Slack](https://www.rebase.energy/join-slack)
- **License**: Apache-2.0

**Full Changelog**: <https://github.com/rebase-energy/timedb/commits/v0.1.4>

**New Contributors**: A huge thank you to everyone who helped shape the initial architecture!

Are you using TimeDB for your projects? We'd love to hear your feedback. Open an issue or join our Slack community to help us build the future of temporal data.