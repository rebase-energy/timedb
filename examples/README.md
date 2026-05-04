# timedb Examples

Interactive Jupyter notebook demonstrating timedb's bitemporal time-series model.

## Try in Google Colab

Click the badge to open the notebook in Colab — no local setup required. The first cell installs PostgreSQL + ClickHouse automatically inside the Colab VM (~2–3 min).

| Notebook | Colab |
|----------|-------|
| Quickstart | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb) |

> **Note**: The Colab setup cell installs PostgreSQL + ClickHouse. Data persists only within the active Colab session.

## Prerequisites (local)

1. **ClickHouse**: timedb writes/reads time-series values through `TIMEDB_CH_URL`.

2. **Environment variable**:
   ```bash
   # Bash/Zsh
   export TIMEDB_CH_URL='http://default:@localhost:8123/default'
   ```

   ```fish
   # Fish
   set -x TIMEDB_CH_URL http://default:@localhost:8123/default
   ```

3. **Install dependencies**:
   ```bash
   pip install -e .
   pip install matplotlib jupyter
   ```

## Notebook

### Quickstart (`quickstart.ipynb`)

Walks through the full bitemporal model in one notebook:

- Setup
- Insert a forecast and read it back
- Insert a revised forecast — see the latest value win
- Read the full version history
- Visualize how forecast revisions evolved
- Correct an erroneous value (immutable storage; corrections are new rows)
- Audit the correction trail

## Running

```bash
jupyter notebook examples/quickstart.ipynb
# OR
jupyter lab examples/quickstart.ipynb
```

## Higher-level API

For asset hierarchies (sites, turbines, edges), unit conversion, and run-metadata tracking, see [`energydb`](../../energydb/examples/quickstart.ipynb), which uses timedb as its time-series backend.
