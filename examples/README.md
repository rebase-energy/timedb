# timedb Examples

This directory contains interactive Jupyter notebooks demonstrating how to use timedb.

## Try in Google Colab

Click a badge to open the notebook in Colab — no local setup required. The first cell installs PostgreSQL + ClickHouse automatically inside the Colab VM (~2–3 min).

| Notebook | Colab |
|----------|-------|
| Quickstart | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb) |
| Single-Series Insert and Read | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_01_single_series.ipynb) |
| Multi-Series Write and Read | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_02_multi_series.ipynb) |
| Forecast Revisions | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_03_forecast_revisions.ipynb) |
| Querying Time Series Data | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_04_reads.ipynb) |
| Forecast Corrections | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_05_corrections.ipynb) |
| REST API Usage | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_06_api.ipynb) |

> **Note**: The Colab setup cell installs PostgreSQL + ClickHouse. Data persists only within the active Colab session.

## Prerequisites (local)

1. **PostgreSQL Database**: You need a PostgreSQL database (version 12+) to run these examples.

2. **Environment Variables**: Set both database connection strings:
   ```bash
   # Bash/Zsh
   export TIMEDB_PG_DSN='postgresql://user:password@host:port/database'
   export TIMEDB_CH_URL='http://default:@localhost:8123/default'
   ```

   ```fish
   # Fish
   set -x TIMEDB_PG_DSN postgresql://user:password@host:port/database
   set -x TIMEDB_CH_URL http://default:@localhost:8123/default
   ```

3. **Install Dependencies**: Make sure you have timedb installed:
   ```bash
   pip install -e .
   ```

4. **Install Jupyter**: The notebooks require Jupyter to run:
   ```bash
   pip install pandas matplotlib jupyter
   ```

## Notebooks

### Notebook 1: Single-Series Insert and Read (`nb_01_single_series.ipynb`)
Learn the fundamentals of writing and reading time series data:
- Single-series insert and read using polars and pandas DataFrames
- Label-based routing via `.where()`
- Working with series IDs

### Notebook 2: Multi-Series Write and Read (`nb_02_multi_series.ipynb`)
Multi-series bulk ingestion and reading:
- Creating multiple series in one call with `create_series_many()`
- Building a long-format DataFrame and inserting with `td.write()`
- Reading multiple series at once with `td.read()` using a manifest DataFrame
- Unit conversion, run tracking with `run_cols`, and `run_params`

### Notebook 3: Forecast Revisions (`nb_03_forecast_revisions.ipynb`)
Shows how timedb handles multiple forecast runs:
- Creating multiple forecast runs
- Understanding flat vs overlapping query modes
- How timedb tracks "time of knowledge"

### Notebook 4: Querying Time Series Data (`nb_04_reads.ipynb`)
All query patterns for reading time series data:
- Latest values, valid-time and knowledge-time range filters
- Full revision history with `read(overlapping=True)`
- Per-window cutoffs with `read_relative()` (`days_ahead`, `time_of_day`)
- Comparing globally-latest vs window-cutoff views

### Notebook 5: Forecast Corrections (`nb_05_corrections.ipynb`)
Correcting erroneous forecast values:
- Updating existing values for flat and overlapping series
- Versioning and audit trail for overlapping series
- Three lookup methods: latest, all versions, relative

### Notebook 6: REST API Usage (`nb_06_api.ipynb`)
Using the REST API:
- Starting the API server
- Making HTTP requests for reading and writing data (JSON and Arrow IPC)
- Single-series (`POST /values`, `GET /values`) and multi-series (`POST /write`, `POST /read`) endpoints

## Running Notebooks

Open any notebook with:

```bash
jupyter notebook examples/nb_01_single_series.ipynb
# OR
jupyter lab examples/nb_01_single_series.ipynb
```

## Notes

- Each notebook creates its own schema, so you can run them independently
- Notebooks use sample data - modify them to use your own data
- The examples are designed to be educational and may need adaptation for production use

## Next Steps

After running these examples, you can:
- Explore the test suite in `tests/` to see more detailed usage patterns
- Check the API documentation in `timedb/api.py` for REST API usage
