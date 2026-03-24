# timedb Examples

This directory contains interactive Jupyter notebooks demonstrating how to use timedb.

## Try in Google Colab

Click a badge to open the notebook in Colab — no local setup required. The first cell installs PostgreSQL + ClickHouse automatically inside the Colab VM (~2–3 min).

| Notebook | Colab |
|----------|-------|
| Quickstart | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/quickstart.ipynb) |
| Writing and Reading with Pandas | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_01_write_read_pandas.ipynb) |
| Write Methods & Multi-Series Insert | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_02_multi_insert.ipynb) |
| Forecast Revisions | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_03_forecast_revisions.ipynb) |
| Relative Forecasts | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_04_relative_forecasts.ipynb) |
| Time Series Changes | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_05_timeseries_changes.ipynb) |
| REST API Usage | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/timedb/blob/main/examples/nb_06_api_usage.ipynb) |

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

### Notebook 1: Writing and Reading with Pandas (`nb_01_write_read_pandas.ipynb`)
Learn the fundamentals of writing and reading time series data:
- Writing time series data from pandas DataFrames
- Reading data back into DataFrames
- Working with series IDs

### Notebook 2: Write Methods & Multi-Series Insert (`nb_02_multi_insert.ipynb`)
Multi-series bulk ingestion using `write()`:
- Creating multiple series in one call with `create_series_many()`
- Building a long-format DataFrame and inserting with `td.write()`
- Unit conversion, batch tracking with `batch_cols`, and `batch_params`
- Reading back per-series results and error handling

### Notebook 3: Forecast Revisions (`nb_03_forecast_revisions.ipynb`)
Shows how timedb handles multiple forecast runs:
- Creating multiple forecast runs
- Understanding flat vs overlapping query modes
- How timedb tracks "time of knowledge"

### Notebook 4: Relative Forecasts (`nb_04_relative_forecasts.ipynb`)
Per-window knowledge_time cutoffs for day-ahead and shifted forecasts:
- Using `read_relative()` with `window_length` and `issue_offset`
- Daily shorthand mode (`days_ahead`, `time_of_day`)
- Comparing globally-latest vs window-cutoff views

### Notebook 5: Time Series Changes (`nb_05_timeseries_changes.ipynb`)
Demonstrates updating records and tracking changes over time:
- Updating existing values for flat and overlapping series
- Versioning and audit trail for overlapping series
- Three lookup methods: latest, all versions, relative

### Notebook 6: REST API Usage (`nb_06_api_usage.ipynb`)
Using the REST API:
- Starting the API server
- Making HTTP requests for reading and writing data

## Running Notebooks

Open any notebook with:

```bash
jupyter notebook examples/nb_01_write_read_pandas.ipynb
# OR
jupyter lab examples/nb_01_write_read_pandas.ipynb
```

## Notes

- Each notebook creates its own schema, so you can run them independently
- Notebooks use sample data - modify them to use your own data
- The examples are designed to be educational and may need adaptation for production use

## Next Steps

After running these examples, you can:
- Explore the test suite in `tests/` to see more detailed usage patterns
- Check the API documentation in `timedb/api.py` for REST API usage
