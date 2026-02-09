# timedb Examples

This directory contains interactive Jupyter notebooks demonstrating how to use timedb.

## Prerequisites

1. **PostgreSQL Database**: You need a PostgreSQL database (version 12+) to run these examples.

2. **Environment Variables**: Set one of these environment variables with your database connection string:
   ```bash
   export TIMEDB_DSN="postgresql://user:password@host:port/database"
   # OR
   export DATABASE_URL="postgresql://user:password@host:port/database"
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

### Notebook 2: Units Validation (`nb_02_units_validation.ipynb`)
Working with physical units in timedb:
- Using pint for unit handling
- Validating units on insert and read
- Unit conversions

### Notebook 3: Forecast Revisions (`nb_03_forecast_revisions.ipynb`)
Shows how timedb handles multiple forecast runs:
- Creating multiple forecast runs
- Understanding flat vs overlapping query modes
- How timedb tracks "time of knowledge"

### Notebook 4: Time Series Changes (`nb_04_timeseries_changes.ipynb`)
Demonstrates human-in-the-loop corrections:
- Updating existing values with annotations
- Adding quality flags using tags
- Tracking who made changes and when

### Notebook 5: Multiple Series (`nb_05_multiple_series.ipynb`)
Working with multiple time series:
- Managing multiple series in a single database
- Querying and filtering by series

### Notebook 6: Advanced Querying (`nb_06_advanced_querying.ipynb`)
Advanced data retrieval patterns:
- Complex filters and queries
- Time range queries
- Aggregations

### Notebook 7: API Usage (`nb_07_api_usage.ipynb`)
Using the REST API:
- Starting the API server
- Making HTTP requests

## Running Notebooks

Open any notebook with:

```bash
jupyter notebook examples/nb_01_write_read_pandas.ipynb
# OR
jupyter lab examples/nb_01_write_read_pandas.ipynb
```

## Workflow Examples

The `workflows/` subdirectory contains real-world workflow examples:

- `workflow_fingrid_wind_forecast.py` - Fetching and storing Fingrid wind forecast data
- `workflow_nordpool_id.py` - Fetching and storing Nord Pool intraday market data

These workflows demonstrate how to use timedb with external data sources and scheduled jobs using Modal.

## Notes

- Each notebook creates its own schema, so you can run them independently
- Notebooks use sample data - modify them to use your own data
- The examples are designed to be educational and may need adaptation for production use

## Next Steps

After running these examples, you can:
- Explore the test suite in `tests/` to see more detailed usage patterns
- Check the API documentation in `timedb/api.py` for REST API usage
