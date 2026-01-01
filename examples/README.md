# timedb Examples

This directory contains example scripts demonstrating how to use timedb.

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

## Examples

### 1. Basic Usage (`01_basic_usage.py`)
Demonstrates the fundamental operations:
- Creating the database schema
- Inserting a run with time series values
- Reading the data back

**Run it:**
```bash
python examples/01_basic_usage.py
```

### 2. Forecast Revisions (`02_forecast_revisions.py`)
Shows how timedb handles multiple forecast runs for the same time period:
- Creating multiple forecast runs
- Understanding flat vs overlapping query modes
- How timedb tracks "time of knowledge"

**Run it:**
```bash
python examples/02_forecast_revisions.py
```

### 3. Updates and Annotations (`03_updates_and_annotations.py`)
Demonstrates human-in-the-loop corrections:
- Updating existing values with annotations
- Adding quality flags using tags
- Tracking who made changes and when

**Run it:**
```bash
python examples/03_updates_and_annotations.py
```

### 4. Interval Values (`04_interval_values.py`)
Shows how to work with time intervals:
- Storing interval values (valid_time to valid_time_end)
- Mixing point-in-time and interval values
- Querying interval data

**Run it:**
```bash
python examples/04_interval_values.py
```

## Jupyter Notebooks

Interactive notebooks demonstrating pandas DataFrame integration with TimeDB:

### Notebook 1: Writing DataFrames (`notebook_01_write_dataframe.ipynb`)
Learn how to convert pandas DataFrames to TimeDB format and insert them:
- Converting simple time series DataFrames
- Handling multiple value keys (mean, quantiles, etc.)
- Working with interval values
- Helper functions for common conversions

**Open it:**
```bash
jupyter notebook examples/notebook_01_write_dataframe.ipynb
# OR
jupyter lab examples/notebook_01_write_dataframe.ipynb
```

### Notebook 2: Reading DataFrames (`notebook_02_read_dataframe.ipynb`)
Learn how to read data from TimeDB into pandas DataFrames:
- Reading in flat vs overlapping modes
- Converting long-format to wide-format (pivot)
- Filtering and querying data
- Analyzing forecast revisions

**Open it:**
```bash
jupyter notebook examples/notebook_02_read_dataframe.ipynb
# OR
jupyter lab examples/notebook_02_read_dataframe.ipynb
```

### Notebook 3: Complete Workflow (`notebook_03_complete_workflow.ipynb`)
A complete end-to-end example combining all concepts:
- Generate forecast data in a DataFrame
- Write to TimeDB
- Read back and analyze
- Insert revised forecasts
- Compare forecast revisions
- Calculate accuracy metrics

**Open it:**
```bash
jupyter notebook examples/notebook_03_complete_workflow.ipynb
# OR
jupyter lab examples/notebook_03_complete_workflow.ipynb
```

**Note:** The notebooks require `pandas`, `matplotlib`, and `jupyter` to be installed:
```bash
pip install pandas matplotlib jupyter
```

## Running All Examples

You can run all examples in sequence:

```bash
for example in examples/*.py; do
    if [[ "$example" != *"__init__"* ]]; then
        echo "Running $example..."
        python "$example"
        echo ""
    fi
done
```

## Notes

- Each example creates its own schema, so you can run them independently
- Examples use sample data - modify them to use your own data
- The examples are designed to be educational and may need adaptation for production use

## Next Steps

After running these examples, you can:
- Explore the test suite in `tests/` to see more detailed usage patterns
- Check the API documentation in `timedb/api.py` for REST API usage
- Review the workflow examples in `timedb/workflows/` for real-world use cases

