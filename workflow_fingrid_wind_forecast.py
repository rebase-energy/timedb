import os
import time
import uuid
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import modal

from pg_insert_table import insert_run_with_values

load_dotenv()

app = modal.App("fingrid-to-neon")
image = (
    modal.Image.debian_slim()
    .pip_install("pandas", "requests", "python-dotenv", "psycopg[binary]")  # dotenv optional if you still use it locally
    .add_local_file("pg_insert_table.py", remote_path="/root/pg_insert_table.py", copy=True)
)

secrets = [
    modal.Secret.from_name("fingrid-api"),   # contains FINGRID_API_KEY
    modal.Secret.from_name("neon-secret"),      # contains NEON_PG_URL
]

def get_fingrid_data(fingrid_dataset, start_time, end_time):
    url = (
    f"https://data.fingrid.fi/api/datasets/{fingrid_dataset[1]}/data"
        f"?startTime={start_time}"
        f"&endTime={end_time}"
        "&pageSize=20000"
    )

    r = requests.get(
        url,
        headers={"x-api-key": os.environ["FINGRID_API_KEY"]}
    )

    data = r.json()

    df = pd.DataFrame(data["data"])
    df = df[["startTime", "value"]]
    df = df.rename(columns={"startTime": "valid_time"})
    df = df.set_index("valid_time")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df.loc[:, "value"] = df["value"].astype(float)
    df = df.reset_index()
    df["series_key"] = fingrid_dataset[0]

    return df

@app.function(
    schedule=modal.Cron("*/5 * * * *"),
    image=image,
    secrets=secrets,
    timeout=60 * 10,
)
def main():
    run_id = str(uuid.uuid4())
    workflow_id = "fingrid-wind-forecast"
    run_start_time = datetime.now(timezone.utc)
    start_time = (run_start_time - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    end_time = (run_start_time + timedelta(days=3)).isoformat().replace("+00:00", "Z")

    df_forecast = get_fingrid_data(("forecast_mean", 245), start_time, end_time)
    time.sleep(4)
    df_capacity = get_fingrid_data(("capacity", 268), start_time, end_time)

    df = pd.concat([df_forecast, df_capacity])
    df["run_id"] = run_id
    value_rows = list(
        df[["run_id", "valid_time", "series_key", "value"]].itertuples(
            index=False,
            name=None
        )
    )

    run_finish_time = datetime.now(timezone.utc)

    insert_run_with_values(
        conninfo=os.getenv("NEON_PG_URL"),
        run_id=run_id,
        workflow_id=workflow_id,
        run_start_time=run_start_time,
        run_finish_time=run_finish_time,
        value_rows=value_rows,
    )