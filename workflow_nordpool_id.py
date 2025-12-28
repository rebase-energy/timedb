import os
import json
import time
import uuid
from curl_cffi import requests
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv
import modal

from pg_insert_table_with_metadata import insert_run_with_values_and_metadata

load_dotenv()

app = modal.App("workflow-nordpool-id")
image = (
    modal.Image.debian_slim()
    .pip_install("pandas", "curl_cffi", "python-dotenv", "psycopg[binary]")  # dotenv optional if you still use it locally
    .add_local_file("pg_insert_table.py", remote_path="/root/pg_insert_table.py", copy=True)
    .add_local_file("pg_insert_table_with_metadata.py", remote_path="/root/pg_insert_table_with_metadata.py", copy=True)
)
secrets = [
    modal.Secret.from_name("neon-secret"),      # contains NEON_PG_URL
]
vol = modal.Volume.from_name("volume-nordpool-id", create_if_missing=True)

MOUNT = "/data"

def get_nordpool_id_data(delivery_date, delivery_area): 
    r = requests.get(
        "https://dataportal-api.nordpoolgroup.com/api/IntradayMarketStatistics",
        params={"date": delivery_date, "deliveryArea": delivery_area},
        impersonate="chrome",
    )
    data = r.json()

    return data

def write_to_volume(path, data): 
    # Write to the volume-mounted filesystem
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"File written: {path}")

@app.function(
    schedule=modal.Cron("*/5 * * * *"),
    image=image,
    secrets=secrets,
    volumes={MOUNT: vol},
    timeout=60 * 10,
)
def workflow_nordpool_id_today(): 
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S.%fZ")

    today = date.today().isoformat()
    tomorrow = (date.today() + timedelta(days=1)).isoformat()

    data_today = get_nordpool_id_data(delivery_date=today, delivery_area="SE4")
    data_tomorrow = get_nordpool_id_data(delivery_date=tomorrow, delivery_area="SE4")

    write_to_volume(path=f"{MOUNT}/nordpool_id_{today}_timestamp_{timestamp}.json", data=data_today)
    write_to_volume(path=f"{MOUNT}/nordpool_id_{tomorrow}_timestamp_{timestamp}.json", data=data_tomorrow)
    
    vol.commit()

    #write to postgres
    run_params = {k: v for k, v in data_today.items() if k != "contracts"}

    run_id = str(uuid.uuid4())
    workflow_id = "nordpool-id-today"
    run_start_time = datetime.now(timezone.utc)

    df = pd.DataFrame(data_today["contracts"])

    # 1) Create tz-aware valid_time from deliveryStart
    df["valid_time"] = pd.to_datetime(df["deliveryStart"], utc=True)
    df["valid_time_end"] = pd.to_datetime(df["deliveryEnd"], utc=True)

    value_cols = [
        "highPrice", "lowPrice", "openPrice", "closePrice", "averagePrice",
        "volume", "buyVolume", "sellVolume", "averagePriceLast3H", "averagePriceLast1H"
    ]
    metadata_cols = [
        "deliveryEnd", "isLocalContract", "contractId", "contractName",
        "contractOpenTime", "contractCloseTime", "openTradeTime", "closeTradeTime"
    ]

    # Melt the dataframe into value rows: (valid_time, value_key, value)
    df_values = df.melt(
        id_vars=["valid_time", "valid_time_end"],
        value_vars=value_cols,
        var_name="value_key",
        value_name="value",
    )

    # Now build the value_rows list ready for insert_run_with_values_and_metadata:
    value_rows = list(df_values[["valid_time", "valid_time_end", "value_key", "value"]].itertuples(index=False, name=None))
    #value_rows = list(df_values[["valid_time", "value_key", "value"]].itertuples(index=False, name=None))

    # Take only those columns plus valid_time
    df_meta = df[["valid_time"] + [c for c in metadata_cols if c in df.columns]].copy()
    df_meta[0] = df_meta[metadata_cols].apply(lambda r: r.to_dict(), axis=1)
    metadata_rows = [
        (row["valid_time"], row.iloc[0])
        for _, row in df_meta.reset_index().iterrows()
    ]

    df_meta = df[["valid_time"] + metadata_cols].copy()
    df_meta[0] = df_meta[metadata_cols].apply(lambda r: r.to_dict(), axis=1)
    metadata_rows = [
        (valid_time, meta_dict)
        for valid_time, meta_dict in df_meta[["valid_time", 0]].itertuples(index=False)
    ]

    conninfo = os.environ["NEON_PG_URL3"]
    insert_run_with_values_and_metadata(
        conninfo,
        run_id=run_id,
        workflow_id=workflow_id,
        run_start_time=run_start_time,
        run_finish_time=run_start_time,
        value_rows=value_rows,
        run_params=run_params,
        metadata_rows=metadata_rows,
    )

@app.local_entrypoint()
def local_main():
    workflow_nordpool_id_today.remote()