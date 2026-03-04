"""
Example: Load EPEX intraday continuous trade data into TimeDB and animate price evolution.

Demonstrates TimeDB's overlapping series with real-world energy market data:
- valid_time  = delivery period (when energy is delivered)
- knowledge_time = execution time (when the trade happened)
- Overlapping reads show how prices evolve as new trades come in

Requires:
- EPEX continuous trade CSVs (zip or plain) with columns:
  DeliveryStart, ExecutionTime, Price, Volume, TradePhase
- Day-ahead auction prices CSV (optional, for baseline)
- PostgreSQL + TimescaleDB running (see README)
"""
import os
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.animation import FFMpegWriter, FuncAnimation

import timedb as td

# --- Configuration (adjust to your data paths) ---
INTRADAY_DIR = Path("path/to/continuous_trades")
DAYAHEAD_CSV = Path("path/to/dayahead_prices.csv")


# ──────────────────────────────────────────────
# Data Loading
# ──────────────────────────────────────────────

def read_trades_from_zip(zip_path):
    """Read EPEX continuous trades from a .csv.zip file."""
    with zipfile.ZipFile(zip_path) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(
                f, comment="#",
                parse_dates=["DeliveryStart", "DeliveryEnd", "ExecutionTime"],
            )


def read_dayahead_prices(csv_path):
    """Read EPEX day-ahead auction prices (wide format) → long format DataFrame."""
    df = pd.read_csv(csv_path, comment="#")
    hour_cols = [c for c in df.columns if c.startswith("Hour")]
    df_long = df.melt(
        id_vars=["Delivery day"], value_vars=hour_cols,
        var_name="hour_label", value_name="price",
    )
    df_long["hour_num"] = df_long["hour_label"].str.extract(r"^Hour (\d+)$").astype(float)
    df_long = df_long.dropna(subset=["hour_num", "price"])
    df_long["hour_num"] = df_long["hour_num"].astype(int) - 1

    df_long["valid_time"] = pd.to_datetime(
        df_long["Delivery day"], dayfirst=True
    ) + pd.to_timedelta(df_long["hour_num"], unit="h")
    df_long["valid_time"] = df_long["valid_time"].dt.tz_localize(
        "Europe/Amsterdam", ambiguous="infer", nonexistent="shift_forward"
    )
    return (
        df_long[["valid_time", "price"]].rename(columns={"price": "value"})
        .sort_values("valid_time").reset_index(drop=True)
    )


# ──────────────────────────────────────────────
# TimeDB Ingestion
# ──────────────────────────────────────────────

def setup_series():
    """Create TimeDB series for intraday and day-ahead data."""
    td.create_series(
        name="epex_intraday", unit="EUR/MWh",
        labels={"market": "continuous"}, overlapping=True,
    )
    td.create_series(
        name="epex_dayahead", unit="EUR/MWh",
        labels={"market": "dayahead"}, overlapping=False,
    )


def load_intraday_trades(file_path):
    """Load one day of continuous trades into TimeDB as VWAP per 1-min bucket."""
    df = read_trades_from_zip(file_path)

    for col in ["DeliveryStart", "DeliveryEnd", "ExecutionTime"]:
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")

    df_cont = df[df["TradePhase"] == "CONT"].copy()
    df_cont["exec_bucket"] = df_cont["ExecutionTime"].dt.floor("1min")

    series = td.get_series("epex_intraday").where(market="continuous")
    inserted = 0

    for exec_time, group in df_cont.groupby("exec_bucket"):
        group = group.copy()
        group["weighted_price"] = group["Price"] * group["Volume"]
        vwap = group.groupby("DeliveryStart").agg(
            weighted_price=("weighted_price", "sum"), total_volume=("Volume", "sum"),
        )
        vwap["value"] = vwap["weighted_price"] / vwap["total_volume"]

        insert_df = pd.DataFrame({
            "valid_time": vwap.index, "value": vwap["value"].values,
        })
        series.insert(df=insert_df, knowledge_time=exec_time.to_pydatetime())
        inserted += len(insert_df)

    return inserted


# ──────────────────────────────────────────────
# Animated Visualization
# ──────────────────────────────────────────────

def create_animation(date_str="2024-01-01", output_path="epex_animation.mp4", fps=15):
    """Create an animated MP4 of intraday price evolution for a given day."""
    date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    next_date = date + timedelta(days=1)

    # Load from TimeDB
    df_da = td.get_series("epex_dayahead").where(market="dayahead").read(
        start_valid=date, end_valid=next_date
    )
    df_intra = td.get_series("epex_intraday").where(market="continuous").read(
        start_valid=date, end_valid=next_date, overlapping=True,
    )

    # Prepare frames (5-min intervals)
    kt = df_intra.index.get_level_values("knowledge_time").unique().sort_values()
    frame_times = pd.date_range(kt.min().floor("5min"), kt.max().ceil("5min"), freq="5min")

    frames = []
    running_min, running_max = {}, {}
    for t in frame_times:
        mask = df_intra.index.get_level_values("knowledge_time") <= t
        df_known = df_intra[mask]
        if len(df_known) == 0:
            continue
        latest = df_known.groupby(level="valid_time").last()
        for vt, row in latest.iterrows():
            v = row["value"]
            running_min[vt] = min(running_min.get(vt, v), v)
            running_max[vt] = max(running_max.get(vt, v), v)
        frames.append({"time": t, "latest": latest, "min": dict(running_min), "max": dict(running_max)})

    # Setup plot
    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.set_xlabel("Delivery Hour (UTC)", color="white", fontsize=12)
    ax.set_ylabel("Price (EUR/MWh)", color="white", fontsize=12)
    ax.tick_params(colors="white")
    for s in ax.spines.values():
        s.set_color("#444")
    ax.set_xlim(date, next_date)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))

    if len(df_da) > 0:
        ax.step(df_da.index.get_level_values("valid_time"), df_da["value"].values,
                where="post", color="#888", linewidth=1.5, linestyle="--", label="Day-Ahead")

    line, = ax.step([], [], where="post", color="#e94560", linewidth=2.5, label="Intraday Latest")
    ts_text = ax.text(0.02, 0.95, "", transform=ax.transAxes, color="white", fontsize=14,
                      fontweight="bold", va="top",
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="#0f3460", alpha=0.8))

    all_vals = df_intra["value"].values
    margin = (np.nanmax(all_vals) - np.nanmin(all_vals)) * 0.1
    ax.set_ylim(np.nanmin(all_vals) - margin, np.nanmax(all_vals) + margin)
    ax.set_title(f"EPEX Intraday Continuous — {date.strftime('%d %B %Y')}",
                 color="white", fontsize=15, fontweight="bold")
    ax.legend(loc="upper right", facecolor="#16213e", edgecolor="#444", labelcolor="white")

    def update(i):
        f = frames[i]
        vt = f["latest"].index.get_level_values("valid_time").sort_values()
        line.set_data(vt, f["latest"].loc[vt, "value"].values)
        while ax.collections:
            ax.collections[0].remove()
        svt = sorted(f["min"])
        ax.fill_between(svt, [f["min"][v] for v in svt], [f["max"][v] for v in svt],
                        step="post", color="#4a90d9", alpha=0.15)
        ts_text.set_text(f["time"].strftime("Execution: %Y-%m-%d %H:%M UTC"))
        return line, ts_text

    anim = FuncAnimation(fig, update, frames=len(frames), interval=1000 // fps, blit=False)
    anim.save(output_path, writer=FFMpegWriter(fps=fps), dpi=120)
    plt.close()
    print(f"Saved {output_path} ({len(frames)} frames, {len(frames)/fps:.1f}s)")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Initialize
    td.create()
    setup_series()

    # 2. Load data
    td.get_series("epex_dayahead").where(market="dayahead").insert(
        df=read_dayahead_prices(DAYAHEAD_CSV)
    )

    for f in sorted(INTRADAY_DIR.glob("*.csv.zip"))[:1]:  # start with 1 day
        print(f"Loading {f.name}...")
        print(f"  {load_intraday_trades(f)} VWAP points inserted")

    # 3. Animate
    create_animation("2024-01-01", "epex_animation.mp4")
