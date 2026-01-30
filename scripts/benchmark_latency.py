"""Benchmark script for TimeDB insert/update/read latency and throughput.

Usage:
    python scripts/benchmark_latency.py --rows 10 100 1000 --series 1 5 10 --repeats 3 --out results.csv

Generates a CSV with measured times and PNG plots in the output directory.
"""
from __future__ import annotations

import argparse
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from timedb import TimeDataClient


def _make_times(n: int, start: Optional[datetime] = None, freq_seconds: int = 3600) -> List[datetime]:
    if start is None:
        start = datetime.now(timezone.utc)
    return [start + i * pd.Timedelta(seconds=freq_seconds) for i in range(n)]


def create_bench_series(td: TimeDataClient, series_name: str, count: int) -> Dict[str, uuid.UUID]:
    """Create `count` series with the same series name and distinct label values.
    Returns mapping from column name (s1..) to series_id.
    """
    mapping: Dict[str, uuid.UUID] = {}
    for i in range(1, count + 1):
        label_val = f"s{i}"
        # Use a predictable label so we can find them if needed
        sid = td.create_series(name=series_name, unit="dimensionless", labels={"id": label_val}, description="benchmark series")
        mapping[label_val] = sid
    return mapping


def build_dataframe(num_rows: int, series_cols: List[str], start: Optional[datetime] = None) -> pd.DataFrame:
    times = _make_times(num_rows, start=start)
    data = {"valid_time": times}
    for col in series_cols:
        # simple sequence values
        data[col] = [float(i + 1) for i in range(num_rows)]
    return pd.DataFrame(data)


def bench_insert(
    td: TimeDataClient,
    series_name: str,
    df: pd.DataFrame,
    series_ids: Dict[str, uuid.UUID],
) -> Tuple[float, object]:
    """Insert by using the TimeDataClient collection API for each series.

    For multi-series benchmarks we perform one insert per series (using the
    same batch_id) so we exercise the collection-based insert path as in
    the quickstart examples.
    """
    from timedb import InsertResult

    batch_id = uuid.uuid4()
    batch_start_time = datetime.now(timezone.utc)

    start_t = time.perf_counter()

    combined_series_ids: Dict[str, uuid.UUID] = {}
    tenant_id = None

    # series_ids keys are label values (s1, s2, ...)
    for label_val, sid in series_ids.items():
        # Build a single-series DataFrame with the column named as the series name
        # to match the quickstart example: column name == series name
        df_single = pd.DataFrame({"valid_time": df["valid_time"], series_name: df[label_val]})

        coll = td.series(series_name).where(id=label_val)
        res = coll.insert_batch(df=df_single, batch_id=batch_id, batch_start_time=batch_start_time)

        # res.series_ids maps the series name to its uuid
        combined_series_ids.update(res.series_ids)
        tenant_id = res.tenant_id

    elapsed = time.perf_counter() - start_t

    # Synthesize an InsertResult similar to the multi-series case
    result = InsertResult(batch_id=batch_id, workflow_id="sdk-workflow", series_ids=combined_series_ids, tenant_id=tenant_id)
    return elapsed, result


def bench_update(td: TimeDataClient, updates: List[dict]) -> float:
    start_t = time.perf_counter()
    td.update_records(updates=updates)
    elapsed = time.perf_counter() - start_t
    return elapsed


def bench_read(td: TimeDataClient, series_name: str, start_valid: datetime, end_valid: datetime) -> float:
    coll = td.series(series_name)
    start_t = time.perf_counter()
    coll.read(start_valid=start_valid, end_valid=end_valid)
    elapsed = time.perf_counter() - start_t
    return elapsed


def run_bench(
    rows_list: List[int],
    series_list: List[int],
    repeats: int = 3,
    series_name: str = "bench_series",
    output_csv: str = "bench_results.csv",
    out_plot_dir: str = "bench_plots",
):
    os.makedirs(out_plot_dir, exist_ok=True)

    td = TimeDataClient()

    results = []

    for num_series in series_list:
        print(f"\n=== Benchmarking {num_series} series ===")
        # Series column keys
        series_cols = [f"s{i}" for i in range(1, num_series + 1)]

        for num_rows in rows_list:
            print(f"- Rows per series: {num_rows}")
            # Repeat measurements to average
            insert_times = []
            update_times = []
            read_times = []

            for r in range(repeats):
                # Reset database before each run to ensure a clean state
                print(f"  run {r+1}/{repeats}: resetting DB (delete/create)")
                td.delete()
                td.create()

                # Create series for this run (fresh DB)
                series_ids = create_bench_series(td, series_name, num_series)

                df = build_dataframe(num_rows=num_rows, series_cols=series_cols)

                # Insert
                elapsed_insert, insert_result = bench_insert(td, series_name, df, series_ids)

                # Prepare updates: update all values to +1.0
                updates = []
                # insert_result.batch_id should contain the batch used
                batch_id = insert_result.batch_id
                tenant_id = insert_result.tenant_id
                # insert_result.series_ids maps resolved names to series_id
                resolved_series_ids = insert_result.series_ids

                for col in series_cols:
                    sid = resolved_series_ids.get(col) or series_ids[col]
                    for v_time, val in zip(df["valid_time"], df[col]):
                        updates.append({
                            "batch_id": batch_id,
                            "tenant_id": tenant_id,
                            "valid_time": v_time,
                            "series_id": sid,
                            "value": float(val) + 1.0,
                            "changed_by": "benchmark",
                        })

                elapsed_update = bench_update(td, updates)

                # Read
                start_v = df["valid_time"].min()
                end_v = df["valid_time"].max()
                elapsed_read = bench_read(td, series_name, start_v, end_v)

                insert_times.append(elapsed_insert)
                update_times.append(elapsed_update)
                read_times.append(elapsed_read)

                print(f"  run {r+1}/{repeats}: insert={elapsed_insert:.3f}s update={elapsed_update:.3f}s read={elapsed_read:.3f}s")

            # Record averaged results
            total_values = num_rows * num_series
            results.append({
                "test": "insert",
                "rows_per_series": num_rows,
                "num_series": num_series,
                "total_values": total_values,
                "time_seconds": float(pd.Series(insert_times).mean()),
                "throughput_rows_per_sec": total_values / float(pd.Series(insert_times).mean()),
            })
            results.append({
                "test": "update",
                "rows_per_series": num_rows,
                "num_series": num_series,
                "total_values": total_values,
                "time_seconds": float(pd.Series(update_times).mean()),
                "throughput_rows_per_sec": total_values / float(pd.Series(update_times).mean()),
            })
            results.append({
                "test": "read",
                "rows_per_series": num_rows,
                "num_series": num_series,
                "total_values": total_values,
                "time_seconds": float(pd.Series(read_times).mean()),
                "throughput_rows_per_sec": total_values / float(pd.Series(read_times).mean()),
            })

    df_results = pd.DataFrame(results)
    df_results.to_csv(output_csv, index=False)
    print(f"Saved results to {output_csv}")

    # Plotting
    for test in df_results["test"].unique():
        sub = df_results[df_results["test"] == test]
        plt.figure()
        for ns in sorted(sub["num_series"].unique()):
            sel = sub[sub["num_series"] == ns]
            plt.plot(sel["total_values"], sel["throughput_rows_per_sec"], marker="o", label=f"{ns} series")
        plt.xlabel("Total values (rows * series)")
        plt.ylabel("Throughput (rows/sec)")
        plt.title(f"TimeDB {test} throughput")
        plt.legend()
        plt.grid(alpha=0.3)
        out_png = os.path.join(out_plot_dir, f"bench_{test}.png")
        plt.savefig(out_png)
        print(f"Saved plot {out_png}")


def parse_args():
    parser = argparse.ArgumentParser(description="TimeDB latency/throughput benchmark")
    parser.add_argument("--rows", type=int, nargs="+", default=[10, 100, 1000], help="Rows per series to test")
    parser.add_argument("--series", type=int, nargs="+", default=[1, 5, 10], help="Number of series to test")
    parser.add_argument("--repeats", type=int, default=3, help="Repetitions per combination")
    parser.add_argument("--out", default="bench_results.csv", help="CSV file for results")
    parser.add_argument("--plots", default="bench_plots", help="Directory to save plots")
    parser.add_argument("--series-name", default="bench_series", help="Series name to use for created series")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_bench(rows_list=args.rows, series_list=args.series, repeats=args.repeats, series_name=args.series_name, output_csv=args.out, out_plot_dir=args.plots)
