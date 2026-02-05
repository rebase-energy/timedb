"""
Simple benchmark for TimeDB overlapping data.

Inserts 10 batches of 72 rows with 1 hour spacing in known_time,
then measures read speed for overlapping and flat data.
"""

import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from timedb import TimeDataClient

# ============================================================================
# CONFIGURATION
# ============================================================================
ROWS_PER_BATCH = 72000
NUM_BATCHES = 10  # Increased significantly
TIME_BETWEEN_BATCHES = timedelta(hours=1)
SERIES_NAME = "forecast_data"

# ============================================================================
# Output paths
# ============================================================================
OUTPUT_DIR = Path(__file__).parent / "benchmark_results_overlapping"
OUTPUT_DIR.mkdir(exist_ok=True)
CSV_RESULTS = OUTPUT_DIR / "benchmark_results.csv"
PLOT_FILE = OUTPUT_DIR / "benchmark_plot.png"


def run_benchmark_once():
    """Run a single benchmark iteration: insert overlapping batches and measure read speed."""
    
    td = TimeDataClient()
    
    # Clean slate
    td.delete()
    td.create()
    
    # Create overlapping series
    td.create_series(
        name=SERIES_NAME,
        unit="MW",
        data_class="overlapping",
        retention="medium"
    )
    
    series_collection = td.series(SERIES_NAME)
    
    # ========================================================================
    # INSERT PHASE
    # ========================================================================
    base_valid_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    base_known_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    
    # Pre-generate all batch timestamps (shared by all batches)
    batch_timestamps = pd.date_range(
        start=base_valid_time,
        periods=ROWS_PER_BATCH,
        freq='h'
    )
    
    # Pre-generate all random values at once (much faster than in loop)
    all_batch_values = np.random.normal(50, 10, (NUM_BATCHES, ROWS_PER_BATCH))
    
    print(f"  Inserting {NUM_BATCHES} batches...", end='', flush=True)
    insert_start = time.time()
    
    for batch_idx in range(NUM_BATCHES):
        batch_start_time = base_known_time + (batch_idx * TIME_BETWEEN_BATCHES)
        
        df_batch = pd.DataFrame({
            'valid_time': batch_timestamps,
            SERIES_NAME: all_batch_values[batch_idx]
        })
        
        # Insert the batch
        series_collection.insert_batch(
            df=df_batch,
            batch_start_time=batch_start_time
        )
        
        # Status update every 25 batches
        if (batch_idx + 1) % 25 == 0:
            print(f" {batch_idx + 1}/{NUM_BATCHES}", end='', flush=True)
    
    insert_elapsed = time.time() - insert_start
    print(f" Done in {insert_elapsed:.1f}s")
    
    # ========================================================================
    # READ PHASE: Test overlapping data read speed with varying known_time filters
    # ========================================================================
    
    # Calculate different known_time thresholds (percentage of total range)
    base_known_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    max_known_time = base_known_time + (NUM_BATCHES - 1) * TIME_BETWEEN_BATCHES
    total_known_time_range = max_known_time - base_known_time
    
    known_time_percentages = [0, 25, 50, 75, 100]
    known_time_thresholds = []
    for pct in known_time_percentages:
        threshold = base_known_time + (pct / 100.0) * total_known_time_range
        known_time_thresholds.append((pct, threshold))
    
    results_by_threshold = {
        'known_time_pct': [],
        'all_versions_no_filter': [],
        'latest_no_filter': [],
        'all_versions_filtered': [],
        'latest_filtered': []
    }
    
    for pct, threshold in known_time_thresholds:
        # Test 1: All data, no filter, all versions
        read_start = time.time()
        df_all_no_filter_all_versions = series_collection.read(versions=True)
        read_elapsed = time.time() - read_start
        rows_read = len(df_all_no_filter_all_versions)
        read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
        results_by_threshold['all_versions_no_filter'].append(read_speed)

        # Test 2: All data, no filter, latest only
        read_start = time.time()
        df_all_no_filter_latest = series_collection.read(versions=False)
        read_elapsed = time.time() - read_start
        rows_read = len(df_all_no_filter_latest)
        read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
        results_by_threshold['latest_no_filter'].append(read_speed)
        
        # Test 3: Filtered by known_time, all versions
        read_start = time.time()
        df_filtered_all_versions = series_collection.read(
            start_known=threshold,
            versions=True
        )
        read_elapsed = time.time() - read_start
        rows_read = len(df_filtered_all_versions)
        read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
        results_by_threshold['all_versions_filtered'].append(read_speed)
        
        # Test 4: Filtered by known_time, latest only
        read_start = time.time()
        df_filtered_latest = series_collection.read(
            start_known=threshold,
            versions=False
        )
        read_elapsed = time.time() - read_start
        rows_read = len(df_filtered_latest)
        read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
        results_by_threshold['latest_filtered'].append(read_speed)
        
        results_by_threshold['known_time_pct'].append(pct)
    
    return results_by_threshold


def run_benchmark(num_runs=5):
    """Run benchmark multiple times and collect statistics."""
    
    print("=" * 80)
    print("TimeDB Overlapping Data Benchmark (Multiple Runs with Statistics)")
    print("=" * 80)
    print(f"Configuration: {NUM_BATCHES} batches × {ROWS_PER_BATCH} rows/batch")
    print(f"Known_time spacing: {TIME_BETWEEN_BATCHES}")
    print(f"Number of runs: {num_runs}")
    print("=" * 80)
    
    all_results = []
    
    for run_num in range(1, num_runs + 1):
        print(f"\n[RUN {run_num}/{num_runs}] Starting benchmark run...")
        results = run_benchmark_once()
        all_results.append(results)
        print(f"[RUN {run_num}/{num_runs}] Complete")
    
    # ========================================================================
    # AGGREGATE STATISTICS
    # ========================================================================
    print(f"\n[STATISTICS] Computing mean and stddev across {num_runs} runs...")
    
    # Initialize result structure
    aggregated_results = {
        'known_time_pct': all_results[0]['known_time_pct'],
        'all_versions_no_filter_mean': [],
        'all_versions_no_filter_stddev': [],
        'latest_no_filter_mean': [],
        'latest_no_filter_stddev': [],
        'all_versions_filtered_mean': [],
        'all_versions_filtered_stddev': [],
        'latest_filtered_mean': [],
        'latest_filtered_stddev': []
    }
    
    # Compute mean and stddev for each threshold and test type
    test_types = [
        ('all_versions_no_filter', 'all_versions_no_filter'),
        ('latest_no_filter', 'latest_no_filter'),
        ('all_versions_filtered', 'all_versions_filtered'),
        ('latest_filtered', 'latest_filtered')
    ]
    
    for threshold_idx in range(len(all_results[0]['known_time_pct'])):
        for key, stats_key in test_types:
            values = [run[key][threshold_idx] for run in all_results]
            mean_val = np.mean(values)
            stddev_val = np.std(values)
            aggregated_results[f'{stats_key}_mean'].append(mean_val)
            aggregated_results[f'{stats_key}_stddev'].append(stddev_val)
    
    # ========================================================================
    # RESULTS
    # ========================================================================
    # Create a summary DataFrame (mean values)
    df_summary = pd.DataFrame({
        'known_time_pct': aggregated_results['known_time_pct'],
        'all_versions_no_filter': aggregated_results['all_versions_no_filter_mean'],
        'latest_no_filter': aggregated_results['latest_no_filter_mean'],
        'all_versions_filtered': aggregated_results['all_versions_filtered_mean'],
        'latest_filtered': aggregated_results['latest_filtered_mean']
    })
    
    # Create detailed results with mean and stddev
    df_detailed = pd.DataFrame({
        'known_time_pct': aggregated_results['known_time_pct'],
        'all_versions_no_filter_mean': aggregated_results['all_versions_no_filter_mean'],
        'all_versions_no_filter_stddev': aggregated_results['all_versions_no_filter_stddev'],
        'latest_no_filter_mean': aggregated_results['latest_no_filter_mean'],
        'latest_no_filter_stddev': aggregated_results['latest_no_filter_stddev'],
        'all_versions_filtered_mean': aggregated_results['all_versions_filtered_mean'],
        'all_versions_filtered_stddev': aggregated_results['all_versions_filtered_stddev'],
        'latest_filtered_mean': aggregated_results['latest_filtered_mean'],
        'latest_filtered_stddev': aggregated_results['latest_filtered_stddev']
    })
    
    df_detailed.to_csv(CSV_RESULTS, index=False)
    
    print(f"\n[SAVE] Detailed results saved to {CSV_RESULTS}")
    print("\n" + "=" * 80)
    print("RESULTS SUMMARY (Mean values across all runs)")
    print("=" * 80)
    print(df_summary.to_string(index=False))
    
    print("\n" + "=" * 80)
    print("RESULTS WITH STATISTICS (Mean ± Stddev)")
    print("=" * 80)
    for idx, pct in enumerate(aggregated_results['known_time_pct']):
        print(f"\n[{pct}% threshold]")
        print(f"  all_versions_no_filter:  {aggregated_results['all_versions_no_filter_mean'][idx]:.0f} ± {aggregated_results['all_versions_no_filter_stddev'][idx]:.0f}")
        print(f"  latest_no_filter:        {aggregated_results['latest_no_filter_mean'][idx]:.0f} ± {aggregated_results['latest_no_filter_stddev'][idx]:.0f}")
        print(f"  all_versions_filtered:   {aggregated_results['all_versions_filtered_mean'][idx]:.0f} ± {aggregated_results['all_versions_filtered_stddev'][idx]:.0f}")
        print(f"  latest_filtered:         {aggregated_results['latest_filtered_mean'][idx]:.0f} ± {aggregated_results['latest_filtered_stddev'][idx]:.0f}")
    
    return df_summary, aggregated_results


def generate_plots(df_summary: pd.DataFrame, aggregated_results: dict):
    """Generate and save performance plots with error bars showing stddev."""
    
    if df_summary.empty:
        print("[⚠] No results to plot")
        return
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot 4 lines, one for each test type, with error bars showing stddev
    x = df_summary['known_time_pct']
    
    # Line 1: All versions (no filter)
    ax.errorbar(x, df_summary['all_versions_no_filter'], 
               yerr=aggregated_results['all_versions_no_filter_stddev'],
               marker='o', linewidth=2.5, markersize=9, capsize=5, capthick=2,
               label='All versions (no filter)', color='#2ecc71', ecolor='#27ae60', alpha=0.8)
    
    # Line 2: Latest only (no filter)
    ax.errorbar(x, df_summary['latest_no_filter'], 
               yerr=aggregated_results['latest_no_filter_stddev'],
               marker='s', linewidth=2.5, markersize=9, capsize=5, capthick=2,
               label='Latest only (no filter)', color='#e74c3c', ecolor='#c0392b', alpha=0.8)
    
    # Line 3: All versions (filtered)
    ax.errorbar(x, df_summary['all_versions_filtered'], 
               yerr=aggregated_results['all_versions_filtered_stddev'],
               marker='^', linewidth=2.5, markersize=9, capsize=5, capthick=2,
               label='All versions (filtered)', color='#3498db', ecolor='#2980b9', alpha=0.8)
    
    # Line 4: Latest only (filtered)
    ax.errorbar(x, df_summary['latest_filtered'], 
               yerr=aggregated_results['latest_filtered_stddev'],
               marker='D', linewidth=2.5, markersize=9, capsize=5, capthick=2,
               label='Latest only (filtered)', color='#f39c12', ecolor='#d68910', alpha=0.8)
    
    ax.set_xlabel('Known_time Filter Threshold (%)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Read Speed (rows/sec)', fontsize=13, fontweight='bold')
    ax.set_title(f'Overlapping Data Read Performance vs Known_time Filter\n({NUM_BATCHES} batches × {ROWS_PER_BATCH} rows, 5 runs, mean ± stddev)', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(loc='best', fontsize=11, framealpha=0.95, edgecolor='black')
    
    plt.tight_layout()
    plt.savefig(PLOT_FILE, dpi=150, bbox_inches='tight')
    print(f"[PLOT] Saved benchmark plot to {PLOT_FILE}\n")
    plt.close()


if __name__ == "__main__":
    df_summary, aggregated_results = run_benchmark(num_runs=3)
    generate_plots(df_summary, aggregated_results)
