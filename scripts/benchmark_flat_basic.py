"""
Benchmark script for TimeDB insert and read performance.

Measures how insert/read speed changes as the database grows.
Saves results to CSV and generates performance plots.

Runs the benchmark 3 times to calculate mean and standard deviation.
"""

import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import matplotlib.pyplot as plt
import uuid
import numpy as np
from timedb import TimeDataClient

# ============================================================================
# CONFIGURATION: Adjust these variables to control benchmark scope
# ============================================================================
NUM_ROWS_PER_ITERATION = 100000  # Rows per iteration
NUM_ITERATIONS = 14  # How many times to repeat insert/read (database grows each time)
NUM_RUNS = 5  # Number of times to run the entire benchmark for statistics
SERIES_NAME = "flat_series"  # Single series name

# ============================================================================
# Output paths
# ============================================================================
OUTPUT_DIR = Path(__file__).parent / "benchmark_results_basic"
OUTPUT_DIR.mkdir(exist_ok=True)
CSV_RESULTS = OUTPUT_DIR / "benchmark_results.csv"
CSV_STATS = OUTPUT_DIR / "benchmark_statistics.csv"
PLOT_INSERT = OUTPUT_DIR / "plot_insert_speed.png"
PLOT_READ = OUTPUT_DIR / "plot_read_speed.png"
PLOT_COMBINED = OUTPUT_DIR / "plot_performance_combined.png"


def calculate_statistics(df_all_results: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate mean and standard deviation for each iteration across all runs.
    
    Args:
        df_all_results: DataFrame with results from all runs
        
    Returns:
        DataFrame with statistics for each iteration
    """
    stats = []
    
    for iteration in sorted(df_all_results['iteration'].unique()):
        df_iter = df_all_results[df_all_results['iteration'] == iteration]
        
        insert_speeds = df_iter['insert_speed_rows_per_sec'].values
        read_speeds = df_iter['read_speed_rows_per_sec'].values
        insert_times = df_iter['insert_time_seconds'].values
        read_times = df_iter['read_time_seconds'].values
        
        stats.append({
            'iteration': iteration,
            'total_rows_in_db': df_iter['total_rows_in_db_before'].iloc[0] + df_iter['rows_inserted'].iloc[0],
            'insert_speed_mean': np.mean(insert_speeds),
            'insert_speed_std': np.std(insert_speeds),
            'insert_speed_min': np.min(insert_speeds),
            'insert_speed_max': np.max(insert_speeds),
            'read_speed_mean': np.mean(read_speeds),
            'read_speed_std': np.std(read_speeds),
            'read_speed_min': np.min(read_speeds),
            'read_speed_max': np.max(read_speeds),
            'insert_time_mean': np.mean(insert_times),
            'insert_time_std': np.std(insert_times),
            'read_time_mean': np.mean(read_times),
            'read_time_std': np.std(read_times),
        })
    
    return pd.DataFrame(stats)

def create_test_dataframe(num_rows, iteration=1):
    """
    Create DataFrame with test data.
    
    Args:
        num_rows: Number of rows to create
        iteration: Which iteration (to create unique non-overlapping timestamps)
    
    Returns:
        DataFrame with test data
    """
    # Create non-overlapping timestamps for each iteration
    # Each iteration appends sequentially after the previous one
    base_start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    offset_minutes = (iteration - 1) * num_rows
    start = base_start + timedelta(minutes=offset_minutes)
    
    # Use minute intervals
    times = [start + timedelta(minutes=i) for i in range(num_rows)]
    values = [100.0 + (j % num_rows) * 0.1 for j in range(num_rows)]
    
    df = pd.DataFrame({
        'valid_time': times,
        SERIES_NAME: values
    })
    
    return df


def run_benchmark():
    """Run a single benchmark suite (one complete run with all iterations)."""
    
    # Initialize client
    td = TimeDataClient()
    
    # Clean slate
    td.delete()
    td.create()
    
    # Create single series
    td.create_series(name=SERIES_NAME, unit="MW", data_class="flat")
    
    # Results storage
    results = {
        'iteration': [],
        'total_rows_in_db_before': [],
        'rows_inserted': [],
        'insert_time_seconds': [],
        'insert_speed_rows_per_sec': [],
        'read_time_seconds': [],
        'read_speed_rows_per_sec': [],
    }
    
    total_rows_in_db = 0
    
    # Run iterations
    for iteration in range(1, NUM_ITERATIONS + 1):
        print(f"\n  [*] Iteration {iteration}/{NUM_ITERATIONS}")
        print(f"      Database has {total_rows_in_db} rows")
        
        # Create test data
        print(f"      Preparing {NUM_ROWS_PER_ITERATION} rows of test data...")
        df = create_test_dataframe(NUM_ROWS_PER_ITERATION, iteration)
        rows_to_insert = NUM_ROWS_PER_ITERATION
        
        # ====================================================================
        # INSERT BENCHMARK
        # ====================================================================
        print(f"      [INSERT] Inserting {rows_to_insert} rows...")
        insert_start = time.time()
        
        td.series(SERIES_NAME).insert(df=df)
        
        insert_elapsed = time.time() - insert_start
        insert_speed = rows_to_insert / insert_elapsed
        
        total_rows_in_db += rows_to_insert
        print(f"      [INSERT] Completed in {insert_elapsed:.2f}s ({insert_speed:.0f} rows/s)")
        
        # ====================================================================
        # READ BENCHMARK
        # ====================================================================
        print(f"      [READ] Reading all {total_rows_in_db} rows...")
        read_start = time.time()
        
        df_read = td.series(SERIES_NAME).read()
        total_rows_read = len(df_read)
        
        assert total_rows_read == total_rows_in_db, \
            f"Read {total_rows_read} rows but expected {total_rows_in_db}"
        
        read_elapsed = time.time() - read_start
        read_speed = total_rows_in_db / read_elapsed
        print(f"      [READ] Completed in {read_elapsed:.2f}s ({read_speed:.0f} rows/s)")
        
        # Store results
        results['iteration'].append(iteration)
        results['total_rows_in_db_before'].append(total_rows_in_db - rows_to_insert)
        results['rows_inserted'].append(rows_to_insert)
        results['insert_time_seconds'].append(insert_elapsed)
        results['insert_speed_rows_per_sec'].append(insert_speed)
        results['read_time_seconds'].append(read_elapsed)
        results['read_speed_rows_per_sec'].append(read_speed)
    
    return pd.DataFrame(results)


def run_all_benchmarks():
    """Run the complete benchmark suite multiple times for statistics."""
    
    print("=" * 80)
    print("TimeDB Flat Benchmark (with statistical analysis)")
    print("=" * 80)
    print(f"SERIES_NAME: {SERIES_NAME}")
    print(f"NUM_ROWS_PER_ITERATION: {NUM_ROWS_PER_ITERATION}")
    print(f"NUM_ITERATIONS per run: {NUM_ITERATIONS}")
    print(f"NUM_RUNS: {NUM_RUNS}")
    print("=" * 80)
    
    all_results = []
    
    # Run benchmark multiple times
    for run_num in range(1, NUM_RUNS + 1):
        print(f"\n{'='*80}")
        print(f"RUN {run_num}/{NUM_RUNS}")
        print(f"{'='*80}\n")
        
        df_run = run_benchmark()
        df_run['run'] = run_num
        all_results.append(df_run)
        
        print(f"[✓] Run {run_num} completed\n")
    
    # Combine all results
    df_all_results = pd.concat(all_results, ignore_index=True)
    
    # Save all raw results
    df_all_results.to_csv(CSV_RESULTS, index=False)
    print(f"\n[✓] All raw results saved to {CSV_RESULTS}\n")
    
    # Calculate statistics grouped by iteration
    stats = calculate_statistics(df_all_results)
    
    # Save statistics
    stats.to_csv(CSV_STATS, index=False)
    print(f"[✓] Statistics saved to {CSV_STATS}\n")
    
    print(f"\n{'='*80}")
    print("BENCHMARK COMPLETE")
    print(f"{'='*80}\n")
    
    print("Summary Statistics:")
    print(stats.to_string(index=False))
    print()
    
    # Generate plots
    generate_plots(stats)
    
    return df_all_results, stats


def generate_plots(stats: pd.DataFrame):
    """Generate and save performance plots with error bars."""
    
    x = stats['total_rows_in_db'] / 1e6
    
    # ========================================================================
    # Plot 1: Insert Speed vs Database Size (with error bars)
    # ========================================================================
    plt.figure(figsize=(12, 6))
    plt.errorbar(
        x,
        stats['insert_speed_mean'],
        yerr=stats['insert_speed_std'],
        fmt='o-',
        linewidth=2,
        markersize=8,
        capsize=5,
        capthick=2,
        color='#2E86AB',
        ecolor='#2E86AB',
        alpha=0.8,
        label='Insert Speed (mean ± std)'
    )
    plt.fill_between(
        x,
        stats['insert_speed_mean'] - stats['insert_speed_std'],
        stats['insert_speed_mean'] + stats['insert_speed_std'],
        alpha=0.2,
        color='#2E86AB'
    )
    plt.xlabel('Total Rows in Database (Millions)', fontsize=12)
    plt.ylabel('Speed (rows/sec)', fontsize=12)
    plt.title('Insert Speed vs Database Size', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig(PLOT_INSERT, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved insert plot to {PLOT_INSERT}")
    plt.close()
    
    # ========================================================================
    # Plot 2: Read Speed vs Database Size (with error bars)
    # ========================================================================
    plt.figure(figsize=(12, 6))
    plt.errorbar(
        x,
        stats['read_speed_mean'],
        yerr=stats['read_speed_std'],
        fmt='s-',
        linewidth=2,
        markersize=8,
        capsize=5,
        capthick=2,
        color='#A23B72',
        ecolor='#A23B72',
        alpha=0.8,
        label='Read Speed (mean ± std)'
    )
    plt.fill_between(
        x,
        stats['read_speed_mean'] - stats['read_speed_std'],
        stats['read_speed_mean'] + stats['read_speed_std'],
        alpha=0.2,
        color='#A23B72'
    )
    plt.xlabel('Total Rows in Database (Millions)', fontsize=12)
    plt.ylabel('Speed (rows/sec)', fontsize=12)
    plt.title('Read Speed vs Database Size', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig(PLOT_READ, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved read plot to {PLOT_READ}")
    plt.close()
    
    # ========================================================================
    # Plot 3: Combined Insert & Read Speed (with error bars)
    # ========================================================================
    fig, ax = plt.subplots(figsize=(14, 7))
    
    ax.errorbar(
        x,
        stats['insert_speed_mean'],
        yerr=stats['insert_speed_std'],
        fmt='o-',
        linewidth=2.5,
        markersize=9,
        capsize=5,
        capthick=2,
        color='#2E86AB',
        ecolor='#2E86AB',
        alpha=0.8,
        label='Insert Speed (mean ± std)'
    )
    ax.fill_between(
        x,
        stats['insert_speed_mean'] - stats['insert_speed_std'],
        stats['insert_speed_mean'] + stats['insert_speed_std'],
        alpha=0.2,
        color='#2E86AB'
    )
    
    ax.errorbar(
        x,
        stats['read_speed_mean'],
        yerr=stats['read_speed_std'],
        fmt='s-',
        linewidth=2.5,
        markersize=9,
        capsize=5,
        capthick=2,
        color='#A23B72',
        ecolor='#A23B72',
        alpha=0.8,
        label='Read Speed (mean ± std)'
    )
    ax.fill_between(
        x,
        stats['read_speed_mean'] - stats['read_speed_std'],
        stats['read_speed_mean'] + stats['read_speed_std'],
        alpha=0.2,
        color='#A23B72'
    )
    
    ax.set_xlabel('Total Rows in Database (Millions)', fontsize=12)
    ax.set_ylabel('Speed (rows/sec)', fontsize=12)
    ax.set_title('TimeDB Performance: Insert vs Read Speed', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc='best')
    
    plt.tight_layout()
    plt.savefig(PLOT_COMBINED, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved combined plot to {PLOT_COMBINED}")
    plt.close()
    
    print()


if __name__ == "__main__":
    df_all_results, stats = run_all_benchmarks()
