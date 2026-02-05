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
OUTPUT_DIR = Path(__file__).parent / "benchmark_results"
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
    
    # Read chunk size names
    read_chunk_sizes = ['all', 'last_batch', '10_percent', '1_percent', '0.1_percent']
    
    for iteration in sorted(df_all_results['iteration'].unique()):
        df_iter = df_all_results[df_all_results['iteration'] == iteration]
        
        insert_speeds = df_iter['insert_speed_rows_per_sec'].values
        insert_times = df_iter['insert_time_seconds'].values
        
        stat_dict = {
            'iteration': iteration,
            'total_rows_in_db': df_iter['total_rows_in_db_before'].iloc[0] + df_iter['rows_inserted'].iloc[0],
            'insert_speed_mean': np.mean(insert_speeds),
            'insert_speed_std': np.std(insert_speeds),
            'insert_speed_min': np.min(insert_speeds),
            'insert_speed_max': np.max(insert_speeds),
            'insert_time_mean': np.mean(insert_times),
            'insert_time_std': np.std(insert_times),
        }
        
        # Add stats for each read chunk size and position (oldest/newest)
        for chunk_name in read_chunk_sizes:
            for position in ['oldest', 'newest']:
                col_speed = f'read_{chunk_name}_{position}_speed_rows_per_sec'
                col_time = f'read_{chunk_name}_{position}_time_seconds'
                
                if col_speed in df_iter.columns:
                    read_speeds = df_iter[col_speed].values
                    read_times = df_iter[col_time].values
                    
                    stat_dict[f'read_{chunk_name}_{position}_speed_mean'] = np.mean(read_speeds)
                    stat_dict[f'read_{chunk_name}_{position}_speed_std'] = np.std(read_speeds)
                    stat_dict[f'read_{chunk_name}_{position}_time_mean'] = np.mean(read_times)
                    stat_dict[f'read_{chunk_name}_{position}_time_std'] = np.std(read_times)
        
        stats.append(stat_dict)
    
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
    
    # Results storage - includes columns for different read chunk sizes
    read_chunk_sizes = {
        'all': None,
        'last_batch': NUM_ROWS_PER_ITERATION,
        '10_percent': int(0.1 * NUM_ROWS_PER_ITERATION),
        '1_percent': int(0.01 * NUM_ROWS_PER_ITERATION),
        '0.1_percent': int(0.001 * NUM_ROWS_PER_ITERATION),
    }
    
    results = {
        'iteration': [],
        'total_rows_in_db_before': [],
        'rows_inserted': [],
        'insert_time_seconds': [],
        'insert_speed_rows_per_sec': [],
    }
    
    # Add columns for each read chunk size and position (oldest/newest)
    for chunk_name in read_chunk_sizes.keys():
        for position in ['oldest', 'newest']:
            results[f'read_{chunk_name}_{position}_time_seconds'] = []
            results[f'read_{chunk_name}_{position}_speed_rows_per_sec'] = []
            results[f'read_{chunk_name}_{position}_rows_read'] = []
    
    total_rows_in_db = 0
    base_start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    
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
        # READ BENCHMARK - Different chunk sizes and positions (oldest/newest)
        # ====================================================================
        print(f"      [READ] Testing different chunk sizes and positions...")
        
        # Get the time range of data in database
        db_start = base_start
        db_end = base_start + timedelta(minutes=total_rows_in_db - 1)
        
        for chunk_name, chunk_rows in read_chunk_sizes.items():
            # ================================================================
            # OLDEST DATA - Read from the beginning
            # ================================================================
            if chunk_rows is None:
                # Read all rows from beginning
                read_start = time.time()
                df_read = td.series(SERIES_NAME).read()
                rows_read = len(df_read)
                read_elapsed = time.time() - read_start
            else:
                # Read chunk_rows from the beginning
                oldest_end = db_start + timedelta(minutes=chunk_rows - 1)
                read_start = time.time()
                df_read = td.series(SERIES_NAME).read(start_valid=db_start, end_valid=oldest_end)
                rows_read = len(df_read)
                read_elapsed = time.time() - read_start
            
            read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
            results[f'read_{chunk_name}_oldest_time_seconds'].append(read_elapsed)
            results[f'read_{chunk_name}_oldest_speed_rows_per_sec'].append(read_speed)
            results[f'read_{chunk_name}_oldest_rows_read'].append(rows_read)
            print(f"          [{chunk_name} OLDEST] {rows_read} rows in {read_elapsed:.3f}s ({read_speed:.0f} rows/s)")
            
            # ================================================================
            # NEWEST DATA - Read from the end
            # ================================================================
            if chunk_rows is None:
                # Read all rows from end (should be same as oldest)
                read_start = time.time()
                df_read = td.series(SERIES_NAME).read()
                rows_read = len(df_read)
                read_elapsed = time.time() - read_start
            else:
                # Read chunk_rows from the end
                newest_start = db_end - timedelta(minutes=chunk_rows - 1)
                read_start = time.time()
                df_read = td.series(SERIES_NAME).read(start_valid=newest_start, end_valid=db_end)
                rows_read = len(df_read)
                read_elapsed = time.time() - read_start
            
            read_speed = rows_read / read_elapsed if read_elapsed > 0 else 0
            results[f'read_{chunk_name}_newest_time_seconds'].append(read_elapsed)
            results[f'read_{chunk_name}_newest_speed_rows_per_sec'].append(read_speed)
            results[f'read_{chunk_name}_newest_rows_read'].append(rows_read)
            print(f"          [{chunk_name} NEWEST] {rows_read} rows in {read_elapsed:.3f}s ({read_speed:.0f} rows/s)")
        
        # Store results
        results['iteration'].append(iteration)
        results['total_rows_in_db_before'].append(total_rows_in_db - rows_to_insert)
        results['rows_inserted'].append(rows_to_insert)
        results['insert_time_seconds'].append(insert_elapsed)
        results['insert_speed_rows_per_sec'].append(insert_speed)
    
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
    # Plot 2: Read Speed vs Database Size for Different Chunk Sizes and Positions
    # ========================================================================
    plt.figure(figsize=(16, 8))
    
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']
    chunk_names = ['all', 'last_batch', '10_percent', '1_percent', '0.1_percent']
    
    # Calculate row counts for each chunk size
    chunk_rows = {
        'all': None,  # Will vary by iteration
        'last_batch': NUM_ROWS_PER_ITERATION,
        '10_percent': int(0.1 * NUM_ROWS_PER_ITERATION),
        '1_percent': int(0.01 * NUM_ROWS_PER_ITERATION),
        '0.1_percent': int(0.001 * NUM_ROWS_PER_ITERATION),
    }
    
    for i, chunk_name in enumerate(chunk_names):
        for position in ['oldest', 'newest']:
            col_speed = f'read_{chunk_name}_{position}_speed_mean'
            col_std = f'read_{chunk_name}_{position}_speed_std'
            
            if col_speed in stats.columns:
                # Create label with row count and position
                if chunk_name == 'all':
                    label = f'All Rows - {position.title()}'
                else:
                    num_rows = chunk_rows[chunk_name]
                    label = f'{chunk_name.replace("_", " ").title()} ({num_rows:,} rows) - {position.title()}'
                
                # Use different line styles for oldest vs newest
                linestyle = '-' if position == 'newest' else '--'
                marker = 'o' if position == 'newest' else 's'
                
                plt.errorbar(
                    x,
                    stats[col_speed],
                    yerr=stats[col_std],
                    fmt=marker + linestyle,
                    linewidth=2,
                    markersize=7,
                    capsize=4,
                    capthick=1.5,
                    color=colors[i],
                    ecolor=colors[i],
                    alpha=0.8,
                    label=label
                )
    
    plt.xlabel('Total Rows in Database (Millions)', fontsize=12)
    plt.ylabel('Speed (rows/sec)', fontsize=12)
    plt.title('Read Speed vs Database Size (Different Chunks & Positions)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=9, loc='best', ncol=2)
    plt.tight_layout()
    plt.savefig(PLOT_READ, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved read plot to {PLOT_READ}")
    plt.close()
    
    # ========================================================================
    # Plot 3: Combined Insert & Read Speed (for all rows)
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
        stats['read_all_newest_speed_mean'],
        yerr=stats['read_all_newest_speed_std'],
        fmt='s-',
        linewidth=2.5,
        markersize=9,
        capsize=5,
        capthick=2,
        color='#A23B72',
        ecolor='#A23B72',
        alpha=0.8,
        label='Read All Rows - Newest (mean ± std)'
    )
    ax.fill_between(
        x,
        stats['read_all_newest_speed_mean'] - stats['read_all_newest_speed_std'],
        stats['read_all_newest_speed_mean'] + stats['read_all_newest_speed_std'],
        alpha=0.2,
        color='#A23B72'
    )
    
    # Read all - oldest data
    ax.errorbar(
        x,
        stats['read_all_oldest_speed_mean'],
        yerr=stats['read_all_oldest_speed_std'],
        fmt='s--',
        linewidth=2.5,
        markersize=9,
        capsize=5,
        capthick=2,
        color='#A23B72',
        ecolor='#A23B72',
        alpha=0.6,
        label='Read All Rows - Oldest (mean ± std)'
    )
    ax.fill_between(
        x,
        stats['read_all_oldest_speed_mean'] - stats['read_all_oldest_speed_std'],
        stats['read_all_oldest_speed_mean'] + stats['read_all_oldest_speed_std'],
        alpha=0.1,
        color='#A23B72'
    )
    
    ax.set_xlabel('Total Rows in Database (Millions)', fontsize=12)
    ax.set_ylabel('Speed (rows/sec)', fontsize=12)
    ax.set_title('TimeDB Performance: Insert vs Read All Speed (Oldest vs Newest)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc='best')
    
    plt.tight_layout()
    plt.savefig(PLOT_COMBINED, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved combined plot to {PLOT_COMBINED}")
    plt.close()
    
    print()


if __name__ == "__main__":
    df_all_results, stats = run_all_benchmarks()
