"""
Monte Carlo query performance benchmark script for TimeDB.

Performs random queries on existing data with varying:
- Query size (number of rows)
- Position in database history (oldest vs newest)
- Time range parameters
- Query parameter combinations

Measures read speed and visualizes how query characteristics affect performance.

Usage:
    python benchmark_monte_carlo_queries.py              # Run benchmark and plot
    python benchmark_monte_carlo_queries.py --plot-only  # Plot from existing CSV only
"""

import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
import matplotlib.pyplot as plt
import random
import sys
from timedb import TimeDataClient

# ============================================================================
# CONFIGURATION
# ============================================================================
NUM_SIMULATIONS = 500  # Number of Monte Carlo simulations
NUM_RUNS = 3  # Run the entire simulation this many times for statistics

# Query size distribution (rows to fetch)
QUERY_SIZES = [1, 10, 32, 100, 212, 500, 1000, 3096, 5000, 8192, 10000, 10902, 50000, 92031, 100000, 497210]

# ============================================================================
# Output paths
# ============================================================================
OUTPUT_DIR = Path(__file__).parent / "benchmark_results_monte_carlo"
OUTPUT_DIR.mkdir(exist_ok=True)
CSV_MONTE_CARLO = OUTPUT_DIR / "monte_carlo_queries.csv"
CSV_MONTE_CARLO_STATS = OUTPUT_DIR / "monte_carlo_queries_stats.csv"
PLOT_SPEED_VS_SIZE = OUTPUT_DIR / "plot_mc_speed_vs_size.png"
PLOT_SPEED_VS_POSITION = OUTPUT_DIR / "plot_mc_speed_vs_position.png"
PLOT_SPEED_VS_PARAMS = OUTPUT_DIR / "plot_mc_speed_vs_params.png"


def get_database_time_range(td: TimeDataClient, series_name: str) -> tuple:
    """
    Get the time range of data in the database.
    
    Returns:
        (start_time, end_time, num_rows)
    """
    print("[*] Querying database for time range...")
    df = td.series(series_name).read()
    
    if len(df) == 0:
        raise ValueError("No data found in database")
    
    start_time = df.index.min()
    end_time = df.index.max()
    num_rows = len(df)
    
    print(f"    Found {num_rows:,} rows")
    print(f"    Time range: {start_time} to {end_time}")
    print(f"    Duration: {end_time - start_time}")
    
    return start_time, end_time, num_rows


def generate_random_query(start_time, end_time, total_rows, query_size):
    """
    Generate a random query with varying characteristics.
    
    Returns:
        dict with query parameters and metadata
    """
    # Determine query position in database (oldest vs newest vs middle)
    position = random.choice(['oldest', 'newest', 'middle'])
    
    # Calculate how many minutes per row
    total_duration = end_time - start_time
    minutes_per_row = total_duration.total_seconds() / 60 / total_rows
    
    # Convert query_size to time duration
    query_duration_minutes = query_size * minutes_per_row
    
    # Select random start time based on position
    if position == 'oldest':
        # Query from the beginning
        query_start = start_time
    elif position == 'newest':
        # Query from the end
        query_start = end_time - timedelta(minutes=query_duration_minutes)
    else:  # middle
        # Query from a random point in the middle
        available_duration = total_duration - timedelta(minutes=query_duration_minutes)
        random_offset = timedelta(seconds=random.randint(0, int(available_duration.total_seconds())))
        query_start = start_time + random_offset
    
    query_end = query_start + timedelta(minutes=query_duration_minutes)
    
    # Ensure query_end doesn't exceed database end
    query_end = min(query_end, end_time)
    
    return {
        'position': position,
        'query_size': query_size,
        'start_valid': query_start,
        'end_valid': query_end,
        'duration_minutes': (query_end - query_start).total_seconds() / 60,
    }


def run_monte_carlo_simulation(series_name: str, num_simulations: int):
    """
    Run a single Monte Carlo simulation suite.
    """
    print("\n[*] Initializing TimeDataClient...")
    td = TimeDataClient()
    
    # Get database metadata
    start_time, end_time, total_rows = get_database_time_range(td, series_name)
    
    results = {
        'simulation': [],
        'position': [],
        'query_size': [],
        'duration_minutes': [],
        'rows_fetched': [],
        'read_time_seconds': [],
        'read_speed_rows_per_sec': [],
    }
    
    print(f"\n[*] Running {num_simulations} Monte Carlo simulations...\n")
    
    for sim_num in range(1, num_simulations + 1):
        # Select random query size
        query_size = random.choice(QUERY_SIZES)
        
        # Generate random query parameters
        query_params = generate_random_query(start_time, end_time, total_rows, query_size)
        
        # Execute query and measure time
        read_start = time.time()
        df_result = td.series(series_name).read(
            start_valid=query_params['start_valid'],
            end_valid=query_params['end_valid']
        )
        read_elapsed = time.time() - read_start
        
        rows_fetched = len(df_result)
        read_speed = rows_fetched / read_elapsed if read_elapsed > 0 else 0
        
        # Store results
        results['simulation'].append(sim_num)
        results['position'].append(query_params['position'])
        results['query_size'].append(query_params['query_size'])
        results['duration_minutes'].append(query_params['duration_minutes'])
        results['rows_fetched'].append(rows_fetched)
        results['read_time_seconds'].append(read_elapsed)
        results['read_speed_rows_per_sec'].append(read_speed)
        
        if sim_num % 10 == 0:
            print(f"  [{sim_num}/{num_simulations}] {query_params['position']:8s} "
                  f"size={query_params['query_size']:6d} "
                  f"fetched={rows_fetched:6d} "
                  f"time={read_elapsed:6.3f}s "
                  f"speed={read_speed:8.0f} rows/s")
    
    return pd.DataFrame(results)


def calculate_statistics(df_all_results: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate mean and standard deviation for each query configuration across all runs.
    """
    stats = []
    
    # Group by position and query_size
    for position in sorted(df_all_results['position'].unique()):
        for query_size in sorted(df_all_results['query_size'].unique()):
            subset = df_all_results[
                (df_all_results['position'] == position) &
                (df_all_results['query_size'] == query_size)
            ]
            
            if len(subset) == 0:
                continue
            
            speeds = subset['read_speed_rows_per_sec'].values
            times = subset['read_time_seconds'].values
            rows_fetched = subset['rows_fetched'].values
            
            stats.append({
                'position': position,
                'query_size': query_size,
                'rows_fetched_mean': np.mean(rows_fetched),
                'read_time_mean': np.mean(times),
                'read_time_std': np.std(times),
                'read_speed_mean': np.mean(speeds),
                'read_speed_std': np.std(speeds),
                'read_speed_min': np.min(speeds),
                'read_speed_max': np.max(speeds),
                'num_samples': len(subset),
            })
    
    return pd.DataFrame(stats)


def run_all_simulations(series_name: str):
    """Run the complete Monte Carlo simulation multiple times for statistics."""
    
    print("=" * 80)
    print("TimeDB Monte Carlo Query Performance Benchmark")
    print("=" * 80)
    print(f"SERIES_NAME: {series_name}")
    print(f"NUM_SIMULATIONS: {NUM_SIMULATIONS}")
    print(f"QUERY_SIZES: {QUERY_SIZES}")
    print(f"NUM_RUNS: {NUM_RUNS}")
    print("=" * 80)
    
    all_results = []
    
    # Run simulation multiple times
    for run_num in range(1, NUM_RUNS + 1):
        print(f"\n{'='*80}")
        print(f"RUN {run_num}/{NUM_RUNS}")
        print(f"{'='*80}")
        
        df_run = run_monte_carlo_simulation(series_name, NUM_SIMULATIONS)
        df_run['run'] = run_num
        all_results.append(df_run)
        
        print(f"\n[✓] Run {run_num} completed")
    
    # Combine all results
    print("\n[*] Combining results from all runs...")
    df_all_results = pd.concat(all_results, ignore_index=True)
    
    # Save all raw results
    print(f"[*] Saving raw results to {CSV_MONTE_CARLO}...")
    df_all_results.to_csv(CSV_MONTE_CARLO, index=False)
    print(f"[✓] All raw results saved")
    
    # Calculate statistics
    print(f"[*] Calculating statistics...")
    stats = calculate_statistics(df_all_results)
    
    # Save statistics
    print(f"[*] Saving statistics to {CSV_MONTE_CARLO_STATS}...")
    stats.to_csv(CSV_MONTE_CARLO_STATS, index=False)
    print(f"[✓] Statistics saved")
    
    print(f"\n{'='*80}")
    print("SIMULATION COMPLETE")
    print(f"{'='*80}\n")
    
    print("Summary Statistics:")
    print(stats.to_string(index=False))
    print()
    
    # Generate plots
    print("[*] Generating plots...")
    generate_plots(stats)
    
    return df_all_results, stats


def generate_plots(stats: pd.DataFrame):
    """Generate and save performance plots."""
    
    # ========================================================================
    # Plot 1: Speed vs Query Size
    # ========================================================================
    plt.figure(figsize=(12, 6))
    
    for position in sorted(stats['position'].unique()):
        subset = stats[stats['position'] == position].sort_values('query_size')
        plt.errorbar(
            subset['query_size'],
            subset['read_speed_mean'],
            yerr=subset['read_speed_std'],
            fmt='o-',
            linewidth=2,
            markersize=8,
            capsize=5,
            capthick=2,
            alpha=0.8,
            label=f'{position.capitalize()} of Database'
        )
    
    plt.xlabel('Query Size (rows)', fontsize=12)
    plt.ylabel('Speed (rows/sec)', fontsize=12)
    plt.title('Read Speed vs Query Size (by Position)', fontsize=14, fontweight='bold')
    plt.xscale('log')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig(PLOT_SPEED_VS_SIZE, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved plot to {PLOT_SPEED_VS_SIZE}")
    plt.close()
    
    # ========================================================================
    # Plot 2: Speed vs Position (by Query Size)
    # ========================================================================
    plt.figure(figsize=(12, 6))
    
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', '#264653', '#E76F51']
    
    for i, query_size in enumerate(sorted(stats['query_size'].unique())):
        subset = stats[stats['query_size'] == query_size]
        positions = ['oldest', 'middle', 'newest']
        speeds = []
        stds = []
        
        for pos in positions:
            pos_data = subset[subset['position'] == pos]
            if len(pos_data) > 0:
                speeds.append(pos_data['read_speed_mean'].iloc[0])
                stds.append(pos_data['read_speed_std'].iloc[0])
            else:
                speeds.append(0)
                stds.append(0)
        
        x_pos = np.arange(len(positions))
        plt.bar(
            x_pos + (i - len(QUERY_SIZES)/2) * 0.12,
            speeds,
            yerr=stds,
            width=0.12,
            capsize=3,
            label=f'{query_size:,} rows',
            color=colors[i % len(colors)],
            alpha=0.8
        )
    
    plt.xlabel('Position in Database', fontsize=12)
    plt.ylabel('Speed (rows/sec)', fontsize=12)
    plt.title('Read Speed vs Position (by Query Size)', fontsize=14, fontweight='bold')
    plt.xticks(np.arange(len(positions)), [p.capitalize() for p in positions])
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend(fontsize=10, loc='best')
    plt.tight_layout()
    plt.savefig(PLOT_SPEED_VS_POSITION, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved plot to {PLOT_SPEED_VS_POSITION}")
    plt.close()
    
    # ========================================================================
    # Plot 3: Read Time vs Query Size (by Position)
    # ========================================================================
    plt.figure(figsize=(14, 7))
    
    for position in sorted(stats['position'].unique()):
        subset = stats[stats['position'] == position].sort_values('query_size')
        plt.errorbar(
            subset['query_size'],
            subset['read_time_mean'],
            yerr=subset['read_time_std'],
            fmt='o-',
            linewidth=2.5,
            markersize=9,
            capsize=5,
            capthick=2,
            alpha=0.8,
            label=f'{position.capitalize()} of Database'
        )
    
    plt.xlabel('Query Size (rows)', fontsize=12)
    plt.ylabel('Read Time (seconds)', fontsize=12)
    plt.title('Read Time vs Query Size (by Position)', fontsize=14, fontweight='bold')
    plt.xscale('log')
    plt.yscale('log')
    plt.grid(True, alpha=0.3, which='both')
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig(PLOT_SPEED_VS_PARAMS, dpi=150, bbox_inches='tight')
    print(f"[✓] Saved plot to {PLOT_SPEED_VS_PARAMS}")
    plt.close()
    
    print()


if __name__ == "__main__":
    SERIES_NAME = "flat_series"
    
    # Check for --plot-only flag
    plot_only = "--plot-only" in sys.argv
    
    if plot_only:
        # Load from CSV and generate plots only
        print("=" * 80)
        print("Loading results from CSV and generating plots...")
        print("=" * 80)
        
        if not CSV_MONTE_CARLO_STATS.exists():
            print(f"Error: Statistics file not found: {CSV_MONTE_CARLO_STATS}")
            print("Please run the benchmark first without --plot-only flag")
            sys.exit(1)
        
        print(f"\n[*] Loading statistics from {CSV_MONTE_CARLO_STATS}...")
        stats = pd.read_csv(CSV_MONTE_CARLO_STATS)
        
        print(f"[✓] Loaded {len(stats)} rows of statistics")
        print("\nSummary Statistics:")
        print(stats.to_string(index=False))
        print()
        
        # Generate plots
        print("[*] Generating plots...")
        generate_plots(stats)
        
        print(f"\n{'='*80}")
        print("PLOTS GENERATED")
        print(f"{'='*80}\n")
    else:
        # Run full benchmark and generate plots
        df_all_results, stats = run_all_simulations(SERIES_NAME)
