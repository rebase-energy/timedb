import matplotlib.pyplot as plt
import numpy as np
import matplotlib

# Set backend to Agg for headless environments
matplotlib.use('Agg')

def run_double_plateau_analysis():
    # --- CONFIGURATION ---
    CONFIG_THEORY = {
        "workflow_scenarios": [10000, 30000, 100000],
        "horizon_hours": 120,
        "resolution_min": 15,
        "avg_row_size_bytes": 133.6,
        "index_multiplier": 2.5,
        "years_to_project": 10,
        "retention_years": 3,    # All customers delete after 3 years
    }

    # Actual/Measured configuration
    CONFIG_ACTUAL = {
        "workflow_scenarios": CONFIG_THEORY["workflow_scenarios"],
        "horizon_hours": CONFIG_THEORY["horizon_hours"],
        "resolution_min": CONFIG_THEORY["resolution_min"],
        "avg_row_size_bytes": 252.71,
        "index_multiplier": 1,
        "years_to_project": CONFIG_THEORY["years_to_project"],
        "retention_years": CONFIG_THEORY["retention_years"],
    }

    points_per_wf = CONFIG_THEORY["horizon_hours"] / (CONFIG_THEORY["resolution_min"] / 60.0)
    days = np.arange(1, (365 * CONFIG_THEORY["years_to_project"]) + 1)
    years = days / 365.25
    cutoff = int(CONFIG_THEORY["retention_years"] * 365.25)

    # Create side-by-side plots: left is theory (old), right is actual (new)
    fig, axes = plt.subplots(1, 2, figsize=(16, 8), sharey=True)
    colors = ['#3498db', '#2ecc71', '#e74c3c']

    def plot_for_config(ax, config, title, linestyle='-'):
        for wf_total, color in zip(config["workflow_scenarios"], colors):
            # Full volume
            daily_gb = (wf_total * points_per_wf * config["avg_row_size_bytes"] * config["index_multiplier"]) / (1024**3)
            # Storage caps at retention years
            storage = (days * daily_gb) / 1024
            storage[cutoff:] = storage[cutoff-1]

            linewidth = 3 if wf_total == 30000 else 1.5
            ax.plot(years, storage, label=f"{wf_total:,} Workflows/Day", color=color, lw=linewidth, linestyle=linestyle)
            ax.fill_between(years, 0, storage, color=color, alpha=0.05)

        ax.set_xlim(0, 6)
        ax.set_xlabel("Years", fontsize=11)
        ax.set_ylabel("Total Storage (Terabytes)", fontsize=11)
        ax.grid(True, linestyle=':', alpha=0.6)
        ax.legend(title="Daily Total Workflows", loc='upper left')

        # Markers
        ax.axvline(x=3, color='orange', linestyle='--', alpha=0.6)
        ax.text(2.85, 0.5, 'Cleanup Starts (3yr)', color='orange', rotation=90, verticalalignment='bottom')
        ax.set_title(title)

    # Plot theory (old config) with dashed lines
    plot_for_config(axes[0], CONFIG_THEORY, "Theory (Old Config)", linestyle='--')
    # Plot actual (new config) with solid lines
    plot_for_config(axes[1], CONFIG_ACTUAL, "Actual (New Config)", linestyle='-')

    # --- VALUES TABLE IN HEADER ---
    table_data = [
        ["Avg Row Size (bytes)", f"{CONFIG_THEORY['avg_row_size_bytes']}", f"{CONFIG_ACTUAL['avg_row_size_bytes']}"],
        ["Index Multiplier", f"{CONFIG_THEORY['index_multiplier']}", f"{CONFIG_ACTUAL['index_multiplier']}"],
        ["Resolution (min)", f"{CONFIG_THEORY['resolution_min']}", f"{CONFIG_ACTUAL['resolution_min']}"],
        ["Horizon (hours)", f"{CONFIG_THEORY['horizon_hours']}", f"{CONFIG_ACTUAL['horizon_hours']}"],
        ["Retention (years)", f"{CONFIG_THEORY['retention_years']}", f"{CONFIG_ACTUAL['retention_years']}"],
    ]
    colLabels = ["Parameter", "Theory", "Actual"]
    table = plt.table(cellText=table_data, colLabels=colLabels, cellLoc='center', loc='upper center', bbox=[0.18, 0.92, 0.64, 0.12])
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    fig.suptitle("Database Terminal Size Projection\n(Data deleted after 3 years)", fontsize=14, y=0.99)
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save combined comparison figure
    fig.savefig("double_plateau_storage_comparison.png", dpi=300)

    # Also save separate theory and actual plots for convenience
    fig_theory = plt.figure(figsize=(12, 8))
    ax_t = fig_theory.add_subplot(111)
    plot_for_config(ax_t, CONFIG_THEORY, "Theory (Old Config)", linestyle='--')
    fig_theory.tight_layout()
    fig_theory.savefig("double_plateau_storage_theory.png", dpi=300)

    fig_actual = plt.figure(figsize=(12, 8))
    ax_a = fig_actual.add_subplot(111)
    plot_for_config(ax_a, CONFIG_ACTUAL, "Actual (New Config)", linestyle='-')
    fig_actual.tight_layout()
    fig_actual.savefig("double_plateau_storage_actual.png", dpi=300)

    plt.close('all')
    print("Graphs saved as double_plateau_storage_comparison.png, double_plateau_storage_theory.png, double_plateau_storage_actual.png")

if __name__ == "__main__":
    run_double_plateau_analysis()