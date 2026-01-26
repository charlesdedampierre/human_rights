"""
Plot distribution of works per century before 1700 (log scale).
Uses all date properties combined.
"""

import sqlite3
import re
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "output" / "instance_properties.db"
OUTPUT_PATH = SCRIPT_DIR / "output" / "centuries_before_1700.png"

# All date columns
DATE_COLUMNS = [
    "publication_date",
    "inception",
    "start_time",
    "end_time",
    "point_in_time",
    "date_of_first_performance",
    "earliest_date",
    "work_period_start",
    "work_period_end",
    "public_domain_date",
]


def extract_year(date_str):
    """Extract year from date string. Returns None if invalid."""
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Handle BC dates (negative years)
    if date_str.startswith("-"):
        match = re.match(r"^-(\d+)", date_str)
        if match:
            return -int(match.group(1))
        return None

    # Handle normal dates
    match = re.match(r"^(\d{1,4})", date_str)
    if match:
        return int(match.group(1))

    return None


def year_to_century_num(year):
    """Convert year to century number (negative for BC)."""
    if year is None:
        return None

    if year <= 0:
        # BC dates: -500 to -401 = 5th century BC = -5
        century = -((abs(year) - 1) // 100 + 1)
        return century
    else:
        # AD dates: 1-100 = 1st century = 1
        century = (year - 1) // 100 + 1
        return century


def century_num_to_label(century_num):
    """Convert century number to readable label."""
    if century_num < 0:
        return f"{abs(century_num)} BC"
    else:
        return f"{century_num} AD"


def main():
    print(f"Connecting to: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build query to get all dates
    date_cols = ", ".join(DATE_COLUMNS)
    cursor.execute(f"SELECT instance_id, {date_cols} FROM instances_properties")

    # Count per century (using earliest date for each instance)
    instance_years = {}

    print("Processing instances...")
    rows = cursor.fetchall()

    for row in tqdm(rows, desc="Extracting dates"):
        instance_id = row[0]
        dates = row[1:]

        # Find earliest year for this instance
        years = []
        for date_val in dates:
            year = extract_year(date_val)
            if year is not None and year < 1700 and year > -3000:  # Filter reasonable dates
                years.append(year)

        if years:
            earliest_year = min(years)
            instance_years[instance_id] = earliest_year

    conn.close()

    # Count per century
    century_counts = defaultdict(int)
    for instance_id, year in instance_years.items():
        century_num = year_to_century_num(year)
        if century_num:
            century_counts[century_num] += 1

    print(f"\nTotal instances before 1700: {len(instance_years):,}")

    # Sort centuries chronologically
    sorted_century_nums = sorted(century_counts.keys())

    # Print table
    print("\n" + "=" * 40)
    print("Century Distribution (before 1700)")
    print("=" * 40)
    for century_num in sorted_century_nums:
        label = century_num_to_label(century_num)
        print(f"{label:>12}: {century_counts[century_num]:>8,}")

    # Prepare data for plotting
    labels = [century_num_to_label(c) for c in sorted_century_nums]
    counts = [century_counts[c] for c in sorted_century_nums]

    # Create plot
    fig, ax = plt.subplots(figsize=(16, 8))

    colors = ['#8B0000' if c < 0 else '#1E3A5F' for c in sorted_century_nums]
    bars = ax.bar(range(len(labels)), counts, color=colors, edgecolor='black', linewidth=0.5)

    ax.set_yscale('log')
    ax.set_xlabel('Century', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count (log scale)', fontsize=14, fontweight='bold')
    ax.set_title('Distribution of Works by Century (before 1700)\nRed = BC, Blue = AD',
                 fontsize=16, fontweight='bold')

    # X-axis labels
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=60, ha='right', fontsize=10)

    # Add count labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height * 1.1,
               f'{count:,}', ha='center', va='bottom', fontsize=8, fontweight='bold')

    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    ax.set_ylim(bottom=0.8)

    # Add vertical line at year 0
    bc_count = sum(1 for c in sorted_century_nums if c < 0)
    if bc_count > 0 and bc_count < len(sorted_century_nums):
        ax.axvline(x=bc_count - 0.5, color='gray', linestyle='--', linewidth=2, alpha=0.7)
        ax.text(bc_count - 0.5, ax.get_ylim()[1] * 0.5, ' Year 0 ',
                ha='center', va='center', fontsize=10, color='gray',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"\nPlot saved to: {OUTPUT_PATH}")
    plt.show()


if __name__ == "__main__":
    main()
