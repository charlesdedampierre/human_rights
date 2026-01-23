"""
Update countries table with macro_region and region based on cached mappings.
Reads from cache/region_mappings.json
"""

import sqlite3
import pandas as pd
import json
from pathlib import Path


def load_region_mappings(cache_path: Path) -> dict:
    """Load region mappings from cache file."""
    with open(cache_path, 'r') as f:
        return json.load(f)


def update_database(db_path: str, cache_dir: str = None):
    """Update the countries table with new region columns."""

    # Determine cache path
    if cache_dir is None:
        cache_dir = Path(db_path).parent.parent / "cache"
    else:
        cache_dir = Path(cache_dir)

    cache_path = cache_dir / "region_mappings.json"

    if not cache_path.exists():
        raise FileNotFoundError(f"Region mappings cache not found at {cache_path}")

    # Load mappings from cache
    mappings = load_region_mappings(cache_path)
    country_mappings = mappings.get("country_mappings", {})
    label_mappings = mappings.get("label_mappings", {})

    print(f"Loaded {len(country_mappings)} country mappings from cache")
    print(f"Loaded {len(label_mappings)} label mappings from cache")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if columns already exist
    cursor.execute("PRAGMA table_info(countries)")
    columns = [col[1] for col in cursor.fetchall()]

    # Add new columns if they don't exist
    if 'macro_region' not in columns:
        cursor.execute("ALTER TABLE countries ADD COLUMN macro_region TEXT")
        print("Added 'macro_region' column")

    if 'region' not in columns:
        cursor.execute("ALTER TABLE countries ADD COLUMN region TEXT")
        print("Added 'region' column")

    # Get all countries from database
    df = pd.read_sql("SELECT DISTINCT modern_country FROM countries", conn)

    # Update each country from country_mappings
    updated = 0
    unmapped = []

    for country in df['modern_country'].dropna().unique():
        if country in country_mappings:
            mapping = country_mappings[country]
            cursor.execute("""
                UPDATE countries
                SET macro_region = ?, region = ?
                WHERE modern_country = ?
            """, (mapping["macro_region"], mapping["region"], country))
            updated += 1
        else:
            unmapped.append(country)

    conn.commit()

    # Step 2: Infer regions from labels where modern_country is null
    print("\n" + "=" * 60)
    print("INFERRING REGIONS FROM LABELS")
    print("=" * 60)

    inferred = 0
    for label, mapping in label_mappings.items():
        cursor.execute("""
            UPDATE countries
            SET macro_region = ?, region = ?
            WHERE label = ? AND (macro_region IS NULL OR region IS NULL)
        """, (mapping["macro_region"], mapping["region"], label))
        if cursor.rowcount > 0:
            print(f"  '{label}' -> {mapping['macro_region']} / {mapping['region']} ({cursor.rowcount} rows)")
            inferred += cursor.rowcount

    conn.commit()
    print(f"\nInferred regions for {inferred} entries from labels")

    # Report results
    print(f"\nUpdated {updated} country mappings")

    if unmapped:
        print(f"\nUnmapped countries ({len(unmapped)}):")
        for c in sorted(unmapped):
            print(f"  - {c}")

    # Show summary by macro region
    print("\n" + "=" * 60)
    print("SUMMARY BY MACRO REGION")
    print("=" * 60)

    summary = pd.read_sql("""
        SELECT macro_region, region, COUNT(DISTINCT modern_country) as countries
        FROM countries
        WHERE macro_region IS NOT NULL
        GROUP BY macro_region, region
        ORDER BY macro_region, region
    """, conn)

    current_macro = None
    for _, row in summary.iterrows():
        if row['macro_region'] != current_macro:
            if current_macro is not None:
                print()
            current_macro = row['macro_region']
            print(f"\n{current_macro}:")
        print(f"  {row['region']}: {row['countries']} countries")

    conn.close()
    print("\nDatabase updated successfully!")


if __name__ == "__main__":
    import sys

    db_path = sys.argv[1] if len(sys.argv) > 1 else "output/literary_works.db"
    cache_dir = sys.argv[2] if len(sys.argv) > 2 else None
    update_database(db_path, cache_dir)
