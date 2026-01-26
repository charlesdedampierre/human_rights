"""Consolidate places: country_of_origin > language_of_work -> regions"""

import sqlite3
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

DB_PATH = Path(__file__).parent.parent / "wikidata_sparql_scripts" / "instance_properties" / "output" / "instance_properties.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Register rule
    cursor.execute("""
        INSERT OR REPLACE INTO consolidation_rules VALUES
        ('place_priority_v1', 'place_priority_v1', 'priority',
         'country_of_origin > language_of_work -> regions',
         '{"priority": ["country_of_origin", "language_of_work"]}', ?)
    """, (datetime.now().isoformat(),))

    # Load lookups (using value_label since that's what's stored in instances_place_properties)
    print("Loading lookups...")
    cursor.execute("SELECT value_label, modern_country FROM prop_PLACE_country_of_origin_ai_enriched WHERE modern_country IS NOT NULL")
    co_map = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT value_label, modern_country FROM prop_PLACE_language_of_work_ai_enriched WHERE modern_country IS NOT NULL")
    lang_map = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT country, macro_region, region FROM country_region_mapping")
    region_map = {r[0]: (r[1], r[2]) for r in cursor.fetchall()}

    # Create table
    cursor.execute("DROP TABLE IF EXISTS prop_PLACE_consolidated")
    cursor.execute("""
        CREATE TABLE prop_PLACE_consolidated (
            instance_id TEXT PRIMARY KEY, instance_label TEXT, modern_country TEXT,
            source_field TEXT, macro_region TEXT, region TEXT, consolidation_id TEXT)
    """)

    # Get all instances with place info
    cursor.execute("""
        SELECT ip.instance_id, ip.instance_label, ipp.country_of_origin, ipp.language_of_work
        FROM instances_properties ip
        LEFT JOIN instances_place_properties ipp ON ip.instance_id = ipp.instance_id
    """)
    rows = cursor.fetchall()

    # Process with tqdm
    inserts = []
    for instance_id, label, co_raw, lang_raw in tqdm(rows, desc="Processing"):
        country, source = None, None

        # Priority 1: country_of_origin (direct lookup)
        if co_raw and co_raw in co_map:
            country, source = co_map[co_raw], "country_of_origin"

        # Priority 2: language (direct lookup)
        if not country and lang_raw and lang_raw in lang_map:
            country, source = lang_map[lang_raw], "language_of_work"

        macro, reg = region_map.get(country, (None, None)) if country else (None, None)
        inserts.append((instance_id, label, country, source, macro, reg, "place_priority_v1"))

    # Insert in batches
    print("Writing...")
    for i in tqdm(range(0, len(inserts), 50000), desc="Writing"):
        cursor.executemany("INSERT INTO prop_PLACE_consolidated VALUES (?,?,?,?,?,?,?)", inserts[i:i+50000])
        conn.commit()

    # Stats
    cursor.execute("SELECT COUNT(*) FROM prop_PLACE_consolidated WHERE modern_country IS NOT NULL")
    with_country = cursor.fetchone()[0]
    cursor.execute("SELECT source_field, COUNT(*) FROM prop_PLACE_consolidated WHERE source_field IS NOT NULL GROUP BY source_field")
    print(f"\nWith country: {with_country:,}")
    for r in cursor.fetchall(): print(f"  {r[0]}: {r[1]:,}")

    cursor.execute("SELECT macro_region, COUNT(*) FROM prop_PLACE_consolidated WHERE macro_region IS NOT NULL GROUP BY macro_region ORDER BY 2 DESC")
    print("\nBy macro_region:")
    for r in cursor.fetchall(): print(f"  {r[0]}: {r[1]:,}")

    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
