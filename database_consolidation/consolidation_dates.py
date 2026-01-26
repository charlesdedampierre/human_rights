"""
Consolidate date fields from instances_properties into a single date column.
Uses priority-based rules to select the best available date.

Input: wikidata_sparql_scripts/instance_properties/output/instance_properties.db
Output:
  - Creates prop_DATE_consolidated table with instance_id, instance_label, year, source_field
  - Creates consolidation_rules table to track the rules used

Date priority (first available wins):
1. publication_date - Most specific for published works
2. inception - Creation/inception date
3. point_in_time - Specific point in time
4. date_of_first_performance - For performances
5. start_time - Start of an event/period
6. earliest_date - Earliest known date
7. work_period_start - Start of work period
"""

import sqlite3
import re
import json
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "wikidata_sparql_scripts" / "instance_properties" / "output" / "instance_properties.db"

# Date fields in priority order
DATE_FIELDS_PRIORITY = [
    ("inception", "Creation/inception date"),
    ("publication_date", "Publication date for works"),
    ("point_in_time", "Specific point in time"),
    ("date_of_first_performance", "For performances"),
    ("start_time", "Start of an event/period"),
    ("earliest_date", "Earliest known date"),
    ("work_period_start", "Start of work period"),
]

CONSOLIDATION_RULE_NAME = "date_priority_v2"
CONSOLIDATION_RULE_DESCRIPTION = "Priority-based date consolidation: inception > publication_date > point_in_time > date_of_first_performance > start_time > earliest_date > work_period_start"


def extract_year(date_str):
    """
    Extract year from date string.
    Handles both AD dates (YYYY-MM-DD) and BC dates (-YYYY-MM-DD).
    Returns integer year (negative for BC).
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Handle multiple dates (take first one)
    if "," in date_str:
        date_str = date_str.split(",")[0].strip()

    # Handle BC dates (negative years like -0800-01-01)
    if date_str.startswith("-"):
        match = re.search(r"^-(\d+)", date_str)
        if match:
            return -int(match.group(1))

    # Handle regular dates (YYYY-MM-DD or just YYYY)
    match = re.search(r"^(\d+)", date_str)
    if match:
        return int(match.group(1))

    return None


def setup_consolidation_rules_table(conn):
    """Create consolidation_rules table to track rules used."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS consolidation_rules (
            consolidation_id TEXT PRIMARY KEY,
            rule_name TEXT,
            rule_type TEXT,
            description TEXT,
            priority_fields TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    print("Created/verified consolidation_rules table")


def register_consolidation_rule(conn) -> str:
    """Register the consolidation rule and return its ID."""
    cursor = conn.cursor()

    # Create a unique ID based on the rule
    consolidation_id = CONSOLIDATION_RULE_NAME

    # Check if rule already exists
    cursor.execute("SELECT consolidation_id FROM consolidation_rules WHERE consolidation_id = ?", (consolidation_id,))
    existing = cursor.fetchone()

    priority_fields_json = json.dumps([
        {"field": field, "description": desc, "priority": i + 1}
        for i, (field, desc) in enumerate(DATE_FIELDS_PRIORITY)
    ])

    if not existing:
        cursor.execute("""
            INSERT INTO consolidation_rules (consolidation_id, rule_name, rule_type, description, priority_fields, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            consolidation_id,
            CONSOLIDATION_RULE_NAME,
            "priority",
            CONSOLIDATION_RULE_DESCRIPTION,
            priority_fields_json,
            datetime.now().isoformat()
        ))
        conn.commit()
        print(f"Registered consolidation rule: {CONSOLIDATION_RULE_NAME}")
    else:
        print(f"Using existing consolidation rule: {CONSOLIDATION_RULE_NAME}")

    return consolidation_id


def create_consolidated_table(conn):
    """Create the consolidated dates table."""
    cursor = conn.cursor()

    # Drop and recreate the table
    cursor.execute("DROP TABLE IF EXISTS prop_DATE_consolidated")

    cursor.execute("""
        CREATE TABLE prop_DATE_consolidated (
            instance_id TEXT PRIMARY KEY,
            instance_label TEXT,
            year INTEGER,
            source_field TEXT,
            consolidation_id TEXT
        )
    """)

    conn.commit()
    print("Created table: prop_DATE_consolidated")


def consolidate_dates(conn, consolidation_id: str):
    """Consolidate dates from multiple fields into a single year."""
    print("\n" + "=" * 60)
    print("Consolidating dates from instances_properties")
    print("=" * 60)

    cursor = conn.cursor()

    # Create the consolidated table
    create_consolidated_table(conn)

    # Build the SELECT query with all date fields
    date_fields = [field for field, _ in DATE_FIELDS_PRIORITY]
    fields_str = ", ".join(date_fields)

    print(f"\nDate fields priority:")
    for i, (field, desc) in enumerate(DATE_FIELDS_PRIORITY, 1):
        print(f"  {i}. {field} - {desc}")

    # Get all records
    print("\nLoading records from database...")
    cursor.execute(f"""
        SELECT instance_id, instance_label, {fields_str}
        FROM instances_properties
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows):,} records")

    # Process each record
    print("Consolidating dates...")
    inserts = []
    source_field_counts = {field: 0 for field in date_fields}
    no_date_count = 0

    for row in tqdm(rows, desc="Processing"):
        instance_id = row[0]
        instance_label = row[1]
        date_values = row[2:]

        # Find first non-null date in priority order
        year = None
        source_field = None

        for i, (field, _) in enumerate(DATE_FIELDS_PRIORITY):
            date_value = date_values[i]
            if date_value:
                extracted_year = extract_year(date_value)
                if extracted_year is not None:
                    year = extracted_year
                    source_field = field
                    source_field_counts[field] += 1
                    break

        if year is None:
            no_date_count += 1

        inserts.append((instance_id, instance_label, year, source_field, consolidation_id))

    # Insert into consolidated table
    print("\nInserting into prop_DATE_consolidated...")
    batch_size = 10000
    for i in tqdm(range(0, len(inserts), batch_size), desc="Writing"):
        batch = inserts[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO prop_DATE_consolidated (instance_id, instance_label, year, source_field, consolidation_id)
            VALUES (?, ?, ?, ?, ?)
        """, batch)
        conn.commit()

    # Statistics
    print("\n" + "-" * 40)
    print("Statistics:")
    print("-" * 40)

    cursor.execute("SELECT COUNT(*) FROM prop_DATE_consolidated")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM prop_DATE_consolidated WHERE year IS NOT NULL")
    with_year = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM prop_DATE_consolidated WHERE year < 0")
    bc_count = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(year), MAX(year) FROM prop_DATE_consolidated WHERE year IS NOT NULL")
    min_year, max_year = cursor.fetchone()

    print(f"Total records: {total:,}")
    print(f"Records with year: {with_year:,} ({100 * with_year / total:.1f}%)")
    print(f"Records without date: {no_date_count:,}")
    print(f"BC dates (negative years): {bc_count:,}")
    if min_year and max_year:
        print(f"Year range: {min_year} to {max_year}")

    print("\nDate source breakdown:")
    for field, count in source_field_counts.items():
        if count > 0:
            pct = 100 * count / with_year if with_year > 0 else 0
            print(f"  - {field}: {count:,} ({pct:.1f}%)")

    # Sample BC dates
    cursor.execute("""
        SELECT instance_id, instance_label, year, source_field
        FROM prop_DATE_consolidated
        WHERE year < 0
        ORDER BY year
        LIMIT 5
    """)
    bc_samples = cursor.fetchall()
    if bc_samples:
        print("\nSample BC dates:")
        for row in bc_samples:
            print(f"  - {row[1]} ({row[0]}): {row[2]} (from {row[3]})")

    # Sample recent dates
    cursor.execute("""
        SELECT instance_id, instance_label, year, source_field
        FROM prop_DATE_consolidated
        WHERE year > 2000
        ORDER BY year DESC
        LIMIT 5
    """)
    recent_samples = cursor.fetchall()
    if recent_samples:
        print("\nSample recent dates:")
        for row in recent_samples:
            print(f"  - {row[1]} ({row[0]}): {row[2]} (from {row[3]})")


def main():
    print("=" * 60)
    print("DATE CONSOLIDATION: instances_properties -> prop_DATE_consolidated")
    print("=" * 60)
    print(f"Database: {DB_PATH}")

    # Check database exists
    if not DB_PATH.exists():
        print(f"ERROR: Database not found: {DB_PATH}")
        return

    # Connect to database
    conn = sqlite3.connect(DB_PATH)

    # Setup consolidation rules table
    setup_consolidation_rules_table(conn)

    # Register the consolidation rule
    consolidation_id = register_consolidation_rule(conn)

    # Consolidate dates
    consolidate_dates(conn, consolidation_id)

    conn.close()

    print("\n" + "=" * 60)
    print(f"Done! Database updated: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
