"""
Create SQLite database from extracted Wikidata JSON.

Input: output/extracted/extracted_data.json
Output: output/instance_properties.db

Tables (in order):
- properties: List of all properties with their IDs and names
- instances_properties: One row per instance with all properties as columns
- instances_content_properties: Content-related properties
- instances_dates_properties: Date-related properties
- instances_type_properties: Type-related properties
- instances_place_properties: Place-related properties
- instances_sitelinks: Sitelinks for each instance
- instances_identifiers: External identifiers for each instance
- prop_*: One table per property with aggregated value counts (value_id, value_label, occurrence_count)
"""

import json
import sqlite3
import re
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
JSON_PATH = SCRIPT_DIR / "output" / "extracted" / "extracted_data.json"
DB_PATH = SCRIPT_DIR / "output" / "instance_properties.db"

# Property categories
DATE_PROPERTIES = {
    "P577": "publication_date",
    "P571": "inception",
    "P580": "start_time",
    "P582": "end_time",
    "P585": "point_in_time",
    "P1191": "date_of_first_performance",
    "P1319": "earliest_date",
    "P2031": "work_period_start",
    "P2032": "work_period_end",
    "P3893": "public_domain_date",
}

PLACE_PROPERTIES = {
    "P495": "country_of_origin",
    "P17": "country",
    "P291": "place_of_publication",
    "P840": "narrative_location",
    "P131": "located_in_admin_entity",
    "P276": "location",
    "P1001": "applies_to_jurisdiction",
    "P407": "language_of_work",
    "P364": "original_language",
}

CONTENT_PROPERTIES = {
    "P953": "full_work_url",
    "P1433": "published_in",
    "P1343": "described_by_source",
    "P973": "described_at_url",
    "P856": "official_website",
    "P18": "image",
    "P996": "document_file_on_commons",
    "P1476": "title",
    "P1680": "subtitle",
    "P6216": "copyright_status",
}

TYPE_PROPERTIES = {
    "P31": "instance_of",
    "P136": "genre",
    "P7937": "form_of_creative_work",
    "P282": "writing_system",
    "P2551": "used_metre",
    "P135": "movement",
    "P921": "main_subject",
}

CREATOR_PROPERTIES = {
    "P50": "author",
    "P2093": "author_name_string",
    "P98": "editor",
    "P655": "translator",
    "P170": "creator",
    "P123": "publisher",
}

RELATIONSHIP_PROPERTIES = {
    "P361": "part_of",
    "P144": "based_on",
    "P179": "part_of_series",
    "P155": "follows",
    "P156": "followed_by",
}

# All properties combined
ALL_PROPERTIES = {
    **DATE_PROPERTIES,
    **PLACE_PROPERTIES,
    **CONTENT_PROPERTIES,
    **TYPE_PROPERTIES,
    **CREATOR_PROPERTIES,
    **RELATIONSHIP_PROPERTIES,
}


def format_date(date_str):
    """Format date to YYYY-MM-DD. Handles BC dates."""
    if not date_str:
        return None
    date_str = str(date_str)

    if date_str.startswith("-"):
        match = re.search(r"^-(\d+)-(\d{2})-(\d{2})", date_str)
        if match:
            year = int(match.group(1))
            return f"-{year}-{match.group(2)}-{match.group(3)}"
        match = re.search(r"^-(\d+)", date_str)
        if match:
            year = int(match.group(1))
            return f"-{year}-01-01"
        return None

    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(\d{4})", date_str)
    if match:
        return f"{match.group(1)}-01-01"
    return None


def extract_value(value, is_date=False):
    """Extract id and label from property value."""
    if isinstance(value, dict):
        return value.get("id"), value.get("label", value.get("id", ""))
    if is_date:
        formatted = format_date(str(value))
        return None, formatted
    return None, str(value) if value else None


def get_property_labels(instance_data, prop_id, is_date=False):
    """Get all labels for a property, joined by ', '."""
    props = instance_data.get("properties", {})
    if prop_id not in props:
        return None
    values = props[prop_id].get("values", [])
    if not values:
        return None
    labels = [extract_value(v, is_date)[1] for v in values]
    labels = [l for l in labels if l]
    return ", ".join(labels) if labels else None


def create_category_table(cursor, data, table_name, properties_dict):
    """Create a category table with specific properties."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    columns = ["instance_id TEXT PRIMARY KEY", "instance_label TEXT"]
    for col_name in properties_dict.values():
        columns.append(f"{col_name} TEXT")

    cursor.execute(f"CREATE TABLE {table_name} ({', '.join(columns)})")

    col_names = ["instance_id", "instance_label"] + list(properties_dict.values())
    placeholders = ", ".join(["?" for _ in col_names])
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
    )

    is_date_table = "dates" in table_name

    count = 0
    for instance_id, instance_data in data.items():
        has_prop = any(
            p in instance_data.get("properties", {}) for p in properties_dict
        )
        if not has_prop:
            continue
        row = [instance_id, instance_data.get("label", instance_id)]
        for prop_id in properties_dict.keys():
            is_date = prop_id in DATE_PROPERTIES
            row.append(get_property_labels(instance_data, prop_id, is_date))
        cursor.execute(insert_sql, row)
        count += 1

    return count


def main():
    print(f"Loading JSON data from {JSON_PATH}...")
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total instances: {len(data):,}")

    # Create database
    print(f"\nCreating SQLite database: {DB_PATH}")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop all existing tables first
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = [row[0] for row in cursor.fetchall()]
    for table in existing_tables:
        if table != "sqlite_sequence":
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    print(f"Dropped {len(existing_tables)} existing tables")

    # Collect property labels from data
    property_labels_from_data = {}
    for instance_id, instance_data in data.items():
        for prop_id, prop_data in instance_data.get("properties", {}).items():
            if prop_id not in property_labels_from_data:
                property_labels_from_data[prop_id] = prop_data.get(
                    "property_label", prop_id
                )

    # =========================================================================
    # 1. CREATE PROPERTIES TABLE (list of all properties)
    # =========================================================================
    print("\n1. Creating properties table...")
    cursor.execute("DROP TABLE IF EXISTS properties")
    cursor.execute(
        """
        CREATE TABLE properties (
            property_id TEXT PRIMARY KEY,
            property_name TEXT,
            column_name TEXT,
            category TEXT
        )
    """
    )

    # Insert with categories
    for prop_id, col_name in DATE_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "date"),
        )

    for prop_id, col_name in PLACE_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "place"),
        )

    for prop_id, col_name in CONTENT_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "content"),
        )

    for prop_id, col_name in TYPE_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "type"),
        )

    for prop_id, col_name in CREATOR_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "creator"),
        )

    for prop_id, col_name in RELATIONSHIP_PROPERTIES.items():
        label = property_labels_from_data.get(prop_id, prop_id)
        cursor.execute(
            "INSERT INTO properties VALUES (?, ?, ?, ?)",
            (prop_id, label, col_name, "relationship"),
        )

    print(f"   - {len(ALL_PROPERTIES)} properties")

    # =========================================================================
    # 2. CREATE INSTANCES_PROPERTIES TABLE (main table)
    # =========================================================================
    print("\n2. Creating instances_properties table...")
    cursor.execute("DROP TABLE IF EXISTS instances_properties")

    columns = [
        "instance_id TEXT PRIMARY KEY",
        "instance_label TEXT",
        "description TEXT",
    ]
    for col_name in ALL_PROPERTIES.values():
        columns.append(f"{col_name} TEXT")

    cursor.execute(f"CREATE TABLE instances_properties ({', '.join(columns)})")

    col_names = ["instance_id", "instance_label", "description"] + list(
        ALL_PROPERTIES.values()
    )
    placeholders = ", ".join(["?" for _ in col_names])
    insert_sql = f"INSERT INTO instances_properties ({', '.join(col_names)}) VALUES ({placeholders})"

    for instance_id, instance_data in data.items():
        row = [
            instance_id,
            instance_data.get("label", instance_id),
            instance_data.get("description", ""),
        ]
        for prop_id in ALL_PROPERTIES.keys():
            is_date = prop_id in DATE_PROPERTIES
            row.append(get_property_labels(instance_data, prop_id, is_date))
        cursor.execute(insert_sql, row)

    print(f"   - {len(data):,} instances")

    # =========================================================================
    # 3. CREATE INSTANCES_CONTENT_PROPERTIES TABLE
    # =========================================================================
    print("\n3. Creating instances_content_properties table...")
    count = create_category_table(
        cursor, data, "instances_content_properties", CONTENT_PROPERTIES
    )
    print(f"   - {count:,} instances")

    # =========================================================================
    # 4. CREATE INSTANCES_DATES_PROPERTIES TABLE
    # =========================================================================
    print("\n4. Creating instances_dates_properties table...")
    count = create_category_table(
        cursor, data, "instances_dates_properties", DATE_PROPERTIES
    )
    print(f"   - {count:,} instances")

    # =========================================================================
    # 5. CREATE INSTANCES_TYPE_PROPERTIES TABLE
    # =========================================================================
    print("\n5. Creating instances_type_properties table...")
    count = create_category_table(
        cursor, data, "instances_type_properties", TYPE_PROPERTIES
    )
    print(f"   - {count:,} instances")

    # =========================================================================
    # 6. CREATE INSTANCES_PLACE_PROPERTIES TABLE
    # =========================================================================
    print("\n6. Creating instances_place_properties table...")
    count = create_category_table(
        cursor, data, "instances_place_properties", PLACE_PROPERTIES
    )
    print(f"   - {count:,} instances")

    # =========================================================================
    # 7. CREATE INSTANCES_SITELINKS TABLE
    # =========================================================================
    print("\n7. Creating instances_sitelinks table...")
    cursor.execute("DROP TABLE IF EXISTS instances_sitelinks")
    cursor.execute(
        """
        CREATE TABLE instances_sitelinks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_id TEXT,
            instance_label TEXT,
            sitelink_url TEXT,
            sitelink_type TEXT
        )
    """
    )

    sitelink_count = 0
    for instance_id, instance_data in data.items():
        instance_label = instance_data.get("label", instance_id)
        for sitelink in instance_data.get("sitelinks", []):
            url = sitelink.get("url", "")
            stype = sitelink.get("type", "")
            cursor.execute(
                "INSERT INTO instances_sitelinks (instance_id, instance_label, sitelink_url, sitelink_type) VALUES (?, ?, ?, ?)",
                (instance_id, instance_label, url, stype),
            )
            sitelink_count += 1
    print(f"   - {sitelink_count:,} sitelinks")

    # =========================================================================
    # 8. CREATE INSTANCES_IDENTIFIERS TABLE
    # =========================================================================
    print("\n8. Creating instances_identifiers table...")
    cursor.execute("DROP TABLE IF EXISTS instances_identifiers")
    cursor.execute(
        """
        CREATE TABLE instances_identifiers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_id TEXT,
            instance_label TEXT,
            identifier_property TEXT,
            identifier_label TEXT,
            identifier_url TEXT
        )
    """
    )

    identifier_count = 0
    for instance_id, instance_data in data.items():
        instance_label = instance_data.get("label", instance_id)
        for identifier in instance_data.get("identifiers", []):
            prop = identifier.get("property", "")
            prop_label = identifier.get("property_label", "")
            url = identifier.get("url", "")
            cursor.execute(
                "INSERT INTO instances_identifiers (instance_id, instance_label, identifier_property, identifier_label, identifier_url) VALUES (?, ?, ?, ?, ?)",
                (instance_id, instance_label, prop, prop_label, url),
            )
            identifier_count += 1
    print(f"   - {identifier_count:,} identifiers")

    # =========================================================================
    # 9. CREATE PROPERTY TABLES (aggregated by value with occurrence counts)
    # =========================================================================
    print("\n9. Creating property tables (prop_*)...")
    property_tables_created = []

    # Map property IDs to their category prefix
    def get_table_name(prop_id, col_name):
        if prop_id in DATE_PROPERTIES:
            return f"prop_DATE_{col_name}"
        elif prop_id in PLACE_PROPERTIES:
            return f"prop_PLACE_{col_name}"
        elif prop_id in CONTENT_PROPERTIES:
            return f"prop_CONTENT_{col_name}"
        elif prop_id in TYPE_PROPERTIES:
            return f"prop_TYPE_{col_name}"
        elif prop_id in CREATOR_PROPERTIES:
            return f"prop_CREATOR_{col_name}"
        elif prop_id in RELATIONSHIP_PROPERTIES:
            return f"prop_RELATIONSHIP_{col_name}"
        else:
            return f"prop_{col_name}"

    for prop_id, col_name in ALL_PROPERTIES.items():
        table_name = get_table_name(prop_id, col_name)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        is_date = prop_id in DATE_PROPERTIES

        if is_date:
            cursor.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    value TEXT UNIQUE,
                    occurrence_count INTEGER
                )
            """
            )
        else:
            cursor.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    value_id TEXT,
                    value_label TEXT,
                    occurrence_count INTEGER
                )
            """
            )

        # Aggregate values and count unique instances per value
        value_instances = {}  # key: (value_id, value_label) or value for dates -> set of instance_ids
        for instance_id, instance_data in data.items():
            props = instance_data.get("properties", {})
            if prop_id not in props:
                continue
            for value in props[prop_id].get("values", []):
                value_id, value_label = extract_value(value, is_date)
                if is_date:
                    key = value_label
                else:
                    key = (value_id, value_label)
                if key not in value_instances:
                    value_instances[key] = set()
                value_instances[key].add(instance_id)

        # Convert sets to counts
        value_counts = {k: len(v) for k, v in value_instances.items()}

        # Insert aggregated data ordered by occurrence count (descending)
        count = 0
        sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
        for key, occ_count in sorted_values:
            if is_date:
                cursor.execute(
                    f"INSERT INTO {table_name} (value, occurrence_count) VALUES (?, ?)",
                    (key, occ_count),
                )
            else:
                value_id, value_label = key
                cursor.execute(
                    f"INSERT INTO {table_name} (value_id, value_label, occurrence_count) VALUES (?, ?, ?)",
                    (value_id, value_label, occ_count),
                )
            count += 1

        if count > 0:
            property_tables_created.append((table_name, count))

    print(f"   - {len(property_tables_created)} property tables created")

    # =========================================================================
    # CREATE INDEXES
    # =========================================================================
    print("\nCreating indexes...")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_prop_id ON instances_properties(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_content_id ON instances_content_properties(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_dates_id ON instances_dates_properties(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_type_id ON instances_type_properties(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_place_id ON instances_place_properties(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_sitelinks_id ON instances_sitelinks(instance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_identifiers_id ON instances_identifiers(instance_id)"
    )

    for table_name, _ in property_tables_created:
        if "DATE" in table_name:
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_value ON {table_name}(value)"
            )
        else:
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_value_id ON {table_name}(value_id)"
            )

    conn.commit()

    # =========================================================================
    # PRINT SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("DATABASE SUMMARY")
    print("=" * 70)

    main_tables = [
        "properties",
        "instances_properties",
        "instances_content_properties",
        "instances_dates_properties",
        "instances_type_properties",
        "instances_place_properties",
        "instances_sitelinks",
        "instances_identifiers",
    ]

    for table in main_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count:,} records")

    print(f"\nprop_* tables: {len(property_tables_created)} tables")

    # Sample data
    print("\n" + "=" * 70)
    print("SAMPLE - properties table")
    print("=" * 70)
    cursor.execute("SELECT * FROM properties LIMIT 10")
    for row in cursor.fetchall():
        print(row)

    print("\n" + "=" * 70)
    print("SAMPLE - instances_properties (first 3)")
    print("=" * 70)
    cursor.execute(
        "SELECT instance_id, instance_label, title, author, publication_date FROM instances_properties LIMIT 3"
    )
    for row in cursor.fetchall():
        print(row)

    print("\n" + "=" * 70)
    print("SAMPLE - instances_identifiers (first 5)")
    print("=" * 70)
    cursor.execute("SELECT * FROM instances_identifiers LIMIT 5")
    for row in cursor.fetchall():
        print(row)

    conn.close()
    print(f"\nDatabase created successfully: {DB_PATH}")


if __name__ == "__main__":
    main()
