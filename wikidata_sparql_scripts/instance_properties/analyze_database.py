"""
Analyze the instance_properties database and generate statistics report.

Output: JSON stats file + Markdown report
"""

import sqlite3
import json
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

# Paths
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "output" / "instance_properties.db"
STATS_PATH = SCRIPT_DIR / "output" / "database_stats.json"
REPORT_PATH = SCRIPT_DIR / "output" / "analysis_report.md"


def get_table_counts(cursor):
    """Get row counts for all tables."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall() if row[0] != "sqlite_sequence"]

    counts = {}
    for table in tqdm(tables, desc="Counting table rows"):
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cursor.fetchone()[0]
    return counts


def get_property_stats(cursor):
    """Get statistics for each property."""
    cursor.execute("SELECT property_id, property_name, column_name, category FROM properties")
    properties = cursor.fetchall()

    stats = []
    for prop_id, prop_name, col_name, category in tqdm(properties, desc="Analyzing properties"):
        prop_stat = {
            "property_id": prop_id,
            "property_name": prop_name,
            "column_name": col_name,
            "category": category,
        }

        # Count non-null values in instances_properties
        cursor.execute(f"SELECT COUNT(*) FROM instances_properties WHERE {col_name} IS NOT NULL")
        prop_stat["instances_with_value"] = cursor.fetchone()[0]

        # Get total instances
        cursor.execute("SELECT COUNT(*) FROM instances_properties")
        total = cursor.fetchone()[0]
        prop_stat["coverage_percent"] = round(prop_stat["instances_with_value"] / total * 100, 2) if total > 0 else 0

        # Get unique values count from prop_* table
        table_name = f"prop_{category.upper()}_{col_name}"
        try:
            if category == "date":
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            else:
                cursor.execute(f"SELECT COUNT(DISTINCT value_id) FROM {table_name}")
            prop_stat["unique_values"] = cursor.fetchone()[0]
        except:
            prop_stat["unique_values"] = 0

        # Get top 10 values
        try:
            if category == "date":
                cursor.execute(f"SELECT value, occurrence_count FROM {table_name} ORDER BY occurrence_count DESC LIMIT 10")
                prop_stat["top_values"] = [{"value": row[0], "count": row[1]} for row in cursor.fetchall()]
            else:
                cursor.execute(f"SELECT value_id, value_label, occurrence_count FROM {table_name} ORDER BY occurrence_count DESC LIMIT 10")
                prop_stat["top_values"] = [{"id": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]
        except:
            prop_stat["top_values"] = []

        stats.append(prop_stat)

    return stats


def get_sitelink_stats(cursor):
    """Get sitelink statistics."""
    stats = {}

    # Total sitelinks
    cursor.execute("SELECT COUNT(*) FROM instances_sitelinks")
    stats["total_sitelinks"] = cursor.fetchone()[0]

    # Instances with sitelinks
    cursor.execute("SELECT COUNT(DISTINCT instance_id) FROM instances_sitelinks")
    stats["instances_with_sitelinks"] = cursor.fetchone()[0]

    # Sitelinks by type
    cursor.execute("""
        SELECT sitelink_type, COUNT(*) as cnt
        FROM instances_sitelinks
        GROUP BY sitelink_type
        ORDER BY cnt DESC
        LIMIT 20
    """)
    stats["by_type"] = [{"type": row[0], "count": row[1]} for row in cursor.fetchall()]

    # Top instances by sitelink count
    cursor.execute("""
        SELECT instance_id, instance_label, sitelinks_count
        FROM instances_properties
        ORDER BY sitelinks_count DESC
        LIMIT 20
    """)
    stats["top_instances"] = [{"id": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]

    return stats


def get_identifier_stats(cursor):
    """Get identifier statistics."""
    stats = {}

    # Total identifiers
    cursor.execute("SELECT COUNT(*) FROM instances_identifiers")
    stats["total_identifiers"] = cursor.fetchone()[0]

    # Instances with identifiers
    cursor.execute("SELECT COUNT(DISTINCT instance_id) FROM instances_identifiers")
    stats["instances_with_identifiers"] = cursor.fetchone()[0]

    # Top identifier types
    cursor.execute("""
        SELECT identifier_property, identifier_label, COUNT(*) as cnt
        FROM instances_identifiers
        GROUP BY identifier_property
        ORDER BY cnt DESC
        LIMIT 30
    """)
    stats["by_type"] = [{"property": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]

    return stats


def get_date_distribution(cursor):
    """Get publication date distribution by century."""
    cursor.execute("""
        SELECT publication_date FROM instances_dates_properties
        WHERE publication_date IS NOT NULL
    """)

    century_counts = defaultdict(int)
    for row in cursor.fetchall():
        date = row[0]
        if date:
            try:
                # Handle BC dates
                if date.startswith("-"):
                    year = -int(date.split("-")[1])
                else:
                    year = int(date.split("-")[0])

                if year < 0:
                    century = (year // 100) - 1
                    century_label = f"{abs(century)}th century BC"
                else:
                    century = (year - 1) // 100 + 1
                    century_label = f"{century}th century"

                century_counts[century_label] = century_counts.get(century_label, 0) + 1
            except:
                pass

    # Sort by century
    def sort_key(item):
        label = item[0]
        if "BC" in label:
            return -int(label.split("th")[0])
        return int(label.split("th")[0])

    sorted_counts = sorted(century_counts.items(), key=sort_key)
    return [{"century": k, "count": v} for k, v in sorted_counts]


def get_instance_of_distribution(cursor):
    """Get instance_of (P31) distribution."""
    cursor.execute("""
        SELECT value_id, value_label, occurrence_count
        FROM prop_TYPE_instance_of
        ORDER BY occurrence_count DESC
        LIMIT 50
    """)
    return [{"id": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]


def get_language_distribution(cursor):
    """Get language distribution."""
    cursor.execute("""
        SELECT value_id, value_label, occurrence_count
        FROM prop_PLACE_language_of_work
        ORDER BY occurrence_count DESC
        LIMIT 30
    """)
    return [{"id": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]


def get_country_distribution(cursor):
    """Get country of origin distribution."""
    cursor.execute("""
        SELECT value_id, value_label, occurrence_count
        FROM prop_PLACE_country_of_origin
        ORDER BY occurrence_count DESC
        LIMIT 30
    """)
    return [{"id": row[0], "label": row[1], "count": row[2]} for row in cursor.fetchall()]


def generate_report(stats):
    """Generate markdown report from stats."""
    lines = []

    lines.append("# Wikidata Instance Properties - Analysis Report\n")
    lines.append(f"**Total Instances**: {stats['total_instances']:,}\n")
    lines.append(f"**Database**: `instance_properties.db`\n")
    lines.append("---\n")

    # Table of Contents
    lines.append("## Table of Contents\n")
    lines.append("1. [Overview](#overview)")
    lines.append("2. [Property Coverage](#property-coverage)")
    lines.append("3. [Instance Types (P31)](#instance-types-p31)")
    lines.append("4. [Date Distribution](#date-distribution)")
    lines.append("5. [Language Distribution](#language-distribution)")
    lines.append("6. [Country Distribution](#country-distribution)")
    lines.append("7. [Sitelinks Statistics](#sitelinks-statistics)")
    lines.append("8. [External Identifiers](#external-identifiers)")
    lines.append("9. [Property Details](#property-details)\n")

    # Overview
    lines.append("## Overview\n")
    lines.append("| Table | Records |")
    lines.append("|-------|---------|")
    for table, count in sorted(stats["table_counts"].items()):
        if not table.startswith("prop_"):
            lines.append(f"| {table} | {count:,} |")
    lines.append(f"\n**Property tables (prop_*)**: {len([t for t in stats['table_counts'] if t.startswith('prop_')])}\n")

    # Property Coverage
    lines.append("## Property Coverage\n")
    lines.append("| Property | Name | Category | Coverage | Unique Values |")
    lines.append("|----------|------|----------|----------|---------------|")

    # Sort by coverage descending
    sorted_props = sorted(stats["properties"], key=lambda x: x["coverage_percent"], reverse=True)
    for prop in sorted_props:
        lines.append(f"| {prop['property_id']} | {prop['property_name']} | {prop['category']} | {prop['coverage_percent']}% | {prop['unique_values']:,} |")
    lines.append("")

    # Instance Types
    lines.append("## Instance Types (P31)\n")
    lines.append("Top 30 instance types:\n")
    lines.append("| Rank | Type | Wikidata ID | Count |")
    lines.append("|------|------|-------------|-------|")
    for i, item in enumerate(stats["instance_of_distribution"][:30], 1):
        lines.append(f"| {i} | {item['label']} | {item['id']} | {item['count']:,} |")
    lines.append("")

    # Date Distribution
    lines.append("## Date Distribution\n")
    lines.append("Works by century (based on publication_date):\n")
    lines.append("| Century | Count |")
    lines.append("|---------|-------|")
    for item in stats["date_distribution"]:
        lines.append(f"| {item['century']} | {item['count']:,} |")
    lines.append("")

    # Language Distribution
    lines.append("## Language Distribution\n")
    lines.append("| Rank | Language | Wikidata ID | Count |")
    lines.append("|------|----------|-------------|-------|")
    for i, item in enumerate(stats["language_distribution"][:30], 1):
        lines.append(f"| {i} | {item['label']} | {item['id']} | {item['count']:,} |")
    lines.append("")

    # Country Distribution
    lines.append("## Country Distribution\n")
    lines.append("| Rank | Country | Wikidata ID | Count |")
    lines.append("|------|---------|-------------|-------|")
    for i, item in enumerate(stats["country_distribution"][:30], 1):
        lines.append(f"| {i} | {item['label']} | {item['id']} | {item['count']:,} |")
    lines.append("")

    # Sitelinks
    lines.append("## Sitelinks Statistics\n")
    sitelinks = stats["sitelinks"]
    lines.append(f"- **Total sitelinks**: {sitelinks['total_sitelinks']:,}")
    lines.append(f"- **Instances with sitelinks**: {sitelinks['instances_with_sitelinks']:,}\n")

    lines.append("### Top 20 Instances by Sitelink Count\n")
    lines.append("| Rank | Instance | Wikidata ID | Sitelinks |")
    lines.append("|------|----------|-------------|-----------|")
    for i, item in enumerate(sitelinks["top_instances"][:20], 1):
        lines.append(f"| {i} | {item['label']} | {item['id']} | {item['count']} |")
    lines.append("")

    lines.append("### Sitelinks by Type\n")
    lines.append("| Type | Count |")
    lines.append("|------|-------|")
    for item in sitelinks["by_type"][:20]:
        lines.append(f"| {item['type']} | {item['count']:,} |")
    lines.append("")

    # Identifiers
    lines.append("## External Identifiers\n")
    identifiers = stats["identifiers"]
    lines.append(f"- **Total identifiers**: {identifiers['total_identifiers']:,}")
    lines.append(f"- **Instances with identifiers**: {identifiers['instances_with_identifiers']:,}\n")

    lines.append("### Top 30 Identifier Types\n")
    lines.append("| Rank | Property | Name | Count |")
    lines.append("|------|----------|------|-------|")
    for i, item in enumerate(identifiers["by_type"][:30], 1):
        lines.append(f"| {i} | {item['property']} | {item['label']} | {item['count']:,} |")
    lines.append("")

    # Property Details
    lines.append("## Property Details\n")

    # Group by category
    categories = defaultdict(list)
    for prop in stats["properties"]:
        categories[prop["category"]].append(prop)

    for category in ["type", "date", "place", "content", "creator", "relationship"]:
        if category in categories:
            lines.append(f"### {category.title()} Properties\n")
            for prop in categories[category]:
                lines.append(f"#### {prop['property_name']} ({prop['property_id']})\n")
                lines.append(f"- **Column**: `{prop['column_name']}`")
                lines.append(f"- **Coverage**: {prop['coverage_percent']}% ({prop['instances_with_value']:,} instances)")
                lines.append(f"- **Unique values**: {prop['unique_values']:,}\n")

                if prop["top_values"]:
                    lines.append("**Top 10 values:**\n")
                    lines.append("| Value | Count |")
                    lines.append("|-------|-------|")
                    for val in prop["top_values"][:10]:
                        if "label" in val:
                            lines.append(f"| {val['label']} ({val['id']}) | {val['count']:,} |")
                        else:
                            lines.append(f"| {val['value']} | {val['count']:,} |")
                    lines.append("")

    return "\n".join(lines)


def main():
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stats = {}

    # Total instances
    cursor.execute("SELECT COUNT(*) FROM instances_properties")
    stats["total_instances"] = cursor.fetchone()[0]
    print(f"Total instances: {stats['total_instances']:,}")

    # Table counts
    print("\nGathering table statistics...")
    stats["table_counts"] = get_table_counts(cursor)

    # Property stats
    print("\nAnalyzing properties...")
    stats["properties"] = get_property_stats(cursor)

    # Sitelinks
    print("\nAnalyzing sitelinks...")
    stats["sitelinks"] = get_sitelink_stats(cursor)

    # Identifiers
    print("\nAnalyzing identifiers...")
    stats["identifiers"] = get_identifier_stats(cursor)

    # Date distribution
    print("\nAnalyzing date distribution...")
    stats["date_distribution"] = get_date_distribution(cursor)

    # Instance of distribution
    print("\nAnalyzing instance types...")
    stats["instance_of_distribution"] = get_instance_of_distribution(cursor)

    # Language distribution
    print("\nAnalyzing languages...")
    stats["language_distribution"] = get_language_distribution(cursor)

    # Country distribution
    print("\nAnalyzing countries...")
    stats["country_distribution"] = get_country_distribution(cursor)

    conn.close()

    # Save stats JSON
    print(f"\nSaving stats to {STATS_PATH}...")
    with open(STATS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    # Generate report
    print(f"Generating report: {REPORT_PATH}...")
    report = generate_report(stats)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print("\nDone!")
    print(f"  - Stats: {STATS_PATH}")
    print(f"  - Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
