"""
Analyze property occurrence across extracted Wikidata items.
Counts how many items have each property (presence/absence).
"""

import json
from collections import Counter
from pathlib import Path

DATA_FILE = "output/extracted/extracted_data.json"

# All property labels
LABELS = {
    # DATE
    "P577": "publication date",
    "P571": "inception",
    "P580": "start time",
    "P582": "end time",
    "P585": "point in time",
    "P1191": "date of first performance",
    "P1319": "earliest date",
    "P1326": "latest date",
    "P2031": "work period (start)",
    "P2032": "work period (end)",
    "P3893": "public domain date",
    "P1249": "time of earliest written record",
    # PLACE
    "P495": "country of origin",
    "P17": "country",
    "P291": "place of publication",
    "P840": "narrative location",
    "P131": "located in admin entity",
    "P276": "location",
    "P1001": "applies to jurisdiction",
    # TYPES
    "P31": "instance of",
    "P136": "genre",
    "P7937": "form of creative work",
    "P282": "writing system",
    "P2551": "used metre",
    "P407": "language of work",
    "P364": "original language",
    "P135": "movement",
    "P921": "main subject",
    # CONTENT
    "P953": "full work available at URL",
    "P1433": "published in",
    "P1343": "described by source",
    "P973": "described at URL",
    "P856": "official website",
    "P18": "image",
    "P996": "document file on Commons",
    "P1476": "title",
    "P1680": "subtitle",
    "P6216": "copyright status",
    # CREATORS
    "P50": "author",
    "P2093": "author name string",
    "P1779": "possible creator",
    "P98": "editor",
    "P655": "translator",
    "P170": "creator",
    "P123": "publisher",
    # RELATIONSHIPS
    "P361": "part of",
    "P144": "based on",
    "P179": "part of the series",
    "P155": "follows",
    "P156": "followed by",
}


# Property categories
CATEGORIES = {
    "date": ["P577", "P571", "P580", "P582", "P585", "P1191", "P1319", "P1326", "P2031", "P2032", "P3893", "P1249"],
    "place": ["P495", "P17", "P291", "P840", "P131", "P276", "P1001"],
    "types": ["P31", "P136", "P7937", "P282", "P2551", "P407", "P364", "P135", "P921"],
    "content": ["P953", "P1433", "P1343", "P973", "P856", "P18", "P996", "P1476", "P1680", "P6216"],
    "creators": ["P50", "P2093", "P1779", "P98", "P655", "P170", "P123"],
    "relationships": ["P361", "P144", "P179", "P155", "P156"],
}


def analyze():
    # Load data
    with open(DATA_FILE) as f:
        data = json.load(f)

    total_items = len(data)
    print(f"Total items: {total_items:,}\n")

    # Count property occurrences
    property_counts = Counter()

    # Category counts (items with ANY property in category)
    category_counts = {cat: 0 for cat in CATEGORIES}

    # Sitelinks by type
    sitelink_type_counts = Counter()
    has_sitelinks = 0

    # Identifiers by property
    identifier_counts = Counter()
    has_identifiers = 0

    for item_id, item in data.items():
        item_props = set(item.get("properties", {}).keys())

        # Count each property
        for prop_id in item_props:
            property_counts[prop_id] += 1

        # Count categories (if item has ANY property in that category)
        for cat, props in CATEGORIES.items():
            if item_props & set(props):
                category_counts[cat] += 1

        # Count sitelinks by type
        sitelinks = item.get("sitelinks", [])
        if sitelinks:
            has_sitelinks += 1
            types_seen = set()
            for sl in sitelinks:
                sl_type = sl.get("type", "other")
                types_seen.add(sl_type)
            for t in types_seen:
                sitelink_type_counts[t] += 1

        # Count identifiers by property
        identifiers = item.get("identifiers", [])
        if identifiers:
            has_identifiers += 1
            props_seen = set()
            for id_item in identifiers:
                prop = id_item.get("property")
                prop_label = id_item.get("property_label", prop)
                props_seen.add((prop, prop_label))
            for prop, label in props_seen:
                identifier_counts[(prop, label)] += 1

    # Print category summary
    print("=" * 70)
    print("CATEGORY SUMMARY (items with ANY property in category)")
    print("=" * 70)
    print(f"{'Category':<20} {'Count':>10} {'%':>10}")
    print("-" * 70)
    for cat in ["date", "place", "types", "content", "creators", "relationships"]:
        count = category_counts[cat]
        pct = count / total_items * 100
        print(f"{cat.upper():<20} {count:>10,} {pct:>9.1f}%")
    print("-" * 70)
    print(f"{'Sitelinks':<20} {has_sitelinks:>10,} {has_sitelinks/total_items*100:>9.1f}%")
    print(f"{'Identifiers':<20} {has_identifiers:>10,} {has_identifiers/total_items*100:>9.1f}%")

    # Print results
    print("\n" + "=" * 70)
    print("MAIN PROPERTIES (all)")
    print("=" * 70)
    print(f"{'Property':<10} {'Label':<35} {'Count':>10} {'%':>10}")
    print("-" * 70)

    for prop_id, count in property_counts.most_common():
        label = LABELS.get(prop_id, "")
        pct = count / total_items * 100
        print(f"{prop_id:<10} {label:<35} {count:>10,} {pct:>9.1f}%")

    print(f"\nTotal properties tracked: {len(property_counts)}")

    # Print properties by category
    print("\n" + "=" * 70)
    print("PROPERTIES BY CATEGORY")
    print("=" * 70)

    for cat_name in ["date", "place", "types", "content", "creators", "relationships"]:
        cat_props = CATEGORIES[cat_name]
        print(f"\n--- {cat_name.upper()} ---")
        for prop_id in cat_props:
            if prop_id in property_counts:
                count = property_counts[prop_id]
                pct = count / total_items * 100
                label = LABELS.get(prop_id, "")
                print(f"  {prop_id:<10} {label:<30} {count:>8,} {pct:>7.1f}%")

    print("\n" + "=" * 70)
    print("SITELINKS BY TYPE")
    print("=" * 70)
    print(f"{'Type':<30} {'Count':>10} {'%':>10}")
    print("-" * 70)

    for sl_type, count in sitelink_type_counts.most_common():
        pct = count / total_items * 100
        print(f"{sl_type:<30} {count:>10,} {pct:>9.1f}%")

    print("-" * 70)
    print(f"{'Items with any sitelink':<30} {has_sitelinks:>10,} {has_sitelinks/total_items*100:>9.1f}%")

    print("\n" + "=" * 70)
    print("TOP 20 IDENTIFIERS")
    print("=" * 70)
    print(f"{'Property':<10} {'Label':<35} {'Count':>10} {'%':>10}")
    print("-" * 70)

    for (prop, label), count in identifier_counts.most_common(20):
        pct = count / total_items * 100
        print(f"{prop:<10} {label:<35} {count:>10,} {pct:>9.1f}%")

    print("-" * 70)
    print(f"{'Items with any identifier':<46} {has_identifiers:>10,} {has_identifiers/total_items*100:>9.1f}%")
    print(f"Total identifier types: {len(identifier_counts)}")

    # Save to markdown file
    md_file = "output/analysis_report.md"
    with open(md_file, "w") as f:
        f.write(f"# Property Analysis Report\n\n")
        f.write(f"**Total items analyzed:** {total_items:,}\n\n")

        # Category summary
        f.write("## Category Summary\n\n")
        f.write("Items with ANY property in each category:\n\n")
        f.write("| Category | Count | % |\n")
        f.write("|----------|------:|--:|\n")
        for cat in ["date", "place", "types", "content", "creators", "relationships"]:
            count = category_counts[cat]
            pct = count / total_items * 100
            f.write(f"| {cat.upper()} | {count:,} | {pct:.1f}% |\n")
        f.write(f"| **Sitelinks** | {has_sitelinks:,} | {has_sitelinks/total_items*100:.1f}% |\n")
        f.write(f"| **Identifiers** | {has_identifiers:,} | {has_identifiers/total_items*100:.1f}% |\n")

        # Properties by category
        f.write("\n## Properties by Category\n")
        for cat_name in ["date", "place", "types", "content", "creators", "relationships"]:
            cat_props = CATEGORIES[cat_name]
            f.write(f"\n### {cat_name.upper()}\n\n")
            f.write("| Property | Label | Count | % |\n")
            f.write("|----------|-------|------:|--:|\n")
            for prop_id in cat_props:
                if prop_id in property_counts:
                    count = property_counts[prop_id]
                    pct = count / total_items * 100
                    label = LABELS.get(prop_id, "")
                    f.write(f"| {prop_id} | {label} | {count:,} | {pct:.1f}% |\n")

        # Sitelinks
        f.write("\n## Sitelinks by Type\n\n")
        f.write("| Type | Count | % |\n")
        f.write("|------|------:|--:|\n")
        for sl_type, count in sitelink_type_counts.most_common():
            pct = count / total_items * 100
            f.write(f"| {sl_type} | {count:,} | {pct:.1f}% |\n")
        f.write(f"| **Any sitelink** | {has_sitelinks:,} | {has_sitelinks/total_items*100:.1f}% |\n")

        # Top identifiers
        f.write(f"\n## Top 20 Identifiers ({len(identifier_counts)} types total)\n\n")
        f.write("| Property | Label | Count | % |\n")
        f.write("|----------|-------|------:|--:|\n")
        for (prop, label), count in identifier_counts.most_common(20):
            pct = count / total_items * 100
            f.write(f"| {prop} | {label} | {count:,} | {pct:.1f}% |\n")
        f.write(f"| **Any identifier** | | {has_identifiers:,} | {has_identifiers/total_items*100:.1f}% |\n")

    print(f"\n>>> Report saved to: {md_file}")


if __name__ == "__main__":
    analyze()
