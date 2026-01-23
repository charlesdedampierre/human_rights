"""
Query instance counts for specific physical writing media items.
These are items that don't fit the standard "written work" hierarchy.
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Specific items to query (manually identified)
SPECIFIC_ITEMS = {
    # Physical media not in written work hierarchy
    "Q178743": "stele",
    "Q226697": "parchment",
    "Q283127": "oracle bone",
    # Inscription-related
    "Q1640824": "inscription",
    "Q669777": "epigraph",
    "Q1430557": "curse tablet",
    "Q28779871": "rock inscription",
    "Q2672128": "epigraphy",
}

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def run_sparql_query(query: str, timeout: int = 60) -> dict:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataResearchBot/1.0 (Academic research)",
    }
    response = requests.get(
        WIKIDATA_SPARQL_ENDPOINT,
        params={"query": query},
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def count_instances(qid: str) -> int:
    """Count direct instances of a class."""
    query = f"""
    SELECT (COUNT(?item) AS ?count) WHERE {{
      ?item wdt:P31 wd:{qid} .
    }}
    """
    results = run_sparql_query(query)
    if results["results"]["bindings"]:
        return int(results["results"]["bindings"][0]["count"]["value"])
    return 0


def count_total_instances(qid: str) -> int:
    """Count all instances including subclasses."""
    query = f"""
    SELECT (COUNT(?item) AS ?count) WHERE {{
      ?item wdt:P31/wdt:P279* wd:{qid} .
    }}
    """
    results = run_sparql_query(query, timeout=120)
    if results["results"]["bindings"]:
        return int(results["results"]["bindings"][0]["count"]["value"])
    return 0


def main():
    print("=" * 70)
    print("SPECIFIC PHYSICAL WRITING MEDIA - INSTANCE COUNTS")
    print("=" * 70)

    results = []

    print(f"\n{'Item':<25} {'QID':<12} {'Direct':>12} {'Total':>12}")
    print("-" * 65)

    for qid, label in SPECIFIC_ITEMS.items():
        try:
            direct = count_instances(qid)
            total = count_total_instances(qid)

            print(f"{label:<25} {qid:<12} {direct:>12,} {total:>12,}")

            results.append(
                {
                    "qid": qid,
                    "label": label,
                    "direct_instance_count": direct,
                    "total_instance_count": total,
                }
            )

            time.sleep(0.3)
        except Exception as e:
            print(f"{label:<25} {qid:<12} ERROR: {e}")
            results.append({"qid": qid, "label": label, "error": str(e)})

    # Save results
    output_file = OUTPUT_DIR / "specific_items.json"

    data = {
        "metadata": {
            "description": "Manually identified physical writing media (not in written work hierarchy)",
            "last_updated": datetime.now().isoformat(),
        },
        "items": results,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()
