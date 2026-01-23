"""
Script to find parent classes of specific items that we want to capture.
This helps identify which Wikidata hierarchies contain physical writing media.
"""

import time
import requests

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Items from the screenshot that were in red (physical media, not content)
TARGET_ITEMS = {
    "Q571": "book",
    "Q178743": "stele",
    "Q226697": "parchment",
    "Q283127": "oracle bone",
    "Q905725": "bamboo and wooden slips",
    "Q16355570": "scroll",
    "Q125576": "papyrus",
    "Q1570005": "clay tablet",
    "Q1428312": "wax tablet",
    "Q16744570": "tablet",
    "Q3327760": "writing surface",
    "Q87167": "manuscript",
    "Q1640824": "inscription",
    "Q48498": "illuminated manuscript",
    "Q213924": "codex",
}


def run_sparql_query(query: str, timeout: int = 60) -> dict:
    """Execute a SPARQL query."""
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


def get_parent_classes(qid: str, depth: int = 3) -> list:
    """Get parent classes up to N levels."""
    query = f"""
    SELECT ?parent ?parentLabel ?level WHERE {{
      VALUES ?level {{ 1 2 3 }}
      {{
        SELECT ?parent (1 AS ?level) WHERE {{
          wd:{qid} wdt:P279 ?parent .
        }}
      }} UNION {{
        SELECT ?parent (2 AS ?level) WHERE {{
          wd:{qid} wdt:P279/wdt:P279 ?parent .
        }}
      }} UNION {{
        SELECT ?parent (3 AS ?level) WHERE {{
          wd:{qid} wdt:P279/wdt:P279/wdt:P279 ?parent .
        }}
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    ORDER BY ?level
    """
    results = run_sparql_query(query, timeout=60)
    return results["results"]["bindings"]


def main():
    print("=" * 80)
    print("FINDING PARENT CLASSES OF PHYSICAL WRITING MEDIA")
    print("=" * 80)

    all_parents = {}

    for qid, label in TARGET_ITEMS.items():
        print(f"\n--- {label} ({qid}) ---")
        try:
            parents = get_parent_classes(qid)
            for p in parents:
                parent_qid = p["parent"]["value"].split("/")[-1]
                parent_label = p["parentLabel"]["value"]
                level = p["level"]["value"]
                print(f"   Level {level}: {parent_label} ({parent_qid})")

                # Track all unique parents
                if parent_qid not in all_parents:
                    all_parents[parent_qid] = {"label": parent_label, "children": []}
                all_parents[parent_qid]["children"].append(label)

            time.sleep(0.3)
        except Exception as e:
            print(f"   Error: {e}")

    # Summary: which parent classes appear most often?
    print("\n" + "=" * 80)
    print("COMMON PARENT CLASSES (potential root classes to query)")
    print("=" * 80)

    # Sort by number of children
    sorted_parents = sorted(
        all_parents.items(), key=lambda x: len(x[1]["children"]), reverse=True
    )

    print(f"\n{'Parent Class':<40} {'QID':<12} {'# Items':<8} Children")
    print("-" * 100)
    for qid, data in sorted_parents[:30]:
        children_str = ", ".join(data["children"][:5])
        if len(data["children"]) > 5:
            children_str += f"... (+{len(data['children'])-5})"
        print(
            f"{data['label'][:38]:<40} {qid:<12} {len(data['children']):<8} {children_str}"
        )


if __name__ == "__main__":
    main()
