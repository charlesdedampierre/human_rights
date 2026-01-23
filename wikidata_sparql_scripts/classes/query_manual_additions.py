"""
Script to query subclasses of items that don't fit standard written work hierarchies.
These are physical objects used for writing that belong to different Wikidata categories.

Manually identified items:
- stele → subclass of monument
- parchment → subclass of animal product, hide
- oracle bone → subclass of bone
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Manually identified root classes for physical writing media
MANUAL_ROOT_CLASSES = {
    "Q4989906": "monument",        # parent of stele
    "Q629103": "animal product",   # parent of parchment
    "Q265868": "bone",             # parent of oracle bone
}

# Specific items we want to ensure are captured
SPECIFIC_ITEMS = {
    "Q178743": "stele",
    "Q226697": "parchment",
    "Q283127": "oracle bone",
}

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def log(msg):
    print(msg, flush=True)


def run_sparql_query(query: str, timeout: int = 300, max_retries: int = 5) -> dict:
    """Execute a SPARQL query with retry logic."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataResearchBot/1.0 (Academic research)",
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(
                WIKIDATA_SPARQL_ENDPOINT,
                params={"query": query},
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            wait_time = 2 ** attempt * 5
            log(f"   Timeout, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                wait_time = 60
                log(f"   Rate limited, waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            wait_time = 2 ** attempt * 5
            log(f"   Error: {e}, waiting {wait_time}s")
            time.sleep(wait_time)

    raise Exception(f"Failed after {max_retries} retries")


def get_subclasses(qid: str) -> list:
    """Get all subclasses (transitive) of a class."""
    query = f"""
    SELECT DISTINCT ?subclass ?subclassLabel WHERE {{
      ?subclass wdt:P279+ wd:{qid} .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """
    results = run_sparql_query(query, timeout=300)
    return results["results"]["bindings"]


def count_instances(qid: str) -> int:
    """Count direct instances of a class."""
    query = f"""
    SELECT (COUNT(?item) AS ?count) WHERE {{
      ?item wdt:P31 wd:{qid} .
    }}
    """
    try:
        results = run_sparql_query(query, timeout=60)
        if results["results"]["bindings"]:
            return int(results["results"]["bindings"][0]["count"]["value"])
    except:
        pass
    return -1


def main():
    start_time = datetime.now()
    log("=" * 75)
    log("MANUAL ADDITIONS - PHYSICAL WRITING MEDIA")
    log("(Items that don't fit standard written work hierarchies)")
    log(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 75)

    all_items = {}

    # Query each root class
    for root_qid, root_label in MANUAL_ROOT_CLASSES.items():
        log(f"\n--- Querying subclasses of '{root_label}' ({root_qid}) ---")

        try:
            subclasses = get_subclasses(root_qid)
            log(f"   Found {len(subclasses)} subclasses")

            # Add root
            if root_qid not in all_items:
                all_items[root_qid] = {
                    "qid": root_qid,
                    "label": root_label,
                    "parent_hierarchy": root_label
                }

            # Add subclasses
            for item in subclasses:
                qid = item["subclass"]["value"].split("/")[-1]
                label = item["subclassLabel"]["value"]
                if qid not in all_items:
                    all_items[qid] = {
                        "qid": qid,
                        "label": label,
                        "parent_hierarchy": root_label
                    }

            time.sleep(1)

        except Exception as e:
            log(f"   Error: {e}")

    # Ensure specific items are included
    for qid, label in SPECIFIC_ITEMS.items():
        if qid not in all_items:
            all_items[qid] = {
                "qid": qid,
                "label": label,
                "parent_hierarchy": "specific_item"
            }

    log(f"\nTotal unique classes: {len(all_items)}")

    # Count instances
    log("\nCounting instances...")
    items_list = list(all_items.values())

    for i, item in enumerate(items_list):
        try:
            item["instance_count"] = count_instances(item["qid"])
            time.sleep(0.2)
            if (i + 1) % 50 == 0:
                log(f"   [{i + 1}/{len(items_list)}] processed")
        except:
            item["instance_count"] = -1

    # Sort by count
    items_list.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    # Save results
    output_file = OUTPUT_DIR / "manual_additions_subclasses.json"

    results = {
        "metadata": {
            "description": "Physical writing media from non-standard hierarchies (manually identified)",
            "root_classes": MANUAL_ROOT_CLASSES,
            "specific_items": SPECIFIC_ITEMS,
            "total_classes": len(items_list),
            "total_instances": sum(x.get("instance_count", 0) for x in items_list if x.get("instance_count", 0) > 0),
            "last_updated": datetime.now().isoformat()
        },
        "subclasses": items_list
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    log("\n" + "=" * 75)
    log("TOP 30 BY INSTANCE COUNT")
    log("=" * 75)
    log(f"\n{'Label':<40} {'QID':<12} {'Hierarchy':<15} {'Count':>10}")
    log("-" * 80)
    for item in items_list[:30]:
        count = item.get("instance_count", 0)
        count_str = f"{count:,}" if count >= 0 else "error"
        log(f"{item['label'][:38]:<40} {item['qid']:<12} {item['parent_hierarchy'][:13]:<15} {count_str:>10}")

    # Show the specific items we care about
    log("\n" + "-" * 75)
    log("SPECIFIC ITEMS OF INTEREST")
    log("-" * 75)
    for qid, label in SPECIFIC_ITEMS.items():
        item = all_items.get(qid, {})
        count = item.get("instance_count", "not found")
        log(f"   {label}: {count:,} instances" if isinstance(count, int) else f"   {label}: {count}")

    log(f"\nSaved to: {output_file}")
    log(f"Completed in {datetime.now() - start_time}")


if __name__ == "__main__":
    main()
