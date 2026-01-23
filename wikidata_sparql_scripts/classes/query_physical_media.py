"""
Script to query all physical media that can contain written works.
Queries multiple Wikidata hierarchies and merges the results.

These are physical containers/carriers of writing (not the intellectual content itself).
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Multiple root classes that contain physical writing media
ROOT_CLASSES = {
    # Document/Publication hierarchy
    "Q49848": "document",
    "Q732577": "publication",
    # Archaeological/Physical objects
    "Q220659": "archaeological artefact",
    "Q4989906": "monument",
    # Communication media
    "Q340169": "communications media",
    # Writing-specific
    "Q3327760": "writing surface",
    "Q121916": "writing implement",
    # Manuscript types (subset of written work but physical)
    "Q87167": "manuscript",
}

# Specific items to include that don't fit hierarchies well
SPECIFIC_ITEMS = {
    "Q226697": "parchment",
    "Q283127": "oracle bone",
    "Q16355570": "scroll",
    "Q571": "book",
    "Q178743": "stele",
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
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            wait_time = 2**attempt * 5
            log(
                f"   Timeout, waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
            )
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                wait_time = 60
                log(f"   Rate limited, waiting {wait_time}s")
                time.sleep(wait_time)
            elif "502" in str(e) or "503" in str(e) or "504" in str(e):
                wait_time = 2**attempt * 10
                log(f"   Server error, waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            wait_time = 2**attempt * 5
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


def get_item_label(qid: str) -> str:
    """Get the label for a specific item."""
    query = f"""
    SELECT ?label WHERE {{
      wd:{qid} rdfs:label ?label .
      FILTER(LANG(?label) = "en")
    }}
    """
    try:
        results = run_sparql_query(query, timeout=30)
        if results["results"]["bindings"]:
            return results["results"]["bindings"][0]["label"]["value"]
    except:
        pass
    return qid


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


def is_also_subclass_of_written_work(qid: str) -> bool:
    """Check if this class is also a subclass of written work (Q47461344)."""
    query = f"""
    ASK {{
      wd:{qid} wdt:P279+ wd:Q47461344 .
    }}
    """
    try:
        results = run_sparql_query(query, timeout=30)
        return results.get("boolean", False)
    except:
        return False


def main():
    start_time = datetime.now()
    log("=" * 80)
    log("PHYSICAL WRITING MEDIA - COMPREHENSIVE QUERY")
    log(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 80)

    # Collect all items from all hierarchies
    all_items = {}  # qid -> item data
    hierarchy_membership = defaultdict(list)  # qid -> list of parent hierarchies

    # Query each root class
    for root_qid, root_label in ROOT_CLASSES.items():
        log(f"\n--- Querying subclasses of '{root_label}' ({root_qid}) ---")

        try:
            subclasses = get_subclasses(root_qid)
            log(f"   Found {len(subclasses)} subclasses")

            # Add the root itself
            if root_qid not in all_items:
                all_items[root_qid] = {
                    "qid": root_qid,
                    "label": root_label,
                }
            hierarchy_membership[root_qid].append(root_label)

            # Add all subclasses
            for item in subclasses:
                qid = item["subclass"]["value"].split("/")[-1]
                label = item["subclassLabel"]["value"]

                if qid not in all_items:
                    all_items[qid] = {
                        "qid": qid,
                        "label": label,
                    }
                hierarchy_membership[qid].append(root_label)

            time.sleep(1)  # Be nice to Wikidata between large queries

        except Exception as e:
            log(f"   Error querying {root_label}: {e}")

    # Add specific items that might be missed
    log(f"\n--- Adding specific items ---")
    for qid, label in SPECIFIC_ITEMS.items():
        if qid not in all_items:
            all_items[qid] = {
                "qid": qid,
                "label": label,
            }
            hierarchy_membership[qid].append("specific_item")
            log(f"   Added: {label} ({qid})")

    log(f"\nTotal unique classes found: {len(all_items)}")

    # Add hierarchy membership to items
    for qid in all_items:
        all_items[qid]["member_of_hierarchies"] = hierarchy_membership[qid]

    # Count instances for each item (with progress)
    log("\nCounting instances (this may take a while)...")
    items_list = list(all_items.values())

    for i, item in enumerate(items_list):
        try:
            count = count_instances(item["qid"])
            item["instance_count"] = count
            time.sleep(0.2)

            if (i + 1) % 100 == 0:
                log(f"   [{i + 1}/{len(items_list)}] processed")

        except Exception as e:
            log(f"   Error counting {item['label']}: {e}")
            item["instance_count"] = -1

    # Check which items are also subclasses of written work
    log("\nChecking overlap with 'written work' hierarchy...")
    for i, item in enumerate(items_list):
        if (i + 1) % 200 == 0:
            log(f"   [{i + 1}/{len(items_list)}] checked")
        try:
            item["is_subclass_of_written_work"] = is_also_subclass_of_written_work(
                item["qid"]
            )
            time.sleep(0.1)
        except:
            item["is_subclass_of_written_work"] = None

    # Sort by instance count
    items_list.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    # Separate into categories
    also_written_work = [
        x for x in items_list if x.get("is_subclass_of_written_work") == True
    ]
    physical_only = [
        x for x in items_list if x.get("is_subclass_of_written_work") == False
    ]
    unknown = [x for x in items_list if x.get("is_subclass_of_written_work") is None]

    # Save results
    output_file = OUTPUT_DIR / "physical_writing_media.json"

    results = {
        "metadata": {
            "description": "Physical media that can contain written works (containers, not content)",
            "root_classes_queried": ROOT_CLASSES,
            "specific_items_added": SPECIFIC_ITEMS,
            "total_classes": len(items_list),
            "classes_also_subclass_of_written_work": len(also_written_work),
            "classes_physical_only": len(physical_only),
            "total_instances": sum(
                x.get("instance_count", 0)
                for x in items_list
                if x.get("instance_count", 0) > 0
            ),
            "last_updated": datetime.now().isoformat(),
        },
        "all_classes": items_list,
        "also_written_work": also_written_work,
        "physical_only": physical_only,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    log("\n" + "=" * 80)
    log("RESULTS")
    log("=" * 80)
    log(f"\nTotal classes found: {len(items_list)}")
    log(f"  - Also subclass of 'written work': {len(also_written_work)}")
    log(f"  - Physical only (not written work): {len(physical_only)}")
    log(f"  - Unknown: {len(unknown)}")
    log(f"\nTotal instances: {results['metadata']['total_instances']:,}")
    log(f"\nSaved to: {output_file}")

    # Show top physical-only items (the ones to potentially exclude from written work analysis)
    log("\n" + "-" * 80)
    log("TOP 30 PHYSICAL-ONLY CLASSES (not subclass of written work)")
    log("These are physical containers, NOT intellectual content")
    log("-" * 80)
    log(f"\n{'Label':<45} {'QID':<12} {'Count':>10} Hierarchies")
    log("-" * 90)
    for item in physical_only[:30]:
        count = item.get("instance_count", 0)
        count_str = f"{count:,}" if count >= 0 else "error"
        hierarchies = ", ".join(item.get("member_of_hierarchies", [])[:2])
        log(f"{item['label'][:43]:<45} {item['qid']:<12} {count_str:>10} {hierarchies}")

    # Show items that are BOTH physical media AND written work (the confusing ones)
    log("\n" + "-" * 80)
    log("TOP 30 CLASSES THAT ARE BOTH PHYSICAL AND WRITTEN WORK")
    log("These cause the ontological confusion in Wikidata")
    log("-" * 80)
    log(f"\n{'Label':<45} {'QID':<12} {'Count':>10}")
    log("-" * 70)
    for item in also_written_work[:30]:
        count = item.get("instance_count", 0)
        count_str = f"{count:,}" if count >= 0 else "error"
        log(f"{item['label'][:43]:<45} {item['qid']:<12} {count_str:>10}")

    duration = datetime.now() - start_time
    log(f"\nCompleted in {duration}")


if __name__ == "__main__":
    main()
