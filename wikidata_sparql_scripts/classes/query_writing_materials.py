"""
Script to query all subclasses of writing material/writing surface from Wikidata.

This helps identify physical media (papyrus, parchment, tablets, etc.)
that are distinct from written works (intellectual content).

Usage:
    python query_writing_materials.py
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Key Wikidata entities for writing materials/surfaces
WRITING_MATERIAL_QIDS = {
    "Q3327760": "writing surface",
    "Q11396020": "writing material",
}

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def log(msg):
    """Print with flush for real-time output."""
    print(msg, flush=True)


def run_sparql_query(query: str, timeout: int = 120, max_retries: int = 5) -> dict:
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
            else:
                raise
        except Exception as e:
            wait_time = 2**attempt * 5
            log(
                f"   Error: {e}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
            )
            time.sleep(wait_time)

    raise Exception(f"Failed after {max_retries} retries")


def get_all_subclasses(qid: str) -> list:
    """Get all subclasses (transitive) of a class."""
    query = f"""
    SELECT DISTINCT ?subclass ?subclassLabel WHERE {{
      ?subclass wdt:P279+ wd:{qid} .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """
    results = run_sparql_query(query, timeout=300)
    return results["results"]["bindings"]


def get_parent_classes(qid: str) -> list:
    """Get direct parent classes (P279) of an entity."""
    query = f"""
    SELECT ?parent ?parentLabel WHERE {{
      wd:{qid} wdt:P279 ?parent .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """
    results = run_sparql_query(query, timeout=60)
    return results["results"]["bindings"]


def count_instances(qid: str) -> int:
    """Count direct instances of a class."""
    query = f"""
    SELECT (COUNT(?item) AS ?count) WHERE {{
      ?item wdt:P31 wd:{qid} .
    }}
    """
    results = run_sparql_query(query, timeout=60)
    if results["results"]["bindings"]:
        return int(results["results"]["bindings"][0]["count"]["value"])
    return 0


def main():
    start_time = datetime.now()
    log("=" * 75)
    log("WRITING MATERIALS - SUBCLASS ANALYSIS")
    log(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 75)

    all_materials = {}

    # Query subclasses for each root writing material class
    for root_qid, root_label in WRITING_MATERIAL_QIDS.items():
        log(f"\n--- Querying subclasses of '{root_label}' ({root_qid}) ---")

        # Add the root itself
        all_materials[root_qid] = {
            "qid": root_qid,
            "label": root_label,
            "parent_root": root_qid,
            "parent_root_label": root_label,
        }

        # Get all subclasses
        subclasses = get_all_subclasses(root_qid)
        log(f"   Found {len(subclasses)} subclasses")

        for item in subclasses:
            qid = item["subclass"]["value"].split("/")[-1]
            label = item["subclassLabel"]["value"]

            if qid not in all_materials:
                all_materials[qid] = {
                    "qid": qid,
                    "label": label,
                    "parent_root": root_qid,
                    "parent_root_label": root_label,
                }

    # Deduplicate and convert to list
    materials_list = list(all_materials.values())
    log(f"\nTotal unique writing material classes: {len(materials_list)}")

    # Count instances for each (with progress)
    log("\nCounting instances for each class...")
    for i, material in enumerate(materials_list):
        try:
            count = count_instances(material["qid"])
            material["instance_count"] = count
            time.sleep(0.2)  # Be nice to Wikidata

            if (i + 1) % 20 == 0:
                log(f"   [{i + 1}/{len(materials_list)}] processed")

        except Exception as e:
            log(f"   Error counting {material['label']}: {e}")
            material["instance_count"] = -1

    # Sort by instance count
    materials_list.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    # Save results
    output_file = OUTPUT_DIR / "writing_materials_subclasses.json"

    results = {
        "metadata": {
            "description": "All subclasses of writing material/writing surface from Wikidata",
            "root_classes": WRITING_MATERIAL_QIDS,
            "total_classes": len(materials_list),
            "total_instances": sum(
                m.get("instance_count", 0)
                for m in materials_list
                if m.get("instance_count", 0) > 0
            ),
            "last_updated": datetime.now().isoformat(),
        },
        "subclasses": materials_list,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    log("\n" + "=" * 75)
    log("RESULTS")
    log("=" * 75)
    log(f"\nTotal writing material classes: {len(materials_list)}")
    log(f"Total instances: {results['metadata']['total_instances']:,}")
    log(f"\nSaved to: {output_file}")

    # Show top items
    log("\n" + "-" * 75)
    log("TOP 20 BY INSTANCE COUNT")
    log("-" * 75)
    log(f"\n{'Label':<40} {'QID':<12} {'Count':>12}")
    log("-" * 65)
    for m in materials_list[:20]:
        count = m.get("instance_count", 0)
        count_str = f"{count:,}" if count >= 0 else "error"
        log(f"{m['label'][:38]:<40} {m['qid']:<12} {count_str:>12}")

    duration = datetime.now() - start_time
    log(f"\nCompleted in {duration}")


if __name__ == "__main__":
    main()
