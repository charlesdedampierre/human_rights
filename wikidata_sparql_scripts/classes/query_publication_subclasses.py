"""
Script to query all subclasses of publication (Q732577) from Wikidata.
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
PUBLICATION_QID = "Q732577"

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
    log("PUBLICATION SUBCLASSES")
    log(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 75)

    # Get all subclasses
    log("\nFetching all subclasses of 'publication' (Q732577)...")
    subclasses = get_subclasses(PUBLICATION_QID)
    log(f"Found {len(subclasses)} subclasses")

    # Build list including the root
    items = [{"qid": PUBLICATION_QID, "label": "publication"}]
    for item in subclasses:
        qid = item["subclass"]["value"].split("/")[-1]
        label = item["subclassLabel"]["value"]
        items.append({"qid": qid, "label": label})

    # Count instances
    log("\nCounting instances...")
    for i, item in enumerate(items):
        try:
            item["instance_count"] = count_instances(item["qid"])
            time.sleep(0.2)
            if (i + 1) % 50 == 0:
                log(f"   [{i + 1}/{len(items)}] processed")
        except Exception as e:
            item["instance_count"] = -1

    # Sort by count
    items.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    # Save
    output_file = OUTPUT_DIR / "publication_subclasses.json"
    results = {
        "metadata": {
            "root_class": {"qid": PUBLICATION_QID, "label": "publication"},
            "total_subclasses": len(items),
            "total_instances": sum(
                x.get("instance_count", 0)
                for x in items
                if x.get("instance_count", 0) > 0
            ),
            "last_updated": datetime.now().isoformat(),
        },
        "subclasses": items,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Show top results
    log("\n" + "=" * 75)
    log("TOP 50 PUBLICATION SUBCLASSES BY INSTANCE COUNT")
    log("=" * 75)
    log(f"\n{'Label':<50} {'QID':<12} {'Count':>12}")
    log("-" * 75)
    for item in items[:50]:
        count = item.get("instance_count", 0)
        count_str = f"{count:,}" if count >= 0 else "error"
        log(f"{item['label'][:48]:<50} {item['qid']:<12} {count_str:>12}")

    log(f"\nSaved to: {output_file}")
    log(f"Completed in {datetime.now() - start_time}")


if __name__ == "__main__":
    main()
