"""
Extract all instances from pre-1900 classes.

This script:
1. Loads the deduplicated pre-1900 classes from classes/output/all_pre1900_unified.json
2. For each class, queries Wikidata for all instances (items with P31 = class QID)
3. Saves results incrementally with progress tracking

Note: This is a long-running process (~2,662 classes).
Progress is saved every 10 classes and can be resumed if interrupted.
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Paths
SCRIPT_DIR = Path(__file__).parent
CLASSES_OUTPUT = SCRIPT_DIR.parent / "classes" / "output"
INPUT_FILE = CLASSES_OUTPUT / "all_pre1900_unified.json"

OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

PROGRESS_FILE = OUTPUT_DIR / "extraction_progress.json"
INSTANCES_FILE = OUTPUT_DIR / "all_pre1900_instances.json"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_sparql_query(query: str, timeout: int = 120, max_retries: int = 5) -> dict:
    """Execute a SPARQL query with retry logic."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataResearchBot/1.0 (Academic research on pre-1900 texts)",
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
            log(f"   Timeout, waiting {wait_time}s (retry {attempt + 1}/{max_retries})")
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

    return None  # Failed after retries


def get_instances(class_qid: str, limit: int = 50000) -> list:
    """Get all instances of a class (items where P31 = class_qid)."""
    query = f"""
    SELECT ?item ?itemLabel WHERE {{
      ?item wdt:P31 wd:{class_qid} .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    LIMIT {limit}
    """

    results = run_sparql_query(query, timeout=180)

    if results is None:
        return None  # Query failed

    instances = []
    for binding in results.get("results", {}).get("bindings", []):
        item_uri = binding.get("item", {}).get("value", "")
        item_qid = item_uri.split("/")[-1] if item_uri else ""
        item_label = binding.get("itemLabel", {}).get("value", "")

        if item_qid:
            instances.append({
                "qid": item_qid,
                "label": item_label
            })

    return instances


def load_progress() -> dict:
    """Load extraction progress."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_classes": [], "instances_by_class": {}}


def save_progress(progress: dict):
    """Save extraction progress."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def save_instances(progress: dict, classes: list):
    """Save all instances to final output file."""
    # Flatten all instances
    all_instances = {}
    class_lookup = {c["qid"]: c["label"] for c in classes}

    for class_qid, instances in progress["instances_by_class"].items():
        for inst in instances:
            inst_qid = inst["qid"]
            if inst_qid not in all_instances:
                all_instances[inst_qid] = {
                    "qid": inst_qid,
                    "label": inst["label"],
                    "classes": []
                }
            all_instances[inst_qid]["classes"].append({
                "qid": class_qid,
                "label": class_lookup.get(class_qid, "")
            })

    # Convert to list
    instances_list = list(all_instances.values())

    result = {
        "metadata": {
            "description": "All instances of pre-1900 classes from Wikidata",
            "total_unique_instances": len(instances_list),
            "classes_processed": len(progress["completed_classes"]),
            "last_updated": datetime.now().isoformat()
        },
        "instances": instances_list
    }

    with open(INSTANCES_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return len(instances_list)


def main():
    log("=" * 70)
    log("EXTRACT PRE-1900 INSTANCES FROM WIKIDATA")
    log("=" * 70)

    # Load pre-1900 classes
    log(f"\nLoading classes from: {INPUT_FILE}")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    classes = data["classes"]
    log(f"Total classes: {len(classes)}")

    # Load progress
    progress = load_progress()
    completed = set(progress["completed_classes"])
    log(f"Already completed: {len(completed)} classes")

    # Filter to remaining classes
    remaining = [c for c in classes if c["qid"] not in completed]
    log(f"Remaining to process: {len(remaining)} classes")

    if not remaining:
        log("\nAll classes already processed!")
        total = save_instances(progress, classes)
        log(f"Total unique instances: {total:,}")
        return

    # Process each class
    start_time = datetime.now()
    total_instances = sum(len(v) for v in progress["instances_by_class"].values())

    for i, cls in enumerate(remaining):
        class_qid = cls["qid"]
        class_label = cls["label"]
        expected_count = cls.get("instance_count", 0)

        log(f"\n[{len(completed) + 1}/{len(classes)}] {class_label} ({class_qid}) - expected: {expected_count:,}")

        # Query instances
        instances = get_instances(class_qid)

        if instances is None:
            log(f"   FAILED - skipping")
            continue

        log(f"   Retrieved: {len(instances):,} instances")

        # Save to progress
        progress["completed_classes"].append(class_qid)
        progress["instances_by_class"][class_qid] = instances
        completed.add(class_qid)
        total_instances += len(instances)

        # Save progress every 10 classes
        if (len(completed)) % 10 == 0:
            save_progress(progress)
            unique_count = save_instances(progress, classes)

            elapsed = (datetime.now() - start_time).total_seconds()
            rate = len(completed) / elapsed if elapsed > 0 else 0
            remaining_time = (len(classes) - len(completed)) / rate / 3600 if rate > 0 else 0

            log(f"   Progress saved. Unique instances so far: {unique_count:,} | ETA: {remaining_time:.1f}h")

        # Rate limiting
        time.sleep(0.3)

    # Final save
    save_progress(progress)
    unique_count = save_instances(progress, classes)

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)
    log(f"Classes processed: {len(completed)}")
    log(f"Total unique instances: {unique_count:,}")
    log(f"Saved to: {INSTANCES_FILE}")


if __name__ == "__main__":
    main()
