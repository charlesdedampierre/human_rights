"""
Extract all instances from pre-1900 classes using parallel processing.

Features:
- Multiprocessing with ThreadPoolExecutor for concurrent API requests
- Batching for efficient processing
- tqdm progress bars for real-time monitoring
- Incremental saving - can resume from where it stopped
- Deduplication of classes before processing
- Automatic retry of failed extractions
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

# Configuration
MAX_WORKERS = 8  # Number of parallel workers
RETRY_WORKERS = 3  # Fewer workers for retries (gentler on API)
BATCH_SIZE = 50  # Save progress every N classes
RATE_LIMIT_DELAY = 0.2  # Delay between requests per worker
RETRY_DELAY = 1.0  # Longer delay for retries
MAX_RETRY_ROUNDS = 3  # Maximum retry rounds for failed classes
WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Paths
SCRIPT_DIR = Path(__file__).parent
CLASSES_OUTPUT = SCRIPT_DIR.parent / "classes" / "output"
INPUT_FILE = CLASSES_OUTPUT / "all_pre1900_unified.json"

OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

PROGRESS_FILE = OUTPUT_DIR / "extraction_progress_parallel.json"
FAILED_FILE = OUTPUT_DIR / "extraction_failed.json"
INSTANCES_DIR = OUTPUT_DIR / "instances_by_class"
INSTANCES_DIR.mkdir(exist_ok=True)

# Thread-safe lock for file operations
progress_lock = threading.Lock()
failed_lock = threading.Lock()


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_sparql_query(query: str, timeout: int = 180, max_retries: int = 5) -> dict:
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
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                wait_time = 60 + attempt * 30
                time.sleep(wait_time)
            elif "502" in str(e) or "503" in str(e) or "504" in str(e):
                wait_time = 2**attempt * 10
                time.sleep(wait_time)
            else:
                raise
        except Exception:
            wait_time = 2**attempt * 5
            time.sleep(wait_time)

    return None


def get_instances_for_class(class_qid: str, page_size: int = 50000) -> list:
    """Get all instances of a class (items where P31 = class_qid).

    Uses OFFSET-based pagination to handle classes with many instances.
    """
    all_instances = []
    offset = 0

    while True:
        query = f"""
        SELECT ?item WHERE {{
          ?item wdt:P31 wd:{class_qid} .
        }}
        LIMIT {page_size}
        OFFSET {offset}
        """

        results = run_sparql_query(query, timeout=300)

        if results is None:
            # If we already have some results, return them
            # Otherwise return None to indicate failure
            return all_instances if all_instances else None

        page_instances = []
        for binding in results.get("results", {}).get("bindings", []):
            item_uri = binding.get("item", {}).get("value", "")
            item_qid = item_uri.split("/")[-1] if item_uri else ""
            if item_qid:
                page_instances.append(item_qid)

        all_instances.extend(page_instances)

        # If we got fewer results than page_size, we've reached the end
        if len(page_instances) < page_size:
            break

        offset += page_size

        # Small delay between pages to be nice to the API
        time.sleep(0.5)

    return all_instances


def process_class(cls: dict, delay: float = RATE_LIMIT_DELAY) -> tuple:
    """Process a single class and return its instances."""
    class_qid = cls["qid"]

    # Add delay for rate limiting
    time.sleep(delay)

    instances = get_instances_for_class(class_qid)

    if instances is not None:
        # Save immediately to individual file
        instance_file = INSTANCES_DIR / f"{class_qid}.json"
        with open(instance_file, "w") as f:
            json.dump(instances, f)

    return class_qid, instances


def load_progress() -> tuple[set, set]:
    """Load sets of completed and failed class QIDs."""
    completed = set()
    failed = set()

    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            data = json.load(f)
            completed = set(data.get("completed_classes", []))

    if FAILED_FILE.exists():
        with open(FAILED_FILE, "r") as f:
            data = json.load(f)
            failed = set(data.get("failed_classes", []))

    return completed, failed


def save_progress(completed_classes: set):
    """Save completed progress to file."""
    with progress_lock:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({
                "completed_classes": list(completed_classes),
                "last_updated": datetime.now().isoformat()
            }, f)


def save_failed(failed_classes: set):
    """Save failed classes to file."""
    with failed_lock:
        with open(FAILED_FILE, "w") as f:
            json.dump({
                "failed_classes": list(failed_classes),
                "last_updated": datetime.now().isoformat()
            }, f)


def deduplicate_classes(classes: list) -> list:
    """Deduplicate classes by QID, keeping the first occurrence."""
    seen = set()
    deduplicated = []
    for cls in classes:
        qid = cls["qid"]
        if qid not in seen:
            seen.add(qid)
            deduplicated.append(cls)
    return deduplicated


def merge_all_instances():
    """Merge all individual instance files into one final file."""
    log("Merging all instance files...")

    all_instances = set()
    class_files = list(INSTANCES_DIR.glob("Q*.json"))

    for instance_file in tqdm(class_files, desc="Merging files"):
        with open(instance_file, "r") as f:
            instances = json.load(f)
            all_instances.update(instances)

    # Save merged result
    output_file = OUTPUT_DIR / "all_pre1900_instance_ids.json"
    result = {
        "metadata": {
            "description": "All unique instance QIDs from pre-1900 classes",
            "total_unique_instances": len(all_instances),
            "classes_processed": len(class_files),
            "last_updated": datetime.now().isoformat()
        },
        "instance_ids": sorted(all_instances)
    }

    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    log(f"Saved {len(all_instances):,} unique instance IDs to {output_file}")
    return len(all_instances)


def process_batch(classes_to_process: list, completed: set, failed: set,
                  workers: int, delay: float, desc: str) -> tuple[int, int]:
    """Process a batch of classes with given configuration."""
    successful = 0
    failed_count = 0
    newly_failed = set()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_class = {
            executor.submit(process_class, cls, delay): cls
            for cls in classes_to_process
        }

        with tqdm(total=len(classes_to_process), desc=desc, unit="class") as pbar:
            batch_completed = []

            for future in as_completed(future_to_class):
                cls = future_to_class[future]
                class_qid = cls["qid"]

                try:
                    qid, instances = future.result()

                    if instances is not None:
                        completed.add(qid)
                        batch_completed.append(qid)
                        # Remove from failed if it was there
                        failed.discard(qid)
                        successful += 1
                        pbar.set_postfix({
                            "success": successful,
                            "failed": failed_count,
                            "instances": len(instances)
                        })
                    else:
                        newly_failed.add(qid)
                        failed_count += 1
                        pbar.set_postfix({
                            "success": successful,
                            "failed": failed_count,
                            "last_failed": qid
                        })

                except Exception as e:
                    newly_failed.add(class_qid)
                    failed_count += 1
                    tqdm.write(f"Error processing {class_qid}: {e}")

                pbar.update(1)

                # Save progress periodically
                if len(batch_completed) >= BATCH_SIZE:
                    save_progress(completed)
                    batch_completed = []

    # Update failed set
    failed.update(newly_failed)
    save_failed(failed)
    save_progress(completed)

    return successful, failed_count


def main():
    log("=" * 70)
    log("EXTRACT PRE-1900 INSTANCES - PARALLEL PROCESSING")
    log("=" * 70)
    log(f"Workers: {MAX_WORKERS} | Retry workers: {RETRY_WORKERS}")
    log(f"Batch size: {BATCH_SIZE} | Max retry rounds: {MAX_RETRY_ROUNDS}")

    # Load pre-1900 classes
    log(f"\nLoading classes from: {INPUT_FILE}")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    classes = data["classes"]
    log(f"Total classes loaded: {len(classes)}")

    # Deduplicate classes
    classes = deduplicate_classes(classes)
    class_lookup = {c["qid"]: c for c in classes}
    log(f"After deduplication: {len(classes)} unique classes")

    # Load progress (completed and failed classes)
    completed, failed = load_progress()
    log(f"Already completed: {len(completed)} classes")
    log(f"Previously failed: {len(failed)} classes")

    # Filter to remaining classes (not completed, including previously failed)
    remaining = [c for c in classes if c["qid"] not in completed]
    log(f"Remaining to process: {len(remaining)} classes")

    if not remaining:
        log("\nAll classes already processed!")
        total = merge_all_instances()
        log(f"Total unique instances: {total:,}")
        return

    # Clear failed set for fresh run (will be rebuilt)
    failed.clear()

    # === MAIN EXTRACTION PASS ===
    log(f"\n{'=' * 70}")
    log("MAIN EXTRACTION PASS")
    log("=" * 70)

    successful, failed_count = process_batch(
        remaining, completed, failed,
        workers=MAX_WORKERS,
        delay=RATE_LIMIT_DELAY,
        desc="Extracting instances"
    )

    log(f"\nMain pass complete: {successful} successful, {failed_count} failed")

    # === RETRY FAILED CLASSES ===
    for retry_round in range(1, MAX_RETRY_ROUNDS + 1):
        if not failed:
            log("\nNo failed classes to retry!")
            break

        log(f"\n{'=' * 70}")
        log(f"RETRY ROUND {retry_round}/{MAX_RETRY_ROUNDS}")
        log("=" * 70)
        log(f"Retrying {len(failed)} failed classes with slower rate...")

        # Wait before retry round
        wait_time = 30 * retry_round
        log(f"Waiting {wait_time}s before retry...")
        time.sleep(wait_time)

        # Get classes to retry
        classes_to_retry = [class_lookup[qid] for qid in failed if qid in class_lookup]

        # Clear failed for this round
        failed.clear()

        retry_success, retry_failed = process_batch(
            classes_to_retry, completed, failed,
            workers=RETRY_WORKERS,
            delay=RETRY_DELAY * retry_round,  # Increase delay each round
            desc=f"Retry round {retry_round}"
        )

        log(f"Retry round {retry_round}: {retry_success} recovered, {retry_failed} still failing")

        if retry_failed == 0:
            log("All retries successful!")
            break

    # === FINAL SUMMARY ===
    log("\n" + "=" * 70)
    total = merge_all_instances()

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)
    log(f"Total classes completed: {len(completed)}")
    log(f"Total classes still failed: {len(failed)}")
    log(f"Total unique instance IDs: {total:,}")

    if failed:
        log(f"\nFailed classes saved to: {FAILED_FILE}")
        log("Run the script again to retry failed classes.")


if __name__ == "__main__":
    main()
