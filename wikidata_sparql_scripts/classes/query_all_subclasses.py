"""
Script to query ALL subclasses of written work from Wikidata.
Designed to run for hours with robust timeout handling and progress saving.

Usage:
    python -u query_all_subclasses.py  # -u for unbuffered output

The script will:
1. Fetch all subclasses of written work (Q47461344)
2. Query each one for its direct instance count
3. Save progress every 10 queries (can resume if interrupted)
4. Handle timeouts gracefully with exponential backoff
5. Save results to JSON incrementally
"""

import json
import sys
import time
import requests
from pathlib import Path
from datetime import datetime


def log(msg):
    """Print with flush for real-time output."""
    print(msg, flush=True)


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WRITTEN_WORK_QID = "Q47461344"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Files for saving progress
PROGRESS_FILE = OUTPUT_DIR / "query_progress.json"
RESULTS_FILE = OUTPUT_DIR / "written_work_all_subclasses.json"


def run_sparql_query(query: str, timeout: int = 60, max_retries: int = 5) -> dict:
    """Execute a SPARQL query with retry logic and exponential backoff."""
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
            wait_time = 2**attempt * 5  # 5, 10, 20, 40, 80 seconds
            log(
                f"      Timeout, waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
            )
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                wait_time = 60  # Wait 1 minute for rate limiting
                log(f"      Rate limited, waiting {wait_time}s")
                time.sleep(wait_time)
            elif "504" in str(e) or "502" in str(e):
                wait_time = 2**attempt * 10
                log(
                    f"      Server error, waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
                )
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            wait_time = 2**attempt * 5
            log(
                f"      Error: {e}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
            )
            time.sleep(wait_time)

    raise Exception(f"Failed after {max_retries} retries")


def get_all_subclasses(qid: str) -> list:
    """Get ALL subclasses (transitive) of a class."""
    log("   Fetching all subclasses (this may take a moment)...")
    query = f"""
    SELECT DISTINCT ?subclass ?subclassLabel WHERE {{
      ?subclass wdt:P279+ wd:{qid} .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """
    results = run_sparql_query(query, timeout=300)
    return results["results"]["bindings"]


def count_direct_instances(qid: str) -> int:
    """Count DIRECT instances only (P31, not transitive)."""
    query = f"""
    SELECT (COUNT(?item) AS ?count) WHERE {{
      ?item wdt:P31 wd:{qid} .
    }}
    """
    results = run_sparql_query(query, timeout=60)
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


def load_progress() -> dict:
    """Load progress from file if it exists."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed": {}, "last_index": 0}


def save_progress(progress: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def save_results_json(results: list, total_written_works: int):
    """Save results to JSON with metadata."""
    json_file = OUTPUT_DIR / "written_work_all_subclasses.json"
    # Only sum successful queries (status == "ok")
    ok_results = [r for r in results if r.get("status") == "ok"]
    error_results = [r for r in results if r.get("status") == "error"]
    sum_counts = sum(r["direct_instance_count"] for r in ok_results)
    coverage = sum_counts / total_written_works * 100 if total_written_works > 0 else 0

    data = {
        "metadata": {
            "total_written_works": total_written_works,
            "subclasses_queried": len(results),
            "subclasses_ok": len(ok_results),
            "subclasses_error": len(error_results),
            "sum_instance_counts": sum_counts,
            "coverage_percent": round(coverage, 2),
            "last_updated": datetime.now().isoformat(),
        },
        "subclasses": sorted(
            results, key=lambda x: x["direct_instance_count"], reverse=True
        ),
    }

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    start_time = datetime.now()
    log("=" * 75)
    log("WRITTEN WORKS - COMPLETE SUBCLASS ANALYSIS")
    log(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 75)

    # Step 1: Get total count
    log("\n1. Counting TOTAL written works...")
    total_written_works = count_total_instances(WRITTEN_WORK_QID)
    log(f"   Total written works: {total_written_works:,}")

    # Step 2: Get all subclasses
    log("\n2. Fetching ALL subclasses...")
    all_subclasses = get_all_subclasses(WRITTEN_WORK_QID)
    log(f"   Found {len(all_subclasses)} subclasses")

    # Build list of QIDs to query (including written work itself)
    qids_to_query = [(WRITTEN_WORK_QID, "written work")]
    for item in all_subclasses:
        qid = item["subclass"]["value"].split("/")[-1]
        label = item["subclassLabel"]["value"]
        qids_to_query.append((qid, label))

    log(f"   Total classes to query: {len(qids_to_query)}")

    # Step 3: Load progress (for resuming)
    progress = load_progress()
    log(f"\n3. Loading progress...")
    log(f"   Already completed: {len(progress['completed'])} queries")

    # Step 4: Query each subclass
    log(f"\n4. Querying instance counts...")
    log(f"   Estimated time: {len(qids_to_query) * 0.3 / 60:.0f} minutes")
    log("   Progress will be saved every 100 queries\n")

    results = []
    errors = []

    # Add already completed results
    for qid, data in progress["completed"].items():
        results.append(
            {
                "qid": qid,
                "label": data["label"],
                "direct_instance_count": data["count"],
                "status": data.get("status", "ok"),  # backwards compatible
            }
        )

    for i, (qid, label) in enumerate(qids_to_query):
        # Skip if already completed
        if qid in progress["completed"]:
            continue

        # Query
        try:
            count = count_direct_instances(qid)
            progress["completed"][qid] = {
                "label": label,
                "count": count,
                "status": "ok",
            }
            results.append(
                {
                    "qid": qid,
                    "label": label,
                    "direct_instance_count": count,
                    "status": "ok",
                }
            )

            # Small delay to be nice to Wikidata
            time.sleep(0.2)

        except Exception as e:
            log(f"   Error with {label} ({qid}): {e}")
            errors.append({"qid": qid, "label": label, "error": str(e)})
            progress["completed"][qid] = {
                "label": label,
                "count": -1,
                "status": "error",
            }
            results.append(
                {
                    "qid": qid,
                    "label": label,
                    "direct_instance_count": -1,
                    "status": "error",
                }
            )

        # Progress update and save every 10 items
        completed = len(progress["completed"])
        if completed % 10 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > 0:
                rate = elapsed / completed
                remaining = (len(qids_to_query) - completed) * rate
                eta_hours = remaining / 3600
                current_sum = sum(r["direct_instance_count"] for r in results)
                coverage = (
                    current_sum / total_written_works * 100
                    if total_written_works > 0
                    else 0
                )
                pct_done = completed / len(qids_to_query) * 100
                log(
                    f"   [{completed}/{len(qids_to_query)}] {pct_done:.1f}% done | {coverage:.1f}% coverage | ETA: {eta_hours:.1f}h"
                )

            # Save progress and JSON
            save_progress(progress)
            save_results_json(results, total_written_works)

    # Final save
    save_progress(progress)
    save_results_json(results, total_written_works)

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    sum_counts = sum(r["direct_instance_count"] for r in results)
    coverage = sum_counts / total_written_works * 100 if total_written_works > 0 else 0

    log("\n" + "=" * 75)
    log("COMPLETE")
    log("=" * 75)
    log(f"\nDuration: {duration}")
    log(f"Total written works:     {total_written_works:,}")
    log(f"Subclasses queried:      {len(results):,}")
    log(f"Sum of instance counts:  {sum_counts:,}")
    log(f"Coverage:                {coverage:.1f}%")
    log(f"Errors:                  {len(errors)}")
    log(f"\nResults saved to: {RESULTS_FILE}")

    if coverage >= 100:
        log("\n✓ Sum of subclass counts >= Total (complete coverage)")
    else:
        log(f"\nNote: {100 - coverage:.1f}% items have multiple P31 values")
        log("      (counted once in total, but appear in multiple subclasses)")

    # Show top 20
    log("\n" + "=" * 75)
    log("TOP 20 SUBCLASSES BY INSTANCE COUNT")
    log("=" * 75)
    sorted_results = sorted(
        results, key=lambda x: x["direct_instance_count"], reverse=True
    )
    log(f"\n{'Rank':<5} {'Label':<45} {'Count':>15}")
    log("-" * 65)
    for i, r in enumerate(sorted_results[:20], 1):
        log(f"{i:<5} {r['label'][:43]:<45} {r['direct_instance_count']:>15,}")


if __name__ == "__main__":
    main()
