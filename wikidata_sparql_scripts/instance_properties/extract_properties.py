"""
Extract properties from Wikidata instances.

Features:
- Batching (50 items per batch)
- Multiprocessing (parallel SPARQL queries)
- Incremental JSON saving (resume capability)
- Automatic restart on failure with exponential backoff

Estimates for 4M items:
- Storage: ~16 GB
- Time: ~5 days (3 workers) or ~2.5 days (6 workers)
"""

import json
import glob
import requests
import time
import os
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import traceback
from tqdm import tqdm

# =============================================================================
# CONFIGURATION
# =============================================================================

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
INSTANCES_DIR = "../instances/output/instances_by_class"
OUTPUT_DIR = "output/extracted"
LOG_FILE = "output/extraction.log"

BATCH_SIZE = 50
NUM_WORKERS = 8
MAX_RETRIES = 5
LIMIT = 100000  # Set to integer for testing, None for full extraction
SAMPLE = True  # If True, randomly sample LIMIT items from full dataset

# =============================================================================
# LOGGING SETUP
# =============================================================================

os.makedirs("output", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# PROPERTY DEFINITIONS
# =============================================================================

MAIN_PROPERTIES = {
    # ----- DATE -----
    "P577": {"label": "publication date", "category": "date"},
    "P571": {"label": "inception", "category": "date"},
    "P580": {"label": "start time", "category": "date"},
    "P582": {"label": "end time", "category": "date"},
    "P585": {"label": "point in time", "category": "date"},
    "P1191": {"label": "date of first performance", "category": "date"},
    "P1319": {"label": "earliest date", "category": "date"},
    "P1326": {"label": "latest date", "category": "date"},
    "P2031": {"label": "work period (start)", "category": "date"},
    "P2032": {"label": "work period (end)", "category": "date"},
    "P3893": {"label": "public domain date", "category": "date"},
    "P1249": {"label": "time of earliest written record", "category": "date"},
    # ----- PLACE -----
    "P495": {"label": "country of origin", "category": "place"},
    "P17": {"label": "country", "category": "place"},
    "P291": {"label": "place of publication", "category": "place"},
    "P840": {"label": "narrative location", "category": "place"},
    "P131": {"label": "located in admin entity", "category": "place"},
    "P276": {"label": "location", "category": "place"},
    "P1001": {"label": "applies to jurisdiction", "category": "place"},
    # ----- TYPES -----
    "P31": {"label": "instance of", "category": "types"},
    "P136": {"label": "genre", "category": "types"},
    "P7937": {"label": "form of creative work", "category": "types"},
    "P282": {"label": "writing system", "category": "types"},
    "P2551": {"label": "used metre", "category": "types"},
    "P407": {"label": "language of work", "category": "types"},
    "P364": {"label": "original language", "category": "types"},
    "P135": {"label": "movement", "category": "types"},
    "P921": {"label": "main subject", "category": "types"},
    # ----- CONTENT -----
    "P953": {"label": "full work available at URL", "category": "content"},
    "P1433": {"label": "published in", "category": "content"},
    "P1343": {"label": "described by source", "category": "content"},
    "P973": {"label": "described at URL", "category": "content"},
    "P856": {"label": "official website", "category": "content"},
    "P18": {"label": "image", "category": "content"},
    "P996": {"label": "document file on Commons", "category": "content"},
    "P1476": {"label": "title", "category": "content"},
    "P1680": {"label": "subtitle", "category": "content"},
    "P6216": {"label": "copyright status", "category": "content"},
    # ----- CREATORS -----
    "P50": {"label": "author", "category": "creators"},
    "P2093": {"label": "author name string", "category": "creators"},
    "P1779": {"label": "possible creator", "category": "creators"},
    "P98": {"label": "editor", "category": "creators"},
    "P655": {"label": "translator", "category": "creators"},
    "P170": {"label": "creator", "category": "creators"},
    "P123": {"label": "publisher", "category": "creators"},
    # ----- RELATIONSHIPS -----
    "P361": {"label": "part of", "category": "relationships"},
    "P144": {"label": "based on", "category": "relationships"},
    "P179": {"label": "part of the series", "category": "relationships"},
    "P155": {"label": "follows", "category": "relationships"},
    "P156": {"label": "followed by", "category": "relationships"},
}


def query_sparql_with_retry(query, max_retries=MAX_RETRIES):
    """Execute SPARQL query with exponential backoff retry."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataExtraction/1.0 (Research Project)",
    }

    wait_time = 2
    for attempt in range(max_retries):
        try:
            time.sleep(1)  # Base rate limiting
            response = requests.get(
                WIKIDATA_SPARQL_ENDPOINT,
                params={"query": query},
                headers=headers,
                timeout=120,
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            elif response.status_code >= 500:
                logger.warning(f"Server error {response.status_code}, attempt {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                logger.warning(f"HTTP {response.status_code}: {response.text[:200]}")
                time.sleep(wait_time)

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout, attempt {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
            wait_time *= 2
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(wait_time)

    return None


def build_main_properties_query(instance_ids):
    """Build SPARQL query for main properties."""
    values = " ".join([f"wd:{qid}" for qid in instance_ids])

    optional_clauses = []
    select_vars = ["?item", "?itemLabel", "?itemDescription"]

    for prop_id, prop_info in MAIN_PROPERTIES.items():
        var_name = prop_id.lower()
        select_vars.append(f"?{var_name}")
        select_vars.append(f"?{var_name}Label")
        optional_clauses.append(f"OPTIONAL {{ ?item wdt:{prop_id} ?{var_name} . }}")

    query = f"""
SELECT DISTINCT {' '.join(select_vars)}
WHERE {{
    VALUES ?item {{ {values} }}
    OPTIONAL {{ ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }}
    OPTIONAL {{ ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "en") }}
    {chr(10).join(optional_clauses)}
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
}}
"""
    return query


def build_identifiers_query(instance_ids):
    """Build SPARQL query for all external identifiers with formatter URL."""
    values = " ".join([f"wd:{qid}" for qid in instance_ids])
    return f"""
SELECT ?item ?prop ?propLabel ?value ?formatterUrl
WHERE {{
    VALUES ?item {{ {values} }}
    ?item ?p ?value .
    ?prop wikibase:directClaim ?p ;
          wikibase:propertyType wikibase:ExternalId .
    OPTIONAL {{ ?prop wdt:P1630 ?formatterUrl . }}
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
"""


def build_sitelinks_query(instance_ids):
    """Build SPARQL query for all sitelinks."""
    values = " ".join([f"wd:{qid}" for qid in instance_ids])
    return f"""
SELECT ?item ?sitelink ?wiki ?title
WHERE {{
    VALUES ?item {{ {values} }}
    ?sitelink schema:about ?item ;
              schema:isPartOf ?wiki ;
              schema:name ?title .
}}
"""


def extract_qid(uri):
    """Extract QID from Wikidata URI (e.g., 'http://www.wikidata.org/entity/Q123' -> 'Q123')."""
    if uri and "/entity/" in uri:
        return uri.split("/entity/")[-1]
    return uri


def extract_batch(batch_ids, batch_num):
    """Extract all data for a batch of instances (runs in thread)."""
    logger.info(f"  [Batch {batch_num}] Extracting {len(batch_ids)} instances...")

    results = {}

    # Initialize all items
    for item_id in batch_ids:
        results[item_id] = {
            "label": "",
            "description": "",
            "properties": {},
            "identifiers": [],
            "sitelinks": [],
        }

    # Extract main properties
    query = build_main_properties_query(batch_ids)
    main_result = query_sparql_with_retry(query)

    if main_result:
        for binding in main_result.get("results", {}).get("bindings", []):
            item_uri = binding.get("item", {}).get("value", "")
            item_id = item_uri.split("/")[-1] if item_uri else None
            if not item_id or item_id not in results:
                continue

            results[item_id]["label"] = binding.get("itemLabel", {}).get("value", "")
            results[item_id]["description"] = binding.get("itemDescription", {}).get("value", "")

            for prop_id, prop_info in MAIN_PROPERTIES.items():
                var_name = prop_id.lower()
                if var_name in binding:
                    raw_value = binding[var_name].get("value", "")
                    value_label = binding.get(f"{var_name}Label", {}).get("value", "")

                    # Extract QID if it's an entity reference
                    value = extract_qid(raw_value) if "/entity/" in raw_value else raw_value

                    # Simplified structure: just value for literals, {id, label} for entities
                    if value.startswith("Q"):
                        prop_data = {"id": value, "label": value_label} if value_label and value_label != value else value
                    else:
                        prop_data = value

                    # Initialize property with label if not exists
                    if prop_id not in results[item_id]["properties"]:
                        results[item_id]["properties"][prop_id] = {
                            "property_label": prop_info["label"],
                            "values": []
                        }
                    if prop_data not in results[item_id]["properties"][prop_id]["values"]:
                        results[item_id]["properties"][prop_id]["values"].append(prop_data)

    # Extract identifiers
    time.sleep(0.5)  # Small delay between queries
    query = build_identifiers_query(batch_ids)
    id_result = query_sparql_with_retry(query)

    if id_result:
        for binding in id_result.get("results", {}).get("bindings", []):
            item_uri = binding.get("item", {}).get("value", "")
            item_id = item_uri.split("/")[-1] if item_uri else None
            prop_uri = binding.get("prop", {}).get("value", "")
            prop_id = prop_uri.split("/")[-1] if prop_uri else None

            if item_id and item_id in results and prop_id:
                raw_value = binding.get("value", {}).get("value", "")
                formatter_url = binding.get("formatterUrl", {}).get("value", "")

                # Construct full URL using formatter URL template ($1 is placeholder)
                full_url = formatter_url.replace("$1", raw_value) if formatter_url else None

                id_data = {
                    "property": prop_id,
                    "property_label": binding.get("propLabel", {}).get("value", prop_id),
                    "url": full_url if full_url else raw_value,
                }
                if id_data not in results[item_id]["identifiers"]:
                    results[item_id]["identifiers"].append(id_data)

    # Extract sitelinks
    time.sleep(0.5)
    query = build_sitelinks_query(batch_ids)
    sl_result = query_sparql_with_retry(query)

    if sl_result:
        for binding in sl_result.get("results", {}).get("bindings", []):
            item_uri = binding.get("item", {}).get("value", "")
            item_id = item_uri.split("/")[-1] if item_uri else None

            if item_id and item_id in results:
                wiki_url = binding.get("wiki", {}).get("value", "")
                sl_type = "other"
                if "wikisource" in wiki_url:
                    sl_type = "wikisource"
                elif "wikipedia" in wiki_url:
                    sl_type = "wikipedia"
                elif "commons" in wiki_url:
                    sl_type = "commons"

                sl_data = {
                    "url": binding.get("sitelink", {}).get("value", ""),
                    "type": sl_type,
                }
                if sl_data not in results[item_id]["sitelinks"]:
                    results[item_id]["sitelinks"].append(sl_data)

    logger.info(f"  [Batch {batch_num}] Completed - extracted {len(results)} items")
    return results


def save_incremental(all_data, output_file):
    """Save data incrementally to JSON file."""
    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=2)
    logger.info(f"  Saved {len(all_data)} items to {output_file}")


def main():
    """Main extraction with resumption."""
    logger.info("=" * 80)
    logger.info("WIKIDATA PROPERTY EXTRACTION")
    logger.info("=" * 80)

    # Create output directory
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load existing data (for resume capability)
    output_file = output_dir / "extracted_data.json"
    if output_file.exists():
        with open(output_file) as f:
            all_data = json.load(f)
        already_extracted = set(all_data.keys())
        logger.info(f"Resume: {len(already_extracted)} items already extracted")
    else:
        all_data = {}
        already_extracted = set()

    # Get instance IDs from class files
    class_files = sorted(glob.glob(f"{INSTANCES_DIR}/*.json"), key=lambda x: Path(x).stat().st_size)

    if not class_files:
        logger.error("No class files found!")
        return

    # Collect all instances
    logger.info("Loading all instances...")
    all_instances = []
    for class_file in class_files:
        with open(class_file) as f:
            instances = json.load(f)
        new_instances = [i for i in instances if i not in already_extracted]
        all_instances.extend(new_instances)

    logger.info(f"Total available instances: {len(all_instances):,}")

    # Apply limit (with optional random sampling)
    if LIMIT and len(all_instances) > LIMIT:
        if SAMPLE:
            random.seed(42)  # For reproducibility
            instance_ids = random.sample(all_instances, LIMIT)
            logger.info(f"Randomly sampled {LIMIT:,} instances from {len(all_instances):,}")
        else:
            instance_ids = all_instances[:LIMIT]
            logger.info(f"Taking first {LIMIT:,} instances")
    else:
        instance_ids = all_instances
        logger.info(f"Extracting all {len(instance_ids):,} instances")

    # Create batches
    batches = []
    for i in range(0, len(instance_ids), BATCH_SIZE):
        batch = instance_ids[i : i + BATCH_SIZE]
        batches.append((batch, i // BATCH_SIZE + 1))

    total_batches = len(batches)
    logger.info(f"Processing {total_batches} batches with {NUM_WORKERS} workers")

    # Process batches with ThreadPoolExecutor
    start_time = time.time()
    completed_batches = 0
    completed_items = len(all_data)

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all batches
        future_to_batch = {}
        skipped = 0
        for batch_ids, batch_num in batches:
            # Check if batch items already extracted
            if all(bid in all_data for bid in batch_ids):
                skipped += 1
                continue

            future = executor.submit(extract_batch, batch_ids, batch_num)
            future_to_batch[future] = (batch_ids, batch_num)

        if skipped > 0:
            logger.info(f"Skipped {skipped} already extracted batches")

        # Process completed batches with tqdm
        pbar = tqdm(
            as_completed(future_to_batch),
            total=len(future_to_batch),
            desc="Extracting",
            unit="batch",
            file=open(LOG_FILE, "a"),
            ncols=80,
        )

        for future in pbar:
            batch_ids, batch_num = future_to_batch[future]
            try:
                results = future.result()

                # Merge results
                for item_id, data in results.items():
                    all_data[item_id] = data

                completed_batches += 1
                completed_items = len(all_data)

                # Update progress bar description
                elapsed = time.time() - start_time
                rate = completed_items / elapsed if elapsed > 0 else 0
                pbar.set_postfix({
                    "items": completed_items,
                    "rate": f"{rate:.1f}/s"
                })

                # Save incrementally
                save_incremental(all_data, output_file)

                # Log progress periodically
                if completed_batches % 10 == 0:
                    logger.info(f"Progress: {completed_batches}/{total_batches} batches, {completed_items} items, {rate:.1f} items/s")

            except Exception as e:
                logger.error(f"  [Batch {batch_num}] FAILED: {e}")
                logger.error(traceback.format_exc())

            # Small delay between batch completions
            time.sleep(1)

        pbar.close()

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 80)
    logger.info(f"EXTRACTION COMPLETE")
    logger.info(f"  Total items: {len(all_data)}")
    logger.info(f"  Time: {elapsed:.1f}s ({elapsed/len(instance_ids):.2f}s per item)")
    logger.info(f"  Output: {output_file}")
    logger.info("=" * 80)

    # Print summary statistics
    total_identifiers = sum(len(d.get("identifiers", [])) for d in all_data.values())
    total_sitelinks = sum(len(d.get("sitelinks", [])) for d in all_data.values())
    wikisource_count = sum(
        1 for d in all_data.values()
        for sl in d.get("sitelinks", [])
        if sl.get("type") == "wikisource"
    )

    logger.info(f"\nStatistics:")
    logger.info(f"  Total identifiers: {total_identifiers}")
    logger.info(f"  Total sitelinks: {total_sitelinks}")
    logger.info(f"  Wikisource links: {wikisource_count}")


if __name__ == "__main__":
    # Auto-restart mechanism
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            main()
            break
        except Exception as e:
            logger.error(f"CRITICAL ERROR (attempt {attempt + 1}/{max_attempts}): {e}")
            logger.error(traceback.format_exc())
            if attempt < max_attempts - 1:
                wait_time = 30 * (attempt + 1)
                logger.info(f"Restarting in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error("Max attempts reached, giving up")
                raise
