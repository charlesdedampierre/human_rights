"""
Add region classification to countries table.
Uses OpenAI to classify countries into world regions.

Input: output/literary_works.db
Output:
  - Adds 'main_region' and 'region_confidence' columns to countries table
  - Saves mappings to cache/region_mappings.json

Requires: OPENAI_API_KEY environment variable (unless cache exists)
"""

import sqlite3
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_PATH = Path(__file__).parent.parent / "output" / "literary_works.db"
CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_FILE = CACHE_DIR / "region_mappings.json"
BATCH_SIZE = 50
MAX_WORKERS = 5  # Parallel API calls


def load_cache():
    """Load cached mappings if available."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(mappings):
    """Save mappings to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(mappings, f, ensure_ascii=False, indent=2)
    print(f"Saved mappings to {CACHE_FILE}")


def get_regions_batch(client, countries: list[str], is_retry: bool = False) -> dict:
    """Get regions for a single batch of modern country names."""
    if is_retry:
        prompt = f"""These countries could not be assigned a region in the first pass. Try again with more careful analysis.
Also provide a confidence score (0-100) for each mapping.

Regions: Western Europe, Eastern Europe, North America, South America, Central America & Caribbean, Middle East, North Africa, Sub-Saharan Africa, Central Asia, South Asia, Southeast Asia, East Asia, Oceania, Global/International

Confidence guidelines:
- 90-100: Very certain
- 70-89: Confident
- 50-69: Moderate
- Below 50: Return null for region

Countries: {json.dumps(countries)}

Return JSON: {{"Country Name": {{"region": "Region" or null, "confidence": 0-100}}, ...}}"""
    else:
        prompt = f"""Assign each country to ONE region from: Western Europe, Eastern Europe, North America, South America, Central America & Caribbean, Middle East, North Africa, Sub-Saharan Africa, Central Asia, South Asia, Southeast Asia, East Asia, Oceania, Global/International
Also provide a confidence score (0-100) for each mapping.

Confidence guidelines:
- 90-100: Very certain (clear geographic placement)
- 70-89: Confident (standard country classification)
- 50-69: Moderate (some ambiguity)
- Below 50: Low confidence (return null for region instead)

Countries: {json.dumps(countries)}

Return JSON: {{"Country Name": {{"region": "Region" or null, "confidence": 0-100}}, ...}}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"\nError processing batch: {e}")
        return {country: {"region": None, "confidence": 0} for country in countries}


def get_regions_parallel(client, countries: list[str], is_retry: bool = False) -> dict:
    """Get regions for all countries using parallel processing."""
    results = {}

    # Create batches
    batches = [countries[i:i + BATCH_SIZE] for i in range(0, len(countries), BATCH_SIZE)]

    # Process batches in parallel
    desc = "OpenAI batches (retry)" if is_retry else "OpenAI batches"
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_regions_batch, client, batch, is_retry): batch
            for batch in batches
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
            batch_results = future.result()
            results.update(batch_results)

    return results


def main():
    print("=" * 60)
    print("ENRICHMENT: Region Classification")
    print("=" * 60)

    # Check if cache exists
    cached_mappings = load_cache()

    # Initialize OpenAI client only if no cache
    client = None
    if not cached_mappings:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not found and no cache available")
            print(f"Either set OPENAI_API_KEY or provide cache file: {CACHE_FILE}")
            return
        client = OpenAI(api_key=api_key)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    # Add main_region column if needed
    cursor.execute("PRAGMA table_info(countries)")
    columns = [col[1] for col in cursor.fetchall()]
    if "main_region" not in columns:
        cursor.execute("ALTER TABLE countries ADD COLUMN main_region TEXT")
        conn.commit()
        print("Added 'main_region' column to countries table")
    else:
        # Reset for idempotent rerun
        print("'main_region' column exists, resetting values...")
        cursor.execute("UPDATE countries SET main_region = NULL")
        conn.commit()

    # Add region_confidence column if needed
    if "region_confidence" not in columns:
        cursor.execute("ALTER TABLE countries ADD COLUMN region_confidence INTEGER")
        conn.commit()
        print("Added 'region_confidence' column to countries table")
    else:
        cursor.execute("UPDATE countries SET region_confidence = NULL")
        conn.commit()

    if cached_mappings:
        print(f"Loaded {len(cached_mappings)} mappings from cache: {CACHE_FILE}")
        region_map = cached_mappings
    else:
        # Get unique modern_country values and assign regions
        cursor.execute("SELECT DISTINCT modern_country FROM countries WHERE modern_country IS NOT NULL AND modern_country != ''")
        unique_countries = [row[0] for row in cursor.fetchall()]
        print(f"Assigning regions to {len(unique_countries)} unique modern countries...")

        # Process in parallel (pass 1)
        print("Calling OpenAI API (pass 1)...")
        region_map = get_regions_parallel(client, unique_countries)

        # Second pass for NULLs - try again with more context
        null_items = [k for k, v in region_map.items() if v is None or (isinstance(v, dict) and v.get("region") is None)]
        if null_items and len(null_items) < len(unique_countries):  # Only if some succeeded
            print(f"\nSecond pass for {len(null_items)} unmapped items...")
            retry_mappings = get_regions_parallel(client, null_items, is_retry=True)
            # Update mappings with successful retries
            for k, v in retry_mappings.items():
                if v is not None and isinstance(v, dict) and v.get("region") is not None:
                    region_map[k] = v

        # Save to cache
        save_cache(region_map)

    # Update database
    print("Updating database...")
    for country, data in tqdm(region_map.items(), desc="Updating"):
        if isinstance(data, dict):
            region = data.get("region")
            confidence = data.get("confidence", 0)
        else:
            # Handle old cache format (just region string)
            region = data
            confidence = None
        cursor.execute(
            "UPDATE countries SET main_region = ?, region_confidence = ? WHERE modern_country = ?",
            (region, confidence, country)
        )
    conn.commit()

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    cursor.execute("SELECT main_region, COUNT(*) FROM countries WHERE main_region IS NOT NULL GROUP BY main_region ORDER BY COUNT(*) DESC")
    print("Countries by region:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.execute("SELECT COUNT(*) FROM countries WHERE main_region IS NULL")
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        print(f"\nCountries without region: {null_count}")

    cursor.execute("SELECT AVG(region_confidence) FROM countries WHERE region_confidence IS NOT NULL")
    avg_conf = cursor.fetchone()[0]
    if avg_conf:
        print(f"Average confidence: {avg_conf:.1f}")

    conn.close()
    print(f"\nDone! Database updated: {DB_PATH}")


if __name__ == "__main__":
    main()
