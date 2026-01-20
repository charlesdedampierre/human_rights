"""
Enrich literary_works.db with modern country mappings.
Uses OpenAI to map historical/regional countries to modern equivalents.

Input: output/literary_works.db
Output:
  - Adds 'modern_country' and 'modern_country_confidence' columns to countries table
  - Adds 'modern_country' column to literary_works table
  - Saves mappings to cache/country_mappings.json

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
CACHE_FILE = CACHE_DIR / "country_mappings.json"
BATCH_SIZE = 50  # Countries per OpenAI request
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


def process_single_batch(client, batch, system_prompt):
    """Process a single batch of items with OpenAI."""
    items_list = "\n".join([f"- {item}" for item in batch])

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": items_list},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )

        content = response.choices[0].message.content
        mappings = json.loads(content)

        # Handle nested "mappings" key if present
        if "mappings" in mappings:
            mappings = mappings["mappings"]

        return mappings

    except Exception as e:
        print(f"\nError processing batch: {e}")
        return {item: None for item in batch}


def batch_openai_mapping(client, items, system_prompt):
    """
    Make batched OpenAI requests to map items using parallel processing.
    Returns a dictionary of item -> mapped_value.
    """
    results = {}

    # Create batches
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_batch, client, batch, system_prompt): batch
            for batch in batches
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="OpenAI batches"):
            batch_results = future.result()
            results.update(batch_results)

    return results


def step1_map_countries(client, conn):
    """Map historical/regional countries to modern countries."""
    print("\n" + "-" * 40)
    print("Step 1: Mapping countries to modern equivalents")
    print("-" * 40)

    cursor = conn.cursor()

    # Add modern_country column to countries table if not exists
    try:
        cursor.execute("ALTER TABLE countries ADD COLUMN modern_country TEXT")
        print("Added 'modern_country' column to countries table")
    except sqlite3.OperationalError:
        print("'modern_country' column already exists, will reset and update")
        # Reset existing values for idempotent rerun
        cursor.execute("UPDATE countries SET modern_country = NULL")
        conn.commit()

    # Add confidence column
    try:
        cursor.execute("ALTER TABLE countries ADD COLUMN modern_country_confidence INTEGER")
        print("Added 'modern_country_confidence' column to countries table")
    except sqlite3.OperationalError:
        cursor.execute("UPDATE countries SET modern_country_confidence = NULL")
        conn.commit()

    # Check cache first
    cached_mappings = load_cache()
    if cached_mappings:
        print(f"Loaded {len(cached_mappings)} mappings from cache: {CACHE_FILE}")
        mappings = cached_mappings
    else:
        # Get unique country labels
        cursor.execute("SELECT DISTINCT label FROM countries WHERE label IS NOT NULL")
        countries = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(countries)} unique countries to map")

        system_prompt = """You are an expert in geography and history.
Map each historical, regional, or ancient entity to its PRIMARY modern country equivalent.
Also provide a confidence score (0-100) for each mapping.

Rules:
1. For historical empires/kingdoms, use the modern country where the capital was located
2. For regions within modern countries, use that country
3. For ancient civilizations, use the modern country that primarily occupies that territory
4. Keep modern country names (e.g., "France", "Germany", "China")
5. Return null for country if: fictional, ambiguous, or it's not a place/country

Confidence guidelines:
- 90-100: Very certain (well-known modern countries or famous historical entities)
- 70-89: Confident (clear historical mapping)
- 50-69: Moderate (some ambiguity but reasonable mapping)
- Below 50: Low confidence (return null for country instead)

Return a JSON object: {"entity_name": {"country": "modern_country" or null, "confidence": 0-100}}

Examples:
- "Ancient Greece" -> {"country": "Greece", "confidence": 95}
- "Roman Empire" -> {"country": "Italy", "confidence": 90}
- "Qing dynasty" -> {"country": "China", "confidence": 98}
- "Prussia" -> {"country": "Germany", "confidence": 85}
- "Catalonia" -> {"country": "Spain", "confidence": 92}
- "Aerican Empire" -> {"country": null, "confidence": 0}
"""

        print("Calling OpenAI API (pass 1)...")
        mappings = batch_openai_mapping(client, countries, system_prompt)

        # Second pass for NULLs - try again with more context
        null_items = [k for k, v in mappings.items() if v is None or (isinstance(v, dict) and v.get("country") is None)]
        if null_items and len(null_items) < len(countries):  # Only if some succeeded
            print(f"\nSecond pass for {len(null_items)} unmapped items...")
            retry_prompt = """You are an expert in geography and history.
These items could not be mapped in the first pass. Try again with more careful analysis.
Also provide a confidence score (0-100) for each mapping.

For each item, consider:
- Is it a historical region, empire, or civilization?
- What modern country now occupies that territory?
- If it's a language or ethnic group name, what country is it primarily associated with?

Confidence guidelines:
- 90-100: Very certain
- 70-89: Confident
- 50-69: Moderate
- Below 50: Return null for country

Return a JSON object: {"entity_name": {"country": "modern_country" or null, "confidence": 0-100}}
"""
            retry_mappings = batch_openai_mapping(client, null_items, retry_prompt)
            # Update mappings with successful retries
            for k, v in retry_mappings.items():
                if v is not None and isinstance(v, dict) and v.get("country") is not None:
                    mappings[k] = v

        # Save to cache
        save_cache(mappings)

    # Update countries table
    print("Updating countries table...")
    for country, data in tqdm(mappings.items(), desc="Updating"):
        if isinstance(data, dict):
            modern = data.get("country")
            confidence = data.get("confidence", 0)
        else:
            # Handle old cache format (just country string)
            modern = data
            confidence = None
        cursor.execute(
            "UPDATE countries SET modern_country = ?, modern_country_confidence = ? WHERE label = ?",
            (modern, confidence, country)
        )
    conn.commit()

    # Statistics
    cursor.execute("SELECT COUNT(*) FROM countries WHERE modern_country IS NOT NULL")
    mapped = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(modern_country_confidence) FROM countries WHERE modern_country_confidence IS NOT NULL")
    avg_conf = cursor.fetchone()[0]
    print(f"Mapped {mapped}/{len(mappings)} countries")
    if avg_conf:
        print(f"Average confidence: {avg_conf:.1f}")

    return mappings


def step2_update_literary_works(conn, country_map):
    """Add modern_country to literary_works based on countryLabel mapping.

    Only updates records that have a countryLabel that can be mapped.
    Does NOT reset language-inferred values - those are handled by script 06.
    """
    print("\n" + "-" * 40)
    print("Step 2: Adding modern_country to literary_works (from countryLabel)")
    print("-" * 40)

    cursor = conn.cursor()

    # Add modern_country column to literary_works if not exists
    try:
        cursor.execute("ALTER TABLE literary_works ADD COLUMN modern_country TEXT")
        print("Added 'modern_country' column to literary_works table")
    except sqlite3.OperationalError:
        print("'modern_country' column already exists, will update country-based mappings")
        # Only reset records that have a countryLabel (keep language-inferred ones)
        cursor.execute("UPDATE literary_works SET modern_country = NULL WHERE countryLabel IS NOT NULL")
        conn.commit()
        print("Reset modern_country for records with countryLabel")

    # Get literary works with countryLabel
    print("Loading literary works with countryLabel...")
    cursor.execute("SELECT rowid, countryLabel FROM literary_works WHERE countryLabel IS NOT NULL")
    rows = cursor.fetchall()
    print(f"Processing {len(rows):,} records...")

    # Calculate modern_country for each work
    updates = []
    for rowid, country_label in tqdm(rows, desc="Processing"):
        modern_country = None
        if country_label and country_label in country_map:
            data = country_map[country_label]
            if isinstance(data, dict):
                modern_country = data.get("country")
            else:
                modern_country = data
        updates.append((modern_country, rowid))

    # Update in batches
    print("Updating literary_works table...")
    batch_size = 10000
    for i in tqdm(range(0, len(updates), batch_size), desc="Writing"):
        batch = updates[i : i + batch_size]
        cursor.executemany(
            "UPDATE literary_works SET modern_country = ? WHERE rowid = ?", batch
        )
        conn.commit()

    # Statistics
    cursor.execute(
        "SELECT COUNT(*) FROM literary_works WHERE modern_country IS NOT NULL"
    )
    with_country = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT modern_country, COUNT(*) as cnt
        FROM literary_works
        WHERE modern_country IS NOT NULL
        GROUP BY modern_country
        ORDER BY cnt DESC
        LIMIT 10
    """
    )
    top_countries = cursor.fetchall()

    print(f"\nWorks with modern_country: {with_country:,}")
    print("Top 10 modern countries:")
    for country, count in top_countries:
        print(f"  - {country}: {count:,}")


def main():
    print("=" * 60)
    print("ENRICHMENT: Country to Modern Country Mapping")
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

    conn = sqlite3.connect(DB_PATH)

    # Step 1: Map countries
    country_map = step1_map_countries(client, conn)

    # Step 2: Update literary_works
    step2_update_literary_works(conn, country_map)

    conn.close()
    print("\n" + "=" * 60)
    print(f"Done! Database updated: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
