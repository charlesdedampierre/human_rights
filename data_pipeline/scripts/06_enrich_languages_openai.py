"""
Enrich literary_works.db with language to modern country mappings.
Uses OpenAI to map languages to their primary modern country.
Then updates literary_works.modern_country for records without a country.

Input: output/literary_works.db
Output:
  - Adds 'inferred_modern_country' and 'inferred_country_confidence' columns to languages table
  - Updates 'modern_country' in literary_works where country is missing but language exists
  - Saves mappings to cache/language_mappings.json

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
CACHE_FILE = CACHE_DIR / "language_mappings.json"
BATCH_SIZE = 50  # Languages per OpenAI request
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
                {"role": "user", "content": items_list}
            ],
            temperature=0,
            response_format={"type": "json_object"}
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


def step1_map_languages(client, conn):
    """Map languages to their primary modern country."""
    print("\n" + "-" * 40)
    print("Step 1: Mapping languages to modern countries")
    print("-" * 40)

    cursor = conn.cursor()

    # Add inferred_modern_country column to languages table if not exists
    try:
        cursor.execute("ALTER TABLE languages ADD COLUMN inferred_modern_country TEXT")
        print("Added 'inferred_modern_country' column to languages table")
    except sqlite3.OperationalError:
        print("'inferred_modern_country' column already exists, will reset and update")
        # Reset existing values for idempotent rerun
        cursor.execute("UPDATE languages SET inferred_modern_country = NULL")
        conn.commit()

    # Add confidence column
    try:
        cursor.execute("ALTER TABLE languages ADD COLUMN inferred_country_confidence INTEGER")
        print("Added 'inferred_country_confidence' column to languages table")
    except sqlite3.OperationalError:
        cursor.execute("UPDATE languages SET inferred_country_confidence = NULL")
        conn.commit()

    # Check cache first
    cached_mappings = load_cache()
    if cached_mappings:
        print(f"Loaded {len(cached_mappings)} mappings from cache: {CACHE_FILE}")
        mappings = cached_mappings
    else:
        # Get unique language labels
        cursor.execute("SELECT DISTINCT label FROM languages WHERE label IS NOT NULL")
        languages = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(languages)} unique languages to map")

        system_prompt = """You are a linguistics expert.
Map each language to its PRIMARY modern country (where it's the official/national language or most widely spoken).
Also provide a confidence score (0-100) for each mapping.

Rules:
1. Use the country where the language originated or is primarily spoken
2. For ancient/historical languages, use the modern country of that region
3. For regional dialects, use the country where the main language is spoken
4. Keep modern country names (e.g., "France", "Japan", "China")
5. If the language is artificial, fictional, or has no clear primary country, return null for country

Confidence guidelines:
- 90-100: Very certain (official national languages)
- 70-89: Confident (clear primary country)
- 50-69: Moderate (some ambiguity but reasonable)
- Below 50: Low confidence (return null for country instead)

Return a JSON object: {"language_name": {"country": "modern_country" or null, "confidence": 0-100}}

Examples:
- "English" -> {"country": "United Kingdom", "confidence": 95}
- "French" -> {"country": "France", "confidence": 98}
- "Mandarin Chinese" -> {"country": "China", "confidence": 99}
- "Ancient Greek" -> {"country": "Greece", "confidence": 90}
- "Latin" -> {"country": "Italy", "confidence": 85}
- "Esperanto" -> {"country": null, "confidence": 0}
- "Catalan" -> {"country": "Spain", "confidence": 80}
"""

        print("Calling OpenAI API (pass 1)...")
        mappings = batch_openai_mapping(client, languages, system_prompt)

        # Second pass for NULLs - try again with more context
        null_items = [k for k, v in mappings.items() if v is None or (isinstance(v, dict) and v.get("country") is None)]
        if null_items and len(null_items) < len(languages):  # Only if some succeeded
            print(f"\nSecond pass for {len(null_items)} unmapped items...")
            retry_prompt = """You are a linguistics expert.
These languages could not be mapped in the first pass. Try again with more careful analysis.
Also provide a confidence score (0-100) for each mapping.

For each language, consider:
- Is it a historical or ancient form of a modern language?
- What modern country is the primary homeland of speakers of this language?
- If it's a dialect, what country uses the parent language?

Confidence guidelines:
- 90-100: Very certain
- 70-89: Confident
- 50-69: Moderate
- Below 50: Return null for country

Return a JSON object: {"language_name": {"country": "modern_country" or null, "confidence": 0-100}}
"""
            retry_mappings = batch_openai_mapping(client, null_items, retry_prompt)
            # Update mappings with successful retries
            for k, v in retry_mappings.items():
                if v is not None and isinstance(v, dict) and v.get("country") is not None:
                    mappings[k] = v

        # Save to cache
        save_cache(mappings)

    # Update languages table
    print("Updating languages table...")
    for language, data in tqdm(mappings.items(), desc="Updating"):
        if isinstance(data, dict):
            country = data.get("country")
            confidence = data.get("confidence", 0)
        else:
            # Handle old cache format (just country string)
            country = data
            confidence = None
        cursor.execute(
            "UPDATE languages SET inferred_modern_country = ?, inferred_country_confidence = ? WHERE label = ?",
            (country, confidence, language)
        )
    conn.commit()

    # Statistics
    cursor.execute("SELECT COUNT(*) FROM languages WHERE inferred_modern_country IS NOT NULL")
    mapped = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(inferred_country_confidence) FROM languages WHERE inferred_country_confidence IS NOT NULL")
    avg_conf = cursor.fetchone()[0]
    print(f"Mapped {mapped}/{len(mappings)} languages")
    if avg_conf:
        print(f"Average confidence: {avg_conf:.1f}")

    return mappings


def step2_update_literary_works(conn, language_map):
    """
    Update literary_works.modern_country for records where:
    - countryLabel is NULL (no direct country info)
    - languageLabel exists and can be mapped

    This only affects records without a countryLabel, so it can be re-run
    independently without affecting country-based mappings from script 05.
    """
    print("\n" + "-" * 40)
    print("Step 2: Updating literary_works with language-inferred countries")
    print("-" * 40)

    cursor = conn.cursor()

    # Ensure modern_country column exists
    try:
        cursor.execute("ALTER TABLE literary_works ADD COLUMN modern_country TEXT")
        print("Added 'modern_country' column to literary_works table")
    except sqlite3.OperationalError:
        # Reset language-inferred values (only where countryLabel is NULL)
        print("Resetting language-inferred modern_country values...")
        cursor.execute("UPDATE literary_works SET modern_country = NULL WHERE countryLabel IS NULL")
        conn.commit()

    # Get records where countryLabel is NULL but languageLabel exists
    print("Loading literary works without countryLabel...")
    cursor.execute("""
        SELECT rowid, languageLabel
        FROM literary_works
        WHERE countryLabel IS NULL AND languageLabel IS NOT NULL
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows):,} records to potentially update")

    # Calculate modern_country from language
    updates = []
    updated_count = 0
    for rowid, language_label in tqdm(rows, desc="Processing"):
        if language_label in language_map:
            data = language_map[language_label]
            if isinstance(data, dict):
                country = data.get("country")
            else:
                country = data
            if country:
                updates.append((country, rowid))
                updated_count += 1

    # Update in batches
    if updates:
        print(f"Updating {len(updates):,} records...")
        batch_size = 10000
        for i in tqdm(range(0, len(updates), batch_size), desc="Writing"):
            batch = updates[i:i + batch_size]
            cursor.executemany(
                "UPDATE literary_works SET modern_country = ? WHERE rowid = ?",
                batch
            )
            conn.commit()

    # Final statistics
    cursor.execute("SELECT COUNT(*) FROM literary_works WHERE modern_country IS NOT NULL")
    total_with_country = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM literary_works")
    total = cursor.fetchone()[0]

    cursor.execute("""
        SELECT modern_country, COUNT(*) as cnt
        FROM literary_works
        WHERE modern_country IS NOT NULL
        GROUP BY modern_country
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_countries = cursor.fetchall()

    print(f"\nUpdated {updated_count:,} records with language-inferred country")
    print(f"Total works with modern_country: {total_with_country:,} / {total:,} ({100*total_with_country/total:.1f}%)")
    print("\nTop 10 modern countries (after language inference):")
    for country, count in top_countries:
        print(f"  - {country}: {count:,}")


def main():
    print("=" * 60)
    print("ENRICHMENT: Language to Modern Country Mapping")
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

    # Step 1: Map languages
    language_map = step1_map_languages(client, conn)

    # Step 2: Update literary_works
    step2_update_literary_works(conn, language_map)

    conn.close()
    print("\n" + "=" * 60)
    print(f"Done! Database updated: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
