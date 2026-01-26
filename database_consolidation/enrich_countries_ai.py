"""
Enrich instance_properties.db with modern country mappings using OpenAI.
Maps historical/regional countries to modern equivalents.

Input: wikidata_sparql_scripts/instance_properties/output/instance_properties.db
Output:
  - Adds columns to prop_PLACE_country_of_origin: modern_country, confidence, prompt_id
  - Creates ai_prompts table to track prompts used
  - Saves mappings to cache/country_mappings.json

Requires: OPENAI_API_KEY environment variable (unless cache exists)
"""

import sqlite3
import json
import os
import hashlib
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "wikidata_sparql_scripts" / "instance_properties" / "output" / "instance_properties.db"
CACHE_DIR = SCRIPT_DIR / "cache"
CACHE_FILE = CACHE_DIR / "country_of_origin_mappings.json"

# Processing settings
BATCH_SIZE = 50  # Countries per OpenAI request
MAX_WORKERS = 8  # Parallel API calls for optimal throughput
MODEL = "gpt-4o-mini"

# Pricing per 1M tokens (as of Jan 2025)
MODEL_PRICING = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
}

# System prompt for mapping
SYSTEM_PROMPT = """You are an expert in geography and history.
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
- "Russian Empire" -> {"country": "Russia", "confidence": 98}
- "Roman Empire" -> {"country": "Italy", "confidence": 90}
- "Qing dynasty" -> {"country": "China", "confidence": 98}
- "Prussia" -> {"country": "Germany", "confidence": 85}
- "Tang dynasty" -> {"country": "China", "confidence": 98}
- "Taiwan" -> {"country": "Taiwan", "confidence": 100}
- "Catalonia" -> {"country": "Spain", "confidence": 92}
- "Fictional Kingdom" -> {"country": null, "confidence": 0}
"""


def generate_prompt_id(prompt_text: str) -> str:
    """Generate a unique prompt ID based on content hash."""
    return hashlib.sha256(prompt_text.encode()).hexdigest()[:16]


def estimate_cost(num_items: int, num_batches: int, model: str = MODEL) -> dict:
    """
    Estimate the cost of an OpenAI API run.

    Returns dict with input_tokens, output_tokens, and estimated_cost_usd.
    """
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-4o-mini"])

    # Estimate tokens per batch
    system_prompt_tokens = 400  # Approximate tokens in system prompt
    tokens_per_item_input = 10  # Average tokens per country name
    tokens_per_item_output = 30  # Average tokens per JSON response entry

    # Calculate totals
    input_tokens_per_batch = system_prompt_tokens + (BATCH_SIZE * tokens_per_item_input)
    output_tokens_per_batch = BATCH_SIZE * tokens_per_item_output

    total_input_tokens = num_batches * input_tokens_per_batch
    total_output_tokens = num_batches * output_tokens_per_batch

    # Calculate cost
    input_cost = (total_input_tokens / 1_000_000) * pricing["input"]
    output_cost = (total_output_tokens / 1_000_000) * pricing["output"]
    total_cost = input_cost + output_cost

    return {
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "estimated_cost_usd": round(total_cost, 4),
    }


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


def process_single_batch(client, batch: list, system_prompt: str) -> dict:
    """Process a single batch of items with OpenAI."""
    items_list = "\n".join([f"- {item}" for item in batch])

    try:
        response = client.chat.completions.create(
            model=MODEL,
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
        return {item: {"country": None, "confidence": 0} for item in batch}


def batch_openai_mapping(client, items: list, system_prompt: str) -> dict:
    """
    Make batched OpenAI requests to map items using parallel processing.
    Returns a dictionary of item -> mapped_value.
    """
    results = {}

    # Create batches
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]

    print(f"Processing {len(items)} items in {len(batches)} batches with {MAX_WORKERS} workers")

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


def setup_ai_prompts_table(conn):
    """Create ai_prompts table to track prompts used for enrichment."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_prompts (
            prompt_id TEXT PRIMARY KEY,
            prompt_name TEXT,
            prompt_content TEXT,
            model TEXT,
            created_at TEXT,
            description TEXT,
            num_items INTEGER,
            num_batches INTEGER,
            input_tokens_estimate INTEGER,
            output_tokens_estimate INTEGER,
            estimated_cost_usd REAL
        )
    """)

    # Add new columns if they don't exist (for existing tables)
    new_columns = [
        ("num_items", "INTEGER"),
        ("num_batches", "INTEGER"),
        ("input_tokens_estimate", "INTEGER"),
        ("output_tokens_estimate", "INTEGER"),
        ("estimated_cost_usd", "REAL"),
    ]
    for col_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE ai_prompts ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()
    print("Created/verified ai_prompts table")


def register_prompt(conn, prompt_content: str, prompt_name: str, description: str) -> str:
    """Register a prompt and return its ID."""
    cursor = conn.cursor()
    prompt_id = generate_prompt_id(prompt_content)

    # Check if prompt already exists
    cursor.execute("SELECT prompt_id FROM ai_prompts WHERE prompt_id = ?", (prompt_id,))
    existing = cursor.fetchone()

    if not existing:
        cursor.execute("""
            INSERT INTO ai_prompts (prompt_id, prompt_name, prompt_content, model, created_at, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (prompt_id, prompt_name, prompt_content, MODEL, datetime.now().isoformat(), description))
        conn.commit()
        print(f"Registered new prompt: {prompt_name} (ID: {prompt_id})")
    else:
        print(f"Using existing prompt: {prompt_name} (ID: {prompt_id})")

    return prompt_id


def update_prompt_cost(conn, prompt_id: str, num_items: int, num_batches: int):
    """Update the prompt record with cost estimation."""
    cursor = conn.cursor()
    cost_info = estimate_cost(num_items, num_batches)

    cursor.execute("""
        UPDATE ai_prompts
        SET num_items = ?,
            num_batches = ?,
            input_tokens_estimate = ?,
            output_tokens_estimate = ?,
            estimated_cost_usd = ?
        WHERE prompt_id = ?
    """, (
        num_items,
        num_batches,
        cost_info["input_tokens"],
        cost_info["output_tokens"],
        cost_info["estimated_cost_usd"],
        prompt_id
    ))
    conn.commit()
    print(f"Updated cost estimate: ${cost_info['estimated_cost_usd']:.4f} "
          f"(~{cost_info['input_tokens']:,} input + ~{cost_info['output_tokens']:,} output tokens)")


def create_enriched_table(conn, source_table: str):
    """Create a new AI-enriched table with original columns + AI enrichment columns."""
    cursor = conn.cursor()
    enriched_table = f"{source_table}_ai_enriched"

    # Drop and recreate the enriched table
    cursor.execute(f"DROP TABLE IF EXISTS {enriched_table}")

    # Create table with original columns + AI enrichment columns
    cursor.execute(f"""
        CREATE TABLE {enriched_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value_id TEXT,
            value_label TEXT,
            occurrence_count INTEGER,
            modern_country TEXT,
            confidence INTEGER,
            prompt_id TEXT
        )
    """)

    # Copy data from source table (original columns only, AI columns will be updated later)
    cursor.execute(f"""
        INSERT INTO {enriched_table} (id, value_id, value_label, occurrence_count)
        SELECT id, value_id, value_label, occurrence_count
        FROM {source_table}
    """)

    conn.commit()
    print(f"Created table: {enriched_table}")
    return enriched_table


def enrich_country_of_origin(client, conn, prompt_id: str):
    """Enrich prop_PLACE_country_of_origin with modern country mappings."""
    print("\n" + "=" * 60)
    print("Enriching prop_PLACE_country_of_origin")
    print("=" * 60)

    cursor = conn.cursor()
    source_table = "prop_PLACE_country_of_origin"

    # Create the enriched table with original + AI columns
    enriched_table = create_enriched_table(conn, source_table)

    # Check cache first
    cached_mappings = load_cache()
    total_batches = 0
    num_items = 0

    if cached_mappings:
        print(f"Loaded {len(cached_mappings)} mappings from cache: {CACHE_FILE}")
        mappings = cached_mappings
        num_items = len(cached_mappings)
        # Estimate batches from cached data
        total_batches = (num_items + BATCH_SIZE - 1) // BATCH_SIZE
    else:
        # Get all unique country labels that need mapping
        cursor.execute(f"""
            SELECT DISTINCT value_label
            FROM {enriched_table}
            WHERE value_label IS NOT NULL
        """)
        countries = [row[0] for row in cursor.fetchall()]
        num_items = len(countries)
        print(f"Found {num_items} unique countries to map")

        if not countries:
            print("No countries to process")
            return

        # First pass with OpenAI
        print("Calling OpenAI API (pass 1)...")
        mappings = batch_openai_mapping(client, countries, SYSTEM_PROMPT)
        first_pass_batches = (len(countries) + BATCH_SIZE - 1) // BATCH_SIZE
        total_batches = first_pass_batches

        # Second pass for NULLs - try again with more context
        null_items = [
            k for k, v in mappings.items()
            if v is None or (isinstance(v, dict) and v.get("country") is None)
        ]

        if null_items and len(null_items) < len(countries):
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
            second_pass_batches = (len(null_items) + BATCH_SIZE - 1) // BATCH_SIZE
            total_batches += second_pass_batches

            # Update mappings with successful retries
            for k, v in retry_mappings.items():
                if v is not None and isinstance(v, dict) and v.get("country") is not None:
                    mappings[k] = v

        # Save to cache
        save_cache(mappings)

    # Update prompt with cost estimate
    update_prompt_cost(conn, prompt_id, num_items, total_batches)

    # Update database
    print("Updating database...")
    update_count = 0

    for country_label, data in tqdm(mappings.items(), desc="Updating records"):
        if isinstance(data, dict):
            modern_country = data.get("country")
            confidence = data.get("confidence", 0)
        else:
            modern_country = data
            confidence = None

        cursor.execute(f"""
            UPDATE {enriched_table}
            SET modern_country = ?, confidence = ?, prompt_id = ?
            WHERE value_label = ?
        """, (modern_country, confidence, prompt_id, country_label))
        update_count += cursor.rowcount

    conn.commit()

    # Print statistics
    print("\n" + "-" * 40)
    print("Statistics:")
    print("-" * 40)

    cursor.execute(f"SELECT COUNT(*) FROM {enriched_table}")
    total = cursor.fetchone()[0]

    cursor.execute(f"SELECT COUNT(*) FROM {enriched_table} WHERE modern_country IS NOT NULL")
    mapped = cursor.fetchone()[0]

    cursor.execute(f"SELECT AVG(confidence) FROM {enriched_table} WHERE confidence IS NOT NULL")
    avg_conf = cursor.fetchone()[0]

    print(f"Total records: {total}")
    print(f"Mapped to modern country: {mapped} ({mapped/total*100:.1f}%)")
    if avg_conf:
        print(f"Average confidence: {avg_conf:.1f}")

    # Top 10 modern countries
    cursor.execute(f"""
        SELECT modern_country, COUNT(*) as cnt, SUM(occurrence_count) as total_occurrences
        FROM {enriched_table}
        WHERE modern_country IS NOT NULL
        GROUP BY modern_country
        ORDER BY total_occurrences DESC
        LIMIT 10
    """)
    top_countries = cursor.fetchall()

    print("\nTop 10 modern countries (by total occurrences):")
    for country, cnt, total_occ in top_countries:
        print(f"  - {country}: {cnt} entries, {total_occ:,} total occurrences")


def main():
    print("=" * 60)
    print("AI ENRICHMENT: Country of Origin to Modern Country Mapping")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Model: {MODEL}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Max workers: {MAX_WORKERS}")

    # Check database exists
    if not DB_PATH.exists():
        print(f"ERROR: Database not found: {DB_PATH}")
        return

    # Check cache or API key
    cached_mappings = load_cache()
    client = None

    if not cached_mappings:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not found and no cache available")
            print(f"Either set OPENAI_API_KEY or provide cache file: {CACHE_FILE}")
            return
        client = OpenAI(api_key=api_key)
        print("OpenAI client initialized")
    else:
        print(f"Using cached mappings ({len(cached_mappings)} entries)")

    # Connect to database
    conn = sqlite3.connect(DB_PATH)

    # Setup AI prompts table
    setup_ai_prompts_table(conn)

    # Register the prompt
    prompt_id = register_prompt(
        conn,
        prompt_content=SYSTEM_PROMPT,
        prompt_name="country_to_modern_country_v1",
        description="Maps historical, regional, or ancient entities to modern country equivalents with confidence scores"
    )

    # Enrich country_of_origin
    enrich_country_of_origin(client, conn, prompt_id)

    conn.close()

    print("\n" + "=" * 60)
    print(f"Done! Database updated: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
