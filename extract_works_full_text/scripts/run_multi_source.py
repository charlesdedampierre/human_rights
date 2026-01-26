"""
Cultura Archive - Multi-Source Text Extraction

Extracts texts from multiple sources with priority ordering:
1. Wikisource (highest priority)
2. full_work_url
3. described_at_url
4. Wikipedia
5. document_on_commons
6. official_website (lowest priority)
"""

import sqlite3
import json
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import sys

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from extractors import (
    WikisourceExtractor,
    WikipediaExtractor,
    CommonsExtractor,
    WebURLExtractor,
)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR.parent / "wikidata_sparql_scripts/instance_properties/output/instance_properties.db"
DATA_DIR = PROJECT_DIR / "data"
RESULTS_FILE = DATA_DIR / "cultura_archive.json"

# Configuration
SAMPLE_SIZE = 1000
MAX_WORKERS = 10
MAX_YEAR = 1800

# Source priority (highest to lowest)
SOURCE_PRIORITY = [
    'wikisource',
    'full_work_url',
    'described_at_url',
    'wikipedia',
    'document_on_commons',
    'official_website',
]

# Language priority for Wikisource/Wikipedia
LANG_PRIORITY = ['en', 'fr', 'de', 'it', 'ru', 'zh']

print_lock = Lock()


def get_items_from_db() -> list[dict]:
    """Get all items with their available sources."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all items before MAX_YEAR with any available source
    cursor.execute("""
        SELECT
            p.instance_id,
            p.instance_label,
            COALESCE(p.publication_date, p.inception, p.earliest_date) as work_date,
            p.full_work_url,
            p.described_at_url,
            p.official_website,
            p.document_file_on_commons
        FROM instances_properties p
        WHERE (
            CAST(SUBSTR(COALESCE(p.publication_date, p.inception, p.earliest_date), 1, 5) AS INTEGER) < ?
            OR SUBSTR(COALESCE(p.publication_date, p.inception, p.earliest_date), 1, 1) = '-'
        )
    """, (MAX_YEAR,))

    property_rows = cursor.fetchall()

    # Get sitelinks for these items
    cursor.execute("""
        SELECT instance_id, sitelink_url, sitelink_type
        FROM instances_sitelinks
        WHERE sitelink_type IN ('wikisource', 'wikipedia')
    """)

    sitelink_rows = cursor.fetchall()
    conn.close()

    # Build sitelinks lookup
    sitelinks = defaultdict(list)
    for row in sitelink_rows:
        sitelinks[row[0]].append({'url': row[1], 'type': row[2]})

    # Build items with all available sources
    items = []
    for row in property_rows:
        qid = row[0]
        item = {
            'qid': qid,
            'label': row[1],
            'publication_date': row[2],
            'sources': {},
        }

        # Add property-based sources
        if row[3]:  # full_work_url
            item['sources']['full_work_url'] = row[3].split(',')[0].strip()
        if row[4]:  # described_at_url
            item['sources']['described_at_url'] = row[4].split(',')[0].strip()
        if row[5]:  # official_website
            item['sources']['official_website'] = row[5].split(',')[0].strip()
        if row[6]:  # document_file_on_commons
            item['sources']['document_on_commons'] = row[6].split(',')[0].strip()

        # Add sitelink-based sources
        for sl in sitelinks.get(qid, []):
            if sl['type'] == 'wikisource':
                # Prefer by language priority
                if 'wikisource' not in item['sources']:
                    item['sources']['wikisource'] = sl['url']
                else:
                    current_lang = get_lang_from_url(item['sources']['wikisource'])
                    new_lang = get_lang_from_url(sl['url'])
                    if get_lang_priority(new_lang) < get_lang_priority(current_lang):
                        item['sources']['wikisource'] = sl['url']

            elif sl['type'] == 'wikipedia':
                if 'wikipedia' not in item['sources']:
                    item['sources']['wikipedia'] = sl['url']
                else:
                    current_lang = get_lang_from_url(item['sources']['wikipedia'])
                    new_lang = get_lang_from_url(sl['url'])
                    if get_lang_priority(new_lang) < get_lang_priority(current_lang):
                        item['sources']['wikipedia'] = sl['url']

        # Only include items with at least one source
        if item['sources']:
            items.append(item)

    return items


def get_lang_from_url(url: str) -> str:
    """Extract language code from URL."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc.split('.')[0]
    except:
        return 'zz'


def get_lang_priority(lang: str) -> int:
    """Get priority for a language (lower is better)."""
    try:
        return LANG_PRIORITY.index(lang)
    except ValueError:
        return len(LANG_PRIORITY)


def get_best_source(item: dict) -> tuple[str, str] | None:
    """Get the best available source for an item."""
    for source_type in SOURCE_PRIORITY:
        if source_type in item['sources']:
            return source_type, item['sources'][source_type]
    return None


def create_extractor(source_type: str):
    """Create the appropriate extractor for a source type."""
    output_dir = DATA_DIR / source_type

    if source_type == 'wikisource':
        return WikisourceExtractor(output_dir)
    elif source_type == 'wikipedia':
        return WikipediaExtractor(output_dir)
    elif source_type == 'document_on_commons':
        return CommonsExtractor(output_dir)
    else:
        return WebURLExtractor(output_dir, source_type)


def extract_item(item: dict) -> dict:
    """Extract text from the best available source."""
    best = get_best_source(item)
    if not best:
        return {
            'qid': item['qid'],
            'label': item['label'],
            'status': 'error',
            'error': 'No source available',
        }

    source_type, url = best

    # Create extractor for this source
    extractor = create_extractor(source_type)

    # Prepare item for extractor
    extract_item = {
        'qid': item['qid'],
        'label': item['label'],
        'url': url,
        'publication_date': item.get('publication_date'),
    }

    # Extract
    result = extractor.extract(extract_item)
    result['available_sources'] = list(item['sources'].keys())

    return result


def main():
    print("=" * 70)
    print("CULTURA ARCHIVE - Multi-Source Text Extraction")
    print(f"Target: {SAMPLE_SIZE} works before {MAX_YEAR}")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 70)

    # Get items
    print("\n[1/3] Loading items from database...")
    all_items = get_items_from_db()
    print(f"  Found {len(all_items)} items with sources before {MAX_YEAR}")

    # Sample if needed
    if len(all_items) > SAMPLE_SIZE:
        import random
        random.shuffle(all_items)
        items = all_items[:SAMPLE_SIZE]
    else:
        items = all_items

    print(f"  Selected {len(items)} items for extraction")

    # Count sources
    source_counts = defaultdict(int)
    for item in items:
        best = get_best_source(item)
        if best:
            source_counts[best[0]] += 1

    print("\n  Sources to extract from:")
    for source in SOURCE_PRIORITY:
        if source_counts[source]:
            print(f"    {source}: {source_counts[source]}")

    # Extract
    print("\n[2/3] Extracting texts...")
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(extract_item, item): item for item in items}

        with tqdm(total=len(items), desc="Extracting", ncols=80) as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)

    # Save results
    print("\n[3/3] Saving results...")
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Summary
    success = [r for r in results if r.get('status') == 'success']
    skipped = [r for r in results if r.get('status') == 'skipped']
    errors = [r for r in results if r.get('status') == 'error']

    # By source
    success_by_source = defaultdict(int)
    for r in success:
        success_by_source[r.get('source', 'unknown')] += 1

    # Skip reasons
    skip_reasons = defaultdict(int)
    for r in skipped:
        skip_reasons[r.get('reason', 'unknown')] += 1

    total_words = sum(r.get('word_count', 0) for r in success)

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"\nSuccess: {len(success)}/{len(results)} ({len(success)/len(results)*100:.1f}%)")

    print("\nBy source:")
    for source in SOURCE_PRIORITY:
        if success_by_source[source]:
            print(f"  {source}: {success_by_source[source]}")

    print(f"\nSkipped: {len(skipped)}")
    for reason, cnt in sorted(skip_reasons.items(), key=lambda x: -x[1]):
        print(f"  - {reason}: {cnt}")

    print(f"\nErrors: {len(errors)}")
    print(f"\nTotal words extracted: {total_words:,}")
    print(f"\nResults saved to: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
