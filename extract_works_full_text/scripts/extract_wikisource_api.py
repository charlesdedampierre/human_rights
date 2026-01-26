"""
Extract full text from Wikisource using the MediaWiki API directly.

This script uses API endpoints instead of HTML parsing:
- action=query with prop=extracts for plain text (if available)
- action=query with prop=revisions for wikitext
- Batch requests (up to 50 pages at once) for efficiency

Saves each text as {QID}.txt in the full_text/ directory.
"""

import sqlite3
import requests
import time
import re
import json
from urllib.parse import urlparse, unquote
from pathlib import Path
from tqdm import tqdm
from dataclasses import dataclass
from datetime import datetime
import mwparserfromhell  # pip install mwparserfromhell

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR.parent / "wikidata_sparql_scripts/instance_properties/output/instance_properties.db"
OUTPUT_DIR = PROJECT_DIR / "data" / "instance_full_text_api"
PROGRESS_FILE = PROJECT_DIR / "data" / "_extraction_progress_api.json"
FAILED_FILE = PROJECT_DIR / "data" / "_failed_api.json"
STATS_FILE = PROJECT_DIR / "data" / "_stats_api.json"

# Request settings
DELAY_BETWEEN_REQUESTS = 0.1
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BATCH_SIZE = 50  # MediaWiki API allows up to 50 titles per request

# Filtering
ENGLISH_ONLY = True
MIN_TEXT_LENGTH = 100
MAX_SUBPAGES = 100
TEST_LIMIT = 10  # Set to None for full run, or a number for testing

HEADERS = {
    'User-Agent': 'WikisourceExtractor/2.0 (academic research project; API-based)'
}

# Session with connection pooling
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount('http://', adapter)
session.mount('https://', adapter)
session.headers.update(HEADERS)


@dataclass
class ExtractionResult:
    """Result of extracting text from Wikisource."""
    qid: str
    url: str
    lang: str
    title: str
    status: str  # 'success', 'failed', 'skipped'
    method: str  # 'extracts', 'wikitext', 'subpages'
    text_length: int
    subpages_fetched: int = 0
    error_message: str | None = None
    text: str | None = None


def parse_wikisource_url(url: str) -> tuple[str, str]:
    """Extract language code and page title from Wikisource URL."""
    parsed = urlparse(url)
    lang = parsed.netloc.split('.')[0]
    title = parsed.path.replace('/wiki/', '')
    title = unquote(title)
    return lang, title


def make_api_request(lang: str, params: dict, retries: int = MAX_RETRIES) -> dict | None:
    """Make an API request with retry logic."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"
    params['format'] = 'json'

    for attempt in range(retries):
        try:
            response = session.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
            else:
                return None
    return None


def get_text_via_extracts(lang: str, title: str) -> str | None:
    """
    Try to get plain text using the TextExtracts API.
    This is the cleanest method if available.
    """
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'extracts',
        'explaintext': True,  # Return plain text, not HTML
        'exsectionformat': 'plain',
    }

    data = make_api_request(lang, params)
    if not data:
        return None

    pages = data.get('query', {}).get('pages', {})
    for page_id, page_data in pages.items():
        if page_id == '-1':  # Page doesn't exist
            return None
        extract = page_data.get('extract', '')
        if extract and len(extract) > MIN_TEXT_LENGTH:
            return extract.strip()

    return None


def wikitext_to_plaintext(wikitext: str) -> str:
    """
    Convert wikitext to plain text using mwparserfromhell.
    This handles templates, links, formatting, etc.
    """
    try:
        parsed = mwparserfromhell.parse(wikitext)

        # Remove templates (like {{header}}, {{PD-old}}, etc.)
        for template in parsed.filter_templates():
            try:
                parsed.remove(template)
            except ValueError:
                pass

        # Remove categories
        for link in parsed.filter_wikilinks():
            link_str = str(link.title)
            if link_str.startswith('Category:') or link_str.startswith(':Category:'):
                try:
                    parsed.remove(link)
                except ValueError:
                    pass

        # Get plain text
        text = parsed.strip_code()

        # Clean up
        text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)  # [[link|text]] -> text
        text = re.sub(r"'{2,}", '', text)  # Remove bold/italic markers
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)  # Remove refs
        text = re.sub(r'<ref[^/]*?/>', '', text)  # Remove self-closing refs
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)  # Remove comments
        text = re.sub(r'__[A-Z]+__', '', text)  # Remove magic words like __NOTOC__

        # Normalize whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)

        return text.strip()
    except Exception as e:
        return wikitext  # Return raw if parsing fails


def get_text_via_revisions(lang: str, title: str) -> str | None:
    """
    Get page content via the revisions API (returns wikitext).
    Then convert wikitext to plain text.
    """
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'revisions',
        'rvprop': 'content',
        'rvslots': 'main',
    }

    data = make_api_request(lang, params)
    if not data:
        return None

    pages = data.get('query', {}).get('pages', {})
    for page_id, page_data in pages.items():
        if page_id == '-1':
            return None

        revisions = page_data.get('revisions', [])
        if not revisions:
            return None

        # Get content from the main slot
        slots = revisions[0].get('slots', {})
        main_slot = slots.get('main', {})
        content = main_slot.get('*', '') or main_slot.get('content', '')

        # Fallback for older API format
        if not content:
            content = revisions[0].get('*', '')

        if content:
            return wikitext_to_plaintext(content)

    return None


def get_subpages(lang: str, title: str) -> list[str]:
    """Get list of subpages for a work."""
    params = {
        'action': 'query',
        'list': 'allpages',
        'apprefix': title + '/',
        'aplimit': 500,
    }

    data = make_api_request(lang, params)
    if not data:
        return []

    pages = data.get('query', {}).get('allpages', [])
    return [p['title'] for p in pages]


def get_batch_revisions(lang: str, titles: list[str]) -> dict[str, str]:
    """
    Get wikitext content for multiple pages in a single API call.
    Returns dict mapping title -> wikitext content.
    """
    if not titles:
        return {}

    # API allows up to 50 titles per request
    titles_str = '|'.join(titles[:50])

    params = {
        'action': 'query',
        'titles': titles_str,
        'prop': 'revisions',
        'rvprop': 'content',
        'rvslots': 'main',
    }

    data = make_api_request(lang, params)
    if not data:
        return {}

    results = {}
    pages = data.get('query', {}).get('pages', {})

    # Handle title normalization
    normalized = {}
    for norm in data.get('query', {}).get('normalized', []):
        normalized[norm['from']] = norm['to']

    for page_id, page_data in pages.items():
        if page_id == '-1':
            continue

        page_title = page_data.get('title', '')
        revisions = page_data.get('revisions', [])

        if revisions:
            slots = revisions[0].get('slots', {})
            main_slot = slots.get('main', {})
            content = main_slot.get('*', '') or main_slot.get('content', '')

            if not content:
                content = revisions[0].get('*', '')

            if content:
                results[page_title] = content

    return results


def sort_subpages(subpages: list[str]) -> list[str]:
    """Sort subpages, handling numeric chapter ordering."""
    def sort_key(s):
        parts = s.rsplit('/', 1)
        if len(parts) > 1:
            suffix = parts[1]
            match = re.match(r'^(\d+)', suffix)
            if match:
                return (0, int(match.group(1)), suffix)
            return (1, suffix.lower())
        return (2, s)

    return sorted(subpages, key=sort_key)


def extract_multipage_batch(lang: str, title: str, subpages: list[str]) -> tuple[str | None, int]:
    """
    Extract text from a multi-page work using batch API requests.
    Much faster than fetching pages one by one.
    """
    subpages = sort_subpages(subpages)[:MAX_SUBPAGES]

    all_texts = []
    fetched = 0

    # Process in batches of BATCH_SIZE
    for i in range(0, len(subpages), BATCH_SIZE):
        batch = subpages[i:i + BATCH_SIZE]
        contents = get_batch_revisions(lang, batch)

        for subpage in batch:
            content = contents.get(subpage)
            if content:
                text = wikitext_to_plaintext(content)
                if text and len(text) > 50:
                    section_name = subpage.split('/')[-1] if '/' in subpage else subpage
                    all_texts.append(f"\n\n=== {section_name} ===\n\n{text}")
                    fetched += 1

        time.sleep(DELAY_BETWEEN_REQUESTS)

    if all_texts:
        return '\n'.join(all_texts), fetched
    return None, 0


def extract_full_text_api(lang: str, title: str) -> ExtractionResult:
    """
    Extract full text from a Wikisource page using API methods.

    Strategy:
    1. Check for subpages first (multi-page works)
    2. Try TextExtracts API (cleanest, if available)
    3. Fall back to revisions API + wikitext parsing
    """
    url = f"https://{lang}.wikisource.org/wiki/{title}"

    result = ExtractionResult(
        qid='',
        url=url,
        lang=lang,
        title=title,
        status='failed',
        method='none',
        text_length=0
    )

    # Step 1: Check for subpages
    subpages = get_subpages(lang, title)

    if subpages:
        # Multi-page work - use batch extraction
        result.method = 'subpages'
        text, fetched = extract_multipage_batch(lang, title, subpages)
        result.subpages_fetched = fetched

        if text and len(text) >= MIN_TEXT_LENGTH:
            result.status = 'success'
            result.text_length = len(text)
            result.text = text
            return result

    # Step 2: Try TextExtracts API
    text = get_text_via_extracts(lang, title)
    if text and len(text) >= MIN_TEXT_LENGTH:
        result.status = 'success'
        result.method = 'extracts'
        result.text_length = len(text)
        result.text = text
        return result

    # Step 3: Fall back to revisions API
    text = get_text_via_revisions(lang, title)
    if text and len(text) >= MIN_TEXT_LENGTH:
        result.status = 'success'
        result.method = 'wikitext'
        result.text_length = len(text)
        result.text = text
        return result

    result.error_message = f'Text too short or not found ({len(text) if text else 0} chars)'
    return result


def load_json_file(filepath: Path, default=None):
    """Load JSON file, return default if not exists."""
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default if default is not None else {}


def save_json_file(filepath: Path, data):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    """Main extraction pipeline using API methods."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build query
    if ENGLISH_ONLY:
        query = """
            SELECT instance_id, instance_label, sitelink_url
            FROM instances_sitelinks
            WHERE sitelink_type = 'wikisource'
            AND sitelink_url LIKE '%en.wikisource%'
        """
    else:
        query = """
            SELECT instance_id, instance_label, sitelink_url
            FROM instances_sitelinks
            WHERE sitelink_type = 'wikisource'
        """

    cursor.execute(query)
    items = cursor.fetchall()
    conn.close()

    print(f"Found {len(items)} Wikisource items in database")
    print("Using API-based extraction (TextExtracts + Revisions + Batch)")

    # Load progress
    progress = load_json_file(PROGRESS_FILE, {'processed': [], 'stats': {}})
    processed = set(progress.get('processed', []))
    failed = load_json_file(FAILED_FILE, {'items': []})
    failed_qids = {f['qid'] for f in failed.get('items', [])}

    # Check already existing files
    existing_files = {f.stem for f in OUTPUT_DIR.glob("Q*.txt")}
    processed = processed.union(existing_files)

    # Filter out already processed
    items_to_process = [
        (qid, label, url) for qid, label, url in items
        if qid not in processed and qid not in failed_qids
    ]

    # Apply test limit if set
    if TEST_LIMIT:
        items_to_process = items_to_process[:TEST_LIMIT]
        print(f"TEST MODE: Limited to {TEST_LIMIT} items")

    print(f"Already processed: {len(processed)}, failed: {len(failed_qids)}, to process: {len(items_to_process)}")

    if not items_to_process:
        print("All items already processed!")
        return

    # Stats
    stats = {
        'start_time': datetime.now().isoformat(),
        'total_items': len(items),
        'by_method': {'extracts': 0, 'wikitext': 0, 'subpages': 0, 'none': 0},
        'by_status': {'success': 0, 'failed': 0},
        'total_chars': 0,
        'total_subpages': 0,
    }

    new_failed = []

    with tqdm(total=len(items_to_process), desc="Extracting (API)") as pbar:
        for qid, label, url in items_to_process:
            try:
                lang, title = parse_wikisource_url(url)
            except Exception as e:
                new_failed.append({'qid': qid, 'url': url, 'error': str(e)})
                stats['by_status']['failed'] += 1
                pbar.update(1)
                continue

            # Extract
            result = extract_full_text_api(lang, title)
            result.qid = qid

            # Update stats
            stats['by_method'][result.method] += 1
            stats['by_status'][result.status] += 1

            if result.status == 'success' and result.text:
                output_file = OUTPUT_DIR / f"{qid}.txt"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(result.text)

                processed.add(qid)
                stats['total_chars'] += result.text_length
                stats['total_subpages'] += result.subpages_fetched
            else:
                new_failed.append({
                    'qid': qid,
                    'url': url,
                    'label': label,
                    'method': result.method,
                    'error': result.error_message
                })

            pbar.set_postfix(
                ok=stats['by_status']['success'],
                fail=stats['by_status']['failed'],
                method=result.method[:4]
            )
            pbar.update(1)

            # Save progress periodically
            if (stats['by_status']['success'] + stats['by_status']['failed']) % 100 == 0:
                progress['processed'] = list(processed)
                progress['stats'] = stats
                save_json_file(PROGRESS_FILE, progress)

                failed['items'].extend(new_failed)
                save_json_file(FAILED_FILE, failed)
                new_failed = []

            time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final save
    stats['end_time'] = datetime.now().isoformat()
    progress['processed'] = list(processed)
    progress['stats'] = stats
    save_json_file(PROGRESS_FILE, progress)

    failed['items'].extend(new_failed)
    save_json_file(FAILED_FILE, failed)
    save_json_file(STATS_FILE, stats)

    # Summary
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE (API Method)")
    print(f"{'='*60}")
    print(f"\nBy status:")
    for status, count in stats['by_status'].items():
        print(f"  {status}: {count}")

    print(f"\nBy extraction method:")
    for method, count in stats['by_method'].items():
        if count > 0:
            print(f"  {method}: {count}")

    print(f"\nTotal characters: {stats['total_chars']:,}")
    print(f"Total subpages fetched: {stats['total_subpages']}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
