"""
Cultura Archive - Multi-Source Text Extraction

For each item, tries sources in priority order until one works:
1. Wikisource (best)
2. full_work_url
3. described_at_url
4. Wikipedia
5. document_on_commons
6. official_website
"""

import sqlite3
import json
import time
import re
import requests
import requests.adapters
import threading
import sys
from pathlib import Path
from collections import defaultdict
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# Counter for progress (thread-safe)
_counter_lock = threading.Lock()
_counter = {'done': 0, 'success': 0, 'total': 0}

# Results list (thread-safe incremental saving)
_results_lock = threading.Lock()
_results = []

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR.parent / "wikidata_sparql_scripts/instance_properties/output/instance_properties.db"
DATA_DIR = PROJECT_DIR / "data"
RESULTS_FILE = DATA_DIR / "cultura_archive.json"

# Config
SAMPLE_SIZE = 1000
MAX_WORKERS = 8
MAX_YEAR = 1800
MIN_TEXT_LENGTH = 200
REQUEST_TIMEOUT = 30

# Priority order
SOURCE_PRIORITY = ['wikisource', 'full_work_url', 'described_at_url',
                   'wikipedia', 'document_on_commons', 'official_website']

LANG_PRIORITY = ['en', 'fr', 'de', 'it', 'ru', 'zh']

# Thread-local sessions
_local = threading.local()

def get_session():
    if not hasattr(_local, 'session'):
        s = requests.Session()
        a = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        s.mount('https://', a)
        s.mount('http://', a)
        s.headers['User-Agent'] = 'CulturaArchive/1.0'
        _local.session = s
    return _local.session


def make_request(url, params=None):
    """Make API request, return JSON or None."""
    try:
        r = get_session().get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except:
        return None


def html_to_text(html):
    """Convert HTML to clean text with formatting."""
    from bs4 import BeautifulSoup
    import warnings
    from bs4 import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    soup = BeautifulSoup(html, 'html.parser')

    # Remove junk
    for tag in soup.find_all(['script', 'style', 'noscript', 'link']):
        tag.decompose()
    for cls in ['mw-editsection', 'noprint', 'navbox', 'toc', 'reference',
                'ws-noexport', 'wst-header', 'pagenum']:
        for tag in soup.find_all(class_=lambda x: x and cls in str(x)):
            tag.decompose()

    # Convert formatting
    for tag in soup.find_all('b'):
        tag.name = 'strong'
    for tag in soup.find_all('i'):
        tag.name = 'em'
    for tag in soup.find_all(class_=lambda x: x and 'center' in str(x)):
        tag['style'] = 'text-align:center'

    return str(soup).strip()


# ============ EXTRACTORS ============

def extract_wikisource(url):
    """Extract from Wikisource."""
    parsed = urlparse(url)
    lang = parsed.netloc.split('.')[0]
    title = unquote(parsed.path.replace('/wiki/', ''))

    api = f"https://{lang}.wikisource.org/w/api.php"

    # Check for subpages (multipage work)
    data = make_request(api, {'action': 'query', 'list': 'allpages',
                              'apprefix': title + '/', 'aplimit': 1, 'format': 'json'})
    if data and data.get('query', {}).get('allpages'):
        return None, 'multipage'

    # Get content
    data = make_request(api, {'action': 'parse', 'page': title, 'prop': 'text',
                              'format': 'json', 'disablelimitreport': True})
    if not data:
        return None, 'api_error'

    html = data.get('parse', {}).get('text', {}).get('*', '')
    if not html:
        return None, 'no_content'

    text = html_to_text(html)
    if len(text) < MIN_TEXT_LENGTH:
        return None, 'too_short'

    return text, None


def extract_wikipedia(url):
    """Extract from Wikipedia."""
    parsed = urlparse(url)
    lang = parsed.netloc.split('.')[0]
    title = unquote(parsed.path.replace('/wiki/', ''))

    api = f"https://{lang}.wikipedia.org/w/api.php"

    data = make_request(api, {'action': 'parse', 'page': title, 'prop': 'text',
                              'format': 'json', 'disablelimitreport': True})
    if not data:
        return None, 'api_error'

    html = data.get('parse', {}).get('text', {}).get('*', '')
    if not html:
        return None, 'no_content'

    text = html_to_text(html)
    if len(text) < MIN_TEXT_LENGTH:
        return None, 'too_short'

    return text, None


def extract_web_url(url):
    """Extract from generic web URL."""
    # Skip known problematic URLs
    domain = urlparse(url).netloc.lower()
    if any(x in domain for x in ['jstor.org', 'doi.org', 'books.google']):
        return None, 'paywalled'
    if url.lower().endswith('.pdf'):
        return None, 'pdf_file'

    try:
        r = get_session().get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = html_to_text(r.text)
        if len(text) < MIN_TEXT_LENGTH:
            return None, 'too_short'
        return text, None
    except:
        return None, 'fetch_error'


def extract_commons(url):
    """Extract from Wikimedia Commons (DjVu only)."""
    # Parse filename
    path = unquote(urlparse(url).path)
    if 'Special:FilePath/' in path:
        filename = path.split('Special:FilePath/')[-1]
    else:
        filename = path.split('/')[-1]

    if not filename.lower().endswith('.djvu'):
        return None, 'not_djvu'

    # Get page count
    api = "https://commons.wikimedia.org/w/api.php"
    data = make_request(api, {'action': 'query', 'titles': f'File:{filename}',
                              'prop': 'imageinfo', 'iiprop': 'pagecount', 'format': 'json'})
    if not data:
        return None, 'api_error'

    pages = data.get('query', {}).get('pages', {})
    pagecount = 1
    for pid, pdata in pages.items():
        if pid != '-1':
            pagecount = pdata.get('imageinfo', [{}])[0].get('pagecount', 1) or 1

    # Get text from first 30 pages
    texts = []
    for pnum in range(1, min(pagecount + 1, 30)):
        data = make_request(api, {'action': 'query', 'titles': f'Page:{filename}/{pnum}',
                                  'prop': 'revisions', 'rvprop': 'content', 'format': 'json'})
        if data:
            for pid, pdata in data.get('query', {}).get('pages', {}).items():
                if pid != '-1':
                    revs = pdata.get('revisions', [])
                    if revs:
                        content = revs[0].get('*', '')
                        content = re.sub(r'<noinclude>.*?</noinclude>', '', content, flags=re.DOTALL)
                        content = re.sub(r'\{\{[^}]+\}\}', '', content)
                        if content.strip():
                            texts.append(content.strip())

    if not texts:
        return None, 'no_text'

    text = '\n\n'.join(texts)
    if len(text) < MIN_TEXT_LENGTH:
        return None, 'too_short'

    return text, None


# ============ MAIN LOGIC ============

def get_items():
    """Get items from database with all their sources."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Get properties
    c.execute("""
        SELECT instance_id, instance_label,
               COALESCE(publication_date, inception, earliest_date) as work_date,
               full_work_url, described_at_url, official_website, document_file_on_commons
        FROM instances_properties
        WHERE (CAST(SUBSTR(COALESCE(publication_date, inception, earliest_date), 1, 5) AS INTEGER) < ?
               OR SUBSTR(COALESCE(publication_date, inception, earliest_date), 1, 1) = '-')
    """, (MAX_YEAR,))
    props = {r[0]: r for r in c.fetchall()}

    # Get sitelinks
    c.execute("""SELECT instance_id, sitelink_url, sitelink_type
                 FROM instances_sitelinks WHERE sitelink_type IN ('wikisource', 'wikipedia')""")
    sitelinks = defaultdict(list)
    for r in c.fetchall():
        sitelinks[r[0]].append((r[1], r[2]))

    conn.close()

    # Build items
    items = []
    for qid, row in props.items():
        sources = {}

        # Add sitelinks (prefer English)
        for url, stype in sitelinks.get(qid, []):
            if stype not in sources:
                sources[stype] = url
            else:
                # Prefer English
                old_lang = urlparse(sources[stype]).netloc.split('.')[0]
                new_lang = urlparse(url).netloc.split('.')[0]
                old_prio = LANG_PRIORITY.index(old_lang) if old_lang in LANG_PRIORITY else 99
                new_prio = LANG_PRIORITY.index(new_lang) if new_lang in LANG_PRIORITY else 99
                if new_prio < old_prio:
                    sources[stype] = url

        # Add URL properties
        if row[3]: sources['full_work_url'] = row[3].split(',')[0].strip()
        if row[4]: sources['described_at_url'] = row[4].split(',')[0].strip()
        if row[5]: sources['official_website'] = row[5].split(',')[0].strip()
        if row[6]: sources['document_on_commons'] = row[6].split(',')[0].strip()

        if sources:
            items.append({
                'qid': qid,
                'label': row[1],
                'date': row[2],
                'sources': sources
            })

    return items


def save_results_incremental():
    """Save results to JSON file (called after each item)."""
    with _results_lock:
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(_results, f, indent=2, ensure_ascii=False)


def log_progress(qid, label, source, status, error=None, words=None):
    """Print a single-line log entry."""
    with _counter_lock:
        _counter['done'] += 1
        if status == 'success':
            _counter['success'] += 1

        done = _counter['done']
        total = _counter['total']
        success = _counter['success']

    # Truncate label to 30 chars
    short_label = (label[:27] + '...') if len(label) > 30 else label

    if status == 'success':
        print(f"[{done:4d}/{total}] ✓ {qid} | {source:12s} | {words:6d} words | {short_label}")
    else:
        print(f"[{done:4d}/{total}] ✗ {qid} | {source:12s} | {error:12s} | {short_label}")

    sys.stdout.flush()


def process_item(item):
    """Try each source in priority order until one works."""
    result = {
        'qid': item['qid'],
        'label': item['label'],
        'publication_date': item['date'],
        'available_sources': list(item['sources'].keys()),
    }

    tried_sources = []

    # Try each source in priority order
    for source_type in SOURCE_PRIORITY:
        if source_type not in item['sources']:
            continue

        url = item['sources'][source_type]
        result['url'] = url
        result['source'] = source_type

        # Extract based on source type
        if source_type == 'wikisource':
            text, error = extract_wikisource(url)
        elif source_type == 'wikipedia':
            text, error = extract_wikipedia(url)
        elif source_type == 'document_on_commons':
            text, error = extract_commons(url)
        else:
            text, error = extract_web_url(url)

        if text:
            # Success!
            result['status'] = 'success'
            result['text_length'] = len(text)
            result['word_count'] = len(text.split())
            result['tried_sources'] = tried_sources

            # Save file
            out_dir = DATA_DIR / source_type
            out_dir.mkdir(parents=True, exist_ok=True)
            filepath = out_dir / f"{item['qid']}.txt"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(text)
            result['file'] = str(filepath)

            log_progress(item['qid'], item['label'], source_type, 'success', words=result['word_count'])

            # Save incrementally
            with _results_lock:
                _results.append(result)
            save_results_incremental()

            return result

        # Failed this source, record why
        tried_sources.append({'source': source_type, 'error': error})
        result[f'{source_type}_error'] = error

    # All sources failed - log with last tried source
    result['status'] = 'failed'
    result['error'] = 'all_sources_failed'
    result['tried_sources'] = tried_sources

    last_source = tried_sources[-1]['source'] if tried_sources else 'none'
    last_error = tried_sources[-1]['error'] if tried_sources else 'no_sources'
    log_progress(item['qid'], item['label'], last_source, 'failed', error=last_error)

    # Save incrementally (even failures)
    with _results_lock:
        _results.append(result)
    save_results_incremental()

    return result


def main():
    print("=" * 70)
    print("CULTURA ARCHIVE - Multi-Source Text Extraction")
    print(f"Extracting {SAMPLE_SIZE} works published before {MAX_YEAR}")
    print("=" * 70)

    # Load items
    print("\n[1] Loading items from database...")
    all_items = get_items()
    print(f"    Found {len(all_items)} items with sources")

    # Sample
    import random
    random.shuffle(all_items)
    items = all_items[:SAMPLE_SIZE]
    print(f"    Selected {len(items)} items")

    # Set total for progress counter
    _counter['total'] = len(items)

    # Count best sources (what will be tried FIRST for each item)
    print("\n[2] Best source available per item (will try this first):")
    best_counts = defaultdict(int)
    for item in items:
        for src in SOURCE_PRIORITY:
            if src in item['sources']:
                best_counts[src] += 1
                break
    for src in SOURCE_PRIORITY:
        if best_counts[src]:
            print(f"    {src}: {best_counts[src]}")

    # Extract
    print(f"\n[3] Extracting with {MAX_WORKERS} workers (saving incrementally)...")
    print("-" * 70)
    print(f"{'Progress':<12} | {'QID':<12} | {'Source':<12} | {'Result':<12} | Label")
    print("-" * 70)

    # Reset global results
    global _results
    _results = []

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_item, item): item for item in items}
        for future in as_completed(futures):
            future.result()  # Results already saved in process_item

    elapsed = time.time() - start_time

    # Final save (in case any were missed)
    print("-" * 70)
    print("\n[4] Final save...")
    save_results_incremental()

    # Summary
    results = _results
    success = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']

    by_source = defaultdict(int)
    for r in success:
        by_source[r['source']] += 1

    total_words = sum(r.get('word_count', 0) for r in success)

    print("\n" + "=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)
    print(f"\nSuccess: {len(success)}/{len(results)} ({100*len(success)/len(results):.1f}%)")
    print(f"Failed:  {len(failed)}")
    print(f"\nSuccess by source:")
    for src in SOURCE_PRIORITY:
        if by_source[src]:
            print(f"  {src}: {by_source[src]}")
    print(f"\nTotal words: {total_words:,}")
    print(f"Time: {elapsed:.1f}s ({elapsed/len(results):.2f}s per item)")
    print(f"\nSaved to: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
