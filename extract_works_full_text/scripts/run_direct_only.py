"""
Extract Wikisource texts - DIRECT PAGES ONLY (skip multipage/portal).
Fast and reliable extraction for single-page works.
"""

import sqlite3
import json
import time
import re
from pathlib import Path
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import requests
import requests.adapters
import threading

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR.parent / "wikidata_sparql_scripts/instance_properties/output/instance_properties.db"
OUTPUT_DIR = PROJECT_DIR / "data" / "direct_texts"
RESULTS_FILE = PROJECT_DIR / "data" / "direct_texts.json"

# Configuration
SAMPLE_SIZE = 1000  # Max items to fetch
MAX_WORKERS = 10
REQUEST_TIMEOUT = 30
MIN_TEXT_LENGTH = 200
MAX_YEAR = 1800  # Only works published before this year

# Language priority (prefer English, then French, etc.)
LANG_PRIORITY = ['en', 'fr', 'de', 'it', 'ru', 'zh']

# Thread-local sessions
_thread_local = threading.local()
print_lock = Lock()

HEADERS = {
    'User-Agent': 'WikisourceExtractor/1.0 (academic research)'
}


def get_session():
    """Thread-local session with connection pooling."""
    if not hasattr(_thread_local, 'session'):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)
        session.headers.update(HEADERS)
        _thread_local.session = session
    return _thread_local.session


def parse_url(url: str) -> tuple[str, str]:
    """Extract lang and title from Wikisource URL."""
    parsed = urlparse(url)
    lang = parsed.netloc.split('.')[0]
    title = unquote(parsed.path.replace('/wiki/', ''))
    return lang, title


def get_text_extract(lang: str, title: str) -> str | None:
    """Get text with formatting preserved using HTML parsing."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"

    # Use HTML parsing to preserve formatting (bold, center, etc.)
    params = {
        'action': 'parse',
        'page': title,
        'prop': 'text',
        'format': 'json',
        'disablelimitreport': True,
    }

    try:
        resp = get_session().get(api_url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        html = data.get('parse', {}).get('text', {}).get('*', '')
        if html:
            return html_to_text(html)
    except Exception:
        pass

    return None


def html_to_text(html: str, preserve_formatting: bool = True) -> str:
    """Convert HTML to clean text, preserving formatting like bold/italic/center."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')

    # Remove unwanted elements
    for tag in soup.find_all(['script', 'style', 'noscript', 'link']):
        tag.decompose()

    # Remove navigation/noprint elements
    for cls in ['mw-editsection', 'noprint', 'navbox', 'toc', 'catlinks', 'mw-empty-elt',
                'ws-noexport', 'wst-header', 'pagenum', 'ws-pagenum']:
        for tag in soup.find_all(class_=lambda x: x and cls in x):
            tag.decompose()

    if preserve_formatting:
        # Keep basic formatting tags
        for tag in soup.find_all('b'):
            tag.name = 'strong'
        for tag in soup.find_all('i'):
            tag.name = 'em'

        # Convert wst-center class to inline style
        for tag in soup.find_all(class_=lambda x: x and 'wst-center' in str(x)):
            tag['style'] = 'text-align:center'
            if tag.has_attr('class'):
                del tag['class']

        # Convert <center> tags
        for tag in soup.find_all('center'):
            tag.name = 'div'
            tag['style'] = 'text-align:center'

        # Handle font-size spans
        for tag in soup.find_all('span', style=re.compile(r'font-size')):
            style = tag.get('style', '')
            if '120%' in style or '130%' in style or '140%' in style:
                tag.name = 'strong'
                del tag['style']

        # Keep only safe tags
        allowed_tags = ['p', 'div', 'br', 'strong', 'em', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'blockquote', 'span']

        for tag in soup.find_all(True):
            if tag.name not in allowed_tags:
                tag.unwrap()

        # Clean attributes except style
        for tag in soup.find_all(True):
            attrs_to_keep = {}
            if tag.has_attr('style'):
                attrs_to_keep['style'] = tag['style']
            tag.attrs = attrs_to_keep

        html_out = str(soup)

        # Clean up wiki artifacts
        html_out = re.sub(r'\[\[[^\]]+\]\]', '', html_out)
        html_out = re.sub(r'\{\{[^}]+\}\}', '', html_out)
        # Remove empty tags
        html_out = re.sub(r'<(div|p|span)[^>]*>\s*</\1>', '', html_out)
        html_out = re.sub(r'\n{3,}', '\n\n', html_out)

        return html_out.strip()
    else:
        # Plain text mode
        text = soup.get_text(separator='\n')
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        text = '\n'.join(lines)
        text = re.sub(r'\[\[[^\]]+\]\]', '', text)
        text = re.sub(r'\{\{[^}]+\}\}', '', text)
        return text.strip()


def has_subpages(lang: str, title: str) -> bool:
    """Check if page has subpages (meaning it's multipage, not direct)."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"
    params = {
        'action': 'query',
        'list': 'allpages',
        'apprefix': title + '/',
        'aplimit': 1,
        'format': 'json',
    }

    try:
        resp = get_session().get(api_url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        pages = data.get('query', {}).get('allpages', [])
        return len(pages) > 0
    except Exception:
        return True  # Assume has subpages on error (skip it)


def is_portal_page(text: str) -> bool:
    """Check if text looks like a portal/disambiguation page."""
    if not text or len(text) < 500:
        return True

    text_lower = text.lower()
    portal_indicators = [
        'translations', 'editions', 'versions',
        'translated by', 'see also:', 'may refer to'
    ]

    indicator_count = sum(1 for ind in portal_indicators if ind in text_lower)

    # If short text with portal indicators, it's likely a portal
    if len(text) < 2000 and indicator_count >= 2:
        return True

    return False


def extract_item(item: dict) -> dict:
    """Extract text for a single item."""
    qid = item['qid']
    url = item['url']
    label = item['label']

    result = {
        'qid': qid,
        'label': label,
        'url': url,
        'sitelinks': item.get('sitelinks', 0),
        'publication_date': item.get('publication_date'),
    }

    try:
        lang, title = parse_url(url)
        result['lang'] = lang
        result['title'] = title
    except Exception as e:
        result['status'] = 'error'
        result['error'] = f'URL parse error: {e}'
        return result

    # Check for subpages first (skip multipage works)
    if has_subpages(lang, title):
        result['status'] = 'skipped'
        result['reason'] = 'multipage'
        return result

    # Get text
    text = get_text_extract(lang, title)

    if not text:
        result['status'] = 'error'
        result['error'] = 'No text returned'
        return result

    # Check if it's a portal page
    if is_portal_page(text):
        result['status'] = 'skipped'
        result['reason'] = 'portal'
        return result

    # Check minimum length
    if len(text) < MIN_TEXT_LENGTH:
        result['status'] = 'skipped'
        result['reason'] = f'too_short ({len(text)} chars)'
        return result

    # Success!
    result['status'] = 'success'
    result['text_length'] = len(text)
    result['word_count'] = len(text.split())
    result['preview'] = text[:300] + '...' if len(text) > 300 else text

    # Save text file
    filename = f"{qid}.txt"
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(text)
    result['file'] = str(filepath)

    return result


def main():
    print("=" * 60)
    print(f"DIRECT PAGES EXTRACTION (sample: {SAMPLE_SIZE}, before {MAX_YEAR})")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get items from database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all pre-1800 works from all Wikisource languages
    cursor.execute("""
        SELECT s.instance_id, s.instance_label, s.sitelink_url, counts.cnt,
               COALESCE(p.publication_date, p.inception, p.earliest_date) as work_date
        FROM instances_sitelinks s
        INNER JOIN (
            SELECT instance_id, COUNT(*) as cnt
            FROM instances_sitelinks
            GROUP BY instance_id
        ) counts ON s.instance_id = counts.instance_id
        INNER JOIN instances_properties p ON s.instance_id = p.instance_id
        WHERE s.sitelink_type = 'wikisource'
        AND (
            CAST(SUBSTR(COALESCE(p.publication_date, p.inception, p.earliest_date), 1, 5) AS INTEGER) < ?
            OR SUBSTR(COALESCE(p.publication_date, p.inception, p.earliest_date), 1, 1) = '-'
        )
        LIMIT ?
    """, (MAX_YEAR, SAMPLE_SIZE * 5,))  # Get more to allow for deduplication

    rows = cursor.fetchall()
    conn.close()

    # Group by QID and pick best language
    from collections import defaultdict
    qid_to_rows = defaultdict(list)
    for r in rows:
        qid_to_rows[r[0]].append(r)

    def get_lang_priority(url):
        """Get language priority (lower is better)."""
        for i, lang in enumerate(LANG_PRIORITY):
            if f'{lang}.wikisource' in url:
                return i
        return len(LANG_PRIORITY)  # Other languages come last

    # Select best language for each QID
    items = []
    for qid, qid_rows in qid_to_rows.items():
        # Sort by language priority
        qid_rows.sort(key=lambda r: get_lang_priority(r[2]))
        best = qid_rows[0]
        items.append({
            'qid': best[0],
            'label': best[1],
            'url': best[2],
            'sitelinks': best[3],
            'publication_date': best[4]
        })

    # Sort by language priority for extraction order
    items.sort(key=lambda x: get_lang_priority(x['url']))

    print(f"\nSelected {len(items)} unique works from database (before {MAX_YEAR})")

    # Show language breakdown
    lang_counts = defaultdict(int)
    for item in items:
        for lang in LANG_PRIORITY + ['other']:
            if f'{lang}.wikisource' in item['url']:
                lang_counts[lang] += 1
                break
        else:
            lang_counts['other'] += 1
    print("By language:", dict(lang_counts))

    # Extract with thread pool
    print(f"\nExtracting (direct pages only)...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(extract_item, item): item for item in items}

        with tqdm(total=len(items), desc="Extracting", ncols=80) as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)

    # Save results
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Summary
    success = [r for r in results if r.get('status') == 'success']
    skipped = [r for r in results if r.get('status') == 'skipped']
    errors = [r for r in results if r.get('status') == 'error']

    skip_reasons = {}
    for r in skipped:
        reason = r.get('reason', 'unknown')
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

    total_words = sum(r.get('word_count', 0) for r in success)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nSuccess: {len(success)}/{len(results)} ({len(success)/len(results)*100:.1f}%)")
    print(f"Skipped: {len(skipped)}")
    for reason, cnt in sorted(skip_reasons.items(), key=lambda x: -x[1]):
        print(f"  - {reason}: {cnt}")
    print(f"Errors:  {len(errors)}")
    print(f"\nTotal words extracted: {total_words:,}")
    print(f"\nResults: {RESULTS_FILE}")
    print(f"Texts: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
