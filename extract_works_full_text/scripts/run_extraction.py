"""
Extract Wikisource texts with multiprocessing.
Saves results to data/instance_full_text.json and texts to data/instance_full_text/
"""

import sqlite3
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
from extract_wikisource import (
    parse_wikisource_url,
    extract_full_text,
    OUTPUT_DIR,
    RESULTS_FILE,
    DB_PATH,
)

# Configuration
TOP_SIZE = 100      # Top items by sitelinks
RANDOM_SIZE = 100   # Random items
MAX_WORKERS = 8     # Number of parallel threads (reliable)

# Thread-safe printing
print_lock = Lock()


def safe_print(msg: str):
    """Thread-safe printing."""
    with print_lock:
        tqdm.write(msg)


def url_to_filename(url: str) -> str:
    """Convert full URL to a safe filename."""
    filename = url.replace('https://', '').replace('http://', '')
    filename = unquote(filename)
    safe_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.')
    filename = ''.join(c if c in safe_chars else '_' for c in filename)
    if len(filename) > 200:
        filename = filename[:200]
    return filename + '.txt'


def select_top_items(cursor, size: int = 100) -> list[dict]:
    """Select top items by sitelink count."""
    cursor.execute("""
        SELECT s.instance_id, s.instance_label, s.sitelink_url, counts.cnt as sitelink_count
        FROM instances_sitelinks s
        INNER JOIN (
            SELECT instance_id, COUNT(*) as cnt
            FROM instances_sitelinks
            GROUP BY instance_id
        ) counts ON s.instance_id = counts.instance_id
        WHERE s.sitelink_type = 'wikisource'
        AND s.sitelink_url LIKE '%en.wikisource%'
        ORDER BY counts.cnt DESC
        LIMIT ?
    """, (size,))

    rows = cursor.fetchall()
    items = []
    for qid, label, url, sitelink_count in rows:
        items.append({
            'qid': qid,
            'label': label,
            'url': url,
            'sitelinks': sitelink_count,
            'source': 'top'
        })
    return items


def select_random_items(cursor, size: int = 100, exclude_qids: set = None) -> list[dict]:
    """Select random items, excluding already selected ones."""
    exclude_qids = exclude_qids or set()

    cursor.execute("""
        SELECT s.instance_id, s.instance_label, s.sitelink_url, counts.cnt as sitelink_count
        FROM instances_sitelinks s
        INNER JOIN (
            SELECT instance_id, COUNT(*) as cnt
            FROM instances_sitelinks
            GROUP BY instance_id
        ) counts ON s.instance_id = counts.instance_id
        WHERE s.sitelink_type = 'wikisource'
        AND s.sitelink_url LIKE '%en.wikisource%'
        ORDER BY RANDOM()
        LIMIT ?
    """, (size * 3,))  # Get extra to account for exclusions

    rows = cursor.fetchall()
    items = []
    for qid, label, url, sitelink_count in rows:
        if qid not in exclude_qids and len(items) < size:
            items.append({
                'qid': qid,
                'label': label,
                'url': url,
                'sitelinks': sitelink_count,
                'source': 'random'
            })
    return items


def validate_text_quality(text: str) -> dict:
    """Check text quality and return validation results."""
    issues = []
    html_tags = len(re.findall(r'<[^>]+>', text))
    if html_tags > 5:
        issues.append(f"HTML tags found: {html_tags}")
    entities = len(re.findall(r'&[a-z]+;', text))
    if entities > 10:
        issues.append(f"HTML entities found: {entities}")
    wiki_templates = len(re.findall(r'\{\{[^}]+\}\}', text))
    if wiki_templates > 3:
        issues.append(f"Wiki templates found: {wiki_templates}")
    wiki_links = len(re.findall(r'\[\[[^\]]+\]\]', text))
    if wiki_links > 5:
        issues.append(f"Wiki links found: {wiki_links}")
    return {'is_valid': len(issues) == 0, 'issues': issues}


def extract_single_item(item: dict) -> dict:
    """Extract text for a single item (called by thread pool)."""
    qid = item['qid']
    url = item['url']
    label = item['label']

    result = {
        'qid': qid,
        'label': label,
        'url': url,
        'sitelinks': item.get('sitelinks', 0),
        'source': item.get('source', 'unknown'),
    }

    try:
        lang, title = parse_wikisource_url(url)
        result['lang'] = lang
        result['title'] = title
    except Exception as e:
        result['status'] = 'error'
        result['error'] = f'URL parse error: {e}'
        return result

    extraction = extract_full_text(lang, title)
    result['page_type'] = extraction.page_type
    result['extraction_status'] = extraction.status

    if extraction.status == 'success':
        text = getattr(extraction, '_text', '')
        if text:
            result['text_stats'] = extraction.text_stats
            result['preview'] = text[:500] + '...' if len(text) > 500 else text
            validation = validate_text_quality(text)
            result['validation'] = validation
            result['status'] = 'success' if validation['is_valid'] else 'quality_issues'

            if extraction.portal_choice:
                result['portal_choice'] = {
                    'chosen_title': extraction.portal_choice.get('chosen_title'),
                    'chosen_url': extraction.portal_choice.get('chosen_url'),
                    'reason': extraction.portal_choice.get('reason'),
                    'alternatives_count': extraction.portal_choice.get('alternatives_count', 0),
                }

            # Save text file
            filename = url_to_filename(url)
            filepath = OUTPUT_DIR / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(text)
            result['saved_file'] = str(filepath.absolute())
        else:
            result['status'] = 'error'
            result['error'] = 'No text returned'
    else:
        result['status'] = extraction.status
        result['error'] = extraction.error_message

    # Print portal choices
    if result.get('page_type') == 'portal' and result.get('portal_choice'):
        choice = result['portal_choice']
        safe_print(f"\n  PORTAL: {label[:50]}")
        safe_print(f"    Original URL: {url}")
        safe_print(f"    Chosen URL:   {choice.get('chosen_url', 'N/A')}")
        safe_print(f"    Reason:       {choice.get('reason', 'N/A')}")
        safe_print(f"    Alternatives: {choice.get('alternatives_count', 0)}\n")

    return result


def main():
    """Run the extraction with multiprocessing."""
    total = TOP_SIZE + RANDOM_SIZE
    print("=" * 60)
    print(f"WIKISOURCE EXTRACTION ({total} ITEMS: {TOP_SIZE} top + {RANDOM_SIZE} random)")
    print(f"Workers: {MAX_WORKERS}")
    print("=" * 60)
    sys.stdout.flush()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Select items
    print("\n[1/3] Selecting top items...")
    top_items = select_top_items(cursor, TOP_SIZE)
    print(f"  Selected {len(top_items)} top items")

    print("\n[2/3] Selecting random items...")
    top_qids = {item['qid'] for item in top_items}
    random_items = select_random_items(cursor, RANDOM_SIZE, exclude_qids=top_qids)
    conn.close()
    print(f"  Selected {len(random_items)} random items")

    all_items = top_items + random_items

    # Extract with thread pool
    print(f"\n[3/3] Extracting texts...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(extract_single_item, item): item for item in all_items}

        with tqdm(total=len(all_items), desc="Extracting", ncols=80) as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)

    # Save results
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Print summary
    success_count = sum(1 for r in results if r.get('status') in ['success', 'quality_issues'])
    portal_count = sum(1 for r in results if r.get('page_type') == 'portal')
    total_pages = sum(r.get('text_stats', {}).get('pages', 0) for r in results if r.get('text_stats'))

    top_success = sum(1 for r in results if r.get('status') in ['success', 'quality_issues'] and r.get('source') == 'top')
    random_success = sum(1 for r in results if r.get('status') in ['success', 'quality_issues'] and r.get('source') == 'random')

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"\nSuccess rate: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
    print(f"  Top items:    {top_success}/{TOP_SIZE}")
    print(f"  Random items: {random_success}/{RANDOM_SIZE}")
    print(f"\nPortal pages: {portal_count}")
    print(f"Total book pages: {total_pages:,.0f}")
    print(f"\nResults saved to: {RESULTS_FILE}")
    print(f"Texts saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
