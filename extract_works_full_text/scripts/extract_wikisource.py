"""
Extract full text from Wikisource for all items in the database.
Handles different page types: direct text, multi-page works, portals, and disambiguation.
Saves each text as {QID}.txt in the full_text/ directory.
"""

import sqlite3
import requests
import time
import os
import json
import re
from urllib.parse import urlparse, unquote
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
from dataclasses import dataclass, asdict
from typing import Literal
from datetime import datetime

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR.parent / "wikidata_sparql_scripts/instance_properties/output/instance_properties.db"
OUTPUT_DIR = PROJECT_DIR / "data" / "instance_full_text"
RESULTS_FILE = PROJECT_DIR / "data" / "instance_full_text.json"
PROGRESS_FILE = PROJECT_DIR / "data" / "_extraction_progress.json"

# Request settings
DELAY_BETWEEN_REQUESTS = 0.15  # Balance speed and rate limits
REQUEST_TIMEOUT = 45
MAX_RETRIES = 5
RETRY_BACKOFF = 0.5  # seconds (with exponential backoff: 0.5, 1, 2, 4, 8)

# Filtering
ENGLISH_ONLY = True  # Only process English Wikisource
MIN_TEXT_LENGTH = 100  # Minimum characters for valid text
MAX_SUBPAGES = 100  # Maximum subpages to fetch for multi-page works

# Load version words for multi-language support
VERSION_WORDS_FILE = Path(__file__).parent.parent / "data_pipeline_literary_works/data/wikisource_lang_words.json"
VERSION_WORDS = {}
if VERSION_WORDS_FILE.exists():
    with open(VERSION_WORDS_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        VERSION_WORDS = data.get('version_words', {})

HEADERS = {
    'User-Agent': 'WikisourceExtractor/1.0 (academic research project; contact: research@example.com)'
}

# Connection pooling for faster requests (thread-local)
import requests.adapters
import threading
_thread_local = threading.local()

def get_session() -> requests.Session:
    """Get a thread-local session with connection pooling."""
    if not hasattr(_thread_local, 'session'):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0  # We handle retries ourselves
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update(HEADERS)
        _thread_local.session = session
    return _thread_local.session

# Page type definitions
PageType = Literal['direct', 'multipage', 'portal', 'disambiguation', 'error', 'empty']


@dataclass
class PageAnalysis:
    """Result of analyzing a Wikisource page."""
    page_type: PageType
    text_length: int
    subpage_count: int
    has_toc: bool
    has_version_links: bool
    error_message: str | None = None


# Book page estimation (standard: ~250 words per page)
WORDS_PER_PAGE = 250


def calculate_text_stats(text: str) -> dict:
    """Calculate text statistics: chars, words, estimated book pages."""
    chars = len(text)
    words = len(text.split())
    pages = round(words / WORDS_PER_PAGE, 1)
    return {
        'chars': chars,
        'words': words,
        'pages': pages  # Estimated book pages (~250 words/page)
    }


@dataclass
class ExtractionResult:
    """Result of extracting text from a Wikisource page."""
    qid: str
    url: str
    lang: str
    title: str
    page_type: PageType
    status: str  # 'success', 'failed', 'skipped'
    text_length: int
    subpages_fetched: int
    error_message: str | None = None
    # Portal-specific: track which translation was chosen
    portal_choice: dict | None = None  # {chosen_title, chosen_url, reason}
    # Text statistics
    text_stats: dict | None = None  # {chars, words, pages}


def parse_wikisource_url(url: str) -> tuple[str, str]:
    """Extract language code and page title from Wikisource URL."""
    parsed = urlparse(url)
    lang = parsed.netloc.split('.')[0]
    title = parsed.path.replace('/wiki/', '')
    title = unquote(title)
    return lang, title


def make_request(url: str, params: dict, retries: int = MAX_RETRIES) -> dict | None:
    """Make a request with retry logic using connection pooling."""
    session = get_session()
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                # Exponential backoff: 1s, 2s, 4s...
                wait_time = RETRY_BACKOFF * (2 ** attempt)
                time.sleep(wait_time)
            else:
                return None
    return None


def html_to_text(html: str, preserve_headers: bool = True) -> str:
    """Convert HTML to clean text, preserving structure and spacing."""
    soup = BeautifulSoup(html, 'html.parser')

    # Remove unwanted elements
    for tag in soup.find_all(['sup', 'script', 'style', 'noscript']):
        tag.decompose()

    # Remove edit sections, metadata, navigation elements
    unwanted_classes = [
        'mw-editsection', 'noprint', 'mw-empty-elt', 'metadata',
        'sister-wikipedia', 'sister-project', 'catlinks', 'printfooter',
        'navbox', 'vertical-navbox', 'infobox', 'toc', 'mw-jump-link'
    ]
    for cls in unwanted_classes:
        for tag in soup.find_all(class_=cls):
            tag.decompose()

    # Remove elements by ID
    unwanted_ids = ['toc', 'catlinks', 'siteSub', 'contentSub']
    for elem_id in unwanted_ids:
        tag = soup.find(id=elem_id)
        if tag:
            tag.decompose()

    # Convert headers to markdown-style
    if preserve_headers:
        for i in range(1, 7):
            for header in soup.find_all(f'h{i}'):
                header_text = header.get_text().strip()
                if header_text:
                    prefix = '#' * i
                    header.replace_with(f"\n\n{prefix} {header_text}\n\n")

    # Add paragraph breaks for block elements
    for tag in soup.find_all(['p', 'div', 'br']):
        if tag.name == 'br':
            tag.replace_with('\n')
        else:
            # Add newlines around paragraphs
            tag.insert_before('\n\n')
            tag.insert_after('\n\n')

    # Get text
    text = soup.get_text()

    # Clean up but preserve intentional spacing
    lines = []
    prev_empty = False
    for line in text.splitlines():
        line = line.strip()
        # Skip invisible characters only
        if line and line != '​' and not re.match(r'^[\u200b\u00a0]+$', line):
            lines.append(line)
            prev_empty = False
        elif not prev_empty:
            # Keep one empty line for paragraph breaks
            lines.append('')
            prev_empty = True

    text = '\n'.join(lines)

    # Normalize excessive newlines (more than 2) but keep paragraph breaks
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    # Remove common wiki artifacts
    text = re.sub(r'\[\[.*?\]\]', '', text)  # [[links]]
    text = re.sub(r'\{\{.*?\}\}', '', text)  # {{templates}}

    return text.strip()


def get_page_content(lang: str, title: str) -> tuple[str | None, BeautifulSoup | None]:
    """Get page HTML and parsed soup from Wikisource."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"

    params = {
        'action': 'parse',
        'page': title,
        'prop': 'text',
        'format': 'json',
        'disablelimitreport': True,
        'disableeditsection': True,
    }

    data = make_request(api_url, params)
    if not data or 'error' in data:
        return None, None

    html = data.get('parse', {}).get('text', {}).get('*', '')
    if not html:
        return None, None

    soup = BeautifulSoup(html, 'html.parser')
    return html, soup


def analyze_page(lang: str, title: str) -> PageAnalysis:
    """
    Analyze a Wikisource page to determine its type.

    Types:
    - direct: Full content on single page (extract directly)
    - multipage: Content split into chapters/sections (follow subpage links)
    - portal: Lists translations or editions (pick best, then extract)
    - disambiguation: Links to different works (skip)
    - empty: Page exists but has no useful content
    - error: Failed to fetch page
    """
    html, soup = get_page_content(lang, title)

    if not html or not soup:
        return PageAnalysis(
            page_type='error',
            text_length=0,
            subpage_count=0,
            has_toc=False,
            has_version_links=False,
            error_message='Failed to fetch page'
        )

    # Get text content
    text = html_to_text(html)
    text_length = len(text)

    # Check for table of contents
    has_toc = bool(soup.find(id='toc') or soup.find(class_='toc'))

    # Look for version/translation links
    version_keywords = ['translation', 'edition', 'version', 'translator', 'translated by']
    text_lower = text.lower()
    has_version_links = any(kw in text_lower for kw in version_keywords)

    # Count internal links to subpages
    base_title = title.split('/')[0]
    subpage_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('/wiki/') and base_title in href:
            subpage_links.append(href)
    subpage_count = len(subpage_links)

    # Check for disambiguation patterns
    disambig_keywords = ['may refer to', 'disambiguation', 'see also']
    is_disambig = any(kw in text_lower for kw in disambig_keywords)

    # Determine page type
    if text_length < 50:
        page_type = 'empty'
    elif is_disambig and text_length < 500:
        page_type = 'disambiguation'
    elif has_version_links and text_length < 3000:
        # Portal page with version/translation links (even if some content)
        page_type = 'portal'
    elif subpage_count > 3 and text_length < 1000:
        page_type = 'multipage'
    elif text_length > 1000:
        # Has substantial content without version links
        page_type = 'direct'
    else:
        # Default to direct for moderate content
        page_type = 'direct'

    return PageAnalysis(
        page_type=page_type,
        text_length=text_length,
        subpage_count=subpage_count,
        has_toc=has_toc,
        has_version_links=has_version_links
    )


def get_subpages(lang: str, title: str) -> list[str]:
    """Get list of subpages for a work (chapters, sections, etc.)."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"

    params = {
        'action': 'query',
        'list': 'allpages',
        'apprefix': title + '/',
        'aplimit': 500,
        'format': 'json',
    }

    data = make_request(api_url, params)
    if not data:
        return []

    pages = data.get('query', {}).get('allpages', [])
    return [p['title'] for p in pages]


def sort_subpages(subpages: list[str]) -> list[str]:
    """Sort subpages, handling numeric chapter ordering."""
    def sort_key(s):
        parts = s.rsplit('/', 1)
        if len(parts) > 1:
            suffix = parts[1]
            # Try to extract numeric prefix
            match = re.match(r'^(\d+)', suffix)
            if match:
                return (0, int(match.group(1)), suffix)
            # Roman numerals
            roman_match = re.match(r'^(I{1,3}|IV|V|VI{0,3}|IX|X{0,3})\.?\s', suffix, re.IGNORECASE)
            if roman_match:
                roman_val = roman_to_int(roman_match.group(1).upper())
                return (1, roman_val, suffix)
            # Alphabetic
            return (2, suffix.lower())
        return (3, s)

    return sorted(subpages, key=sort_key)


def roman_to_int(s: str) -> int:
    """Convert Roman numeral to integer."""
    values = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    total = 0
    prev = 0
    for char in reversed(s):
        curr = values.get(char, 0)
        if curr < prev:
            total -= curr
        else:
            total += curr
        prev = curr
    return total


def extract_direct(lang: str, title: str) -> str | None:
    """Extract text directly from a single page using TextExtracts API first, fallback to HTML parsing."""
    api_url = f"https://{lang}.wikisource.org/w/api.php"

    # Try TextExtracts API first (faster and cleaner)
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'extracts',
        'explaintext': True,  # Plain text, no HTML
        'exsectionformat': 'plain',
        'format': 'json',
    }

    data = make_request(api_url, params)
    if data:
        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':  # Page exists
                extract = page_data.get('extract', '')
                if extract and len(extract) > MIN_TEXT_LENGTH:
                    return extract.strip()

    # Fallback to HTML parsing
    html, soup = get_page_content(lang, title)
    if not html:
        return None
    return html_to_text(html)


def extract_multipage(lang: str, title: str) -> tuple[str | None, int]:
    """
    Extract text from a multi-page work by fetching all subpages.
    Returns (text, num_subpages_fetched).
    """
    # Get main page text first
    main_text = extract_direct(lang, title)

    # Get and sort subpages
    subpages = get_subpages(lang, title)
    subpages = sort_subpages(subpages)

    if not subpages:
        return main_text, 0

    # Limit subpages
    subpages = subpages[:MAX_SUBPAGES]

    all_texts = []
    if main_text and len(main_text) > 100:
        all_texts.append(main_text)

    fetched = 0
    for subpage in subpages:
        time.sleep(DELAY_BETWEEN_REQUESTS)
        sub_text = extract_direct(lang, subpage)
        if sub_text and len(sub_text) > 50:
            # Extract section name from subpage
            section_name = subpage.split('/')[-1] if '/' in subpage else subpage
            all_texts.append(f"\n\n=== {section_name} ===\n\n{sub_text}")
            fetched += 1

    if all_texts:
        return '\n'.join(all_texts), fetched
    return None, 0


def extract_key_terms(title: str) -> list[str]:
    """
    Extract meaningful key terms from a Wikisource title.
    Handles cases like "1_Enoch", "Anna_Karenina", "Mark_(Bible)", "Portal:Minor_Prophets", etc.
    Returns list of key terms sorted by relevance.
    """
    # Common stop words to filter out
    STOP_WORDS = {
        'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for',
        'with', 'by', 'from', 'as', 'into', 'through', 'during', 'before',
        'after', 'above', 'below', 'between', 'under', 'again', 'further',
        'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how',
        'all', 'each', 'few', 'more', 'most', 'other', 'some', 'such',
        'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too',
        'very', 'just', 'can', 'will', 'should', 'now'
    }

    # Remove namespace prefixes like "Portal:", "Author:"
    if ':' in title:
        title = title.split(':', 1)[1]

    # Remove parenthetical suffixes like "(Bible)", "(Xenophon)"
    base = re.sub(r'\([^)]+\)$', '', title).strip('_').strip()

    # Split by underscore
    parts = [p for p in base.split('_') if p]

    key_terms = []

    # Add the full base name (highest priority) - but only if it has meaningful words
    meaningful_parts = [p for p in parts if p.lower() not in STOP_WORDS and len(p) > 2]
    if meaningful_parts:
        key_terms.append(base)

    # Add individual parts, filtering out stop words and short words
    for part in parts:
        if part and part.lower() not in STOP_WORDS and not part.isdigit() and len(part) > 2:
            key_terms.append(part)

    # Add numeric parts last (lowest priority)
    for part in parts:
        if part and part.isdigit():
            key_terms.append(part)

    return key_terms


def extract_chapter_with_subpages(lang: str, chapter_title: str) -> str | None:
    """
    Extract text from a chapter, including any subpages it may have.
    This is recursive - if a chapter has subpages, fetch them all.
    """
    # First check if this chapter has subpages
    subpages = get_subpages(lang, chapter_title)

    if subpages:
        # Chapter has subpages - extract them all
        subpages = sort_subpages(subpages)[:MAX_SUBPAGES]

        all_texts = []

        # Get main chapter page content first
        main_text = extract_direct(lang, chapter_title)
        if main_text and len(main_text) > 50:
            all_texts.append(main_text)

        # Get all subpages
        for subpage in subpages:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            sub_text = extract_direct(lang, subpage)
            if sub_text and len(sub_text) > 50:
                section_name = subpage.split('/')[-1] if '/' in subpage else subpage
                all_texts.append(f"\n\n--- {section_name} ---\n\n{sub_text}")

        if all_texts:
            return '\n'.join(all_texts)
        return None
    else:
        # No subpages - just extract directly
        return extract_direct(lang, chapter_title)


def extract_portal(lang: str, title: str, depth: int = 0, max_depth: int = 2) -> tuple[str | None, dict | None]:
    """
    Extract from a portal page using smart link analysis.
    Recursively follows nested portals up to max_depth.

    Logic (from notebook analysis):
    1. Get all links from the page
    2. If links contain the base title → these are chapters/subpages (same work)
    3. If links don't contain base title → these are versions/translations (pick best)
    4. If result is short (<3000 chars), may be nested portal → follow recursively

    Returns (text, portal_choice_info).
    """
    if depth >= max_depth:
        return None, {'error': f'Max portal depth ({max_depth}) reached'}
    html, soup = get_page_content(lang, title)
    if not soup:
        return None, None

    # Get base name (without any subpage suffix)
    base_name = title.split('/')[0]

    # Extract meaningful key terms for matching
    key_terms = extract_key_terms(base_name)

    # Collect all internal wiki links
    all_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        link_text = link.get_text().strip()

        # Only consider internal wiki links
        if not href.startswith('/wiki/'):
            continue

        # Skip special pages, files, categories, namespaced pages
        link_title = unquote(href.replace('/wiki/', ''))
        if ':' in link_title.split('/')[0]:  # Namespaced like "Category:", "File:"
            continue

        # Skip if it's the same page
        if link_title == title:
            continue

        all_links.append({
            'title': link_title,
            'text': link_text,
            'href': href,
            'url': f"https://{lang}.wikisource.org{href}",
            'contains_base': base_name in link_title
        })

    if not all_links:
        return None, None

    # Separate links into chapters (contain base name) vs versions (don't contain base name)
    chapter_links = [l for l in all_links if l['contains_base']]
    version_links = [l for l in all_links if not l['contains_base']]

    portal_choice = None

    # Case 1: Links contain base name → these are chapters/subpages
    chapter_text = None
    chapter_choice = None

    if chapter_links:
        # This is actually a multi-page work, extract as chapters
        # Use recursive extraction to get subpages of each chapter
        all_texts = []
        fetched = 0

        for link in chapter_links[:MAX_SUBPAGES]:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            # Use recursive extraction instead of extract_direct
            sub_text = extract_chapter_with_subpages(lang, link['title'])
            if sub_text and len(sub_text) > 50:
                section_name = link['title'].split('/')[-1] if '/' in link['title'] else link['title']
                all_texts.append(f"\n\n=== {section_name} ===\n\n{sub_text}")
                fetched += 1

        if all_texts:
            chapter_text = '\n'.join(all_texts)
            chapter_choice = {
                'chosen_title': f"{base_name} (chapters)",
                'chosen_url': f"https://{lang}.wikisource.org/wiki/{title}",
                'link_text': 'Multiple chapters',
                'reason': f'Extracted {fetched} chapters with subpages (links contained base name)',
                'alternatives_count': len(chapter_links),
                'type': 'chapters'
            }
            # If chapter extraction is substantial, return it
            # Otherwise, also check version links to see if they have more content
            if len(chapter_text) >= 3000:
                return chapter_text, chapter_choice

    # Case 2: Links don't contain base name → these are versions/translations
    if version_links:
        # Score links based on how well they match the work title
        def score_link(link):
            """Score a link by how relevant it is to the original work."""
            link_title = link['title']
            link_text = link['text'].lower()
            score = 0

            # Check each key term (ordered by relevance)
            for i, term in enumerate(key_terms):
                term_lower = term.lower()
                # Higher score for matching key terms, weighted by position
                weight = len(key_terms) - i
                if term_lower in link_title.lower():
                    score += 10 * weight
                if term_lower in link_text:
                    score += 5 * weight

            # Bonus for links that look like full translations
            translation_indicators = ['translation', 'translated', 'version']
            for indicator in translation_indicators:
                if indicator in link_title.lower() or indicator in link_text:
                    score += 3

            # Bonus for parenthetical qualifiers like "(Charles)", "(Dakyns)"
            if '(' in link_title:
                score += 2

            # Bonus for specific sections (links with "/" are more targeted)
            # This helps pick "Book/Chapter" over just "Book"
            if '/' in link_title:
                score += 5

            # Penalty for very short titles (likely disambiguation)
            if len(link_title) < 10:
                score -= 5

            # Penalty for links that are completely unrelated
            if score == 0:
                score = -10

            return score

        # Score and sort links
        scored_links = [(score_link(l), l) for l in version_links]
        scored_links.sort(key=lambda x: x[0], reverse=True)

        # Filter to only consider links with positive scores
        good_links = [(s, l) for s, l in scored_links if s > 0]

        chosen = None
        reason = None

        if good_links:
            # Pick the highest scored link
            best_score, chosen = good_links[0]
            matched_terms = [t for t in key_terms if t.lower() in chosen['title'].lower()]
            reason = f"Best match for '{', '.join(matched_terms[:2])}' (score: {best_score})"
        elif version_links:
            # Fallback: first link with substantive title
            substantive_links = [l for l in version_links if len(l['title']) > 10]
            if substantive_links:
                chosen = substantive_links[0]
                reason = "First available translation (no key term matches)"
            else:
                chosen = version_links[0]
                reason = "First link (fallback)"

        if chosen:
            time.sleep(DELAY_BETWEEN_REQUESTS)

            # Use recursive extraction to get full content including subpages
            text = extract_chapter_with_subpages(lang, chosen['title'])

            portal_choice = {
                'chosen_title': chosen['title'],
                'chosen_url': chosen['url'],
                'link_text': chosen['text'],
                'reason': reason,
                'alternatives_count': len(version_links),
                'type': 'version'
            }

            # Check if result looks like a nested portal (short text)
            if text and MIN_TEXT_LENGTH < len(text) < 3000:
                # Try following recursively
                nested_text, nested_choice = extract_portal(lang, chosen['title'], depth + 1, max_depth)
                if nested_text and len(nested_text) > len(text):
                    # Nested extraction got more content
                    portal_choice['nested_from'] = chosen['title']
                    if nested_choice:
                        portal_choice['nested_choice'] = nested_choice
                    portal_choice['reason'] = f"Followed nested portal: {reason}"
                    return nested_text, portal_choice

            if text and len(text) > MIN_TEXT_LENGTH:
                return text, portal_choice

            # Try other good links if first didn't work
            for score, link in good_links[1:5]:  # Try up to 4 more
                if link == chosen:
                    continue
                time.sleep(DELAY_BETWEEN_REQUESTS)
                text = extract_chapter_with_subpages(lang, link['title'])
                if text and len(text) > MIN_TEXT_LENGTH:
                    portal_choice = {
                        'chosen_title': link['title'],
                        'chosen_url': link['url'],
                        'link_text': link['text'],
                        'reason': f"Fallback (primary choice '{chosen['title']}' had no content)",
                        'alternatives_count': len(version_links),
                        'type': 'version'
                    }
                    return text, portal_choice

    # If we have chapter text but no version text (or version failed), return chapter text
    if chapter_text and len(chapter_text) > MIN_TEXT_LENGTH:
        return chapter_text, chapter_choice

    return None, portal_choice


def extract_full_text(lang: str, title: str) -> ExtractionResult:
    """
    Extract full text from a Wikisource page, handling different page types.
    ALWAYS checks for subpages first to ensure we get complete content.
    """
    url = f"https://{lang}.wikisource.org/wiki/{title}"

    result = ExtractionResult(
        qid='',  # Will be set by caller
        url=url,
        lang=lang,
        title=title,
        page_type='unknown',
        status='failed',
        text_length=0,
        subpages_fetched=0
    )

    # FIRST: Always check for subpages (chapters, sections, etc.)
    subpages = get_subpages(lang, title)

    if subpages:
        # Has subpages - extract as multipage work
        result.page_type = 'multipage'
        text, subpages_fetched = extract_multipage(lang, title)
        result.subpages_fetched = subpages_fetched

        if text and len(text) >= MIN_TEXT_LENGTH:
            result.status = 'success'
            result.text_length = len(text)
            result.text_stats = calculate_text_stats(text)
            result._text = text
            return result

    # No subpages or subpage extraction failed - analyze the page
    analysis = analyze_page(lang, title)
    result.page_type = analysis.page_type

    if analysis.page_type == 'error':
        result.error_message = analysis.error_message
        return result

    if analysis.page_type == 'empty':
        result.error_message = 'Page has no content'
        return result

    if analysis.page_type == 'disambiguation':
        result.status = 'skipped'
        result.error_message = 'Disambiguation page'
        return result

    # Extract based on type
    text = None

    if analysis.page_type == 'portal':
        text, portal_choice = extract_portal(lang, title)
        result.portal_choice = portal_choice
    else:
        # Direct extraction
        result.page_type = 'direct'
        text = extract_direct(lang, title)

    # Validate result
    if text and len(text) >= MIN_TEXT_LENGTH:
        result.status = 'success'
        result.text_length = len(text)
        result.text_stats = calculate_text_stats(text)
        result._text = text
    else:
        result.error_message = f'Text too short ({len(text) if text else 0} chars)'

    return result


def validate_text(text: str) -> list[str]:
    """Validate extracted text for quality issues. Returns list of warnings."""
    warnings = []

    # Check minimum length
    if len(text) < MIN_TEXT_LENGTH:
        warnings.append(f'Text too short: {len(text)} chars')

    # Check for HTML artifacts
    if '<' in text or '>' in text:
        html_count = text.count('<') + text.count('>')
        if html_count > 10:
            warnings.append(f'Contains HTML artifacts: {html_count} tags')

    # Check for wiki markup
    if '{{' in text or '}}' in text:
        template_count = text.count('{{')
        if template_count > 5:
            warnings.append(f'Contains wiki templates: {template_count}')

    # Check for non-text characters
    if '&nbsp;' in text or '&amp;' in text:
        warnings.append('Contains HTML entities')

    return warnings


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
    """Main extraction pipeline."""
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build query based on language filter
    if ENGLISH_ONLY:
        query = """
            SELECT instance_id, instance_label, sitelink_url
            FROM instances_sitelinks
            WHERE sitelink_type = 'wikisource'
            AND sitelink_url LIKE '%en.wikisource%'
        """
    else:
        query = """
            SELECT instance_id, sitelink_url
            FROM instances_sitelinks
            WHERE sitelink_type = 'wikisource'
        """

    cursor.execute(query)
    items = cursor.fetchall()
    conn.close()

    print(f"Found {len(items)} Wikisource items in database")
    if ENGLISH_ONLY:
        print("Filtering: English Wikisource only")

    # Load progress
    progress = load_json_file(PROGRESS_FILE, {'processed': [], 'stats': {}})
    processed = set(progress.get('processed', []))
    failed = load_json_file(FAILED_FILE, {'items': []})
    failed_qids = {f['qid'] for f in failed.get('items', [])}
    portal_choices = load_json_file(PORTAL_CHOICES_FILE, {})

    # Check already existing files
    existing_files = {f.stem for f in OUTPUT_DIR.glob("Q*.txt")}
    processed = processed.union(existing_files)

    # Filter out already processed
    items_to_process = [
        (qid, label, url) for qid, label, url in items
        if qid not in processed and qid not in failed_qids
    ]
    print(f"Already processed: {len(processed)}, failed: {len(failed_qids)}, remaining: {len(items_to_process)}")

    if not items_to_process:
        print("All items already processed!")
        return

    # Statistics tracking
    stats = {
        'start_time': datetime.now().isoformat(),
        'total_items': len(items),
        'already_processed': len(processed),
        'by_type': {'direct': 0, 'multipage': 0, 'portal': 0, 'disambiguation': 0, 'error': 0, 'empty': 0},
        'by_status': {'success': 0, 'failed': 0, 'skipped': 0},
        'total_chars': 0,
        'total_words': 0,
        'total_pages': 0,  # Estimated book pages (~250 words/page)
        'total_subpages': 0,
    }

    # Process items
    new_failed = []

    with tqdm(total=len(items_to_process), desc="Extracting") as pbar:
        for qid, label, url in items_to_process:
            try:
                lang, title = parse_wikisource_url(url)
            except Exception as e:
                new_failed.append({
                    'qid': qid,
                    'url': url,
                    'error': f'URL parse error: {e}'
                })
                stats['by_status']['failed'] += 1
                pbar.update(1)
                continue

            # Extract text
            result = extract_full_text(lang, title)
            result.qid = qid

            # Update stats
            stats['by_type'][result.page_type] += 1
            stats['by_status'][result.status] += 1

            if result.status == 'success':
                # Save text to file
                text = getattr(result, '_text', None)
                if text:
                    output_file = OUTPUT_DIR / f"{qid}.txt"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(text)

                    processed.add(qid)
                    stats['total_chars'] += result.text_length
                    stats['total_subpages'] += result.subpages_fetched

                    # Track words and pages
                    if result.text_stats:
                        stats['total_words'] += result.text_stats['words']
                        stats['total_pages'] += result.text_stats['pages']

                    # Log portal choice if applicable
                    if result.portal_choice:
                        portal_choices[qid] = {
                            'original_url': url,
                            'original_title': title,
                            'label': label,
                            **result.portal_choice,
                            'text_stats': result.text_stats
                        }
            else:
                new_failed.append({
                    'qid': qid,
                    'url': url,
                    'label': label,
                    'page_type': result.page_type,
                    'error': result.error_message
                })

            pbar.set_postfix(
                ok=stats['by_status']['success'],
                fail=stats['by_status']['failed'],
                skip=stats['by_status']['skipped']
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

                # Save portal choices
                if portal_choices:
                    save_json_file(PORTAL_CHOICES_FILE, portal_choices)

            # Rate limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final save
    stats['end_time'] = datetime.now().isoformat()
    stats['portal_choices_count'] = len(portal_choices)
    progress['processed'] = list(processed)
    progress['stats'] = stats
    save_json_file(PROGRESS_FILE, progress)

    failed['items'].extend(new_failed)
    save_json_file(FAILED_FILE, failed)
    save_json_file(STATS_FILE, stats)

    # Save portal choices log
    if portal_choices:
        save_json_file(PORTAL_CHOICES_FILE, portal_choices)
        print(f"\nPortal translation choices logged: {len(portal_choices)}")

    # Print summary
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"\nBy status:")
    for status, count in stats['by_status'].items():
        print(f"  {status}: {count}")

    print(f"\nBy page type:")
    for ptype, count in stats['by_type'].items():
        if count > 0:
            print(f"  {ptype}: {count}")

    print(f"\n{'='*60}")
    print("TEXT STATISTICS")
    print(f"{'='*60}")
    print(f"  Characters: {stats['total_chars']:,}")
    print(f"  Words:      {stats['total_words']:,}")
    print(f"  Book pages: {stats['total_pages']:,.0f} (~250 words/page)")

    if stats['by_status']['success'] > 0:
        avg_pages = stats['total_pages'] / stats['by_status']['success']
        print(f"\n  Average per work: {avg_pages:.1f} pages")

    print(f"\nTotal subpages fetched: {stats['total_subpages']}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
