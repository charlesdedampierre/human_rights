"""
Base extractor class with common functionality.
"""

import re
import time
import threading
import requests
import requests.adapters
from pathlib import Path
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """Base class for all text extractors."""

    REQUEST_TIMEOUT = 30
    MIN_TEXT_LENGTH = 200

    # Thread-local sessions
    _thread_local = threading.local()

    HEADERS = {
        'User-Agent': 'CulturaArchive/1.0 (academic research project)'
    }

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_session(self) -> requests.Session:
        """Get a thread-local session with connection pooling."""
        if not hasattr(self._thread_local, 'session'):
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            session.headers.update(self.HEADERS)
            self._thread_local.session = session
        return self._thread_local.session

    def make_request(self, url: str, params: dict = None, retries: int = 3) -> dict | None:
        """Make a request with retry logic."""
        session = self.get_session()
        for attempt in range(retries):
            try:
                response = session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
        return None

    def make_text_request(self, url: str, retries: int = 3) -> str | None:
        """Make a request and return text content."""
        session = self.get_session()
        for attempt in range(retries):
            try:
                response = session.get(url, timeout=self.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.text
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
        return None

    def html_to_text(self, html: str, preserve_formatting: bool = True) -> str:
        """Convert HTML to clean text, preserving formatting."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, 'html.parser')

        # Remove unwanted elements
        for tag in soup.find_all(['script', 'style', 'noscript', 'link', 'meta']):
            tag.decompose()

        # Remove navigation/noprint elements
        for cls in ['mw-editsection', 'noprint', 'navbox', 'toc', 'catlinks',
                    'mw-empty-elt', 'ws-noexport', 'wst-header', 'pagenum',
                    'ws-pagenum', 'reference', 'references', 'reflist']:
            for tag in soup.find_all(class_=lambda x: x and cls in str(x)):
                tag.decompose()

        if preserve_formatting:
            # Keep basic formatting tags
            for tag in soup.find_all('b'):
                tag.name = 'strong'
            for tag in soup.find_all('i'):
                tag.name = 'em'

            # Convert centered text
            for tag in soup.find_all(class_=lambda x: x and 'center' in str(x)):
                tag['style'] = 'text-align:center'

            # Keep only safe tags
            allowed_tags = ['p', 'div', 'br', 'strong', 'em', 'h1', 'h2', 'h3',
                           'h4', 'h5', 'h6', 'blockquote', 'span']

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
            html_out = re.sub(r'<(div|p|span)[^>]*>\s*</\1>', '', html_out)
            html_out = re.sub(r'\n{3,}', '\n\n', html_out)
            return html_out.strip()
        else:
            text = soup.get_text(separator='\n')
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            return '\n'.join(lines)

    def save_text(self, qid: str, text: str) -> Path:
        """Save extracted text to file."""
        filepath = self.output_dir / f"{qid}.txt"
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(text)
        return filepath

    def count_words(self, text: str) -> int:
        """Count words in text (handles HTML)."""
        from bs4 import BeautifulSoup
        if text.startswith('<'):
            soup = BeautifulSoup(text, 'html.parser')
            text = soup.get_text()
        return len(text.split())

    @abstractmethod
    def extract(self, item: dict) -> dict:
        """
        Extract text from source.

        Args:
            item: Dict with qid, label, url, and source-specific fields

        Returns:
            Dict with status, text_length, word_count, file, error, etc.
        """
        pass

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of this source type."""
        pass
