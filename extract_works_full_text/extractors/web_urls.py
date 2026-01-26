"""
Web URL text extractor.
Handles full_work_url, described_at_url, official_website.
"""

import re
from urllib.parse import urlparse
from .base import BaseExtractor


class WebURLExtractor(BaseExtractor):
    """Extract text from various web URLs."""

    # Known domains with special handling
    GOOGLE_BOOKS_DOMAINS = ['books.google.com', 'books.google.co.uk']
    ARCHIVE_DOMAINS = ['archive.org', 'web.archive.org']
    SKIP_DOMAINS = ['jstor.org', 'doi.org', 'dx.doi.org']  # Paywalled

    def __init__(self, output_dir, source_type: str = 'full_work_url'):
        super().__init__(output_dir)
        self._source_type = source_type

    @property
    def source_name(self) -> str:
        return self._source_type

    def get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def is_skippable(self, url: str) -> tuple[bool, str]:
        """Check if URL should be skipped."""
        domain = self.get_domain(url)

        # Skip paywalled sites
        for skip_domain in self.SKIP_DOMAINS:
            if skip_domain in domain:
                return True, f'paywalled ({skip_domain})'

        # Skip PDFs (can't easily extract without downloading)
        if url.lower().endswith('.pdf'):
            return True, 'pdf_file'

        return False, ''

    def extract_google_books(self, url: str) -> str | None:
        """Extract text from Google Books."""
        # Google Books doesn't provide API access for full text
        # We can only get metadata and snippets
        return None

    def extract_internet_archive(self, url: str) -> str | None:
        """Extract text from Internet Archive."""
        # Parse the archive.org URL to get the item ID
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')

        if len(path_parts) < 2:
            return None

        # Get item metadata
        if path_parts[0] == 'details':
            item_id = path_parts[1]
        else:
            item_id = path_parts[0]

        # Try to get OCR text
        text_url = f"https://archive.org/download/{item_id}/{item_id}_djvu.txt"
        text = self.make_text_request(text_url)
        if text and len(text) > self.MIN_TEXT_LENGTH:
            return text

        return None

    def extract_generic(self, url: str) -> str | None:
        """Extract text from generic webpage."""
        html = self.make_text_request(url)
        if not html:
            return None

        text = self.html_to_text(html, preserve_formatting=True)

        # Check if it's a meaningful page (not just navigation)
        if len(text) < self.MIN_TEXT_LENGTH:
            return None

        return text

    def extract(self, item: dict) -> dict:
        """Extract text from web URL."""
        qid = item['qid']
        url = item['url']
        label = item['label']

        result = {
            'qid': qid,
            'label': label,
            'url': url,
            'source': self.source_name,
            'publication_date': item.get('publication_date'),
        }

        # Handle multiple URLs (comma-separated)
        if ',' in url:
            url = url.split(',')[0].strip()

        # Check if should skip
        should_skip, reason = self.is_skippable(url)
        if should_skip:
            result['status'] = 'skipped'
            result['reason'] = reason
            return result

        domain = self.get_domain(url)
        result['domain'] = domain

        # Try domain-specific extractors
        text = None

        if any(d in domain for d in self.GOOGLE_BOOKS_DOMAINS):
            text = self.extract_google_books(url)
            if not text:
                result['status'] = 'skipped'
                result['reason'] = 'google_books_no_text'
                return result

        elif any(d in domain for d in self.ARCHIVE_DOMAINS):
            text = self.extract_internet_archive(url)

        else:
            text = self.extract_generic(url)

        if not text:
            result['status'] = 'error'
            result['error'] = 'No text extracted'
            return result

        # Check minimum length
        if len(text) < self.MIN_TEXT_LENGTH:
            result['status'] = 'skipped'
            result['reason'] = f'too_short ({len(text)} chars)'
            return result

        # Success
        result['status'] = 'success'
        result['text_length'] = len(text)
        result['word_count'] = self.count_words(text)
        result['file'] = str(self.save_text(qid, text))

        return result
