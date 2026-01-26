"""
Wikisource text extractor.
"""

import re
from urllib.parse import urlparse, unquote
from .base import BaseExtractor


class WikisourceExtractor(BaseExtractor):
    """Extract full texts from Wikisource."""

    @property
    def source_name(self) -> str:
        return "wikisource"

    def parse_url(self, url: str) -> tuple[str, str]:
        """Extract language and title from Wikisource URL."""
        parsed = urlparse(url)
        lang = parsed.netloc.split('.')[0]
        title = unquote(parsed.path.replace('/wiki/', ''))
        return lang, title

    def has_subpages(self, lang: str, title: str) -> bool:
        """Check if page has subpages (multipage work)."""
        api_url = f"https://{lang}.wikisource.org/w/api.php"
        params = {
            'action': 'query',
            'list': 'allpages',
            'apprefix': title + '/',
            'aplimit': 1,
            'format': 'json',
        }
        data = self.make_request(api_url, params)
        if data:
            pages = data.get('query', {}).get('allpages', [])
            return len(pages) > 0
        return True  # Assume has subpages on error

    def is_portal_page(self, text: str) -> bool:
        """Check if text looks like a portal/disambiguation page."""
        if not text or len(text) < 500:
            return True

        text_lower = text.lower() if not text.startswith('<') else text.lower()
        portal_indicators = ['translations', 'editions', 'versions',
                            'translated by', 'see also:', 'may refer to']
        indicator_count = sum(1 for ind in portal_indicators if ind in text_lower)

        if len(text) < 2000 and indicator_count >= 2:
            return True
        return False

    def get_text(self, lang: str, title: str) -> str | None:
        """Get text with formatting preserved using HTML parsing."""
        api_url = f"https://{lang}.wikisource.org/w/api.php"
        params = {
            'action': 'parse',
            'page': title,
            'prop': 'text',
            'format': 'json',
            'disablelimitreport': True,
        }
        data = self.make_request(api_url, params)
        if data:
            html = data.get('parse', {}).get('text', {}).get('*', '')
            if html:
                return self.html_to_text(html)
        return None

    def extract(self, item: dict) -> dict:
        """Extract text from Wikisource."""
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

        try:
            lang, title = self.parse_url(url)
            result['lang'] = lang
            result['title'] = title
        except Exception as e:
            result['status'] = 'error'
            result['error'] = f'URL parse error: {e}'
            return result

        # Check for subpages (skip multipage works)
        if self.has_subpages(lang, title):
            result['status'] = 'skipped'
            result['reason'] = 'multipage'
            return result

        # Get text
        text = self.get_text(lang, title)

        if not text:
            result['status'] = 'error'
            result['error'] = 'No text returned'
            return result

        # Check if it's a portal page
        if self.is_portal_page(text):
            result['status'] = 'skipped'
            result['reason'] = 'portal'
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
