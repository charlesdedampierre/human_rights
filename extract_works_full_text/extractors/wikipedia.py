"""
Wikipedia text extractor using the Wikipedia API.
"""

from urllib.parse import urlparse, unquote
from .base import BaseExtractor


class WikipediaExtractor(BaseExtractor):
    """Extract articles from Wikipedia."""

    @property
    def source_name(self) -> str:
        return "wikipedia"

    def parse_url(self, url: str) -> tuple[str, str]:
        """Extract language and title from Wikipedia URL."""
        parsed = urlparse(url)
        lang = parsed.netloc.split('.')[0]
        title = unquote(parsed.path.replace('/wiki/', ''))
        return lang, title

    def get_text(self, lang: str, title: str) -> str | None:
        """Get Wikipedia article text using the API."""
        api_url = f"https://{lang}.wikipedia.org/w/api.php"

        # Use extracts prop for clean text
        params = {
            'action': 'query',
            'titles': title,
            'prop': 'extracts',
            'explaintext': False,  # Get HTML to preserve formatting
            'format': 'json',
        }

        data = self.make_request(api_url, params)
        if data:
            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                if page_id != '-1':
                    extract = page_data.get('extract', '')
                    if extract:
                        return self.html_to_text(extract)

        # Fallback to parse API
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

    def is_disambiguation(self, text: str) -> bool:
        """Check if this is a disambiguation page."""
        if not text:
            return True
        text_lower = text.lower() if not text.startswith('<') else text.lower()
        return 'may refer to' in text_lower or 'disambiguation' in text_lower

    def extract(self, item: dict) -> dict:
        """Extract text from Wikipedia."""
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

        # Get text
        text = self.get_text(lang, title)

        if not text:
            result['status'] = 'error'
            result['error'] = 'No text returned'
            return result

        # Check if disambiguation
        if self.is_disambiguation(text):
            result['status'] = 'skipped'
            result['reason'] = 'disambiguation'
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
