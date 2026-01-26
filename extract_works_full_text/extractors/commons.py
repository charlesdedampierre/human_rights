"""
Wikimedia Commons document extractor.
Handles PDF and DjVu files.
"""

import re
from urllib.parse import urlparse, unquote
from .base import BaseExtractor


class CommonsExtractor(BaseExtractor):
    """Extract text from documents on Wikimedia Commons."""

    @property
    def source_name(self) -> str:
        return "document_on_commons"

    def parse_url(self, url: str) -> str:
        """Extract filename from Commons URL."""
        # URL format: http://commons.wikimedia.org/wiki/Special:FilePath/Filename.pdf
        parsed = urlparse(url)
        path = unquote(parsed.path)

        if 'Special:FilePath/' in path:
            filename = path.split('Special:FilePath/')[-1]
        elif 'File:' in path:
            filename = path.split('File:')[-1]
        else:
            filename = path.split('/')[-1]

        return filename

    def get_file_info(self, filename: str) -> dict | None:
        """Get file info from Commons API."""
        api_url = "https://commons.wikimedia.org/w/api.php"
        params = {
            'action': 'query',
            'titles': f'File:{filename}',
            'prop': 'imageinfo',
            'iiprop': 'url|mime|size',
            'format': 'json',
        }

        data = self.make_request(api_url, params)
        if data:
            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                if page_id != '-1':
                    imageinfo = page_data.get('imageinfo', [{}])[0]
                    return {
                        'url': imageinfo.get('url'),
                        'mime': imageinfo.get('mime'),
                        'size': imageinfo.get('size'),
                    }
        return None

    def get_djvu_text(self, filename: str) -> str | None:
        """Get text from DjVu file using Commons API."""
        api_url = "https://commons.wikimedia.org/w/api.php"

        # Get the number of pages first
        params = {
            'action': 'query',
            'titles': f'File:{filename}',
            'prop': 'imageinfo',
            'iiprop': 'pagecount',
            'format': 'json',
        }

        data = self.make_request(api_url, params)
        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        pagecount = 1
        for page_id, page_data in pages.items():
            if page_id != '-1':
                imageinfo = page_data.get('imageinfo', [{}])[0]
                pagecount = imageinfo.get('pagecount', 1) or 1

        # Limit to first 50 pages for speed
        max_pages = min(pagecount, 50)

        # Get text from each page
        texts = []
        for page_num in range(1, max_pages + 1):
            params = {
                'action': 'query',
                'titles': f'Page:{filename}/{page_num}',
                'prop': 'revisions',
                'rvprop': 'content',
                'format': 'json',
            }

            data = self.make_request(api_url, params)
            if data:
                pages = data.get('query', {}).get('pages', {})
                for pid, pdata in pages.items():
                    if pid != '-1':
                        revisions = pdata.get('revisions', [])
                        if revisions:
                            content = revisions[0].get('*', '')
                            # Clean wiki markup
                            content = re.sub(r'<noinclude>.*?</noinclude>', '', content, flags=re.DOTALL)
                            content = re.sub(r'\{\{[^}]+\}\}', '', content)
                            if content.strip():
                                texts.append(content.strip())

        return '\n\n'.join(texts) if texts else None

    def extract(self, item: dict) -> dict:
        """Extract text from Commons document."""
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
            filename = self.parse_url(url)
            result['filename'] = filename
        except Exception as e:
            result['status'] = 'error'
            result['error'] = f'URL parse error: {e}'
            return result

        # Get file info
        file_info = self.get_file_info(filename)
        if not file_info:
            result['status'] = 'error'
            result['error'] = 'Could not get file info'
            return result

        mime = file_info.get('mime', '')
        result['mime'] = mime

        # Only handle DjVu for now (PDF requires downloading and parsing)
        if 'djvu' in mime.lower() or filename.lower().endswith('.djvu'):
            text = self.get_djvu_text(filename)
        else:
            # Skip PDFs for now - would need to download and parse
            result['status'] = 'skipped'
            result['reason'] = f'unsupported_format ({mime})'
            return result

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
