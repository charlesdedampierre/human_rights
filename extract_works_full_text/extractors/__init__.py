"""
Cultura Archive - Text Extractors

Extractors for various source types:
- Wikisource: Full literary texts
- Wikipedia: Encyclopedia articles
- Commons: PDF/DjVu documents
- Web URLs: Various digital libraries
"""

from .base import BaseExtractor
from .wikisource import WikisourceExtractor
from .wikipedia import WikipediaExtractor
from .commons import CommonsExtractor
from .web_urls import WebURLExtractor

__all__ = [
    'BaseExtractor',
    'WikisourceExtractor',
    'WikipediaExtractor',
    'CommonsExtractor',
    'WebURLExtractor',
]
