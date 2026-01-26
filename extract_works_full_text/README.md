# Full Text Extraction Sources

This document outlines the main sources available for extracting full text content from the works in `instance_properties.db`.

## Database Summary

The database contains **99,958 works** with the following text-related resources:

| Source Type | Count | Description |
|-------------|-------|-------------|
| Wikisource sitelinks | 36,165 | Direct links to full text on Wikisource |
| Wikipedia sitelinks | 26,297 | Context/summaries (not full text) |
| Full work URLs | 11,578 | Direct URLs to digitized works |
| Open Library ID | 5,246 | Links to Open Library records |
| Internet Archive ID | 3,134 | Links to Archive.org |
| Gallica ID | 1,670 | French National Library digitized works |
| BHL bibliography ID | 1,593 | Biodiversity Heritage Library |
| HathiTrust ID | 396 | HathiTrust Digital Library |
| Project Gutenberg ID | 116 | Public domain ebooks |

---

## Primary Sources (Priority Order)

### 1. Wikisource (Best Source)
- **Coverage**: 36,165 works
- **API**: `https://en.wikisource.org/w/api.php` (or other language subdomains)
- **Advantages**: Clean, structured text; multiple languages; community-verified
- **Python library**: `mwparserfromhell` for parsing wiki markup

```python
import requests

def get_wikisource_text(title, lang='en'):
    url = f"https://{lang}.wikisource.org/w/api.php"
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'extracts',
        'explaintext': True,
        'format': 'json'
    }
    response = requests.get(url, params=params)
    data = response.json()
    pages = data['query']['pages']
    page = next(iter(pages.values()))
    return page.get('extract', '')
```

---

### 2. Internet Archive (Archive.org)
- **Coverage**: 3,134 works + many in `full_work_url`
- **API**: `https://archive.org/metadata/{identifier}`
- **Full text API**: `https://archive.org/stream/{identifier}/{identifier}_djvu.txt`
- **Advantages**: Large collection; OCR text available; free API
- **Python library**: `internetarchive`

```python
from internetarchive import get_item, download

def get_archive_text(identifier):
    item = get_item(identifier)
    # Get the full text file
    for file in item.files:
        if file['name'].endswith('_djvu.txt'):
            url = f"https://archive.org/download/{identifier}/{file['name']}"
            response = requests.get(url)
            return response.text
    return None
```

---

### 3. Project Gutenberg
- **Coverage**: 116 works (but high-quality public domain texts)
- **API**: Direct download from `https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt`
- **Advantages**: Clean text; no OCR errors; well-formatted
- **Python library**: `gutenberg` (or direct HTTP requests)

```python
def get_gutenberg_text(ebook_id):
    url = f"https://www.gutenberg.org/cache/epub/{ebook_id}/pg{ebook_id}.txt"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    return None
```

---

### 4. Open Library / Internet Archive Books API
- **Coverage**: 5,246 works
- **API**: `https://openlibrary.org/works/{work_id}.json`
- **Full text**: Available through Internet Archive if book is borrowable
- **Advantages**: Rich metadata; connected to Archive.org

```python
def get_open_library_info(work_id):
    url = f"https://openlibrary.org/works/{work_id}.json"
    response = requests.get(url)
    return response.json()
```

---

### 5. Gallica (Bibliotheque nationale de France)
- **Coverage**: 1,670 works
- **API**: IIIF API for images, OAI-PMH for metadata
- **Full text**: Available via OCR downloads
- **URL pattern**: `https://gallica.bnf.fr/ark:/12148/{ark_id}/f1.texte`

```python
def get_gallica_text(ark_id):
    # Extract ark from URL like: https://gallica.bnf.fr/ark:/12148/btv1b90602709
    url = f"https://gallica.bnf.fr/ark:/12148/{ark_id}/f1.texte"
    response = requests.get(url)
    return response.text
```

---

### 6. HathiTrust Digital Library
- **Coverage**: 396 works
- **API**: `https://babel.hathitrust.org/cgi/htd/volume/pageocr/{htid}`
- **Limitations**: Some texts restricted to partner institutions
- **Registration**: API key required

```python
def get_hathitrust_text(htid):
    # Note: May require authentication for full text
    url = f"https://babel.hathitrust.org/cgi/htd/volume/pageocr/{htid}"
    response = requests.get(url)
    return response.json()
```

---

### 7. BHL (Biodiversity Heritage Library)
- **Coverage**: 1,593 works
- **API**: `https://www.biodiversitylibrary.org/api3`
- **Specialization**: Scientific/natural history texts
- **API key**: Required (free)

```python
def get_bhl_text(item_id, api_key):
    url = f"https://www.biodiversitylibrary.org/api3?op=GetItemMetadata&id={item_id}&ocr=true&apikey={api_key}&format=json"
    response = requests.get(url)
    return response.json()
```

---

### 8. Direct Full Work URLs
- **Coverage**: 11,578 URLs in `prop_CONTENT_full_work_url`
- **Sources include**: Archive.org, Google Books, university libraries
- **Approach**: Parse and fetch based on domain

---

## Extraction Strategy (Waterfall)

For each work, try sources in this order:

```
1. Wikisource (if sitelink exists)
      |
      v
2. Project Gutenberg (if ID exists) - highest quality
      |
      v
3. Internet Archive (if ID or URL exists)
      |
      v
4. Open Library (if ID exists)
      |
      v
5. Gallica (if ID exists, French works)
      |
      v
6. HathiTrust (if ID exists, may have access restrictions)
      |
      v
7. BHL (if ID exists, scientific works)
      |
      v
8. Direct full_work_url (fallback)
      |
      v
9. Google Books (limited preview, harder to extract)
```

---

## SQL Queries to Extract Source Data

### Get all Wikisource URLs
```sql
SELECT instance_id, instance_label, sitelink_url
FROM instances_sitelinks
WHERE sitelink_type = 'wikisource';
```

### Get Internet Archive IDs
```sql
SELECT instance_id, instance_label, identifier_url
FROM instances_identifiers
WHERE identifier_label = 'Internet Archive ID';
```

### Get all full work URLs
```sql
SELECT * FROM prop_CONTENT_full_work_url;
```

### Get Gutenberg IDs
```sql
SELECT instance_id, instance_label, identifier_url
FROM instances_identifiers
WHERE identifier_label = 'Project Gutenberg ebook ID';
```

---

## Coverage Estimate

| Source | Works with Full Text Potential |
|--------|-------------------------------|
| Wikisource | ~36,000 (direct full text) |
| Internet Archive | ~3,000 + URLs |
| Open Library | ~5,000 (variable availability) |
| Gallica | ~1,600 (French works) |
| Project Gutenberg | ~116 (high quality) |
| HathiTrust | ~400 (access varies) |
| Direct URLs | ~11,500 |

**Estimated total unique works with accessible full text: ~45,000-50,000** (with overlap between sources)

---

## Notes

- **Rate limiting**: Respect API rate limits (Wikisource: 200 req/s, Archive.org: be reasonable)
- **Language**: Wikisource exists in many languages (en, fr, de, ru, zh, etc.)
- **Quality**: Project Gutenberg > Wikisource > Archive.org OCR > others
- **Copyright**: Most sources focus on public domain works; check copyright status before use
