# Literary Works Data Pipeline

Data pipeline for extracting and enriching literary works data from Wikidata.

## Quick Start

```bash
cd data_pipeline

# Run full pipeline
./run_pipeline.sh

# Or run without OpenAI enrichment
./run_pipeline.sh --no-openai

# Or run only OpenAI enrichment (if database exists)
./run_pipeline.sh --openai-only
```

## Directory Structure

```
data_pipeline/
├── README.md
├── run_pipeline.sh                   # Main pipeline script
├── scripts/
│   ├── 01_fetch_wikidata.py          # Fetch data from Wikidata API
│   ├── 02_create_database.py         # Create SQLite database
│   ├── 03_enrich_dates.py            # Add year column
│   ├── 04_filter_instances.py        # Mark excluded instance types
│   ├── 05_enrich_countries_openai.py # Map countries (OpenAI, parallel)
│   ├── 06_enrich_languages_openai.py # Map languages (OpenAI, parallel)
│   └── 07_add_regions_openai.py      # Add regions (OpenAI, parallel)
├── data/                             # Input data files
├── enriched_literary_works/          # CSV batches from Wikidata
├── cache/                            # Cached OpenAI results (auto-generated)
│   ├── country_mappings.json
│   ├── language_mappings.json
│   └── region_mappings.json
├── output/
│   └── literary_works.db             # Final SQLite database
└── notebooks/
    └── analysis.ipynb                # Analysis notebook
```

## Pipeline Steps

### Step 1-4: Core Pipeline (No OpenAI required)

These steps create the base database without any external API dependencies:

```bash
cd data_pipeline

# Step 1: Fetch data from Wikidata (requires data/all_literary_works.csv)
python scripts/01_fetch_wikidata.py

# Step 2: Create SQLite database from CSV files
python scripts/02_create_database.py

# Step 3: Add year column (from inception_date or publication_date)
python scripts/03_enrich_dates.py

# Step 4: Mark excluded instance types (comics, journals, patents, etc.)
python scripts/04_filter_instances.py
```

### Step 5-7: OpenAI Enrichment (Optional)

These steps use OpenAI to enrich the database with geographic mappings.
Requires `OPENAI_API_KEY` environment variable on first run.

**Features:**

- **Caching:** Results are automatically saved to `cache/` directory. On subsequent runs, cached results are used instead of calling OpenAI again.
- **Confidence requirement:** Prompts instruct the model to only return mappings when confident, otherwise return null.
- **Two-pass approach:** After the first pass, a second pass attempts to map remaining NULL items with additional context.
- **Parallel processing:** Uses ThreadPoolExecutor with 5 workers for faster API calls.

```bash
# Step 5: Map historical countries to modern equivalents
python scripts/05_enrich_countries_openai.py

# Step 6: Map languages to primary modern country
python scripts/06_enrich_languages_openai.py

# Step 7: Add world region classification to countries
python scripts/07_add_regions_openai.py
```

To force a fresh OpenAI call, delete the corresponding cache file:

```bash
rm cache/country_mappings.json   # Re-run country mapping
rm cache/language_mappings.json  # Re-run language mapping
rm cache/region_mappings.json    # Re-run region mapping
```

## Running Scripts Independently

Each script (except 01 and 02) is **idempotent** - you can re-run it without breaking the database. The script will reset and update only the columns it manages:

| Script | Updates | Can re-run? |
|--------|---------|-------------|
| 02_create_database.py | Recreates entire database | Yes (fresh start) |
| 03_enrich_dates.py | `literary_works.year` | Yes |
| 04_filter_instances.py | `instances.excluded` | Yes |
| 05_enrich_countries_openai.py | `countries.modern_country`, `literary_works.modern_country` (where countryLabel exists) | Yes |
| 06_enrich_languages_openai.py | `languages.inferred_modern_country`, `literary_works.modern_country` (where countryLabel is NULL) | Yes |
| 07_add_regions_openai.py | `countries.main_region` | Yes |

**Example:** Re-run only the country mapping:

```bash
python scripts/05_enrich_countries_openai.py
```

**Note:** Scripts 05 and 06 update different records in `literary_works.modern_country`:

- Script 05: Records with a `countryLabel`
- Script 06: Records without a `countryLabel` (language-inferred)

## Database Schema

### `literary_works`

Main table with literary works data.

| Column | Type | Description |
|--------|------|-------------|
| `item_id` | TEXT | Wikidata ID (e.g., Q12345) |
| `itemLabel` | TEXT | Name of the literary work |
| `instanceLabel` | TEXT | Type of work (e.g., novel, poem) |
| `countryLabel` | TEXT | Country of origin |
| `languageLabel` | TEXT | Language of the work |
| `authorLabel` | TEXT | Author name |
| `publication_date` | TEXT | Publication date (YYYY-MM-DD, negative for BC) |
| `inception_date` | TEXT | Creation date (YYYY-MM-DD, negative for BC) |
| `sitelink_count` | INTEGER | Number of Wikipedia/Wikisource pages for this work |
| `year` | INTEGER | Year (negative for BC dates) |
| `modern_country` | TEXT | Modern country equivalent (OpenAI enriched) |

Table is sorted by `sitelink_count` descending (most linked works first).

### `sitelinks`

Wikipedia sitelinks for each item.

| Column | Type | Description |
|--------|------|-------------|
| `item_id` | TEXT | Wikidata ID |
| `itemLabel` | TEXT | Name of the literary work |
| `sitelink` | TEXT | Wikipedia/Wikisource URL |

### `instances`

Lookup table for instance types.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Wikidata ID |
| `label` | TEXT | Instance type name |
| `excluded` | INTEGER | 1 if excluded from analysis, 0 otherwise |

### `countries`

Lookup table for countries.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Wikidata ID |
| `label` | TEXT | Country name |
| `modern_country` | TEXT | Modern country equivalent |
| `main_region` | TEXT | World region classification |

### `languages`

Lookup table for languages.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Wikidata ID |
| `label` | TEXT | Language name |
| `inferred_modern_country` | TEXT | Primary modern country for this language |

### `authors`

Lookup table for authors.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Wikidata ID |
| `label` | TEXT | Author name |

## Excluded Instance Types

The following instance types are marked as excluded (step 4):

- comics, comic book, comic book series, comic book album, comic strip, webcomic
- manga, manga series, webtoon
- scientific journal, academic journal, open-access journal
- Wikimedia list article, Wikipedia overview article
- United States patent
- magazine, report, Hansard
- and more...

## Usage Example

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("output/literary_works.db")

# Get works excluding comics/journals/etc.
df = pd.read_sql("""
    SELECT lw.item_id, lw.itemLabel, lw.year, lw.modern_country
    FROM literary_works lw
    JOIN instances i ON lw.instanceLabel = i.label
    WHERE i.excluded = 0
    AND lw.year BETWEEN -500 AND 1800
    ORDER BY lw.year
""", conn)

conn.close()
```

## Environment Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install pandas requests tqdm python-dotenv openai

# For OpenAI enrichment, set API key
export OPENAI_API_KEY="your-key-here"
# Or create a .env file with OPENAI_API_KEY=your-key-here
```
