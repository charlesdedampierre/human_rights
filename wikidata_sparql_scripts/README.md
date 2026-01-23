# Wikidata Pre-1900 Written Works Extraction

## Goal

Extract all **pre-1900 written works** from Wikidata for historical text analysis, then extract their **metadata properties** (date, place, content, identifiers) for NLP analysis.

## Directory Structure

```
wikidata_sparql_scripts/
├── README.md
│
├── classes/                    # STEP 1: Query and classify document types
│   ├── query_*.py              # Scripts to query Wikidata for class hierarchies
│   ├── classify_*.py           # Scripts to classify classes as pre-1900 vs modern
│   └── output/
│       ├── raw/                # Raw extracted class data
│       ├── pre1900/            # Classified pre-1900 data
│       ├── all_pre1900_unified.json    # Final deduplicated pre-1900 classes
│       └── all_modern_excluded.json    # Modern classes excluded
│
├── instances/                  # STEP 2: Extract actual items from pre-1900 classes
│   ├── extract_instances_parallel.py   # Main instance extraction
│   ├── extract_pre1900_instances.py    # Pre-1900 filter
│   └── output/
│       └── instances_by_class/         # 4M+ instance IDs by class
│
├── properties/                 # STEP 3: Property documentation
│   └── output/
│       ├── exhaustive_properties.md    # Full property documentation
│       ├── exhaustive_properties.json  # JSON version
│       └── property_categories.json    # Category mapping
│
└── instance_properties/        # STEP 4: Extract properties from instances
    ├── extract_instance_properties.py  # Step 4a: Main properties extraction
    └── extract_author_properties.py    # Step 4b: Author enrichment
```

## Workflow Overview

| Step | Folder | Description | Output |
|------|--------|-------------|--------|
| 1 | `classes/` | Query & classify pre-1900 document types | 2,662 classes |
| 2 | `instances/` | Extract instance IDs for each class | 4,008,966 unique instances |
| 3 | `properties/` | Document useful properties | 142+ properties |
| 4 | `instance_properties/` | Extract metadata from instances | DATE, PLACE, CONTENT, IDENTIFIERS |

---

## Step 1: Classes (`classes/`)

Query Wikidata for subclasses of root types (written_work, publication, law, etc.) and classify each as pre-1900 or modern using GPT-4o-mini.

### Output

- **2,662 pre-1900 classes** (historical types)
- **5,762 modern classes** excluded (Wikimedia content, digital artifacts)

---

## Step 2: Instances (`instances/`)

Extract all instance IDs for each pre-1900 class.

### Output

- **4,008,966 unique instances** (after deduplication)
- Stored in `output/instances_by_class/{QID}.json`

### Top Classes by Instance Count

| Class | Instances |
|-------|-----------|
| Q3331189 (version, edition or translation) | 801,952 |
| Q13433827 (encyclopedia article) | 658,352 |
| Q7725634 (literary work) | 487,078 |
| Q87167 (manuscript) | 103,815 |
| Q11032 (newspaper) | 51,474 |

---

## Step 3: Properties (`properties/`)

Documentation of all Wikidata properties useful for extracting metadata.

### Property Categories

| Category | Count | Purpose |
|----------|-------|---------|
| **DATE** | 28 | Publication dates, inception, author birth/death |
| **PLACE** | 21 | Country of origin, author nationality/birthplace |
| **CONTENT** | 27 | Full text URLs, Wikisource sitelinks, images |
| **IDENTIFIERS** | 51+ | VIAF, ISBN, OCLC, DOI, Open Library |
| **TYPES** | 20 | Instance of, genre, form, language |
| **CREATORS** | 23 | Author, editor, translator |

### Key Files

- `output/exhaustive_properties.md` - Full documentation
- `output/exhaustive_properties.json` - JSON version
- `output/property_categories.json` - Category mapping

---

## Step 4: Instance Properties (`instance_properties/`)

Extract metadata from instances using SPARQL queries.

### Step 4a: Main Properties (`extract_instance_properties.py`)

Extracts from each instance:

- **Main properties**: date, place, content, types, creators
- **ALL identifiers**: via `wikibase:ExternalId` (VIAF, ISBN, etc.)
- **ALL sitelinks**: Wikipedia, Wikisource, Commons

### Step 4b: Author Enrichment (`extract_author_properties.py`)

Extracts author properties for DATE/PLACE inference:

- Birth/death dates → terminus post/ante quem
- Nationality/birthplace → geographic origin

### SPARQL Query Examples

**Main Properties:**

```sparql
SELECT ?item ?publicationDate ?inception ?country ?language ?author
WHERE {
    VALUES ?item { wd:Q480 }
    OPTIONAL { ?item wdt:P577 ?publicationDate . }
    OPTIONAL { ?item wdt:P571 ?inception . }
    OPTIONAL { ?item wdt:P495 ?country . }
    OPTIONAL { ?item wdt:P407 ?language . }
    OPTIONAL { ?item wdt:P50 ?author . }
}
```

**All Identifiers:**

```sparql
SELECT ?item ?prop ?propLabel ?value
WHERE {
    VALUES ?item { wd:Q480 }
    ?item ?p ?value .
    ?prop wikibase:directClaim ?p ;
          wikibase:propertyType wikibase:ExternalId .
}
```

**All Sitelinks:**

```sparql
SELECT ?item ?sitelink ?wiki ?title
WHERE {
    VALUES ?item { wd:Q480 }
    ?sitelink schema:about ?item ;
              schema:isPartOf ?wiki ;
              schema:name ?title .
}
```

---

## Key Properties for Extraction

### DATE (when was it created?)

| Property | Label | Description |
|----------|-------|-------------|
| P577 | publication date | Date of first publication |
| P571 | inception | Date when created |
| P569 | date of birth (author) | Terminus post quem |
| P570 | date of death (author) | Terminus ante quem |

### PLACE (where did it originate?)

| Property | Label | Description |
|----------|-------|-------------|
| P495 | country of origin | Country where work originated |
| P291 | place of publication | Where published |
| P27 | country of citizenship (author) | Author's nationality |
| P19 | place of birth (author) | Author's birthplace |

### CONTENT (how to access full text?)

| Property | Label | Description |
|----------|-------|-------------|
| P953 | full work available at URL | Direct link to full text |
| P1433 | published in | Journal/collection |
| P18 | image | Image (can be OCR'd) |
| Wikisource | sitelink | Full text on Wikisource |

### IDENTIFIERS (how to find in other databases?)

| Property | Label | Description |
|----------|-------|-------------|
| P214 | VIAF ID | Virtual International Authority File |
| P243 | OCLC control number | WorldCat ID |
| P648 | Open Library ID | Internet Archive |
| P212 | ISBN-13 | Book identifier |

---

## Usage

```bash
# Step 1: Classify classes (already done)
cd classes && python classify_all_pre1900.py

# Step 2: Extract instances (already done)
cd instances && python extract_instances_parallel.py

# Step 4: Extract properties from instances
cd instance_properties && python extract_instance_properties.py

# Step 4b: Enrich with author properties
cd instance_properties && python extract_author_properties.py
```

---

## Statistics

| Metric | Value |
|--------|-------|
| Pre-1900 classes | 2,662 |
| Unique instances | 4,008,966 |
| Properties documented | 142+ |
| Wikisource-linked works | ~100,000+ |

---

## Requirements

```
requests
```

Optional (for classification):

```
openai
```
