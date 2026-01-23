# Exhaustive Wikidata Properties for NLP Metadata Extraction

This document lists all Wikidata properties useful for extracting metadata from literary works and written documents for NLP analysis.

## Overview

| Category | Count | Purpose |
|----------|-------|---------|
| **DATE** | 28 | Determine WHEN a work was created/published |
| **PLACE** | 21 | Determine WHERE a work originated |
| **CONTENT** | 27 | Access FULL TEXT for NLP analysis |
| **IDENTIFIERS** | 51+ | Find works in EXTERNAL DATABASES |
| **TYPES** | 20 | CLASSIFY the work |
| **CREATORS** | 23 | Identify WHO created the work |

---

## Extraction Pipeline

### Step 1: Extract Instance Properties
**Script**: `instances/instance_properties/extract_instance_properties.py`

Extracts from each work:
- Main properties (date, place, content, types)
- Creator IDs (author, editor, translator)
- ALL external identifiers (via `wikibase:ExternalId`)
- ALL sitelinks (Wikipedia, Wikisource, Commons)

### Step 2: Extract Author Properties
**Script**: `instances/instance_properties/extract_author_properties.py`

Enriches with author data for inference:
- Birth/death dates → DATE inference (terminus post/ante quem)
- Nationality/birthplace → PLACE inference
- Author identifiers

---

## DATE

Properties to determine when a work was created or published.

### Direct

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P577 | publication date | 1 | Date of first publication |
| P571 | inception | 1 | Date when the work was created |
| P580 | start time | 2 | Start of a time period |
| P582 | end time | 2 | End of a time period |
| P585 | point in time | 2 | Specific time point |
| P1191 | date of first performance | 2 | First performance of a play/opera |
| P1319 | earliest date | 2 | Earliest possible date |
| P1326 | latest date | 2 | Latest possible date |
| P2031 | work period (start) | 3 | Start of creative period |
| P2032 | work period (end) | 3 | End of creative period |
| P3893 | public domain date | 3 | Date work entered public domain |
| P1249 | time of earliest written record | 3 | First documented mention |
| P575 | time of discovery or invention | 3 | Discovery date |
| P2754 | production date | 3 | Date of production |

### Via Author (Step 2)

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P569 | date of birth | 2 | Author's birth date (terminus post quem) |
| P570 | date of death | 2 | Author's death date (terminus ante quem) |
| P1317 | floruit | 3 | When author was active |
| P2031 | work period (start) | 3 | Author's creative period start |
| P2032 | work period (end) | 3 | Author's creative period end |

### Indirect Indicators

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P407 | language of work | 3 | Language evolution can indicate period |
| P282 | writing system | 3 | Writing system can indicate period |
| P136 | genre | 4 | Genre can indicate literary period |
| P135 | movement | 4 | Literary movement indicates period |

---

## PLACE

Properties to determine geographic origin.

### Direct

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P495 | country of origin | 1 | Country where work originated |
| P17 | country | 1 | Country associated with item |
| P291 | place of publication | 1 | Where the work was published |
| P840 | narrative location | 2 | Where the story takes place |
| P131 | located in administrative territorial entity | 2 | Administrative location |
| P276 | location | 2 | Physical location |
| P1001 | applies to jurisdiction | 3 | Jurisdiction where applicable |
| P625 | coordinate location | 3 | Geographic coordinates |

### Via Author (Step 2)

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P27 | country of citizenship | 2 | Author's nationality |
| P19 | place of birth | 2 | Author's birthplace |
| P20 | place of death | 3 | Author's death place |
| P551 | residence | 3 | Where author lived |
| P937 | work location | 3 | Where author worked |

### Via Publisher

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P17 | country | 2 | Publisher's country |
| P131 | located in administrative entity | 3 | Publisher's location |
| P159 | headquarters location | 3 | Publisher's headquarters |

### Indirect Indicators

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P407 | language of work | 2 | Language indicates likely origin |
| P364 | original language | 2 | Original language |
| P282 | writing system | 3 | Writing system indicates region |

---

## CONTENT

Properties to access full text or content for NLP analysis.

### Direct URLs

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P953 | full work available at URL | 1 | Direct link to full text |
| P973 | described at URL | 2 | URL with description |
| P856 | official website | 2 | Official website |
| P854 | reference URL | 3 | Reference URL |
| P1065 | archive URL | 2 | Archived version URL |
| P2699 | URL | 3 | General URL |

### Publication Info

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P1433 | published in | 1 | Journal/collection containing work |
| P1343 | described by source | 2 | Source that describes work |
| P123 | publisher | 2 | Publisher of the work |
| P291 | place of publication | 2 | Publication location |

### Media Files

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P18 | image | 2 | Image (can be OCR'd + AI translated) |
| P996 | document file on Wikimedia Commons | 1 | Document file |
| P51 | audio | 3 | Audio file |
| P10 | video | 3 | Video file |

### Text Properties

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P1476 | title | 1 | Title of the work |
| P1680 | subtitle | 2 | Subtitle |
| P921 | main subject | 2 | Main topic |
| P1922 | first line | 3 | Opening line of text |
| rdfs:label | labels | 1 | Labels in ALL languages |
| schema:description | descriptions | 1 | Descriptions in ALL languages |

### Copyright Info

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P6216 | copyright status | 1 | Copyright status |
| P275 | license | 2 | License type |
| P3893 | public domain date | 2 | When entered public domain |

### Sitelinks (Extracted Separately)

| Type | Priority | Description |
|------|----------|-------------|
| Wikisource | 1 | **Full text** of public domain works |
| Wikipedia | 2 | Wikipedia articles (summary, context) |
| Wikimedia Commons | 2 | Media files (images, documents) |

**Note**: Sitelinks are extracted using `schema:about` and `schema:isPartOf`.

---

## IDENTIFIERS

External identifiers for finding works in other databases.

**Note**: All identifiers are extracted automatically using `wikibase:propertyType = wikibase:ExternalId`.

### Library Authorities

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P214 | VIAF ID | 1 | Virtual International Authority File |
| P244 | Library of Congress authority ID | 1 | LCNAF |
| P227 | GND ID | 1 | German National Library |
| P268 | BnF ID | 1 | Bibliothèque nationale de France |
| P269 | IdRef ID | 2 | French SUDOC authority |
| P349 | NDL ID | 2 | National Diet Library Japan |
| P950 | BNE ID | 2 | Spanish National Library |
| P1006 | NTA ID | 2 | Dutch National Thesaurus |
| P1017 | BAV ID | 2 | Vatican Library |
| P8189 | National Library of Israel J9U ID | 3 | Israeli library |

### Book Identifiers

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P212 | ISBN-13 | 1 | ISBN 13-digit |
| P957 | ISBN-10 | 1 | ISBN 10-digit |
| P236 | ISSN | 1 | Serial publication ID |
| P243 | OCLC control number | 1 | WorldCat ID |
| P5331 | OCLC work ID | 2 | OCLC work-level ID |
| P1036 | Dewey Decimal Classification | 2 | DDC number |

### Digital Library IDs

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P648 | Open Library ID | 1 | Internet Archive Open Library |
| P8383 | Goodreads work ID | 2 | Goodreads |
| P1085 | LibraryThing work ID | 2 | LibraryThing |
| P4223 | Trove work ID | 2 | Australian library |

### Academic IDs

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P356 | DOI | 1 | Digital Object Identifier |
| P698 | PubMed ID | 1 | PubMed article ID |
| P932 | PMCID | 2 | PubMed Central ID |
| P888 | JSTOR article ID | 2 | JSTOR |

### General IDs

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P213 | ISNI | 1 | International Standard Name Identifier |
| P2671 | Google Knowledge Graph ID | 2 | Google KG |
| P373 | Commons category | 2 | Wikimedia Commons category |

---

## TYPES

Properties for classification and genre.

### Classification

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P31 | instance of | 1 | What type of thing this is |
| P279 | subclass of | 2 | Parent class |
| P361 | part of | 2 | What this is part of |
| P527 | has part(s) | 3 | Components |

### Genre Form

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P136 | genre | 1 | Genre of the work |
| P7937 | form of creative work | 1 | Form (novel, poem, etc.) |
| P2551 | used metre | 2 | Verse meter/prosody |
| P921 | main subject | 2 | Main topic |
| P135 | movement | 2 | Literary/artistic movement |
| P101 | field of work | 3 | Field/discipline |

### Writing Style

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P282 | writing system | 2 | Script used |
| P407 | language of work | 1 | Language |
| P364 | original language | 2 | Original language |

### Relationships

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P144 | based on | 2 | Source material |
| P4969 | derivative work | 3 | Works derived from this |
| P737 | influenced by | 3 | Influences |
| P155 | follows | 2 | Previous in sequence |
| P156 | followed by | 2 | Next in sequence |
| P179 | part of the series | 2 | Series membership |

---

## CREATORS

Properties related to authors, editors, and other creators.

### Direct (Step 1)

| Property | Label | Priority | Description |
|----------|-------|----------|-------------|
| P50 | author | 1 | Author of the work (Wikidata ID) |
| P2093 | author name string | 1 | Author name as text |
| P1779 | possible creator | 2 | Attributed author |
| P98 | editor | 2 | Editor |
| P655 | translator | 2 | Translator |
| P170 | creator | 2 | General creator |
| P86 | composer | 2 | Music composer |
| P110 | illustrator | 2 | Illustrator |
| P123 | publisher | 2 | Publisher |

### Author Properties (Step 2)

| Property | Label | Category | Description |
|----------|-------|----------|-------------|
| P569 | date of birth | DATE | For dating works |
| P570 | date of death | DATE | For dating works |
| P27 | country of citizenship | PLACE | For location |
| P19 | place of birth | PLACE | For location |
| P20 | place of death | PLACE | For location |
| P551 | residence | PLACE | For location |
| P1317 | floruit | DATE | For dating |
| P106 | occupation | context | Author's profession |
| P21 | sex or gender | context | Gender |

---

## SPARQL Query Examples

### Main Properties Query

```sparql
SELECT ?item ?itemLabel ?itemDescription
       ?publicationDate ?inception ?country ?countryLabel
       ?language ?languageLabel ?genre ?genreLabel
       ?author ?authorLabel ?authorNameString
       ?fullWorkURL ?image ?title
WHERE {
    VALUES ?item { wd:Q480 wd:Q8258 }

    OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
    OPTIONAL { ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "en") }

    OPTIONAL { ?item wdt:P577 ?publicationDate . }
    OPTIONAL { ?item wdt:P571 ?inception . }
    OPTIONAL { ?item wdt:P495 ?country . }
    OPTIONAL { ?item wdt:P407 ?language . }
    OPTIONAL { ?item wdt:P136 ?genre . }
    OPTIONAL { ?item wdt:P50 ?author . }
    OPTIONAL { ?item wdt:P2093 ?authorNameString . }
    OPTIONAL { ?item wdt:P953 ?fullWorkURL . }
    OPTIONAL { ?item wdt:P18 ?image . }
    OPTIONAL { ?item wdt:P1476 ?title . }

    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
```

### All Identifiers Query

```sparql
SELECT ?item ?prop ?propLabel ?value
WHERE {
    VALUES ?item { wd:Q480 }

    ?item ?p ?value .
    ?prop wikibase:directClaim ?p ;
          wikibase:propertyType wikibase:ExternalId .

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
```

### All Sitelinks Query

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

## File Structure

```
wikidata_sparql_scripts/
├── properties/
│   └── output/
│       ├── exhaustive_properties.md    # This file
│       ├── exhaustive_properties.json  # JSON version
│       └── property_categories.json    # Category mapping
│
└── instances/
    ├── output/
    │   └── instances_by_class/         # Instance IDs per class
    │
    └── instance_properties/
        ├── extract_instance_properties.py  # Step 1
        └── extract_author_properties.py    # Step 2
```

---

## Summary

**Total unique properties**: 142+

**Key high-priority properties**:
- **DATE**: P577 (publication), P571 (inception), P569/P570 (author birth/death)
- **PLACE**: P495 (country of origin), P27 (author citizenship), P291 (publication place)
- **CONTENT**: P953 (full work URL), Wikisource sitelinks, P18 (image for OCR)
- **IDENTIFIERS**: All ExternalId properties (VIAF, ISBN, OCLC, DOI, etc.)
- **TYPES**: P31 (instance of), P136 (genre), P7937 (form), P407 (language)
- **CREATORS**: P50 (author), P2093 (author name string), P98 (editor)
