"""
Compile the final exhaustive list of Wikidata properties organized by category.

Categories:
- DATE: Properties to infer temporal information
- PLACE: Properties to infer geographic origin
- CONTENT: Properties to access full text or content
- IDENTIFIERS: External identifiers (VIAF, ISBN, etc.)
- TYPES: Classification and genre properties

Output: A comprehensive JSON and markdown file with all properties.
"""

import json
from pathlib import Path

OUTPUT_DIR = "output"

# =============================================================================
# EXHAUSTIVE PROPERTY LIST - MANUALLY CURATED AND VALIDATED
# =============================================================================

PROPERTIES = {
    # =========================================================================
    # DATE PROPERTIES
    # Properties that help determine when a work was created/published
    # =========================================================================
    "date": {
        "direct": {
            # Primary date properties
            "P577": {
                "label": "publication date",
                "priority": 1,
                "description": "Date of first publication",
            },
            "P571": {
                "label": "inception",
                "priority": 1,
                "description": "Date when the work was created",
            },
            "P580": {
                "label": "start time",
                "priority": 2,
                "description": "Start of a time period",
            },
            "P582": {
                "label": "end time",
                "priority": 2,
                "description": "End of a time period",
            },
            "P585": {
                "label": "point in time",
                "priority": 2,
                "description": "Specific time point",
            },
            # Performance/Release dates
            "P1191": {
                "label": "date of first performance",
                "priority": 2,
                "description": "First performance of a play/opera",
            },
            "P1619": {
                "label": "date of official opening",
                "priority": 3,
                "description": "Official opening date",
            },
            "P729": {
                "label": "service entry",
                "priority": 3,
                "description": "Date when entered service",
            },
            # Approximate dates
            "P1319": {
                "label": "earliest date",
                "priority": 2,
                "description": "Earliest possible date",
            },
            "P1326": {
                "label": "latest date",
                "priority": 2,
                "description": "Latest possible date",
            },
            "P1317": {
                "label": "floruit",
                "priority": 3,
                "description": "Period when person was active",
            },
            "P1249": {
                "label": "time of earliest written record",
                "priority": 3,
                "description": "First documented mention",
            },
            # Work periods
            "P2031": {
                "label": "work period (start)",
                "priority": 3,
                "description": "Start of creative period",
            },
            "P2032": {
                "label": "work period (end)",
                "priority": 3,
                "description": "End of creative period",
            },
            # Legal/Copyright dates
            "P3893": {
                "label": "public domain date",
                "priority": 3,
                "description": "Date work entered public domain",
            },
            "P7588": {
                "label": "effective date",
                "priority": 3,
                "description": "Date when something became effective",
            },
            # Other date indicators
            "P575": {
                "label": "time of discovery or invention",
                "priority": 3,
                "description": "Discovery date",
            },
            "P746": {
                "label": "date of disappearance",
                "priority": 4,
                "description": "When something disappeared",
            },
            "P2754": {
                "label": "production date",
                "priority": 3,
                "description": "Date of production",
            },
        },
        "via_author": {
            "P569": {
                "label": "date of birth",
                "priority": 2,
                "description": "Author's birth date (terminus post quem)",
            },
            "P570": {
                "label": "date of death",
                "priority": 2,
                "description": "Author's death date (terminus ante quem)",
            },
            "P1317": {
                "label": "floruit",
                "priority": 3,
                "description": "When author was active",
            },
            "P2031": {
                "label": "work period (start)",
                "priority": 3,
                "description": "Author's creative period start",
            },
            "P2032": {
                "label": "work period (end)",
                "priority": 3,
                "description": "Author's creative period end",
            },
        },
        "indirect_indicators": {
            "P407": {
                "label": "language of work",
                "priority": 3,
                "description": "Language evolution can indicate period",
            },
            "P282": {
                "label": "writing system",
                "priority": 3,
                "description": "Writing system can indicate period",
            },
            "P136": {
                "label": "genre",
                "priority": 4,
                "description": "Genre can indicate literary period",
            },
            "P135": {
                "label": "movement",
                "priority": 4,
                "description": "Literary movement indicates period",
            },
        },
    },
    # =========================================================================
    # PLACE PROPERTIES
    # Properties that help determine geographic origin
    # =========================================================================
    "place": {
        "direct": {
            # Primary location properties
            "P495": {
                "label": "country of origin",
                "priority": 1,
                "description": "Country where work originated",
            },
            "P17": {
                "label": "country",
                "priority": 1,
                "description": "Country associated with item",
            },
            "P291": {
                "label": "place of publication",
                "priority": 1,
                "description": "Where the work was published",
            },
            "P840": {
                "label": "narrative location",
                "priority": 2,
                "description": "Where the story takes place",
            },
            # Administrative/Geographic
            "P131": {
                "label": "located in administrative territorial entity",
                "priority": 2,
                "description": "Administrative location",
            },
            "P276": {
                "label": "location",
                "priority": 2,
                "description": "Physical location",
            },
            "P1001": {
                "label": "applies to jurisdiction",
                "priority": 3,
                "description": "Jurisdiction where applicable",
            },
            # Coordinates
            "P625": {
                "label": "coordinate location",
                "priority": 3,
                "description": "Geographic coordinates",
            },
            # Organization locations
            "P159": {
                "label": "headquarters location",
                "priority": 3,
                "description": "Where organization is based",
            },
            "P740": {
                "label": "location of formation",
                "priority": 3,
                "description": "Where something was formed",
            },
        },
        "via_author": {
            "P27": {
                "label": "country of citizenship",
                "priority": 2,
                "description": "Author's nationality",
            },
            "P19": {
                "label": "place of birth",
                "priority": 2,
                "description": "Author's birthplace",
            },
            "P20": {
                "label": "place of death",
                "priority": 3,
                "description": "Author's death place",
            },
            "P551": {
                "label": "residence",
                "priority": 3,
                "description": "Where author lived",
            },
            "P937": {
                "label": "work location",
                "priority": 3,
                "description": "Where author worked",
            },
        },
        "via_publisher": {
            "P17": {
                "label": "country",
                "priority": 2,
                "description": "Publisher's country",
            },
            "P131": {
                "label": "located in administrative entity",
                "priority": 3,
                "description": "Publisher's location",
            },
            "P159": {
                "label": "headquarters location",
                "priority": 3,
                "description": "Publisher's headquarters",
            },
        },
        "indirect_indicators": {
            "P407": {
                "label": "language of work",
                "priority": 2,
                "description": "Language indicates likely origin",
            },
            "P364": {
                "label": "original language",
                "priority": 2,
                "description": "Original language",
            },
            "P282": {
                "label": "writing system",
                "priority": 3,
                "description": "Writing system indicates region",
            },
        },
    },
    # =========================================================================
    # CONTENT PROPERTIES
    # Properties to access full text or content
    # =========================================================================
    "content": {
        "direct_urls": {
            "P953": {
                "label": "full work available at URL",
                "priority": 1,
                "description": "Direct link to full text",
            },
            "P973": {
                "label": "described at URL",
                "priority": 2,
                "description": "URL with description",
            },
            "P856": {
                "label": "official website",
                "priority": 2,
                "description": "Official website",
            },
            "P854": {
                "label": "reference URL",
                "priority": 3,
                "description": "Reference URL",
            },
            "P1065": {
                "label": "archive URL",
                "priority": 2,
                "description": "Archived version URL",
            },
            "P2699": {"label": "URL", "priority": 3, "description": "General URL"},
        },
        "publication_info": {
            "P1433": {
                "label": "published in",
                "priority": 1,
                "description": "Journal/collection containing work",
            },
            "P1343": {
                "label": "described by source",
                "priority": 2,
                "description": "Source that describes work",
            },
            "P123": {
                "label": "publisher",
                "priority": 2,
                "description": "Publisher of the work",
            },
            "P291": {
                "label": "place of publication",
                "priority": 2,
                "description": "Publication location",
            },
        },
        "media_files": {
            "P18": {
                "label": "image",
                "priority": 2,
                "description": "Image (can be OCR'd)",
            },
            "P996": {
                "label": "document file on Wikimedia Commons",
                "priority": 1,
                "description": "Document file",
            },
            "P51": {"label": "audio", "priority": 3, "description": "Audio file"},
            "P10": {"label": "video", "priority": 3, "description": "Video file"},
            "P154": {"label": "logo image", "priority": 4, "description": "Logo"},
        },
        "text_properties": {
            "P1476": {
                "label": "title",
                "priority": 1,
                "description": "Title of the work",
            },
            "P1680": {"label": "subtitle", "priority": 2, "description": "Subtitle"},
            "P921": {
                "label": "main subject",
                "priority": 2,
                "description": "Main topic",
            },
            "P1922": {
                "label": "first line",
                "priority": 3,
                "description": "Opening line of text",
            },
            "P1814": {
                "label": "name in kana",
                "priority": 4,
                "description": "Japanese kana name",
            },
        },
        "copyright_info": {
            "P6216": {
                "label": "copyright status",
                "priority": 1,
                "description": "Copyright status",
            },
            "P275": {"label": "license", "priority": 2, "description": "License type"},
            "P3893": {
                "label": "public domain date",
                "priority": 2,
                "description": "When entered public domain",
            },
        },
        "sitelinks": {
            "wikisource": {
                "label": "Wikisource sitelinks",
                "priority": 1,
                "description": "Full text on Wikisource",
            },
            "wikipedia": {
                "label": "Wikipedia sitelinks",
                "priority": 2,
                "description": "Wikipedia articles",
            },
            "wikidata": {
                "label": "Wikidata item",
                "priority": 3,
                "description": "Wikidata entry",
            },
            "commons": {
                "label": "Wikimedia Commons",
                "priority": 2,
                "description": "Media files on Commons",
            },
        },
    },
    # =========================================================================
    # IDENTIFIER PROPERTIES
    # External identifiers for finding works in other databases
    # =========================================================================
    "identifiers": {
        "library_authorities": {
            "P214": {
                "label": "VIAF ID",
                "priority": 1,
                "description": "Virtual International Authority File",
            },
            "P244": {
                "label": "Library of Congress authority ID",
                "priority": 1,
                "description": "LCNAF",
            },
            "P227": {
                "label": "GND ID",
                "priority": 1,
                "description": "German National Library",
            },
            "P268": {
                "label": "BnF ID",
                "priority": 1,
                "description": "Bibliothèque nationale de France",
            },
            "P269": {
                "label": "IdRef ID",
                "priority": 2,
                "description": "French SUDOC authority",
            },
            "P349": {
                "label": "NDL ID",
                "priority": 2,
                "description": "National Diet Library Japan",
            },
            "P906": {
                "label": "SELIBR ID",
                "priority": 2,
                "description": "Swedish library",
            },
            "P950": {
                "label": "BNE ID",
                "priority": 2,
                "description": "Spanish National Library",
            },
            "P1006": {
                "label": "NTA ID",
                "priority": 2,
                "description": "Dutch National Thesaurus",
            },
            "P1015": {
                "label": "NORAF ID",
                "priority": 2,
                "description": "Norwegian authority",
            },
            "P1017": {
                "label": "BAV ID",
                "priority": 2,
                "description": "Vatican Library",
            },
            "P1273": {
                "label": "CANTIC ID",
                "priority": 3,
                "description": "Catalan authority",
            },
            "P1368": {
                "label": "LNB ID",
                "priority": 3,
                "description": "Latvian National Library",
            },
            "P1695": {
                "label": "NLP ID",
                "priority": 3,
                "description": "Polish National Library",
            },
            "P5034": {
                "label": "National Library of Korea ID",
                "priority": 3,
                "description": "Korean library",
            },
            "P7369": {
                "label": "National Library of Brazil ID",
                "priority": 3,
                "description": "Brazilian library",
            },
            "P8189": {
                "label": "National Library of Israel J9U ID",
                "priority": 3,
                "description": "Israeli library",
            },
        },
        "book_identifiers": {
            "P212": {"label": "ISBN-13", "priority": 1, "description": "ISBN 13-digit"},
            "P957": {"label": "ISBN-10", "priority": 1, "description": "ISBN 10-digit"},
            "P236": {
                "label": "ISSN",
                "priority": 1,
                "description": "Serial publication ID",
            },
            "P243": {
                "label": "OCLC control number",
                "priority": 1,
                "description": "WorldCat ID",
            },
            "P5331": {
                "label": "OCLC work ID",
                "priority": 2,
                "description": "OCLC work-level ID",
            },
            "P1036": {
                "label": "Dewey Decimal Classification",
                "priority": 2,
                "description": "DDC number",
            },
            "P1104": {
                "label": "number of pages",
                "priority": 3,
                "description": "Page count",
            },
            "P5199": {
                "label": "British Library system number",
                "priority": 2,
                "description": "BL ID",
            },
            "P5361": {
                "label": "BNB ID",
                "priority": 2,
                "description": "British National Bibliography",
            },
            "P1292": {
                "label": "DNB edition ID",
                "priority": 2,
                "description": "German edition ID",
            },
            "P1044": {
                "label": "SWB editions ID",
                "priority": 3,
                "description": "German regional ID",
            },
        },
        "digital_library_ids": {
            "P648": {
                "label": "Open Library ID",
                "priority": 1,
                "description": "Internet Archive Open Library",
            },
            "P8383": {
                "label": "Goodreads work ID",
                "priority": 2,
                "description": "Goodreads",
            },
            "P1085": {
                "label": "LibraryThing work ID",
                "priority": 2,
                "description": "LibraryThing",
            },
            "P4223": {
                "label": "Trove work ID",
                "priority": 2,
                "description": "Australian library",
            },
            "P1003": {
                "label": "NLR ID",
                "priority": 3,
                "description": "Romanian National Library",
            },
            "P1143": {
                "label": "BN (Argentine) editions ID",
                "priority": 3,
                "description": "Argentine library",
            },
        },
        "academic_ids": {
            "P356": {
                "label": "DOI",
                "priority": 1,
                "description": "Digital Object Identifier",
            },
            "P698": {
                "label": "PubMed ID",
                "priority": 1,
                "description": "PubMed article ID",
            },
            "P932": {
                "label": "PMCID",
                "priority": 2,
                "description": "PubMed Central ID",
            },
            "P888": {
                "label": "JSTOR article ID",
                "priority": 2,
                "description": "JSTOR",
            },
            "P2860": {
                "label": "cites work",
                "priority": 3,
                "description": "Works cited",
            },
            "P496": {"label": "ORCID iD", "priority": 2, "description": "Author ORCID"},
            "P2427": {
                "label": "GRID ID",
                "priority": 3,
                "description": "Institution ID",
            },
            "P6782": {
                "label": "ROR ID",
                "priority": 3,
                "description": "Research Organization Registry",
            },
        },
        "general_ids": {
            "P213": {
                "label": "ISNI",
                "priority": 1,
                "description": "International Standard Name Identifier",
            },
            "P345": {
                "label": "IMDb ID",
                "priority": 2,
                "description": "Internet Movie Database",
            },
            "P646": {
                "label": "Freebase ID",
                "priority": 3,
                "description": "Freebase (archived)",
            },
            "P2671": {
                "label": "Google Knowledge Graph ID",
                "priority": 2,
                "description": "Google KG",
            },
            "P8168": {
                "label": "FactGrid item ID",
                "priority": 3,
                "description": "FactGrid",
            },
            "P1566": {
                "label": "GeoNames ID",
                "priority": 3,
                "description": "Geographic names DB",
            },
        },
        "wikimedia_ids": {
            "P373": {
                "label": "Commons category",
                "priority": 2,
                "description": "Wikimedia Commons category",
            },
            "P935": {
                "label": "Commons gallery",
                "priority": 2,
                "description": "Commons gallery page",
            },
            "P910": {
                "label": "topic's main category",
                "priority": 3,
                "description": "Wikipedia category",
            },
        },
    },
    # =========================================================================
    # TYPE/CLASS PROPERTIES
    # Properties for classification and genre
    # =========================================================================
    "types": {
        "classification": {
            "P31": {
                "label": "instance of",
                "priority": 1,
                "description": "What type of thing this is",
            },
            "P279": {
                "label": "subclass of",
                "priority": 2,
                "description": "Parent class",
            },
            "P361": {
                "label": "part of",
                "priority": 2,
                "description": "What this is part of",
            },
            "P527": {
                "label": "has part(s)",
                "priority": 3,
                "description": "Components",
            },
        },
        "genre_form": {
            "P136": {
                "label": "genre",
                "priority": 1,
                "description": "Genre of the work",
            },
            "P7937": {
                "label": "form of creative work",
                "priority": 1,
                "description": "Form (novel, poem, etc.)",
            },
            "P2551": {
                "label": "has edition or translation",
                "priority": 2,
                "description": "Verse meter",
            },
            "P921": {
                "label": "main subject",
                "priority": 2,
                "description": "Main topic",
            },
            "P135": {
                "label": "movement",
                "priority": 2,
                "description": "Literary/artistic movement",
            },
            "P101": {
                "label": "field of work",
                "priority": 3,
                "description": "Field/discipline",
            },
        },
        "writing_style": {
            "P282": {
                "label": "writing system",
                "priority": 2,
                "description": "Script used",
            },
            "P407": {
                "label": "language of work",
                "priority": 1,
                "description": "Language",
            },
            "P364": {
                "label": "original language",
                "priority": 2,
                "description": "Original language",
            },
        },
        "relationships": {
            "P144": {
                "label": "based on",
                "priority": 2,
                "description": "Source material",
            },
            "P4969": {
                "label": "derivative work",
                "priority": 3,
                "description": "Works derived from this",
            },
            "P737": {
                "label": "influenced by",
                "priority": 3,
                "description": "Influences",
            },
            "P941": {
                "label": "inspired by",
                "priority": 3,
                "description": "Inspiration",
            },
            "P155": {
                "label": "follows",
                "priority": 2,
                "description": "Previous in sequence",
            },
            "P156": {
                "label": "followed by",
                "priority": 2,
                "description": "Next in sequence",
            },
            "P179": {
                "label": "part of the series",
                "priority": 2,
                "description": "Series membership",
            },
        },
    },
    # =========================================================================
    # CREATOR/CONTRIBUTOR PROPERTIES
    # Properties related to authors, editors, etc.
    # =========================================================================
    "creators": {
        "direct": {
            "P50": {
                "label": "author",
                "priority": 1,
                "description": "Author of the work",
            },
            "P2093": {
                "label": "author name string",
                "priority": 1,
                "description": "Author name as text",
            },
            "P1779": {
                "label": "possible creator",
                "priority": 2,
                "description": "Attributed author",
            },
            "P98": {"label": "editor", "priority": 2, "description": "Editor"},
            "P655": {"label": "translator", "priority": 2, "description": "Translator"},
            "P767": {
                "label": "contributor",
                "priority": 3,
                "description": "Contributor",
            },
            "P170": {
                "label": "creator",
                "priority": 2,
                "description": "General creator",
            },
            "P86": {
                "label": "composer",
                "priority": 2,
                "description": "Music composer",
            },
            "P676": {"label": "lyrics by", "priority": 2, "description": "Lyricist"},
            "P57": {
                "label": "director",
                "priority": 2,
                "description": "Film/play director",
            },
            "P58": {
                "label": "screenwriter",
                "priority": 2,
                "description": "Screenplay author",
            },
            "P110": {
                "label": "illustrator",
                "priority": 2,
                "description": "Illustrator",
            },
        },
        "author_properties": {
            "P569": {
                "label": "date of birth",
                "priority": 1,
                "description": "For dating",
            },
            "P570": {
                "label": "date of death",
                "priority": 1,
                "description": "For dating",
            },
            "P27": {
                "label": "country of citizenship",
                "priority": 1,
                "description": "For location",
            },
            "P19": {
                "label": "place of birth",
                "priority": 2,
                "description": "For location",
            },
            "P20": {
                "label": "place of death",
                "priority": 2,
                "description": "For location",
            },
            "P551": {
                "label": "residence",
                "priority": 3,
                "description": "For location",
            },
            "P1317": {"label": "floruit", "priority": 2, "description": "For dating"},
            "P106": {
                "label": "occupation",
                "priority": 3,
                "description": "Author's profession",
            },
            "P101": {
                "label": "field of work",
                "priority": 3,
                "description": "Author's field",
            },
            "P108": {
                "label": "employer",
                "priority": 4,
                "description": "Author's employer",
            },
            "P463": {"label": "member of", "priority": 4, "description": "Memberships"},
        },
    },
}


def compile_flat_list():
    """Compile a flat list of all unique properties."""
    all_props = {}

    for category, subcats in PROPERTIES.items():
        for subcat, props in subcats.items():
            for prop_id, data in props.items():
                if prop_id.startswith(
                    "P"
                ):  # Skip non-property entries like 'wikisource'
                    if prop_id not in all_props:
                        all_props[prop_id] = {
                            "label": data["label"],
                            "description": data.get("description", ""),
                            "categories": [],
                            "priority": data.get("priority", 5),
                        }
                    all_props[prop_id]["categories"].append(f"{category}/{subcat}")

    return all_props


def generate_markdown_report():
    """Generate a comprehensive markdown report."""
    lines = [
        "# Exhaustive Wikidata Properties for NLP Metadata Extraction",
        "",
        "This document lists all Wikidata properties useful for extracting:",
        "- **Date**: When a work was created/published",
        "- **Place**: Geographic origin of the work",
        "- **Content**: Full text and media access",
        "- **Identifiers**: External database links",
        "- **Types**: Classification and genre",
        "",
        "---",
        "",
    ]

    for category, subcats in PROPERTIES.items():
        lines.append(f"## {category.upper()}")
        lines.append("")

        for subcat, props in subcats.items():
            lines.append(f"### {subcat.replace('_', ' ').title()}")
            lines.append("")
            lines.append("| Property | Label | Priority | Description |")
            lines.append("|----------|-------|----------|-------------|")

            for prop_id, data in props.items():
                if prop_id.startswith("P"):
                    priority = data.get("priority", "-")
                    desc = data.get("description", "")
                    lines.append(
                        f"| {prop_id} | {data['label']} | {priority} | {desc} |"
                    )
                else:
                    lines.append(
                        f"| (sitelink) | {data['label']} | {data.get('priority', '-')} | {data.get('description', '')} |"
                    )

            lines.append("")

    return "\n".join(lines)


def generate_sparql_template():
    """Generate SPARQL query template for extraction."""
    # Collect unique properties
    all_props = compile_flat_list()

    # Group by category for the query
    prop_vars = []
    optional_blocks = []

    for prop_id, data in sorted(all_props.items(), key=lambda x: x[1]["priority"]):
        var_name = prop_id.lower()
        prop_vars.append(f"?{var_name}")
        optional_blocks.append(f"    OPTIONAL {{ ?item wdt:{prop_id} ?{var_name} . }}")

    sparql = f"""
# Comprehensive SPARQL query for metadata extraction
# Generated from exhaustive property list

SELECT DISTINCT ?item ?itemLabel ?itemDescription
{chr(10).join(['       ' + v for v in prop_vars[:30]])}  # Showing first 30
WHERE {{
    VALUES ?item {{ wd:Q12345 }}  # Replace with instance IDs

    # Item labels and descriptions
    OPTIONAL {{ ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }}
    OPTIONAL {{ ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "en") }}

{chr(10).join(optional_blocks[:30])}  # Showing first 30

    # Sitelinks (Wikisource, Wikipedia, etc.)
    OPTIONAL {{
        ?sitelink schema:about ?item ;
                  schema:isPartOf ?wiki ;
                  schema:name ?sitelinkTitle .
    }}
}}
"""
    return sparql


def main():
    """Compile and save the final property list."""
    print("=" * 80)
    print("COMPILING FINAL EXHAUSTIVE PROPERTY LIST")
    print("=" * 80)

    # Compile flat list
    flat_list = compile_flat_list()
    print(f"\nTotal unique properties: {len(flat_list)}")

    # Count by category
    for category in PROPERTIES:
        count = sum(len(props) for props in PROPERTIES[category].values())
        print(f"  {category}: {count} properties")

    # Save JSON
    output = {
        "properties_by_category": PROPERTIES,
        "flat_property_list": flat_list,
        "total_properties": len(flat_list),
        "sparql_template": generate_sparql_template(),
    }

    Path(OUTPUT_DIR).mkdir(exist_ok=True)

    json_file = f"{OUTPUT_DIR}/exhaustive_properties.json"
    with open(json_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved JSON to: {json_file}")

    # Save markdown
    md_file = f"{OUTPUT_DIR}/exhaustive_properties.md"
    with open(md_file, "w") as f:
        f.write(generate_markdown_report())
    print(f"Saved Markdown to: {md_file}")

    # Print summary by priority
    print("\n" + "=" * 80)
    print("HIGH PRIORITY PROPERTIES (Priority 1)")
    print("=" * 80)

    for prop_id, data in sorted(flat_list.items(), key=lambda x: x[1]["priority"]):
        if data["priority"] == 1:
            cats = ", ".join(data["categories"])
            print(f"  {prop_id}: {data['label']} [{cats}]")

    print("\n" + "=" * 80)
    print("COMPILATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
