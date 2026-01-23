"""
Comprehensive property discovery for Wikidata instances.

This script:
1. Samples instances from each top class
2. Discovers ALL properties used by those instances
3. Analyzes author-linked properties (birthplace, nationality, etc.)
4. Extracts sitelink patterns (Wikisource, Wikipedia)
5. Categorizes properties by usefulness: date, place, content, identifiers, types

Run with: python discover_all_properties.py
"""

import json
import glob
import requests
import time
from collections import defaultdict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
INSTANCES_DIR = "../instances/output/instances_by_class"
OUTPUT_DIR = "output"

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests


def query_sparql(query, retries=3, timeout=180):
    """Execute SPARQL query with retries and rate limiting."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataPropertyAnalysis/1.0 (Research Project)"
    }

    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(
                WIKIDATA_SPARQL_ENDPOINT,
                params={"query": query},
                headers=headers,
                timeout=timeout
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning("Rate limited, waiting 60s...")
                time.sleep(60)
            elif response.status_code == 500:
                logger.warning(f"Server error, attempt {attempt + 1}/{retries}")
                time.sleep(10)
            else:
                logger.warning(f"HTTP {response.status_code}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout, attempt {attempt + 1}/{retries}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)

    return None


def get_top_classes(n=50):
    """Get top N classes by instance count."""
    files = glob.glob(f"{INSTANCES_DIR}/*.json")
    class_counts = []

    for f in files:
        class_id = Path(f).stem
        with open(f) as fp:
            data = json.load(fp)
            class_counts.append((class_id, len(data)))

    class_counts.sort(key=lambda x: x[1], reverse=True)
    return class_counts[:n]


def get_class_label(class_id):
    """Get English label for a class."""
    query = f"""
    SELECT ?label WHERE {{
        wd:{class_id} rdfs:label ?label .
        FILTER(LANG(?label) = "en")
    }} LIMIT 1
    """
    result = query_sparql(query)
    if result and result.get("results", {}).get("bindings"):
        return result["results"]["bindings"][0]["label"]["value"]
    return class_id


def discover_properties_for_class(class_id, sample_size=50):
    """
    Discover all properties used by instances of a class.
    Returns dict with property IDs and their usage counts.
    """
    logger.info(f"Analyzing properties for class {class_id}...")

    # Query to find most common properties for this class
    query = f"""
    SELECT ?prop ?propLabel (COUNT(?prop) as ?count) WHERE {{
        SELECT ?item ?prop ?propLabel WHERE {{
            ?item wdt:P31 wd:{class_id} .
            ?item ?p ?statement .
            ?prop wikibase:claim ?p .
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". ?prop rdfs:label ?propLabel. }}
        }} LIMIT 5000
    }}
    GROUP BY ?prop ?propLabel
    ORDER BY DESC(?count)
    LIMIT 100
    """

    result = query_sparql(query, timeout=300)
    if not result:
        return {}

    properties = {}
    for binding in result.get("results", {}).get("bindings", []):
        prop_uri = binding["prop"]["value"]
        prop_id = prop_uri.split("/")[-1]
        prop_label = binding.get("propLabel", {}).get("value", prop_id)
        count = int(binding["count"]["value"])
        properties[prop_id] = {"label": prop_label, "count": count}

    return properties


def discover_author_linked_properties(class_id):
    """
    Discover properties accessible through the author (P50) relationship.
    These can help infer date (birth/death) and place (birthplace, nationality).
    """
    logger.info(f"Analyzing author-linked properties for class {class_id}...")

    query = f"""
    SELECT ?authorProp ?authorPropLabel (COUNT(?authorProp) as ?count) WHERE {{
        SELECT ?item ?author ?authorProp ?authorPropLabel WHERE {{
            ?item wdt:P31 wd:{class_id} .
            ?item wdt:P50 ?author .
            ?author ?p ?statement .
            ?authorProp wikibase:claim ?p .
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". ?authorProp rdfs:label ?authorPropLabel. }}
        }} LIMIT 3000
    }}
    GROUP BY ?authorProp ?authorPropLabel
    ORDER BY DESC(?count)
    LIMIT 50
    """

    result = query_sparql(query, timeout=300)
    if not result:
        return {}

    properties = {}
    for binding in result.get("results", {}).get("bindings", []):
        prop_uri = binding["authorProp"]["value"]
        prop_id = prop_uri.split("/")[-1]
        prop_label = binding.get("authorPropLabel", {}).get("value", prop_id)
        count = int(binding["count"]["value"])
        properties[prop_id] = {"label": prop_label, "count": count, "via": "author (P50)"}

    return properties


def analyze_sitelinks_for_class(class_id):
    """
    Analyze what sitelinks (Wikipedia, Wikisource, etc.) are available for instances.
    """
    logger.info(f"Analyzing sitelinks for class {class_id}...")

    query = f"""
    SELECT ?wikiGroup (COUNT(DISTINCT ?item) as ?count) WHERE {{
        ?item wdt:P31 wd:{class_id} .
        ?sitelink schema:about ?item ;
                  schema:isPartOf ?wiki .
        ?wiki wikibase:wikiGroup ?wikiGroup .
    }}
    GROUP BY ?wikiGroup
    ORDER BY DESC(?count)
    LIMIT 20
    """

    result = query_sparql(query, timeout=300)
    if not result:
        return {}

    sitelinks = {}
    for binding in result.get("results", {}).get("bindings", []):
        wiki_group = binding["wikiGroup"]["value"]
        count = int(binding["count"]["value"])
        sitelinks[wiki_group] = count

    return sitelinks


def get_property_datatype(prop_id):
    """Get the datatype of a property."""
    query = f"""
    SELECT ?datatype WHERE {{
        wd:{prop_id} wikibase:propertyType ?datatype .
    }}
    """
    result = query_sparql(query)
    if result and result.get("results", {}).get("bindings"):
        datatype = result["results"]["bindings"][0]["datatype"]["value"]
        return datatype.split("#")[-1]
    return "Unknown"


def categorize_property(prop_id, prop_label, datatype=None):
    """
    Categorize a property into: date, place, content, identifiers, types, other.
    Returns list of categories (a property can belong to multiple).
    """
    categories = []
    label_lower = prop_label.lower()

    # Date indicators
    date_keywords = ["date", "time", "year", "period", "inception", "publication",
                     "birth", "death", "start", "end", "founded", "established",
                     "created", "written", "composed", "performed", "released",
                     "floruit", "earliest", "latest", "century", "era"]
    if any(kw in label_lower for kw in date_keywords):
        categories.append("date")
    if datatype == "Time":
        categories.append("date")

    # Place indicators
    place_keywords = ["country", "place", "location", "city", "region", "territory",
                      "nationality", "citizenship", "birth", "death", "residence",
                      "headquarters", "origin", "publication", "jurisdiction",
                      "administrative", "geographic", "coordinate"]
    if any(kw in label_lower for kw in place_keywords):
        categories.append("place")
    if datatype == "GlobeCoordinate":
        categories.append("place")

    # Content indicators
    content_keywords = ["url", "website", "link", "full text", "available at",
                        "archive", "source", "reference", "described", "image",
                        "file", "document", "pdf", "text", "wikisource",
                        "published in", "appears in"]
    if any(kw in label_lower for kw in content_keywords):
        categories.append("content")
    if datatype == "Url":
        categories.append("content")

    # Identifier indicators
    if datatype == "ExternalId":
        categories.append("identifiers")
    id_keywords = ["id", "identifier", "number", "code", "isbn", "issn", "doi",
                   "orcid", "viaf", "gnd", "bnf", "lccn", "oclc"]
    if any(kw in label_lower for kw in id_keywords):
        categories.append("identifiers")

    # Type/class indicators
    type_keywords = ["instance of", "subclass", "type", "class", "genre", "form",
                     "category", "classification", "kind", "nature", "format",
                     "movement", "style", "school"]
    if any(kw in label_lower for kw in type_keywords):
        categories.append("types")

    # Language (helps with both date and place inference)
    if "language" in label_lower or "writing system" in label_lower:
        categories.append("date")  # Language can help date texts
        categories.append("place")  # Language indicates origin

    if not categories:
        categories.append("other")

    return categories


def get_all_property_datatypes(property_ids):
    """Batch query to get datatypes for multiple properties."""
    if not property_ids:
        return {}

    # Split into batches of 50
    batches = [property_ids[i:i+50] for i in range(0, len(property_ids), 50)]
    datatypes = {}

    for batch in batches:
        values = " ".join([f"wd:{p}" for p in batch])
        query = f"""
        SELECT ?prop ?datatype WHERE {{
            VALUES ?prop {{ {values} }}
            ?prop wikibase:propertyType ?datatype .
        }}
        """
        result = query_sparql(query)
        if result:
            for binding in result.get("results", {}).get("bindings", []):
                prop_id = binding["prop"]["value"].split("/")[-1]
                datatype = binding["datatype"]["value"].split("#")[-1]
                datatypes[prop_id] = datatype

    return datatypes


def main():
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE WIKIDATA PROPERTY DISCOVERY")
    logger.info("=" * 80)

    # Get top classes
    logger.info("\n1. Loading top classes...")
    top_classes = get_top_classes(30)  # Analyze top 30 classes

    # Get labels for top classes
    class_info = {}
    for class_id, count in top_classes[:20]:  # Get labels for top 20
        label = get_class_label(class_id)
        class_info[class_id] = {"label": label, "count": count}
        logger.info(f"  {class_id} ({label}): {count:,} instances")

    # Aggregate all properties across classes
    all_properties = defaultdict(lambda: {"label": "", "count": 0, "classes": [], "via": "direct"})
    all_author_properties = defaultdict(lambda: {"label": "", "count": 0, "classes": [], "via": "author"})
    all_sitelinks = defaultdict(lambda: {"count": 0, "classes": []})

    # Analyze each class
    logger.info("\n2. Discovering properties for each class...")

    for i, (class_id, count) in enumerate(top_classes[:15]):  # Top 15 for detailed analysis
        logger.info(f"\n--- Class {i+1}/15: {class_id} ({class_info.get(class_id, {}).get('label', 'Unknown')}) ---")

        # Direct properties
        props = discover_properties_for_class(class_id)
        for prop_id, data in props.items():
            all_properties[prop_id]["label"] = data["label"]
            all_properties[prop_id]["count"] += data["count"]
            all_properties[prop_id]["classes"].append(class_id)

        # Author-linked properties
        author_props = discover_author_linked_properties(class_id)
        for prop_id, data in author_props.items():
            all_author_properties[prop_id]["label"] = data["label"]
            all_author_properties[prop_id]["count"] += data["count"]
            all_author_properties[prop_id]["classes"].append(class_id)

        # Sitelinks
        sitelinks = analyze_sitelinks_for_class(class_id)
        for wiki_group, wiki_count in sitelinks.items():
            all_sitelinks[wiki_group]["count"] += wiki_count
            all_sitelinks[wiki_group]["classes"].append(class_id)

        time.sleep(2)  # Rate limiting between classes

    # Get datatypes for all discovered properties
    logger.info("\n3. Getting property datatypes...")
    all_prop_ids = list(all_properties.keys()) + list(all_author_properties.keys())
    datatypes = get_all_property_datatypes(list(set(all_prop_ids)))

    # Categorize properties
    logger.info("\n4. Categorizing properties...")

    categorized = {
        "date": [],
        "place": [],
        "content": [],
        "identifiers": [],
        "types": [],
        "other": []
    }

    # Process direct properties
    for prop_id, data in all_properties.items():
        datatype = datatypes.get(prop_id, "Unknown")
        categories = categorize_property(prop_id, data["label"], datatype)
        for cat in categories:
            categorized[cat].append({
                "property": prop_id,
                "label": data["label"],
                "datatype": datatype,
                "total_count": data["count"],
                "num_classes": len(data["classes"]),
                "via": "direct"
            })

    # Process author-linked properties
    for prop_id, data in all_author_properties.items():
        datatype = datatypes.get(prop_id, "Unknown")
        categories = categorize_property(prop_id, data["label"], datatype)
        for cat in categories:
            categorized[cat].append({
                "property": prop_id,
                "label": data["label"],
                "datatype": datatype,
                "total_count": data["count"],
                "num_classes": len(data["classes"]),
                "via": "author (P50)"
            })

    # Sort each category by count
    for cat in categorized:
        categorized[cat].sort(key=lambda x: x["total_count"], reverse=True)

    # Remove duplicates within categories (keep highest count)
    for cat in categorized:
        seen = set()
        unique = []
        for item in categorized[cat]:
            if item["property"] not in seen:
                seen.add(item["property"])
                unique.append(item)
        categorized[cat] = unique

    # Print results
    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    for cat, props in categorized.items():
        logger.info(f"\n--- {cat.upper()} ({len(props)} properties) ---")
        for p in props[:30]:  # Show top 30 per category
            via_str = f" [via {p['via']}]" if p['via'] != 'direct' else ""
            logger.info(f"  {p['property']}: {p['label']} ({p['datatype']}) - {p['total_count']:,} uses{via_str}")

    logger.info(f"\n--- SITELINKS ({len(all_sitelinks)} wiki groups) ---")
    sorted_sitelinks = sorted(all_sitelinks.items(), key=lambda x: x[1]["count"], reverse=True)
    for wiki_group, data in sorted_sitelinks[:15]:
        logger.info(f"  {wiki_group}: {data['count']:,} instances across {len(data['classes'])} classes")

    # Save results
    output = {
        "analysis_info": {
            "classes_analyzed": len(top_classes[:15]),
            "total_direct_properties": len(all_properties),
            "total_author_properties": len(all_author_properties),
        },
        "class_info": class_info,
        "categorized_properties": categorized,
        "sitelinks": {k: v for k, v in sorted_sitelinks},
        "all_direct_properties": {k: v for k, v in sorted(all_properties.items(), key=lambda x: x[1]["count"], reverse=True)},
        "all_author_properties": {k: v for k, v in sorted(all_author_properties.items(), key=lambda x: x[1]["count"], reverse=True)},
    }

    output_file = f"{OUTPUT_DIR}/discovered_properties.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=list)

    logger.info(f"\n\nResults saved to {output_file}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
