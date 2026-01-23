"""
Validate and verify properties by testing on real Wikidata instances.

This script:
1. Takes a list of properties
2. Tests them on sample instances
3. Validates that they return expected data types
4. Reports on coverage and usefulness
"""

import json
import requests
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
REQUEST_DELAY = 1.0


def query_sparql(query, retries=3, timeout=120):
    """Execute SPARQL query."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataPropertyValidation/1.0"
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
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)
    return None


def validate_property_on_class(prop_id, class_id, sample_size=10):
    """
    Validate a property by checking its values on sample instances.
    Returns sample values and coverage percentage.
    """
    query = f"""
    SELECT ?item ?itemLabel ?value ?valueLabel WHERE {{
        ?item wdt:P31 wd:{class_id} .
        ?item wdt:{prop_id} ?value .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT {sample_size}
    """

    result = query_sparql(query)
    if not result:
        return None

    bindings = result.get("results", {}).get("bindings", [])
    samples = []
    for b in bindings:
        item = b.get("item", {}).get("value", "").split("/")[-1]
        item_label = b.get("itemLabel", {}).get("value", "")
        value = b.get("value", {}).get("value", "")
        value_label = b.get("valueLabel", {}).get("value", "")
        value_type = b.get("value", {}).get("type", "")

        samples.append({
            "item": item,
            "item_label": item_label,
            "value": value,
            "value_label": value_label,
            "value_type": value_type
        })

    return samples


def get_property_coverage(prop_id, class_id):
    """Get the percentage of instances that have this property."""
    # Count instances with property
    query_with = f"""
    SELECT (COUNT(DISTINCT ?item) as ?count) WHERE {{
        ?item wdt:P31 wd:{class_id} .
        ?item wdt:{prop_id} ?value .
    }}
    """

    # Count total instances (sample)
    query_total = f"""
    SELECT (COUNT(DISTINCT ?item) as ?count) WHERE {{
        ?item wdt:P31 wd:{class_id} .
    }} LIMIT 10000
    """

    result_with = query_sparql(query_with)
    result_total = query_sparql(query_total)

    if result_with and result_total:
        count_with = int(result_with["results"]["bindings"][0]["count"]["value"])
        count_total = int(result_total["results"]["bindings"][0]["count"]["value"])
        if count_total > 0:
            return count_with, count_total, (count_with / count_total) * 100

    return 0, 0, 0


def test_comprehensive_query_on_instance(instance_id):
    """
    Test extracting all relevant properties from a single instance.
    This helps validate the full extraction pipeline.
    """
    query = f"""
    SELECT ?prop ?propLabel ?value ?valueLabel WHERE {{
        wd:{instance_id} ?p ?statement .
        ?prop wikibase:claim ?p .
        ?statement ?ps ?value .
        ?prop wikibase:statementProperty ?ps .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    result = query_sparql(query, timeout=60)
    if not result:
        return None

    properties = {}
    for b in result.get("results", {}).get("bindings", []):
        prop_id = b["prop"]["value"].split("/")[-1]
        prop_label = b.get("propLabel", {}).get("value", prop_id)
        value = b.get("value", {}).get("value", "")
        value_label = b.get("valueLabel", {}).get("value", "")

        if prop_id not in properties:
            properties[prop_id] = {
                "label": prop_label,
                "values": []
            }
        properties[prop_id]["values"].append({
            "value": value,
            "label": value_label
        })

    return properties


def get_instance_sitelinks(instance_id):
    """Get all sitelinks (Wikipedia, Wikisource, etc.) for an instance."""
    query = f"""
    SELECT ?sitelink ?wiki WHERE {{
        ?sitelink schema:about wd:{instance_id} ;
                  schema:isPartOf ?wiki ;
                  schema:name ?title .
    }}
    """

    result = query_sparql(query)
    if not result:
        return []

    sitelinks = []
    for b in result.get("results", {}).get("bindings", []):
        sitelink_url = b["sitelink"]["value"]
        wiki = b["wiki"]["value"]
        sitelinks.append({
            "url": sitelink_url,
            "wiki": wiki
        })

    return sitelinks


def get_author_properties(instance_id):
    """Get properties of the author(s) of an instance."""
    query = f"""
    SELECT ?author ?authorLabel ?authorProp ?authorPropLabel ?value ?valueLabel WHERE {{
        wd:{instance_id} wdt:P50 ?author .
        ?author ?p ?statement .
        ?authorProp wikibase:claim ?p .
        ?statement ?ps ?value .
        ?authorProp wikibase:statementProperty ?ps .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    result = query_sparql(query, timeout=60)
    if not result:
        return {}

    author_props = {}
    for b in result.get("results", {}).get("bindings", []):
        author_id = b["author"]["value"].split("/")[-1]
        author_label = b.get("authorLabel", {}).get("value", author_id)
        prop_id = b["authorProp"]["value"].split("/")[-1]
        prop_label = b.get("authorPropLabel", {}).get("value", prop_id)
        value = b.get("value", {}).get("value", "")

        if author_id not in author_props:
            author_props[author_id] = {
                "label": author_label,
                "properties": {}
            }

        if prop_id not in author_props[author_id]["properties"]:
            author_props[author_id]["properties"][prop_id] = {
                "label": prop_label,
                "values": []
            }
        author_props[author_id]["properties"][prop_id]["values"].append(value)

    return author_props


def main():
    """Run validation tests on sample instances."""
    logger.info("=" * 80)
    logger.info("PROPERTY VALIDATION ON REAL INSTANCES")
    logger.info("=" * 80)

    # Test on a well-known literary work: Don Quixote (Q480)
    test_instances = [
        ("Q480", "Don Quixote"),
        ("Q8258", "The Divine Comedy"),
        ("Q40185", "Hamlet"),
        ("Q1192186", "Pride and Prejudice"),
        ("Q46717", "War and Peace"),
    ]

    for instance_id, instance_name in test_instances:
        logger.info(f"\n{'='*60}")
        logger.info(f"TESTING: {instance_name} ({instance_id})")
        logger.info(f"{'='*60}")

        # Get all properties
        logger.info("\n--- Direct Properties ---")
        props = test_comprehensive_query_on_instance(instance_id)
        if props:
            for prop_id, data in sorted(props.items()):
                values_str = ", ".join([v["label"] or v["value"][:50] for v in data["values"][:3]])
                logger.info(f"  {prop_id} ({data['label']}): {values_str}")

        # Get author properties
        logger.info("\n--- Author Properties ---")
        author_props = get_author_properties(instance_id)
        if author_props:
            for author_id, author_data in author_props.items():
                logger.info(f"  Author: {author_data['label']} ({author_id})")
                for prop_id, prop_data in author_data["properties"].items():
                    values_str = ", ".join([str(v)[:50] for v in prop_data["values"][:2]])
                    logger.info(f"    {prop_id} ({prop_data['label']}): {values_str}")

        # Get sitelinks
        logger.info("\n--- Sitelinks ---")
        sitelinks = get_instance_sitelinks(instance_id)
        for sl in sitelinks[:10]:
            logger.info(f"  {sl['wiki']}: {sl['url']}")

        time.sleep(2)

    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
