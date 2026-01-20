#!/usr/bin/env python3
"""
Fetch enriched literary works data from Wikidata.
Queries Wikidata SPARQL endpoint in batches.

Input: data/all_literary_works.csv (list of Wikidata item URIs)
Output: enriched_literary_works/*.csv (batch files with enriched data)
"""

import requests
import pandas as pd
import time
import os
from pathlib import Path
from tqdm import tqdm

# Paths relative to script location
SCRIPT_DIR = Path(__file__).parent.parent
INPUT_FILE = SCRIPT_DIR / "data" / "all_literary_works.csv"
OUTPUT_DIR = SCRIPT_DIR / "enriched_literary_works"

ENDPOINT = "https://query.wikidata.org/sparql"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load existing IDs
df_items = pd.read_csv(INPUT_FILE)
items = df_items["item"].tolist()

print(f"Total items to enrich: {len(items)}")


def query_batch(item_list):
    """Query info for a batch of items"""
    values_clause = " ".join([f"<{item}>" for item in item_list])

    sparql = f"""
    SELECT ?item ?itemLabel ?inceptionDate ?publicationDate 
           ?instance ?instanceLabel
           ?author ?authorLabel
           ?language ?languageLabel 
           ?country ?countryLabel 
           ?sitelink
    WHERE {{
      VALUES ?item {{ {values_clause} }}
      
      OPTIONAL {{ ?item wdt:P31 ?instance }}
      OPTIONAL {{ ?item wdt:P571 ?inceptionDate }}
      OPTIONAL {{ ?item wdt:P577 ?publicationDate }}
      OPTIONAL {{ ?item wdt:P50 ?author }}
      OPTIONAL {{ ?item wdt:P407 ?language }}
      OPTIONAL {{ ?item wdt:P495 ?country }}
      OPTIONAL {{ ?sitelink schema:about ?item . }}
      
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,fr". }}
    }}
    """

    try:
        r = requests.get(
            ENDPOINT, params={"query": sparql, "format": "json"}, timeout=300
        )
        if r.ok:
            return r.json()["results"]["bindings"]
        else:
            print(f"\n  HTTP Error {r.status_code}")
            return None
    except Exception as e:
        print(f"\n  Exception: {e}")
        return None


# Process par batches de 50 items
batch_size = 50
batch_num = 1
total_batches = (len(items) + batch_size - 1) // batch_size
failed_batches = 0
empty_batches = 0

for i in tqdm(
    range(0, len(items), batch_size), total=total_batches, desc="Processing batches"
):
    batch_items = items[i : i + batch_size]

    results = query_batch(batch_items)

    if results is None:
        print(f"\n  ✗ Batch {batch_num}: Query failed")
        failed_batches += 1
    elif len(results) == 0:
        print(f"\n  ⚠ Batch {batch_num}: No results returned (0 rows)")
        empty_batches += 1
    else:
        data = [{k: v.get("value", "") for k, v in r.items()} for r in results]
        df = pd.DataFrame(data)

        filename = OUTPUT_DIR / f"enriched_batch_{batch_num:04d}.csv"
        df.to_csv(filename, index=False)
        tqdm.write(f"  ✓ Batch {batch_num}: Saved {len(df)} rows")

    batch_num += 1
    time.sleep(1)
