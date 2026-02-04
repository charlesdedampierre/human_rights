"""
Create SQLite database from enriched Wikidata CSV files.

Input: enriched_literary_works/*.csv
Output: output/literary_works.db
"""

import pandas as pd
import sqlite3
import glob
import re
from pathlib import Path

# Paths relative to script location
SCRIPT_DIR = Path(__file__).parent.parent
CSV_PATTERN = SCRIPT_DIR / "enriched_literary_works" / "enriched_batch_*.csv"
DB_PATH = SCRIPT_DIR / "output" / "literary_works.db"


def clean_wellknown_uri(value):
    """Replace .well-known/ placeholder URIs with None."""
    if pd.isna(value) or value == "":
        return None
    if ".well-known/" in str(value):
        return None
    return value


def extract_wikidata_id(uri):
    """Extract Wikidata ID (e.g., Q12345) from URI."""
    if pd.isna(uri) or uri == "":
        return None
    # Skip .well-known placeholder URIs
    if ".well-known/" in str(uri):
        return None
    match = re.search(r"Q\d+", str(uri))
    return match.group(0) if match else None


def format_date(date_str):
    """Format date to YYYY-MM-DD. Preserves BC dates with negative sign (no leading zeros)."""
    if pd.isna(date_str) or date_str == "":
        return None
    date_str = str(date_str)

    # Handle BC dates (negative years like -0800-01-01T00:00:00Z -> -800-01-01)
    if date_str.startswith("-"):
        match = re.search(r"^-(\d+)-(\d{2})-(\d{2})", date_str)
        if match:
            year = int(match.group(1))  # Remove leading zeros
            return f"-{year}-{match.group(2)}-{match.group(3)}"
        match = re.search(r"^-(\d+)", date_str)
        if match:
            year = int(match.group(1))  # Remove leading zeros
            return f"-{year}-01-01"
        return None

    # Handle AD dates (positive years)
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(\d{4})", date_str)
    if match:
        return f"{match.group(1)}-01-01"
    return None


# Get all enriched batch CSV files
files = sorted(glob.glob(str(CSV_PATTERN)))
print(f"Found {len(files)} CSV files")

# Read all CSV files and concatenate
dfs = []
for i, file in enumerate(files):
    df = pd.read_csv(file)
    dfs.append(df)
    if (i + 1) % 5000 == 0:
        print(f"Processed {i + 1}/{len(files)} files...")

print("Concatenating all dataframes...")
data = pd.concat(dfs, ignore_index=True)
print(f"Total rows: {len(data)}")

# Extract Wikidata IDs from URIs
print("Extracting Wikidata IDs...")
data["item_id"] = data["item"].apply(extract_wikidata_id)
data["instance_id"] = data["instance"].apply(extract_wikidata_id)
data["country_id"] = data["country"].apply(extract_wikidata_id)
data["language_id"] = data["language"].apply(extract_wikidata_id)
data["author_id"] = data["author"].apply(extract_wikidata_id)

# Format dates
print("Formatting dates...")
data["publication_date"] = data["publicationDate"].apply(format_date)
data["inception_date"] = data["inceptionDate"].apply(format_date)

# Clean .well-known/ placeholder URIs from label columns
print("Cleaning placeholder URIs...")
label_columns = ["itemLabel", "instanceLabel", "countryLabel", "languageLabel", "authorLabel"]
for col in label_columns:
    if col in data.columns:
        data[col] = data[col].apply(clean_wellknown_uri)

# Create SQLite database
print(f"\nCreating SQLite database: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Count sitelinks per item
print("Counting sitelinks per item...")
sitelink_counts = (
    data[["item_id", "sitelink"]]
    .dropna(subset=["item_id", "sitelink"])
    .drop_duplicates()
    .groupby("item_id")
    .size()
    .reset_index(name="sitelink_count")
)

# Create main table with only item_id and labels (no other IDs)
print("Creating main table: literary_works...")
main_table = data[
    [
        "item_id",
        "itemLabel",
        "instanceLabel",
        "countryLabel",
        "languageLabel",
        "authorLabel",
        "publication_date",
        "inception_date",
    ]
].copy()
main_table = main_table.drop_duplicates()

# Add sitelink count to main table
main_table = main_table.merge(sitelink_counts, on="item_id", how="left")
main_table["sitelink_count"] = main_table["sitelink_count"].fillna(0).astype(int)

# Sort by sitelink_count descending
main_table = main_table.sort_values("sitelink_count", ascending=False)

main_table.to_sql("literary_works", conn, if_exists="replace", index=False)

# Create sitelinks table with item_id, itemLabel, sitelink
print("Creating sitelinks table...")
sitelinks = (
    data[["item_id", "itemLabel", "sitelink"]]
    .dropna(subset=["item_id", "sitelink"])
    .drop_duplicates()
)
sitelinks.to_sql("sitelinks", conn, if_exists="replace", index=False)

# Create lookup tables
print("Creating lookup tables...")

# Instances lookup table
instances = (
    data[["instance_id", "instanceLabel"]]
    .dropna(subset=["instance_id"])
    .drop_duplicates()
    .rename(columns={"instance_id": "id", "instanceLabel": "label"})
)
instances.to_sql("instances", conn, if_exists="replace", index=False)

# Countries lookup table
countries = (
    data[["country_id", "countryLabel"]]
    .dropna(subset=["country_id"])
    .drop_duplicates()
    .rename(columns={"country_id": "id", "countryLabel": "label"})
)
countries.to_sql("countries", conn, if_exists="replace", index=False)

# Languages lookup table
languages = (
    data[["language_id", "languageLabel"]]
    .dropna(subset=["language_id"])
    .drop_duplicates()
    .rename(columns={"language_id": "id", "languageLabel": "label"})
)
languages.to_sql("languages", conn, if_exists="replace", index=False)

# Authors lookup table
authors = (
    data[["author_id", "authorLabel"]]
    .dropna(subset=["author_id"])
    .drop_duplicates()
    .rename(columns={"author_id": "id", "authorLabel": "label"})
)
authors.to_sql("authors", conn, if_exists="replace", index=False)

# Create indexes
print("Creating indexes...")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_lw_item_id ON literary_works(item_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sitelinks_item_id ON sitelinks(item_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_id ON instances(id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_countries_id ON countries(id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_languages_id ON languages(id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_authors_id ON authors(id)")

conn.commit()

# Print summary statistics
print("\n" + "=" * 60)
print("DATABASE SUMMARY")
print("=" * 60)

cursor.execute("SELECT COUNT(*) FROM literary_works")
print(f"literary_works table: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM sitelinks")
print(f"sitelinks table: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM instances")
print(f"instances table: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM countries")
print(f"countries table: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM languages")
print(f"languages table: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM authors")
print(f"authors table: {cursor.fetchone()[0]:,} records")

# Show sample data
print("\n" + "=" * 60)
print("SAMPLE DATA - literary_works (first 5 rows)")
print("=" * 60)
sample = pd.read_sql("SELECT * FROM literary_works LIMIT 5", conn)
print(sample.to_string())

print("\n" + "=" * 60)
print("SAMPLE DATA - sitelinks (first 5 rows)")
print("=" * 60)
sample = pd.read_sql("SELECT * FROM sitelinks LIMIT 5", conn)
print(sample.to_string())

conn.close()
print(f"\nDatabase created successfully: {DB_PATH}")
