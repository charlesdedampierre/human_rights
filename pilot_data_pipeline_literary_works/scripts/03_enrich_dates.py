"""
Enrich literary_works.db with a 'year' column.
Uses inception_date by default, falls back to publication_date.
BC dates (negative years) are preserved with '-' prefix.

Input: output/literary_works.db
Output: Adds 'year' column to literary_works table
"""

import sqlite3
import re
from pathlib import Path
from tqdm import tqdm

DB_PATH = Path(__file__).parent.parent / "output" / "literary_works.db"


def extract_year(date_str):
    """
    Extract year from date string.
    Handles both AD dates (YYYY-MM-DD) and BC dates (-YYYY-MM-DD).
    Returns integer year (negative for BC).
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Handle BC dates (negative years like -0800-01-01)
    if date_str.startswith("-"):
        match = re.search(r"^-(\d+)", date_str)
        if match:
            return -int(match.group(1))

    # Handle regular dates (YYYY-MM-DD or just YYYY)
    match = re.search(r"^(\d+)", date_str)
    if match:
        return int(match.group(1))

    return None


def main():
    print("=" * 60)
    print("ENRICHMENT: Adding 'year' column to literary_works")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Add year column if not exists
    try:
        cursor.execute("ALTER TABLE literary_works ADD COLUMN year INTEGER")
        print("Added 'year' column to literary_works table")
    except sqlite3.OperationalError:
        print("'year' column already exists, will update values")

    # Get all records with dates
    print("\nLoading records from database...")
    cursor.execute("""
        SELECT rowid, inception_date, publication_date
        FROM literary_works
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows):,} records")

    # Calculate year for each record
    print("Extracting years...")
    updates = []
    for rowid, inception_date, publication_date in tqdm(rows, desc="Processing"):
        # Use inception_date by default, fall back to publication_date
        year = extract_year(inception_date)
        if year is None:
            year = extract_year(publication_date)
        updates.append((year, rowid))

    # Update database in batches
    print("\nUpdating database...")
    batch_size = 10000
    for i in tqdm(range(0, len(updates), batch_size), desc="Writing"):
        batch = updates[i:i + batch_size]
        cursor.executemany(
            "UPDATE literary_works SET year = ? WHERE rowid = ?",
            batch
        )
        conn.commit()

    # Statistics
    cursor.execute("SELECT COUNT(*) FROM literary_works WHERE year IS NOT NULL")
    with_year = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM literary_works WHERE year < 0")
    bc_count = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(year), MAX(year) FROM literary_works WHERE year IS NOT NULL")
    min_year, max_year = cursor.fetchone()

    cursor.execute("SELECT COUNT(*) FROM literary_works")
    total = cursor.fetchone()[0]

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total records: {total:,}")
    print(f"Records with year: {with_year:,} ({100*with_year/total:.1f}%)")
    print(f"BC dates (negative years): {bc_count:,}")
    print(f"Year range: {min_year} to {max_year}")

    # Sample data
    print("\nSample records with BC dates:")
    cursor.execute("""
        SELECT itemLabel, year, inception_date, publication_date
        FROM literary_works
        WHERE year < 0
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  - {row[0]}: year={row[1]}, inception={row[2]}, pub={row[3]}")

    conn.close()
    print(f"\nDone! Database updated: {DB_PATH}")


if __name__ == "__main__":
    main()
