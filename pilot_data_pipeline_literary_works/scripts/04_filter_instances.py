"""
Mark excluded instance types in the database.

Adds an 'excluded' column to the instances table to flag instance types
that should be filtered out from analysis (comics, journals, patents, etc.)

Input: output/literary_works.db
Output: Updates instances table with 'excluded' column (1 = excluded, 0 = included)
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "output" / "literary_works.db"

EXCLUDED_INSTANCES = [
    'events in a specific year or time period',
    'comics',
    'open-access journal',
    'comic book series',
    'comic book album',
    'scientific journal',
    'academic journal',
    'Wikimedia list article',
    'United States patent',
    'Wikipedia overview article',
    'Wikimedia glossary list article',
    'comic book storyline',
    'Star Trek comic',
    'webcomic',
    'magazine',
    'webtoon',
    'comic book',
    'Saturday Night Live sketch',
    'comic strip',
    'manga',
    'manga series',
    'Publications of the Independent Commission of Experts Switzerland – Second World War',
    'electronic literature',
    'scientific publication',
    'diamond open-access journal',
    'report',
    'Hansard',
    'APC-free journal',
    'biographical article',
]


def main():
    print("=" * 60)
    print("FILTER: Marking excluded instance types")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Add excluded column if not exists
    try:
        cursor.execute("ALTER TABLE instances ADD COLUMN excluded INTEGER DEFAULT 0")
        print("Added 'excluded' column to instances table")
    except sqlite3.OperationalError:
        print("'excluded' column already exists, will update values")
        cursor.execute("UPDATE instances SET excluded = 0")

    # Mark excluded instances
    print(f"\nMarking {len(EXCLUDED_INSTANCES)} instance types as excluded...")

    excluded_count = 0
    for instance_label in EXCLUDED_INSTANCES:
        cursor.execute(
            "UPDATE instances SET excluded = 1 WHERE label = ?",
            (instance_label,)
        )
        rows_affected = cursor.rowcount
        if rows_affected > 0:
            excluded_count += rows_affected
            print(f"  - {instance_label}")

    conn.commit()

    # Statistics
    cursor.execute("SELECT COUNT(*) FROM instances")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM instances WHERE excluded = 1")
    excluded = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(DISTINCT lw.item_id)
        FROM literary_works lw
        JOIN instances i ON lw.instanceLabel = i.label
        WHERE i.excluded = 1
    """)
    excluded_works = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT item_id) FROM literary_works")
    total_works = cursor.fetchone()[0]

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Instance types marked as excluded: {excluded}/{total}")
    print(f"Literary works affected: {excluded_works:,}/{total_works:,} ({100*excluded_works/total_works:.1f}%)")

    # Show excluded instance types found in database
    print("\nExcluded instance types in database:")
    cursor.execute("""
        SELECT label, COUNT(*) as cnt
        FROM (
            SELECT i.label
            FROM literary_works lw
            JOIN instances i ON lw.instanceLabel = i.label
            WHERE i.excluded = 1
        )
        GROUP BY label
        ORDER BY cnt DESC
        LIMIT 15
    """)
    for label, count in cursor.fetchall():
        print(f"  - {label}: {count:,} works")

    conn.close()
    print(f"\nDone! Database updated: {DB_PATH}")


if __name__ == "__main__":
    main()
