"""Split extracted_data.json into batches using streaming."""

import json
from pathlib import Path
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).parent
INPUT_FILE = SCRIPT_DIR / "output" / "extracted_data.json"
OUTPUT_DIR = SCRIPT_DIR / "output" / "extracted_batches"
BATCH_SIZE = 500_000

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # First count total items (fast scan)
    print("Counting items...")
    total = 0
    with open(INPUT_FILE, "r") as f:
        for line in f:
            if line.strip().startswith('"Q'):
                total += 1
    print(f"Total items: {total:,}")

    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Creating {num_batches} batches...")

    # Now parse and split
    print("Loading JSON...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    print(f"Loaded {len(data):,} items")

    items = list(data.items())
    del data  # Free memory

    for i in tqdm(range(num_batches), desc="Writing batches"):
        start = i * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(items))
        batch_data = dict(items[start:end])

        batch_file = OUTPUT_DIR / f"batch_{i+1:02d}.json"
        with open(batch_file, "w") as f:
            json.dump(batch_data, f)

        tqdm.write(f"  Batch {i+1}: {len(batch_data):,} items -> {batch_file.name}")
        del batch_data  # Free memory

    print(f"\nDone! {num_batches} batches in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
