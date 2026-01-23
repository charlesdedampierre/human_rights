"""
Fast classification of written work subclasses as pre-1900 or modern using OpenAI.
Uses multiprocessing and retries for speed and reliability.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

# Load .env
load_dotenv(Path(__file__).parent.parent / ".env")

# Configuration
INPUT_FILE = Path(__file__).parent / "output" / "written_work_all_subclasses.json"
OUTPUT_FILE = Path(__file__).parent / "output" / "written_work_pre1900.json"
PROGRESS_FILE = Path(__file__).parent / "output" / "classify_progress.json"

MIN_INSTANCES = 1
MODEL = "gpt-4o-mini"
BATCH_SIZE = 30  # Classes per API call
MAX_WORKERS = 5  # Parallel API calls
MAX_RETRIES = 5


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def classify_batch(client, classes_batch):
    """Classify a batch of classes. Returns dict of {qid: {"pre1900": bool, "reason": str}}"""
    class_list = "\n".join([f"- {c['qid']}: {c['label']}" for c in classes_batch])

    prompt = f"""Classify these written work types for historical research (before 1900).

Answer "yes" (pre1900=true) if:
- Type existed before 1900 (manuscript, chronicle, letter, poem, treaty)
- Generic/timeless type (literary work, religious text, legal document)

Answer "no" (pre1900=false) if:
- Modern type (blog, tweet, podcast, web article, email)
- Requires modern tech (software doc, digital publication)
- Post-1900 concept (UN resolution, ISO standard, Wikinews)

Classes:
{class_list}

JSON response:
{{"classifications": [{{"qid": "Q...", "pre1900": true/false, "reason": "brief"}}]}}"""

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0,
                timeout=30
            )
            result = json.loads(response.choices[0].message.content)

            classifications = {}
            for item in result.get("classifications", []):
                classifications[item["qid"]] = {
                    "pre1900": item["pre1900"],
                    "reason": item["reason"]
                }
            return classifications

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                # Return empty on final failure
                return {c["qid"]: {"pre1900": True, "reason": "classification_failed"} for c in classes_batch}


def process_batch(args):
    """Worker function for parallel processing."""
    client, batch, batch_idx = args
    result = classify_batch(client, batch)
    return batch_idx, result


def main():
    print("=" * 70)
    print("CLASSIFY WRITTEN WORK SUBCLASSES - PRE-1900 FILTER (FAST)")
    print("=" * 70)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY not found in .env")
        return

    client = OpenAI(api_key=api_key)

    # Load data
    print(f"\n1. Loading data...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    all_subclasses = data["subclasses"]
    metadata = data["metadata"]

    to_classify = [s for s in all_subclasses if s["direct_instance_count"] >= MIN_INSTANCES]
    print(f"   Total with instances: {len(to_classify):,}")

    # Load progress
    progress = load_progress()
    remaining = [s for s in to_classify if s["qid"] not in progress]
    print(f"   Already done: {len(progress):,}")
    print(f"   Remaining: {len(remaining):,}")

    if not remaining:
        print("   All classified!")
    else:
        # Create batches
        batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
        print(f"\n2. Classifying {len(remaining)} classes in {len(batches)} batches...")
        print(f"   Workers: {MAX_WORKERS}, Batch size: {BATCH_SIZE}")

        # Process with thread pool
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_batch, (client, batch, i)): i
                for i, batch in enumerate(batches)
            }

            with tqdm(total=len(batches), desc="   Classifying") as pbar:
                for future in as_completed(futures):
                    batch_idx, result = future.result()
                    progress.update(result)
                    save_progress(progress)
                    pbar.update(1)

    # Build output
    print(f"\n3. Building output...")

    pre1900_classes = []
    modern_classes = []
    skipped_classes = []

    for subclass in all_subclasses:
        qid = subclass["qid"]
        if subclass["direct_instance_count"] < MIN_INSTANCES:
            skipped_classes.append({**subclass, "skip_reason": "zero_instances"})
        elif qid in progress:
            classification = progress[qid]
            enriched = {
                **subclass,
                "pre1900": classification["pre1900"],
                "classification_reason": classification["reason"]
            }
            if classification["pre1900"]:
                pre1900_classes.append(enriched)
            else:
                modern_classes.append(enriched)
        else:
            skipped_classes.append({**subclass, "skip_reason": "not_classified"})

    pre1900_classes.sort(key=lambda x: x["direct_instance_count"], reverse=True)
    modern_classes.sort(key=lambda x: x["direct_instance_count"], reverse=True)

    pre1900_instances = sum(c["direct_instance_count"] for c in pre1900_classes)
    modern_instances = sum(c["direct_instance_count"] for c in modern_classes)

    output = {
        "metadata": {
            **metadata,
            "classification_model": MODEL,
            "classification_date": datetime.now().isoformat(),
            "min_instances_threshold": MIN_INSTANCES,
            "pre1900_classes": len(pre1900_classes),
            "pre1900_total_instances": pre1900_instances,
            "modern_classes": len(modern_classes),
            "modern_total_instances": modern_instances,
            "skipped_classes": len(skipped_classes)
        },
        "pre1900_subclasses": pre1900_classes,
        "modern_subclasses": modern_classes,
        "skipped_subclasses": skipped_classes
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Pre-1900:  {len(pre1900_classes):,} classes ({pre1900_instances:,} instances)")
    print(f"Modern:    {len(modern_classes):,} classes ({modern_instances:,} instances)")
    print(f"Skipped:   {len(skipped_classes):,} classes")
    print(f"\nSaved to: {OUTPUT_FILE.name}")

    print(f"\nTop 15 pre-1900 classes:")
    print(f"{'Label':<45} {'Count':>12}")
    print("-" * 58)
    for c in pre1900_classes[:15]:
        print(f"{c['label'][:43]:<45} {c['direct_instance_count']:>12,}")


if __name__ == "__main__":
    main()
