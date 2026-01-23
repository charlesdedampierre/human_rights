"""
Classify publication subclasses as pre-1900 or modern using OpenAI.
Only processes classes with > 0 instances.
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime
from openai import OpenAI

OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = OUTPUT_DIR / "publication_subclasses.json"
OUTPUT_FILE = OUTPUT_DIR / "publication_pre1900.json"
PROGRESS_FILE = OUTPUT_DIR / "publication_classify_progress.json"

BATCH_SIZE = 30  # Number of classes to classify per API call


def log(msg):
    print(msg, flush=True)


def load_progress() -> dict:
    """Load classification progress."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"classified": {}}


def save_progress(progress: dict):
    """Save classification progress."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def classify_batch(client: OpenAI, labels: list[str]) -> dict:
    """Classify a batch of labels as pre-1900 or modern."""

    labels_text = "\n".join(f"- {label}" for label in labels)

    prompt = f"""Classify each of these publication/document types as either "pre1900" (true) or "modern" (false).

Rules:
- pre1900=true: Types that EXISTED before 1900 (books, newspapers, manuscripts, letters, maps, etc.)
- pre1900=true: Generic/timeless types (periodical, article, report, etc.)
- pre1900=false: Types requiring modern technology (website, podcast, video game, software, digital file, etc.)
- pre1900=false: Types referencing post-1900 concepts (Wikimedia, TV, radio, ISO standard, etc.)
- pre1900=false: Sports seasons, film/TV episodes, modern awards

Be STRICT: if unsure, mark as false. We want to study deep historical past.

Types to classify:
{labels_text}

Return ONLY a JSON object with each label as key and true/false as value. Example:
{{"book": true, "podcast": false, "newspaper": true}}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"}
    )

    result = json.loads(response.choices[0].message.content)
    return result


def main():
    # Check for API key
    if not os.environ.get("OPENAI_API_KEY"):
        log("Error: OPENAI_API_KEY environment variable not set")
        return

    client = OpenAI()

    log("=" * 70)
    log("CLASSIFY PUBLICATION SUBCLASSES - PRE-1900 vs MODERN")
    log("=" * 70)

    # Load data
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    all_subclasses = data["subclasses"]

    # Filter to only classes with > 0 instances
    subclasses = [s for s in all_subclasses if s.get("instance_count", 0) > 0]
    log(f"\nTotal subclasses: {len(all_subclasses)}")
    log(f"With > 0 instances: {len(subclasses)}")

    # Load progress
    progress = load_progress()
    log(f"Already classified: {len(progress['classified'])}")

    # Filter out already classified
    to_classify = [s for s in subclasses if s["label"] not in progress["classified"]]
    log(f"Remaining to classify: {len(to_classify)}")

    # Classify in batches
    total_batches = (len(to_classify) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(to_classify), BATCH_SIZE):
        batch = to_classify[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        labels = [s["label"] for s in batch]

        log(f"\nBatch {batch_num}/{total_batches} ({len(labels)} items)...")

        try:
            results = classify_batch(client, labels)

            for label, is_pre1900 in results.items():
                progress["classified"][label] = is_pre1900

            save_progress(progress)
            log(f"   Classified: {sum(1 for v in results.values() if v)} pre-1900, {sum(1 for v in results.values() if not v)} modern")

            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            log(f"   Error: {e}")
            time.sleep(5)

    # Build final results
    pre1900_subclasses = []
    modern_subclasses = []

    for s in subclasses:
        label = s["label"]
        is_pre1900 = progress["classified"].get(label)

        if is_pre1900 is True:
            pre1900_subclasses.append(s)
        elif is_pre1900 is False:
            modern_subclasses.append(s)

    # Sort by instance count
    pre1900_subclasses.sort(key=lambda x: x.get("instance_count", 0), reverse=True)
    modern_subclasses.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    # Calculate totals
    pre1900_total = sum(s.get("instance_count", 0) for s in pre1900_subclasses)
    modern_total = sum(s.get("instance_count", 0) for s in modern_subclasses)

    # Save results
    results = {
        "metadata": {
            "description": "Publication subclasses classified as pre-1900 vs modern",
            "classification_model": "gpt-4o-mini",
            "pre1900_classes": len(pre1900_subclasses),
            "pre1900_total_instances": pre1900_total,
            "modern_classes": len(modern_subclasses),
            "modern_total_instances": modern_total,
            "last_updated": datetime.now().isoformat()
        },
        "pre1900_subclasses": pre1900_subclasses,
        "modern_subclasses": modern_subclasses
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    log("\n" + "=" * 70)
    log("RESULTS")
    log("=" * 70)
    log(f"\nPre-1900 classes: {len(pre1900_subclasses)} ({pre1900_total:,} instances)")
    log(f"Modern classes:   {len(modern_subclasses)} ({modern_total:,} instances)")

    log("\n" + "-" * 70)
    log("TOP 30 PRE-1900 PUBLICATION TYPES")
    log("-" * 70)
    log(f"{'Label':<45} {'QID':<12} {'Count':>10}")
    log("-" * 70)
    for s in pre1900_subclasses[:30]:
        log(f"{s['label'][:43]:<45} {s['qid']:<12} {s['instance_count']:>10,}")

    log("\n" + "-" * 70)
    log("TOP 30 MODERN PUBLICATION TYPES (EXCLUDED)")
    log("-" * 70)
    log(f"{'Label':<45} {'QID':<12} {'Count':>10}")
    log("-" * 70)
    for s in modern_subclasses[:30]:
        log(f"{s['label'][:43]:<45} {s['qid']:<12} {s['instance_count']:>10,}")

    log(f"\nSaved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
