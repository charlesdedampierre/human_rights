"""
Classify law_subclasses, type_of_law_instances, and writing_materials_subclasses
as pre-1900 or modern using OpenAI.
Only processes classes with > 0 instances.
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime
from openai import OpenAI

OUTPUT_DIR = Path(__file__).parent / "output"

BATCH_SIZE = 30


def log(msg):
    print(msg, flush=True)


def classify_batch(client: OpenAI, labels: list[str], context: str) -> dict:
    """Classify a batch of labels as pre-1900 or modern."""

    labels_text = "\n".join(f"- {label}" for label in labels)

    prompt = f"""Classify each of these {context} types as either "pre1900" (true) or "modern" (false).

Rules:
- pre1900=true: Types that EXISTED before 1900 (ancient materials, historical law types, etc.)
- pre1900=true: Generic/timeless types (constitution, decree, ordinance, papyrus, clay tablet, etc.)
- pre1900=false: Types referencing post-1900 countries or modern institutions
- pre1900=false: Types with specific modern technology requirements

Be GENEROUS for {context}: most historical/legal/material concepts existed before 1900.
When in doubt for ancient materials or generic law types, mark as true.

Types to classify:
{labels_text}

Return ONLY a JSON object with each label as key and true/false as value."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"}
    )

    result = json.loads(response.choices[0].message.content)
    return result


def classify_file(client: OpenAI, input_file: Path, output_file: Path,
                  items_key: str, context: str, label_key: str = "label"):
    """Classify items in a file as pre-1900 or modern."""

    log(f"\n{'='*70}")
    log(f"CLASSIFYING: {input_file.name}")
    log(f"{'='*70}")

    with open(input_file, "r") as f:
        data = json.load(f)

    items = data[items_key]

    # Filter to items with > 0 instances
    items_with_data = [i for i in items if i.get("instance_count", 0) > 0]
    log(f"Total items: {len(items)}")
    log(f"With > 0 instances: {len(items_with_data)}")

    if len(items_with_data) == 0:
        log("No items to classify!")
        return

    # Classify in batches
    classifications = {}
    total_batches = (len(items_with_data) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(items_with_data), BATCH_SIZE):
        batch = items_with_data[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        labels = [item[label_key] for item in batch]

        log(f"Batch {batch_num}/{total_batches}...")

        try:
            results = classify_batch(client, labels, context)
            classifications.update(results)
            pre = sum(1 for v in results.values() if v)
            log(f"   {pre} pre-1900, {len(results) - pre} modern")
            time.sleep(0.5)
        except Exception as e:
            log(f"   Error: {e}")
            time.sleep(2)

    # Build results
    pre1900_items = []
    modern_items = []

    for item in items_with_data:
        label = item[label_key]
        is_pre1900 = classifications.get(label, True)  # Default to True if not classified

        if is_pre1900:
            pre1900_items.append(item)
        else:
            modern_items.append(item)

    # Sort by instance count
    pre1900_items.sort(key=lambda x: x.get("instance_count", 0), reverse=True)
    modern_items.sort(key=lambda x: x.get("instance_count", 0), reverse=True)

    pre1900_total = sum(i.get("instance_count", 0) for i in pre1900_items)
    modern_total = sum(i.get("instance_count", 0) for i in modern_items)

    # Save
    results = {
        "metadata": {
            "source_file": input_file.name,
            "classification_model": "gpt-4o-mini",
            "pre1900_classes": len(pre1900_items),
            "pre1900_total_instances": pre1900_total,
            "modern_classes": len(modern_items),
            "modern_total_instances": modern_total,
            "last_updated": datetime.now().isoformat()
        },
        "pre1900": pre1900_items,
        "modern": modern_items
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    log(f"\nResults: {len(pre1900_items)} pre-1900 ({pre1900_total:,}), {len(modern_items)} modern ({modern_total:,})")
    log(f"Saved to: {output_file.name}")

    return results


def main():
    if not os.environ.get("OPENAI_API_KEY"):
        log("Error: OPENAI_API_KEY not set")
        return

    client = OpenAI()

    log("=" * 70)
    log("CLASSIFY ALL FILES - PRE-1900 vs MODERN")
    log("=" * 70)

    # 1. Law subclasses
    classify_file(
        client,
        OUTPUT_DIR / "law_subclasses.json",
        OUTPUT_DIR / "law_pre1900.json",
        items_key="subclasses",
        context="law/legal document"
    )

    # 2. Type of law instances
    classify_file(
        client,
        OUTPUT_DIR / "type_of_law_instances.json",
        OUTPUT_DIR / "type_of_law_pre1900.json",
        items_key="types",
        context="type of law"
    )

    # 3. Writing materials
    classify_file(
        client,
        OUTPUT_DIR / "writing_materials_subclasses.json",
        OUTPUT_DIR / "writing_materials_pre1900.json",
        items_key="subclasses",
        context="writing material/surface"
    )

    log("\n" + "=" * 70)
    log("ALL CLASSIFICATIONS COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
