# Wikidata SPARQL Scripts

Extract and process metadata from Wikidata for pre-1900 written works.

## Pipeline Overview

```
classes/          → Extract class definitions (subclasses of written work)
instances/        → Extract instance IDs for each class
instance_properties/ → Extract detailed properties for each instance
```

## Instance Extraction

### Included Classes

- 2,661 classes from Wikidata subclasses of "written work"
- ~3.8 million unique instances (pre-1900)

### Excluded Classes

The following classes are excluded from property extraction:

| QID | Class Name | Reason | Count |
|-----|------------|--------|-------|
| Q19389637 | biographical article | Not relevant for literary analysis | 181,810 |

Excluded files are stored in `instances/output/excluded/` for reference.

## Running the Property Extraction

```bash
cd instance_properties

# Full extraction (3-5 days)
nohup python extract_properties.py > nohup.out 2>&1 &

# Monitor progress
tail -f output/extraction.log      # Live log
cat output/status.json             # Current status (JSON)
python monitor.py                  # Formatted status
python monitor.py --watch          # Auto-refresh every 10s
python monitor.py --errors         # View recent errors
```

## Output Files

- `instance_properties/output/extracted_data.json` - Main output with all properties
- `instance_properties/output/status.json` - Real-time extraction status
- `instance_properties/output/errors.json` - Failed batches log
- `instance_properties/output/extraction.log` - Full extraction log
