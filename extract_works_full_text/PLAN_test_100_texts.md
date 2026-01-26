# Plan: Test Extraction on 100 Texts

## Objective

Validate that the Wikisource extraction produces good quality text by testing on 100 diverse items.

---

## Step 1: Select 100 Diverse Test Items

**Criteria:**

- Mix of page types (direct, portal, multipage)
- Mix of content types (poems, novels, legal, religious, encyclopedia)
- English Wikisource only
- Prioritize items with high sitelink counts (more notable works)

**Output:** `full_text/_test_100_items.json`

---

## Step 2: Extract All 100 Texts

**For each item, save:**

- QID
- Label (work title)
- Original URL
- Page type detected
- Text stats (chars, words, pages)
- First 500 chars preview
- Portal choice (if applicable)

**Output:** `full_text/_test_100_results.json`

---

## Step 3: Quality Validation Checks

**Automated checks:**

1. **Minimum length:** At least 100 chars
2. **No HTML artifacts:** No `<`, `>`, `&nbsp;`
3. **No wiki markup:** No `{{`, `}}`, `[[`, `]]`
4. **Readable text:** Words separated by spaces, not garbled
5. **Language check:** Text appears to be in expected language

**Manual spot-check (10 items):**

- Compare extracted text with original Wikisource page
- Verify content makes sense
- Check for missing sections

---

## Step 4: Generate Quality Report

**Report includes:**

- Success rate
- Distribution by page type
- Total book pages extracted
- List of failures with reasons
- Sample of good extractions
- Sample of problematic extractions (if any)

**Output:** `full_text/_test_100_report.md`

---

## Files to Create

| File | Purpose |
|------|---------|
| `test_100_extraction.py` | Main test script |
| `_test_100_items.json` | Selected test items |
| `_test_100_results.json` | Extraction results |
| `_test_100_report.md` | Quality report |

---

## Success Criteria

- [ ] 90%+ success rate
- [ ] No HTML/wiki artifacts in successful extractions
- [ ] Portal choices logged correctly
- [ ] Text stats (chars, words, pages) calculated
- [ ] Manual spot-check confirms quality

---

## Execution

```bash
cd extract_works_full_text
python test_100_extraction.py
```

Estimated time: ~5-10 minutes (100 items × 0.3s delay)
