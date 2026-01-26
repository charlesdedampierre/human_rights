# Wikisource Extraction Logic

## Page Types & Handling Strategy

### 1. **Direct Text** (`direct`)
**Description:** Full content is directly on the page (poems, articles, short works)

**Detection:** Text length > 1000 chars with few internal links

**Handling:** Extract text directly via parse API

**Example:**
- `https://en.wikisource.org/wiki/A_Letter_Concerning_Toleration`
- Single API call → clean HTML → text

---

### 2. **Multi-page Work** (`multipage`)
**Description:** Content split into chapters/sections as subpages

**Detection:** Short main page (<500 chars) + many links containing the base title (e.g., `Work/Chapter_1`, `Work/Chapter_2`)

**Handling:**
1. Get list of subpages via `allpages` API
2. Sort by chapter number (numeric, roman numerals)
3. Fetch each subpage (up to 100)
4. Concatenate with section headers

**Example:**
- `https://en.wikisource.org/wiki/Constitution_of_Slovenia`
- Main page → fetch `/Article_1`, `/Article_2`, etc.

---

### 3. **Portal - Versions/Translations** (`portal`)
**Description:** Index page listing different translations or editions

**Detection:** Short text + links that DON'T contain the base title + version keywords

**Handling:**
1. Extract all links from page
2. Separate: links containing base name (chapters) vs not (versions)
3. For versions: prefer specific links containing key term (e.g., "Mark")
4. Pick preferred translation (King James > American Standard > etc.)
5. Follow to actual content
6. **If result is short (<3000 chars), recursively follow** (nested portals)

**Example:**
```
Gospel_of_Mark → Mark_(Bible) → Bible_(Douay-Rheims,_Challoner)/Mark
     ↓                ↓                        ↓
  (portal)      (nested portal)         (actual text: 78K chars)
```

---

### 4. **Portal - Chapters** (detected as `portal`, handled as chapters)
**Description:** Index page where links DO contain the base title

**Detection:** Links contain base name (e.g., `Medea_(Euripides)/Scene_1`)

**Handling:** Same as multipage - fetch all chapter links

---

### 5. **Disambiguation** (`disambiguation`)
**Description:** Page listing multiple unrelated works with same name

**Detection:** Contains keywords "may refer to", "disambiguation"

**Handling:** **Skip** - logged to `_failed.json` for manual review

**Example:** "The Guardian" could be the newspaper, a poem, etc.

---

### 6. **Empty** (`empty`)
**Description:** Page exists but has minimal content

**Detection:** Text < 50 chars

**Handling:** **Skip** - logged as failed

---

### 7. **Error** (`error`)
**Description:** Page doesn't exist or API error

**Detection:** API returns error or timeout

**Handling:** Retry 3x with backoff, then log as failed

---

## Decision Tracking

All portal decisions are saved to `_portal_choices.json`:

```json
{
  "Q107388": {
    "original_url": "https://en.wikisource.org/wiki/Gospel_of_Mark",
    "original_title": "Gospel_of_Mark",
    "label": "Gospel of Mark",
    "chosen_title": "Mark_(Bible)",
    "chosen_url": "https://en.wikisource.org/wiki/Mark_(Bible)",
    "reason": "Followed nested portal: First available version",
    "nested_from": "Mark_(Bible)",
    "nested_choice": {
      "chosen_title": "Bible_(Douay-Rheims,_Challoner)/Mark",
      "reason": "Preferred translation (specific): Douay-Rheims",
      "alternatives_count": 37
    },
    "type": "version"
  }
}
```

---

## Visual Summary

```
Wikisource URL
      │
      ▼
┌─────────────────┐
│  Analyze Page   │
└────────┬────────┘
         │
    ┌────┴────┬──────────┬──────────┬─────────┐
    ▼         ▼          ▼          ▼         ▼
 DIRECT   MULTIPAGE   PORTAL    DISAMBIG   EMPTY
    │         │          │          │         │
    ▼         ▼          ▼          ▼         ▼
 Extract   Fetch all   Analyze    SKIP     SKIP
 directly  subpages    links
              │          │
              ▼          ▼
           Concat    ┌───┴───┐
                     ▼       ▼
                  Chapters  Versions
                  (base in  (base not
                   links)   in links)
                     │         │
                     ▼         ▼
                  Fetch    Pick best
                  all      translation
                            │
                            ▼
                        Short text?
                        (<3000 chars)
                            │
                      ┌─────┴─────┐
                      ▼           ▼
                     No         Yes
                      │           │
                      ▼           ▼
                   Return    Recurse
                              (depth+1)
```

---

## Output Files

| File | Description |
|------|-------------|
| `full_text/{QID}.txt` | Extracted text for each work |
| `_progress.json` | List of successfully processed QIDs |
| `_failed.json` | Failed extractions with error reasons |
| `_stats.json` | Statistics (counts by type, success rate) |
| `_portal_choices.json` | Portal translation decisions |
