# Release v3.4 — Pipe-After-Colon Parser Fix (+164,494 Sub-Values)

**Date:** March 5, 2026
**Triggered by:** Data quality audit following [Issue #15](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/15) feedback

---

## Summary

Fixed a pipe delimiter placement bug in the content normalization step that prevented dedicated parsers from extracting sub-values from ~135,000 fields (primarily 2009-2014). One line added to `normalize_content()` in `parse_field_values.py` recovered **164,494 new sub-values**, bringing the total from 1,611,094 to 1,775,588 (+10.2%).

---

## v3.3 vs v3.4 — By the Numbers

| Metric | v3.3 | v3.4 | Change |
|--------|------|------|--------|
| Structured sub-values | 1,611,094 | 1,775,588 | +164,494 (+10.2%) |
| Distinct sub-fields | 2,379 | 2,599 | +220 |
| Parser coverage | 96.2% | 97.6% | +1.4% |
| Numeric values | 988,550 | 1,109,460 | +120,910 |
| Database size (SQLite) | ~638 MB | ~656 MB | +18 MB |
| FieldNameMappings | 1,132 | 1,132 | unchanged |
| IsComputed values | 640 | 640 | unchanged |

No data was lost. All v3.3 values remain intact; this release only adds previously-unparsed values.

---

## The Bug

### Root Cause

When `build_archive.py` parses 2009-2014 HTML (CollapsiblePanel format), it extracts DOM labels and values as **separate list items**, then joins them with `' | '`:

```python
# build_archive.py line 566
content = ' | '.join(content_parts)
```

This produces Content like:

```
total: | 9,826,675 sq km | land: | 9,161,966 sq km | water: | 664,709 sq km
```

The pipe lands **between the label colon and its value** instead of between complete sub-fields. All 28 dedicated parser functions use regex patterns like `total\s*:?\s*([\d,]+)\s*sq\s*km` which cannot match through the `| `.

### Affected Years

| Years | Fields Affected | Cause |
|-------|----------------|-------|
| 2009-2014 | ~128,653 | `parse_collapsiblepanel_format()` HTML parser |
| 1992-1993 | ~4,993 | `extract_indented_fields()` text parser |
| 2021-2025 | ~3,361 | `strip_html()` JSON pipeline |
| Other | ~1,318 | Scattered edge cases |

### Why It Wasn't Caught Earlier

The v3.1 pipe delimiter migration (February 2026) intentionally changed spaces to pipes for unambiguous sub-field boundaries. The concept was correct. The bug was that some HTML parsers built `content_parts` lists with labels and values as separate items instead of paired `"label: value"` strings. The validation suite checked total coverage (which was still 96%+) but didn't flag the year-over-year coverage drop in 2009-2014.

---

## The Fix

One line added to `normalize_content()` in `parse_field_values.py`:

```python
def normalize_content(content):
    if not content:
        return ""
    content = re.sub(r'\s*\|?\s*country comparison to the world:\s*\d+', '', content)
    # Fix pipe-after-colon pattern from 2009-2014 HTML parser (label: | value -> label: value)
    content = re.sub(r':\s*\|\s*', ': ', content)
    return content.strip()
```

The regex `:\s*\|\s*` matches a pipe that appears immediately after a colon. This pattern only occurs in the buggy Content — legitimate pipes separate complete sub-fields (e.g., `"total: 9,826,675 sq km | land: 9,161,966 sq km"`), never after a colon.

Every parser already calls `normalize_content()` first, so this single change fixes all 28 dedicated parser functions and the generic fallback simultaneously.

---

## Validation Results

### FieldValues Validation (validate_field_values.py)

```
29/29 spot checks passed
Coverage: 97.6% (was 96.2%)
SourceFragment: 100%
IsComputed: 640 values, all Life expectancy / total_population
```

### Year-over-Year Impact

The biggest improvements are in the previously-affected years:

```
Year    v3.3 Values    v3.4 Values    Gain
────    ───────────    ───────────    ────
2009         41,505         53,015    +11,510
2010         41,968         53,585    +11,617
2011         46,605         60,169    +13,564
2012         48,781         62,800    +14,019
2013         53,340         69,285    +15,945
2014         54,157         70,289    +16,132
```

---

## Files Changed

| File | What |
|------|------|
| `etl/structured_parsing/parse_field_values.py` | Added pipe-after-colon fix to `normalize_content()` |
| `docs/RELEASE_v3.4.md` | This document |

---

## GitHub Release Layout

```
v1.0 — Complete Archive (1990-2025)
v2.0 — Data Quality Repair
v3.0 — Structured Field Parsing
v3.1 — Pipe Delimiters, SourceFragment, 18 New Parsers
  factbook.db (636 MB)
v3.2 — StarDict Dictionaries
  72 x tar.gz (19,010 entries, ~99 MB)
v3.3 — IsComputed Flag, Case-Sensitive Field Mappings
  factbook.db (638 MB)
v3.4 — Pipe-After-Colon Parser Fix          <-- this release
  factbook.db (656 MB, +164,494 sub-values)
```

---

## Acknowledgments

This fix was identified during a comprehensive data quality audit prompted by [@jarofromel](https://github.com/jarofromel)'s feedback across Issues [#9](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/9), [#10](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/10), and [#15](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/15). Their suggestion to use pipe delimiters (Issue #10) was the right design; this release fixes an implementation detail in how the pipes were placed during HTML parsing.
