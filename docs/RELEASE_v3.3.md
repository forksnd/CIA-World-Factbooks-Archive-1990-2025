# Release v3.3 — IsComputed Flag and Case-Sensitive Field Mappings

**Date:** March 4, 2026
**Triggered by:** [Issue #15](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/15) — feedback from [@jarofromel](https://github.com/jarofromel)

---

## What Changed and Why

A contributor identified two problems in the v3.1 database:

1. **41 unmapped field names** — A `LEFT JOIN` between `CountryFields.FieldName` and `FieldNameMappings.OriginalName` revealed 3,311 rows (41 field names) with no mapping, plus one parser artifact (`Text`, 36 rows). These fields were invisible to any query that joined through FieldNameMappings.

2. **Computed values indistinguishable from source data** — FieldValues contained rows where `NumericVal` was computed by averaging neighboring sub-values (e.g. total life expectancy averaged from male/female in pre-1995 data), but there was no way to distinguish these from values extracted directly from the CIA's text.

Both problems were real. We fixed both.

---

## v3.1 vs v3.3 — By the Numbers

| Metric | v3.1 | v3.3 | Change |
|--------|------|------|--------|
| Field name mappings | 1,090 | 1,132 | +42 |
| Unmapped field names | 42 | 0 | -42 |
| Structured sub-values | 1,610,973 | 1,611,094 | +121 |
| Distinct sub-fields | 2,386 | 2,379 | -7 (consolidated) |
| IsComputed=1 values | — | 640 | new column |
| Database size (SQLite) | ~636 MB | ~638 MB | +2 MB |

No data was lost. All original values remain intact.

---

## Issue 1: Case-Sensitive Field Name Mappings

### Root Cause

`build_field_mappings.py` runs against SQL Server, whose default collation (`SQL_Latin1_General_CP1_CI_AS`) is **case-insensitive**. When grouping distinct field names, case variants like `Natural hazards` and `natural hazards`, or `Telephone system` and `telephone system`, collapsed into a single mapping row.

SQLite's `=` operator is **case-sensitive** by default. When the data was exported to SQLite, the `LEFT JOIN` on `cf.FieldName = fm.OriginalName` failed for 42 case variants (3,347 rows) because the mapping row stored `Natural hazards` but the field was `natural hazards`.

### The Fix

Three-level fix:

1. **`build_field_mappings.py`** — Added `COLLATE Latin1_General_CS_AS` to the `OriginalName` column definition in `FieldNameMappings`, forcing case-sensitive grouping. This generates a separate mapping row for each case variant. Result: 1,132 mappings (was 1,090).

2. **SQLite export scripts** — Both `export_to_sqlite.py` and `export_field_values_to_sqlite.py` now run an integrity check after copying data. Unmapped field names are detected, matched case-insensitively to existing mappings, and auto-backfilled with the correct classification.

3. **`validate_integrity.py`** — Added explicit `COLLATE Latin1_General_CS_AS` to all JOIN conditions between `CountryFields.FieldName` and `FieldNameMappings.OriginalName`. Check 10 now runs the exact gap query to verify zero unmapped fields.

### The 42 Previously Unmapped Field Names

These are all lowercase variants of fields that appear in 1993-1994 data, where the CIA restructured their internal database and sub-fields appeared at column 0 instead of indented:

```
natural hazards, telephone system, inland waterways, civil air,
ports, merchant marine, railways, highways, pipelines,
defense forces, national holiday, executive branch, constitution,
judicial branch, legislative branch, diplomatic representation in the us,
diplomatic representation from the us, flag description,
total fertility rate, birth rate, death rate, net migration rate,
infant mortality rate, national product, national product per capita,
inflation rate, unemployment rate, revenues, expenditures,
external debt, industrial production, electricity, agriculture,
industries, illicit drugs, economic aid, currency, exchange rates,
fiscal year, membership in international organizations, contiguous zone
```

---

## Issue 2: IsComputed Column

### The Problem

For pre-1995 life expectancy data, the CIA published only male and female values with no explicit total. Our parser averaged the two to produce a `total_population` row. The `SourceFragment` pointed to the male/female text, but the computed number didn't appear anywhere in `CountryFields.Content`.

### The Fix

Added `IsComputed` column to FieldValues:

- **SQL Server:** `BIT NOT NULL DEFAULT 0`
- **SQLite:** `INTEGER NOT NULL DEFAULT 0`

Every value extracted directly from source text has `IsComputed = 0`. The 640 averaged life expectancy totals are flagged with `IsComputed = 1`.

```sql
-- Only source-extracted values
SELECT * FROM FieldValues WHERE IsComputed = 0;

-- See which values were computed
SELECT c.Name, c.Year, fv.SubField, fv.NumericVal, fv.SourceFragment
FROM FieldValues fv
JOIN CountryFields cf ON fv.FieldID = cf.FieldID
JOIN Countries c ON cf.CountryID = c.CountryID
JOIN FieldNameMappings fnm ON cf.FieldName = fnm.OriginalName
WHERE fnm.CanonicalName = 'Life expectancy at birth'
  AND fv.IsComputed = 1
ORDER BY c.Year, c.Name;
```

### Scope

| Metric | Count |
|--------|-------|
| Total FieldValues | 1,611,094 |
| IsComputed = 0 | 1,610,454 |
| IsComputed = 1 | 640 |
| Affected field | Life expectancy at birth |
| Affected sub-field | total_population |
| Affected years | 1990-1994 |

---

## Validation Results

### Integrity Check (validate_integrity.py)

```
Check                              Status
─────────────────────────────────  ──────
[1]  Table existence               PASS
[2]  Row count benchmarks          PASS
[3]  US population ground truth    PASS
[4]  US GDP ground truth           PASS
[5]  Year-over-year consistency    PASS
[6]  NULL detection                PASS
[7]  Source provenance             PASS
[8]  Encoding (0 U+FFFD)          PASS
[9]  FieldValues coverage          PASS
[10] Unmapped field names          PASS (0 gaps)

Result: HIGH CONFIDENCE
```

### FieldValues Validation (validate_field_values.py)

```
29/29 spot checks passed
IsComputed validated: 640 values, all Life expectancy at birth / total_population
```

### Database Comparison (v3.1 vs v3.3)

```
                    v3.1         v3.3         Delta
FieldNameMappings   1,090        1,132        +42
FieldValues         1,610,973    1,611,094    +121
Unmapped fields     42           0            -42
IsComputed=1        —            640          new
```

---

## Files Changed

| File | What |
|------|------|
| `etl/build_field_mappings.py` | `COLLATE Latin1_General_CS_AS` on OriginalName column + verify JOIN |
| `etl/structured_parsing/parse_field_values.py` | IsComputed column in schema, INSERT, and life expectancy parser |
| `etl/structured_parsing/export_field_values_to_sqlite.py` | IsComputed in SELECT/INSERT, backfill integrity check |
| `etl/validate_integrity.py` | Collation-aware JOINs in Check 10 |
| `etl/structured_parsing/validate_field_values.py` | Collation-aware JOINs, IsComputed spot checks |
| `schema/create_field_values.sql` | `IsComputed BIT NOT NULL DEFAULT 0` |
| `README.md` | Updated stats: 1,132 mappings, 1,611,094 values, IsComputed documentation |
| `docs/RELEASE_v3.3.md` | This document |

---

## Acknowledgments

This release was directly driven by feedback from [@jarofromel](https://github.com/jarofromel) in [Issue #15](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/issues/15). Their SQL query exposed both the unmapped field name gap and the computed value transparency problem.
