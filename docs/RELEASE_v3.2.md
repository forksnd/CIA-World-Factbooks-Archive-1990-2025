# Release v3.2 — StarDict Dictionaries and Encoding Repair

**Date:** March 1, 2026

---

## Summary

Rebuilt all 72 StarDict offline dictionaries from the v3.1 `factbook.db` database, fixed all remaining encoding corruption (117 U+FFFD characters reduced to 0), reorganized GitHub releases so the database and dictionaries ship separately, and updated the build pipeline to prefer the consolidated `factbook.db`.

---

## What This Release Contains

72 StarDict dictionaries (36 years x 2 editions) for KOReader, GoldenDict, and other StarDict-compatible apps.

| Metric | Value |
|--------|-------|
| Dictionaries | 72 |
| Total entries | 19,010 |
| Compressed size | ~99 MB |
| Encoding errors | 0 / 338,187,838 bytes |
| Source database | `factbook.db` (636 MB) |
| Validation | 13/16 tests pass |

---

## Change 1: Encoding Repair — 117 U+FFFD Characters Fixed

### The Problem

37 fields across 2006-2017 contained U+FFFD (Unicode replacement character) where accented Latin characters, currency symbols, and special punctuation should have been. These originated from HTML-to-text conversion during ETL, where Windows-1252 or Latin-1 encoded bytes were misinterpreted as invalid UTF-8.

The corruption affected both `CountryFields.Content` (37 fields) and `FieldValues.TextVal`/`SourceFragment` (35 rows) — totaling 117 individual bad characters across 338 million bytes.

### The Fix

Created `scripts/repair_encoding_fffd.py` — a targeted repair script with a hand-verified map of all 37 corrupted fields. Each U+FFFD was identified by context and cross-referenced against adjacent clean years in the database and authoritative sources.

### Characters Restored

```
 Character    Unicode    Context                              Countries Affected
 ──────────── ────────── ──────────────────────────────────── ────────────────────
 i (acute)    U+00ED     Rio San Juan, Rio Mamore             Costa Rica, Bolivia,
                                                              Brazil (2006-2009)
 a (acute)    U+00E1     Isla Suarez, Guajara-Mirim, Parana   Bolivia, Brazil,
                                                              Paraguay (2008-2017)
 e (acute)    U+00E9     Armees, Republique, Democratique,    DRC, Monaco, Thailand,
                         Revoires, melange, coup d'etat       Comoros (2009-2017)
 u (acute)    U+00FA     Itaipu Dam                           Brazil (2008)
 E (acute)    U+00C9     SEHOUETO (Lazare)                    Benin (2008-2015)
 S (caron)    U+0160     MATSASA, Sakiai, Silale, Silute,     Lesotho, Lithuania
                         Sirvintos, Svencionys                (2015-2017)
 s (caron)    U+0161     Anyksciai, Birstono, Joniskis,       Lithuania (2015)
                         Kaisiadorys, Kupiskis, Rokiskis,
                         Radviliskis, Telsiai, Vilkaviskis
 z (caron)    U+017E     Birzai, Mazeikiai, Panevezys         Lithuania (2015)
 e (dot)      U+0117     Elektrenai, Pagegiai                 Lithuania (2015)
 pound        U+00A3     375 billion                          UK (2015)
 degree       U+00B0     82 W meridian                        Colombia (2008)
 half         U+00BD     5 1/2-year high                      Chile (2006)
 plus-minus   U+00B1     4.0% +/- 1.0%                       Dominican Rep. (2016)
 em dash      U+2014     constitution -- due in May 2011 --   Nepal (2010)
 left quote   U+2018     'second wave', Al 'Asimah            Vietnam, Jordan (2017)
 right quote  U+2019     EC's external                        EU (2015)
 left dquote  U+201C     "BOKK GIS GIS"                       Senegal (2015)
 comma        U+002C     products, edible oil                 Burma (2016)
 semicolon    U+003B     products; edible oil                 Burma (2017)
```

### Methodology

1. Queried `CountryFields` for all rows where `Content LIKE '%' || X'EFBFBD' || '%'` — found 37 fields.
2. For each field, extracted exact byte position and +-40 character context around every U+FFFD.
3. Queried the same country + field name in adjacent years (year-1, year+1, year-2, year+2) where the data was clean. This identified 80% of the correct characters.
4. For years 2006-2010 where adjacent years also had stripped accents (plain ASCII instead of accented), used domain knowledge: Spanish place names (Rio, Suarez, Parana), French terms (Armees, Republique), Portuguese names (Itaipu, Guajara), Lithuanian municipality names, and standard symbols.
5. Applied the same fixes to `FieldValues.TextVal` and `FieldValues.SourceFragment` (35 rows).
6. Ran on both `factbook.db` and `factbook_field_values.db`.
7. Verified: 0 U+FFFD remaining in either database across all 3 text columns.

### Repair Script

```
scripts/repair_encoding_fffd.py
  --apply    Apply fixes (default: dry run)

  37 fields, 73 substring replacements
  Applied to: CountryFields.Content, FieldValues.TextVal, FieldValues.SourceFragment
  Databases: factbook.db, factbook_field_values.db
```

---

## Change 2: Build Pipeline — Database Preference Fix

### The Problem

`etl/stardict/build_stardict.py` preferred `factbook_field_values.db` over `factbook.db`. Since v3.1 consolidated everything into `factbook.db` (which the webapp uses), the StarDict dictionaries were being built from the wrong database. `factbook.db` had 2 more fields (1996 Tuvalu data from the repair) that `factbook_field_values.db` was missing.

### The Fix

Updated `build_stardict.py` to prefer `factbook.db` (the webapp database), falling back to `factbook_field_values.db` only if `factbook.db` doesn't exist. Added a runtime check that verifies the `FieldValues` table exists before attempting the structured edition build.

```
 Old preference:  factbook_field_values.db > factbook.db
 New preference:  factbook.db > factbook_field_values.db
```

### File Changed

```
 etl/stardict/build_stardict.py
   - PRIMARY_DB = factbook.db (was FIELD_VALUES_DB = factbook_field_values.db)
   - FALLBACK_DB = factbook_field_values.db (was GENERAL_DB = factbook.db)
   - Added FieldValues table existence check for structured editions
```

---

## Change 3: Release Reorganization

### The Problem

v3.1 was "StarDict Dictionaries" (72 tars built from old data) and v3.2 was "Pipe Delimiters" (factbook.db). This mixed the data release with the dictionary release, and the StarDict tars on v3.1 were built before the encoding fixes.

### The Fix

Swapped the release content:

```
 Before                                    After
 ─────────────────────────────────────     ─────────────────────────────────────
 v3.1: StarDict Dictionaries              v3.1: Pipe Delimiters, SourceFragment
       72 tar.gz (old data)                     factbook.db (636 MB)
                                                1 asset
 v3.2: Pipe Delimiters, SourceFragment    v3.2: StarDict Dictionaries
       factbook.db (636 MB)                     72 tar.gz (new data, 0 encoding errors)
       72 tar.gz (uploaded mid-session)         72 assets, ~99 MB total
```

Steps taken:
1. Deleted all 72 old StarDict tars from v3.1
2. Uploaded `factbook.db` to v3.1, updated title and release notes
3. Deleted all 73 assets (factbook.db + 72 tars) from v3.2
4. Uploaded 72 new StarDict tars to v3.2, updated title and release notes
5. Renamed `docs/RELEASE_v3.2.md` to `docs/RELEASE_v3.1.md`, updated header

---

## Change 4: StarDict Dictionary Rebuild

Rebuilt all 72 dictionaries from the repaired `factbook.db`:

```
 Building 72 StarDict dictionaries...
   Database:      factbook.db (636 MB)
   Years:         1990-2025 (36 years)
   Editions:      general, structured
   Compression:   dictzip

 Built 72 dictionaries (19,010 total entries) in 33.1s

 Validation:
   Dictionaries: 72/72
   Total size:   98.4 MB
   All files present.
```

---

## Validation Results

### StarDict Deep Validation (16 tests)

```
 Test                          Result    Notes
 ───────────────────────────── ──────── ────────────────────────────────────
 [T1]  File presence           PASS     288/288 files
 [T2]  No empty entries        PASS     0 empty
 [T3]  DB count match          FAIL     3/36 years (known: Redirect/Unknown rows)
 [T4]  Every entry has <h3>    PASS     0 missing
 [T5]  Gen/Struct lists match  PASS     0/36 mismatches
 [T6]  No duplicates           PASS     0 duplicates
 [T7]  Min entry size          PASS     0 under 50 bytes
 [T8]  ISO synonyms            PASS     15/15 codes
 [T9]  HTML tag balance        PASS     All balanced
 [T10] Gen/Struct differs      PASS     All differ
 [T11] Ground truth            FAIL     48/50 (Yugoslavia/GDR missing)
 [T12] Structured sub-fields   PASS     20/20
 [T13] Historical names        FAIL     Yugoslavia/GDR (MasterCountryID = NULL)
 [T14] Encoding                PASS     0 bad / 338,187,838 bytes (0.000000%)
 [T15] Name match              PASS     0 errors
 [T16] Round-trip read          PASS     pyglossary verified
```

**Score: 13/16** (3 pre-existing data mapping issues, not regressions)

### Known Failures (pre-existing)

**T3 — DB count mismatch (2006, 2007, 2012):**
These years contain "Redirect page" (2006: 12, 2007: 7) and "Unknown" (2012: 9) placeholder entries in the `Countries` table. The StarDict builder correctly excludes them because they have no real content. The validation test counts raw DB rows rather than meaningful entries.

**T11/T13 — Yugoslavia and German Democratic Republic (1990-1991):**
These historical entities have `MasterCountryID = NULL` in the `Countries` table. The StarDict builder joins through `MasterCountries` for ISO/FIPS synonym lookup, so entries without a MasterCountryID are skipped. This is a data mapping issue in the source database, not a build problem.

---

## Database Integrity Verification

```
 factbook.db:
   CountryFields:            1,071,603
   FieldValues:              1,610,973
   Distinct SubFields:       2,386
   U+FFFD in Content:        0
   U+FFFD in TextVal:        0
   U+FFFD in SourceFragment: 0
   Spot: Bolivia 2008 Rio=True, Suarez=True
   Spot: Lithuania 2015 s-caron=True
   Spot: UK 2015 pound=True

 factbook_field_values.db:
   CountryFields:            1,071,601
   FieldValues:              1,610,973
   Distinct SubFields:       2,386
   U+FFFD in Content:        0
   U+FFFD in TextVal:        0
   U+FFFD in SourceFragment: 0
```

---

## Files Changed

| File | What |
|------|------|
| `etl/stardict/build_stardict.py` | Database preference: factbook.db > factbook_field_values.db |
| `scripts/repair_encoding_fffd.py` | New: targeted U+FFFD repair for 37 fields across both databases |
| `docs/RELEASE_v3.1.md` | Renamed from RELEASE_v3.2.md, updated header to v3.1 |
| `docs/RELEASE_v3.2.md` | New: this document |
| `data/factbook.db` | 37 CountryFields + 35 FieldValues rows repaired (0 U+FFFD remaining) |
| `data/factbook_field_values.db` | Same repairs applied |

---

## GitHub Release Layout

```
 v1.0 — Complete Archive (1990-2025)
   factbook.db (original)

 v2.0 — Data Quality Repair
   factbook.db (encoding + dedup fixes)

 v3.0 — Structured Field Parsing
   factbook.db + factbook_field_values.db

 v3.1 — Pipe Delimiters, SourceFragment, 18 New Parsers
   factbook.db (636 MB, single consolidated database)

 v3.2 — StarDict Dictionaries          <-- this release
   72 x tar.gz (19,010 entries, ~99 MB total)
   Built from v3.1 factbook.db with 0 encoding errors
```
