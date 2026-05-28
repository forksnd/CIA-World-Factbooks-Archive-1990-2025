# SQL Server Cleanup Plan

Generated: 2026-05-27
**Status: EXECUTED on 2026-05-28. SQL Server is now in sync with SQLite.**
Author: handoff from L0-L3b validation pass.

## Outcome (post-execution)

Final row counts after cleanup:

| Table | SQLite | SQL Server | Status |
|---|---|---|---|
| Countries | 9,535 | 9,535 | MATCH |
| CountryFields | 1,071,489 | 1,071,489 | MATCH (was +112 stale) |
| CountryCategories | 83,682 | 83,673 | -9 (orphaned from deleted Serbia rb categories; benign) |
| MasterCountries | 284 | 284 | MATCH |
| FieldNameMappings | 1,132 | 1,132 | MATCH |

All 36 years of CountryFields match exactly between the two DBs. Madagascar
Territorial sea now reads `12 nm` in both (was `3 nm` in SQL Server).
Somalia Birth rate now reads `47 births/1,000` in both (was `18` in SQL
Server). The Serbia 2008 duplicate is gone.

## Root cause uncovered during execution

The sync script `etl/sync_sqlite_to_sqlserver.py` was originally keyed by
`(Year, Code, CategoryTitle, FieldName)`. Within 1990-2001 Gutenberg data,
seven Code values collide because the parser's `make_code()` produces
2-letter codes by truncating country names:

| Code | Colliding countries |
|---|---|
| `'st'` (×5) | St. Helena, St. Kitts and Nevis, St. Lucia, St. Pierre and Miquelon, St. Vincent and the Grenadines |
| `'so'` (×2) | **Somalia, Soviet Union** |
| `'ma'` (×2) | Madagascar, Man, Isle of |
| `'ca'` (×2) | Canada, Cape Verde |
| `'pa'` (×2) | Pacific Islands, Paraguay |
| `'ym'` (×2) | Yemen Arab Republic, Yemen, People's Democratic Republic of |
| `'iz'` (×2) | Iraq, Iraq-Saudi Arabia Neutral Zone |

When the sync built a `(Code, Cat, Field) -> Content` lookup dict from
SQLite, the second country with a colliding code silently overwrote the
first. Then when comparing against SQL Server rows, the dict's last-wins
value would match SQL Server's stored value (which was itself the result
of an earlier sync run hitting the same bug), so the sync would report
0 updates needed — even though Madagascar had Isle of Man's value and
Somalia had Soviet Union's value.

**Fix:** changed the key from `(Code, Cat, Field)` to
`(Name, Cat, Field)`. Country Names are unique within a year. Same fix
applied to `sync_master_countries` which keyed by `(Year, Code)`.

The fixed script lives in `etl/sync_sqlite_to_sqlserver.py`. After the fix,
the dry-run correctly identified 2,621 CountryFields updates and 33
MasterCountryID updates concentrated in 1990-2001.

## Execution log (2026-05-28)

| Step | Result |
|---|---|
| 0. Backup CIA_WorldFactbook to MSSQL Backup dir | 442 MB raw / 133 MB compressed; `RESTORE VERIFYONLY` passed |
| 1. Fix sync script (Code -> Name in both functions) | Committed |
| 2. Dry-run | 2,621 CountryFields updates + 33 MasterCountryID updates predicted |
| 3. Apply CountryFields sync | 2,621 rows updated across 1990-2017; Madagascar / Somalia / etc. verified |
| 4. Drop `UQ_MasterCountries_CanonicalCode` (SQLite has no such constraint and intentionally lets dissolved entities share codes, e.g. Slovakia + Czechoslovakia both = `'LO'`) | Constraint dropped |
| 5. INSERT 3 missing MasterCountries (Soviet Union UR, Czechoslovakia LO, Yugoslavia YU; EntityType `'dissolved'`) | Total now 284 |
| 6. DELETE Serbia 2008 stale `'rb'` row + 114 CountryFields + 9 CountryCategories children | Countries Year=2008 went from 263 to 262 |
| 7. UPDATE 3 FieldNameMappings rows (CanonicalName `'Military expenditures - percent of GDP'` -> `'Military expenditures'`) | 3 rows updated |
| 8. Re-run sync for MasterCountryID linking | 33 MasterCountryID updates applied (was previously failing on FK to missing MasterCountries) |
| 9. Insert 2 missing Serbia 2008 CountryFields (`Economic aid - recipient`, `Population below poverty line`) that were orphaned to the deleted `'rb'` row | Serbia 2008 row count now 119 in both DBs |
| 10. Re-validate via L3b | Matched rows: 1,063,060 -> 1,065,234; per-year row counts: all 36 years match exactly |

Total elapsed: ~30 minutes including investigation and verification.
Backup remains in place at the SQL Server default backup directory.

## What we are NOT doing (deferred)

- **Not migrating `FieldValues` to SQL Server.** No consumer. Webapp reads
  SQLite. Same reasoning as the original plan section 3.
- **Not migrating `ISOCountryCodes` to SQL Server.** No consumer.
- **Not shrinking the SQL Server data file.** Optional disk reclamation;
  can be done off-hours with `DBCC SHRINKFILE` if desired.

## 1. Diagnosis

The local SQL Server instance `CIA_WorldFactbook` has drifted from the
canonical SQLite mirror at `data/factbook.db`. Three distinct issues are
present, ranked by severity.

### 1.1 Schema drift (3 missing tables)

| Table | SQLite rows | SQL Server | Note |
|---|---|---|---|
| `FieldValues` | 1,778,669 | MISSING | Structured-parse output from Mar 2026. Webapp uses this for typed/numeric queries. |
| `ISOCountryCodes` | 250 | MISSING | ISO 3166-1 lookup table. |
| `CountryFieldsFTS` (+4 shadow tables) | n/a | MISSING (SQL Server uses a different FTS engine) | SQLite FTS5 — not portable, do not migrate. |

SQL Server also has two tables that SQLite does not:

| Table | SQL Server rows | Note |
|---|---|---|
| `Incidents` | 9 | User's incident log (system tracking, not Factbook data). KEEP. |
| `sysdiagrams` | n/a | SSMS database-diagram metadata. KEEP. |

### 1.2 Stale Serbia row (1 duplicate)

SQL Server `Countries` for year 2008 contains both:

```
CountryID=2341, Code='rb', Name='Serbia', MasterCountryID=208  <-- stale, should be deleted
CountryID=2342, Code='ri', Name='Serbia', MasterCountryID=208  <-- correct
```

SQLite has only `CountryID=2342`. This is a pre-2026 leftover from the
2008 Factbook code-change (CIA switched Serbia from `rb` to `ri`). The
stale `rb` row carries 114 child `CountryFields` rows that disagree with
both the raw 2008 HTML zip and the canonical SQLite.

Per-year `Countries` totals match SQLite exactly except 2008
(SQL Server 263 vs SQLite 262).

### 1.3 Real content drift in 1990-2001 (10,154 UPDATEs needed)

This is the serious finding. SQL Server's `CountryFields.Content` for the
Gutenberg `.txt` era (1990-2001) contains values that disagree with both
the raw source files and the canonical SQLite. Spot-check against
`samples/text_samples/1990.txt`:

| Country / Field | SQL Server | Raw .txt + SQLite |
|---|---|---|
| Madagascar / Territorial sea (1990) | `3 nm` | `12 nm` |
| Madagascar / Total fertility rate (1990) | `1.8 children born/woman` | `6.9 children born/woman` |
| Somalia / Birth rate (1990) | `18 births/1,000 population` | `47 births/1,000 population` |

Pattern: looks like cross-country field leakage from a pre-2026 buggy
parse. Full evidence in `L3B_REPORT.md`.

UPDATE volume by year (computed by replaying the
`sync_sqlite_to_sqlserver.py` matching logic against SQL Server):

| Year | UPDATEs | Year | UPDATEs |
|---|---|---|---|
| 1990 | 949 | 1996 | 627 |
| 1991 | 516 | 1997 | 682 |
| 1992 | 554 | 1998 | 628 |
| 1993 | 284 | 1999 | 452 |
| 1994 | 4,180 | 2001 | 697 |
| 1995 | 585 | 2000 | 0 (HTML) |
| | | 2002-2025 | 0 |
| | | **TOTAL** | **10,154** |

(The 1994 spike of 4,180 lines up with the year SQL Server appears to
have been most heavily corrupted — likely an asterisk-format parser run
before the 1994 column-0 sub-field fix.)

### 1.4 MasterCountries drift (3 missing)

SQLite has 284 `MasterCountries` rows, SQL Server has 281. The three
extra rows in SQLite are historical entities added during the v3.0+
master-country pass:

| CanonicalCode | CanonicalName | ISOAlpha2 | SQLite MasterID |
|---|---|---|---|
| LO | Czechoslovakia | (null) | 291 |
| UR | Soviet Union | SU | 290 |
| YU | Yugoslavia | YU | 292 |

These were added so that 1990-1992 entries for those dissolved states
have a `MasterCountryID` parent. SQL Server is missing them.

### 1.5 FieldNameMappings drift (3 mappings)

Same canonical-name reconciliation: SQL Server has the older
`Military expenditures - percent of GDP` -> self-mapping; SQLite has
the corrected mapping consolidating those three variants under
`Military expenditures`. Three rows.

### 1.6 Totals summary

| Object | SQLite | SQL Server | Diff |
|---|---|---|---|
| `CountryFields` rows | 1,071,489 | 1,071,601 | +112 stale |
| `Countries` rows | 9,535 | 9,536 | +1 stale (Serbia 2008 'rb') |
| `MasterCountries` rows | 284 | 281 | -3 missing |
| `FieldValues` rows | 1,778,669 | 0 (table missing) | -1,778,669 missing |
| `ISOCountryCodes` rows | 250 | 0 (table missing) | -250 missing |
| `FieldNameMappings` rows | 1,132 | 1,132 | 3 mappings disagree |
| Database file size | 703 MB | ~9 GB | SQL Server is heavily fragmented |

## 2. Constraints discovered

Schema differences that affect any rebuild strategy:

- SQL Server has **5 foreign keys** that SQLite does not enforce:
  - `FK_Countries_MasterCountries` — Countries.MasterCountryID -> MasterCountries
  - `FK_CountryCategories_Countries`
  - `FK_CountryFields_Categories`
  - `FK_CountryFields_Countries`
  - `FK_MasterCountries_Administering` — self-FK on MasterCountries
- All primary keys are **IDENTITY** columns. Inserting explicit IDs
  requires `SET IDENTITY_INSERT TableName ON` per table.
- `CountryID`/`CategoryID`/`FieldID` values in SQLite and SQL Server
  largely line up for years where the row counts match, but the
  Serbia 2008 stale row consumes `CountryID=2341` and its 114 child rows
  occupy `FieldID` values that do not exist in SQLite. The two databases
  cannot be made byte-identical on primary keys without re-keying.
- SQL Server's `Incidents` table must be preserved.

## 3. Recommended approach

**Recommendation: in-place fix with `sync_sqlite_to_sqlserver.py`, plus
three small targeted scripts. Do NOT do a full schema rebuild.**

### Justification

- The existing `etl/sync_sqlite_to_sqlserver.py` already implements
  exactly the UPDATE logic we need for 1.3 and 1.5 — sync was its
  designed purpose and the row-match logic is correct. The 10,154
  UPDATEs it would issue match the drift we measured.
- Total cleanup is small: ~10K UPDATEs + 115 DELETEs + 3-row INSERT
  + one schema migration. A full rebuild would re-create 1.07M rows
  for no benefit (the other 99% of CountryFields already matches).
- A schema rebuild would break the user's `Incidents` log unless
  preserved separately, and would also lose any SSMS diagrams.
- SQL Server's 9 GB file size is mostly log/index fragmentation, not
  real data. A `DBCC SHRINKFILE` after cleanup recovers space without
  a rebuild.

### What about adding `FieldValues` (1.78M rows) to SQL Server?

**Skip it unless there is a concrete consumer.** Per
`feedback_data_boundaries.md`, the webapp now reads SQLite and SQL Server
is dev/ETL-only. The structured parser's output is consumed via SQLite,
which is the canonical store. Adding `FieldValues` to SQL Server costs
~1 GB and an hour of bulk-load time and would gain nothing today.

If the user later wants a SQL Server `FieldValues` mirror, the same
`export_to_sqlite.py` pattern (in reverse) handles it: ~10 lines per
table. Defer.

## 4. Step-by-step plan

All steps are **manual / human-greenlit**. No automation runs without
explicit user approval. SQL Server backup is a hard prerequisite.

### Step 0 — Take a SQL Server backup (mandatory rollback)

```sql
BACKUP DATABASE CIA_WorldFactbook
TO DISK = '<your_backup_directory>\CIA_WorldFactbook_pre_cleanup.bak'
WITH FORMAT, COMPRESSION,
NAME = 'Full backup before drift cleanup 2026-05-27';
```

This is the rollback for all four subsequent steps. ~9 GB compressed
will be ~1-2 GB. **Verify the .bak file exists and `RESTORE VERIFYONLY
FROM DISK = '...'` succeeds before proceeding.**

### Step 1 — Dry-run the existing sync script

```powershell
python etl/sync_sqlite_to_sqlserver.py --dry-run
```

Expected output:
- `CountryFields updated: 10154` (matches our measurement)
- `MasterCountryID updated: 0` or small number

If the count differs significantly from 10,154, stop and investigate
before proceeding.

### Step 2 — Apply the content sync (UPDATEs)

```powershell
python etl/sync_sqlite_to_sqlserver.py
```

This fixes section 1.3 (10,154 content drifts) and section 1.5 partially.
Expected runtime: ~3-5 minutes (autocommit per year).

**Rollback**: `RESTORE DATABASE CIA_WorldFactbook FROM DISK = '...'`
from the Step 0 backup.

### Step 3 — Delete the stale Serbia 2008 row + dependents

Write a new one-off script `scripts/fix_sql_server_serbia_2008.py`
(does NOT exist yet — create only if the user approves this step):

```python
# Delete order: CountryFields -> CountryCategories -> Countries
# Use a single transaction so FK violations surface immediately.

DELETE_SQL = """
BEGIN TRANSACTION;

DELETE cf FROM CountryFields cf
  JOIN Countries c ON cf.CountryID = c.CountryID
  WHERE c.CountryID = 2341 AND c.Year = 2008 AND c.Code = 'rb';

DELETE cc FROM CountryCategories cc
  JOIN Countries c ON cc.CountryID = c.CountryID
  WHERE c.CountryID = 2341;

DELETE FROM Countries WHERE CountryID = 2341;

-- Confirm the row count went from 263 to 262 for 2008
SELECT COUNT(*) AS Countries2008 FROM Countries WHERE Year = 2008;

-- IF SAFE: COMMIT; ELSE: ROLLBACK;
ROLLBACK;   -- Default to ROLLBACK until manually flipped to COMMIT
"""
```

Run with `ROLLBACK` first (zero-cost preview), then flip to `COMMIT`.
This removes 1 Country + 14 CountryCategories + 114 CountryFields = ~129
rows.

**Rollback**: same DB-backup restore.

### Step 4 — Add the 3 missing MasterCountries

Write `scripts/add_missing_mastercountries.py`:

```python
# IDENTITY_INSERT required because MasterCountryID is IDENTITY.
SQL = """
SET IDENTITY_INSERT MasterCountries ON;
INSERT INTO MasterCountries (MasterCountryID, CanonicalCode, CanonicalName, ISOAlpha2, EntityType, AdministeringMasterCountryID)
VALUES
  (290, 'UR', 'Soviet Union', 'SU', NULL, NULL),
  (291, 'LO', 'Czechoslovakia', NULL, NULL, NULL),
  (292, 'YU', 'Yugoslavia', 'YU', NULL, NULL);
SET IDENTITY_INSERT MasterCountries OFF;
"""
```

Wrap in `BEGIN TRANSACTION` / `ROLLBACK` preview, then `COMMIT` when
verified. Re-read the SQLite `EntityType` field first — the example
above assumes `NULL` but the SQLite row may have a value.

After Step 4, also re-run `sync_sqlite_to_sqlserver.py` once to populate
the `Countries.MasterCountryID` back-references for any 1990-1992
records that now have a master parent.

### Step 5 — Reconcile FieldNameMappings (3 rows)

Smallest change. Either:

(a) Just UPDATE the three rows in place:

```sql
UPDATE FieldNameMappings
SET CanonicalName = 'Military expenditures',
    ConsolidatedTo = 'Military expenditures'
WHERE OriginalName IN (
  'Military expenditures - percent of GDP',
  'Military expenditures--percent of GDP',
  'Military expenditures-percent of GDP'
);
```

(b) Or just accept the drift — it's a derived table and the webapp
doesn't read it from SQL Server.

Recommend (a) for consistency; it's three rows.

### Step 6 — Reclaim disk space (optional)

After the deletes, the 9 GB file is mostly free space:

```sql
DBCC SHRINKFILE (CIA_WorldFactbook, 2000);  -- target 2 GB
```

Run during off-hours. Not strictly necessary.

### Step 7 — Re-validate

```powershell
python scripts/validate_raw_l3b.py
```

Success criteria:
- `Content diffs` should drop from 7,948 to 0
- `Missing in re-parse` should drop from 575 to 0 (1996 repaired set)
- `Extra in re-parse` should stay at 41 (sub-field-from-1996-repair —
  benign)
- Per-year row counts for 2008 should be 30,641 (down from 30,755).

If any of the above is unexpectedly different, restore from the Step 0
backup.

## 5. What we are NOT doing (and why)

- **Not migrating `FieldValues` into SQL Server.** No consumer. Section 3
  rationale.
- **Not dropping/recreating tables.** Foreign keys, IDENTITY columns,
  and the user's `Incidents` log make a full rebuild riskier than
  in-place edits for a 1% drift.
- **Not changing CountryID/FieldID values.** They differ by a few units
  in 2008 onward due to the Serbia row, and that's fine — these are
  internal surrogate keys, not exported in any API.
- **Not modifying the webapp.** Webapp reads `data/factbook.db` (SQLite);
  this cleanup doesn't affect production traffic.

## 6. Risk and time

| Risk | Mitigation |
|---|---|
| `sync_sqlite_to_sqlserver.py` could silently UPDATE wrong rows | Use `--dry-run` first; row-count must equal 10,154 |
| FK violation on Step 3 (Serbia DELETE) | Use BEGIN TRANSACTION + ROLLBACK preview |
| IDENTITY_INSERT misuse on Step 4 | Test with ROLLBACK first |
| Disk space during Step 6 SHRINKFILE | Optional; can skip |
| Webapp impact | None — webapp reads SQLite, not SQL Server |

**Estimated wall-clock time, end to end**: 30-60 minutes including the
backup. UPDATEs themselves are ~5 min; the rest is verification.

**Reversibility**: full restore from the Step 0 backup at any point
returns SQL Server to its current pre-cleanup state.

## 7. Greenlight checklist for the user

Before executing, confirm:

- [ ] Step 0 backup file exists and `RESTORE VERIFYONLY` succeeds.
- [ ] `--dry-run` of `sync_sqlite_to_sqlserver.py` reports ~10,154
      updates (within +/- 50).
- [ ] No other agents are mid-way through ETL work that touches
      SQL Server.
- [ ] Reviewer is okay with not migrating `FieldValues` to SQL Server
      (deferring that table).

Once these are checked, Steps 1-7 can be executed in order. Each step
is independently rollback-able via the Step 0 backup; Steps 3 and 4
also have local ROLLBACK previews.
