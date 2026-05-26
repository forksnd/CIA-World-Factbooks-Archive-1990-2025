# Raw Source Availability — Handoff Note

> Purpose: hand this file to a Claude/agent running **locally on the Windows
> machine** (the one that originally built the archive). That agent has the
> filesystem access this note's author did not. Everything needed to act is
> below — read it cold, then do the **Task** section.

## Background

The CIA World Factbooks Archive (`CIA-World-Factbooks-Archive-1990-2025`) ships
a fully processed product: `factbook.db` (~662 MB, Release v3.5) plus the SQL
dumps in `data/`. That is the **output** of the ETL pipeline.

In [discussion #30](https://github.com/MilkMp/CIA-World-Factbooks-Archive-1990-2025/discussions/30),
a user (jarofromel) asked for something different: the **original, unprocessed
source files** that were fed into **step 1** of the pipeline — *before* any
parsing. They want them for (a) verifying DB values against the originals
offline, and (b) full-text searching the raw sources locally.

**Key distinction:** `factbook.db` is NOT what they asked for. They want the
raw inputs (HTML zips, plain-text editions, JSON snapshots).

## Why we don't already have these published

- The repo does **not** track raw sources. `.gitignore` excludes them:
  `work/`, `text_samples/`, `html_samples/`, `*.zip`, `samples/`,
  `Website_Archive/`.
- `etl/build_archive.py` downloads each HTML zip into `./work` and then
  **deletes it after parsing to save space** (search "Delete zip to save
  space"). `etl/reload_json_years.py` clones into `./work` as well.
- So the raw files may or may not still exist locally, depending on whether
  those working folders survived after the build.

## Task (for the local Windows agent)

1. **Inventory** whether the raw sources still exist on this machine. Check, in
   and around the repo root and any build/working directories:
   - `work/` (esp. `factbook-YYYY.zip` files and `factbook-json-cache/`)
   - `text_samples/`, `html_samples/`, `samples/`
   - any loose `*.zip`, `*.txt.gz`, or `Website_Archive/`
   - the Gutenberg `.txt` downloads (cached as `pgNNNN.txt` or similar)
2. **Report** which years/sources are present, with file sizes, and which are
   missing.
3. If a frozen snapshot is desired, **assemble** the present raw files into a
   `raw-sources/` folder (organized by year/format) for a potential GitHub
   Release asset. Do NOT commit large binaries into git — use a Release.
4. For anything **missing**, it is re-fetchable from the public URLs in the
   next section. A ready-to-run fetch script can regenerate the whole set.

## Complete raw-source URL list

These are extracted directly from the ETL scripts. All sources are public and
public-domain (US Gov work, repo is CC0).

### HTML zips — Wayback Machine (`etl/build_archive.py`)

Pattern:
`https://web.archive.org/web/{TIMESTAMP}id_/https://www.cia.gov/the-world-factbook/about/archives/download/factbook-{YEAR}.zip`

| Year | Timestamp |
|------|-----------|
| 2000 | 20210115043153 |
| 2002 | 20210115043238 |
| 2003 | 20210115043307 |
| 2004 | 20210115043330 |
| 2005 | 20210115043355 |
| 2006 | 20210115043418 |
| 2007 | 20210115043445 |
| 2008 | 20201028120645 |
| 2009 | 20210115043527 |
| 2010 | 20210115043556 |
| 2011 | 20210115043622 |
| 2012 | 20201028120347 |
| 2013 | 20210115043720 |
| 2014 | 20210115043803 |
| 2015 | 20201028121353 |
| 2016 | 20210115043915 |
| 2017 | 20210115043959 |
| 2018 | 20210115044100 |
| 2019 | 20201028120752 |
| 2020 | 20210115044405 |

> Note: the `id_/` form returns the raw original capture. Wayback can re-resolve
> a timestamp to the nearest capture over time, which is why `build_archive.py`
> has a CDX-API fallback. These URLs are reproducible-ish, not guaranteed
> permanent — another reason to freeze a Release if durability matters.

### Plain text — Project Gutenberg (`etl/load_gutenberg_years.py`)

Pattern: `https://www.gutenberg.org/cache/epub/{ID}/pg{ID}.txt`

| Year | Ebook ID |
|------|----------|
| 1990 | 14 |
| 1991 | 25 |
| 1992 | 48 |
| 1993 | 87 |
| 1994 | 180 |
| 1995 | 571 |
| 1996 | 27675 |
| 1997 | 1662 |
| 1998 | 2016 |
| 1999 | 27676 |
| 2001 | 27638 (text fallback — 2001 HTML zip was corrupted) |

### 1996 CIA original (`etl/repair_1996_truncated.py`)

Used to repair 7 truncated Gutenberg countries (Venezuela, Armenia, Greece,
Luxembourg, Malta, Monaco, Tuvalu):

`https://web.archive.org/web/19970528151800id_/http://www.odci.gov:80/cia/publications/nsolo/wfb-96.txt.gz`

### JSON era 2021-2025 (`etl/reload_json_years.py`)

Not single files — a git repo checked out at year-end commits.

- Repo: `https://github.com/factbook/cache.factbook.json.git`
- Use the **last commit before** each cutoff date:

| Year | Cutoff (use last commit before this) |
|------|--------------------------------------|
| 2021 | 2022-01-01 |
| 2022 | 2023-01-01 |
| 2023 | 2024-01-01 |
| 2024 | 2025-01-01 |
| 2025 | 2026-02-04 (CIA discontinued the Factbook this day) |

## Recommended response to discussion #30

Whether or not local copies survive, the strongest answer to jarofromel is:

1. Point them at the URL list above (the pipeline is a reproducible downloader).
2. Note their actual goals are already served by existing artifacts:
   - **Verification** → the `SourceFragment` column in `FieldValues` shows the
     exact source text slice each parsed value came from.
   - **Local full-text search** → `factbook.db` already ships an FTS5 index.
3. Only if they want a frozen byte-for-byte snapshot, publish a `raw-sources`
   GitHub Release built from step 3 of the Task above.

## Decision points for the human (Milan)

- Do you want to host a frozen `raw-sources` Release, or just give people the
  reproducible URL list / fetch script?
- If hosting: it's gigabytes and must live as a Release asset (not in-repo,
  GitHub caps files at 100 MB).
