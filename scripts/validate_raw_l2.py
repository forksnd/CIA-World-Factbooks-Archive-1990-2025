"""L2 hash baseline: SHA256 every raw file destined for the Release.

For HTML zips and Gutenberg .txt files, hash the existing files in place.
For JSON 2021-2025, build year snapshot zips via `git archive` into a staging
directory, then hash the resulting zips. The same zips will be reused for the
Release upload.

Writes results to docs/RAW_SOURCES_MANIFEST.json (working manifest).
"""
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

import os as _os
REPO = Path(__file__).resolve().parent.parent
WORK = REPO / "work"
TEXT = REPO / "samples" / "text_samples"
JSON_REPO = Path(_os.environ.get("FACTBOOK_JSON_REPO", str(WORK / "factbook-json-cache")))
STAGE = REPO / "raw-sources-staging"  # gitignored; intentional separate path
MANIFEST = REPO / "docs" / "RAW_SOURCES_MANIFEST.json"

JSON_COMMITS = {
    2021: "01df1072",
    2022: "756fb110",
    2023: "e87ac6fc",
    2024: "d5b7d4ca",
    2025: "d8115495",
}

# Wayback timestamps from etl/build_archive.py
WAYBACK_TS = {
    2000: "20210115043153", 2002: "20210115043238", 2003: "20210115043307",
    2004: "20210115043330", 2005: "20210115043355", 2006: "20210115043418",
    2007: "20210115043445", 2008: "20201028120645", 2009: "20210115043527",
    2010: "20210115043556", 2011: "20210115043622", 2012: "20201028120347",
    2013: "20210115043720", 2014: "20210115043803", 2015: "20201028121353",
    2016: "20210115043915", 2017: "20210115043959", 2018: "20210115044100",
    2019: "20201028120752", 2020: "20210115044405", 2001: "20210115044405",
}

# Gutenberg ebook IDs
GUTENBERG = {
    1990: 14, 1991: 25, 1992: 48, 1993: 87, 1994: 180, 1995: 571,
    1996: 27675, 1997: 1662, 1998: 2016, 1999: 27676, 2001: 27638,
}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_html_zips():
    entries = []
    for year in sorted(WAYBACK_TS):
        if year == 2001:
            # 2001 HTML zip was corrupted; recorded but flagged unused
            zip_path = WORK / "factbook-2001.zip"
        else:
            zip_path = WORK / f"factbook-{year}.zip"
        if not zip_path.exists():
            print(f"  MISSING: {zip_path}", file=sys.stderr)
            continue
        t0 = time.time()
        digest = sha256(zip_path)
        size = zip_path.stat().st_size
        dt = time.time() - t0
        ts = WAYBACK_TS[year]
        url = f"https://web.archive.org/web/{ts}id_/https://www.cia.gov/the-world-factbook/about/archives/download/factbook-{year}.zip"
        entry = {
            "year": year,
            "era": "html",
            "filename": f"factbook-{year}.zip",
            "size_bytes": size,
            "sha256": digest,
            "upstream_url": url,
            "parser_script": "etl/build_archive.py",
            "produced_db_rows": (year != 2001),
            "notes": "Downloaded but corrupted; Gutenberg .txt fallback was used instead." if year == 2001 else "",
        }
        entries.append(entry)
        print(f"  {entry['filename']}  {size/1e6:6.1f} MB  {digest[:16]}...  ({dt:.1f}s)")
    return entries


def hash_text():
    entries = []
    for year in sorted(GUTENBERG):
        path = TEXT / f"{year}.txt"
        if not path.exists():
            print(f"  MISSING: {path}", file=sys.stderr)
            continue
        digest = sha256(path)
        size = path.stat().st_size
        gid = GUTENBERG[year]
        url = f"https://www.gutenberg.org/cache/epub/{gid}/pg{gid}.txt"
        entries.append({
            "year": year,
            "era": "text",
            "filename": f"{year}.txt",
            "size_bytes": size,
            "sha256": digest,
            "upstream_url": url,
            "parser_script": "etl/load_gutenberg_years.py",
            "produced_db_rows": True,
            "notes": "Used as 2001 source because the 2001 HTML zip was corrupted." if year == 2001 else "",
        })
        print(f"  {entries[-1]['filename']}  {size/1e6:6.1f} MB  {digest[:16]}...")
    # Add 1996 CIA original (used by repair_1996_truncated.py)
    p1996 = TEXT / "1996_cia_original.txt"
    if p1996.exists():
        digest = sha256(p1996)
        size = p1996.stat().st_size
        entries.append({
            "year": 1996,
            "era": "text-repair",
            "filename": "1996_cia_original.txt",
            "size_bytes": size,
            "sha256": digest,
            "upstream_url": "https://web.archive.org/web/19970528151800id_/http://www.odci.gov:80/cia/publications/nsolo/wfb-96.txt.gz",
            "parser_script": "etl/repair_1996_truncated.py",
            "produced_db_rows": True,
            "notes": "Wayback ODCI 1997-05-28 capture; repairs 7 truncated countries in 1996 Gutenberg edition.",
        })
        print(f"  1996_cia_original.txt  {size/1e6:6.1f} MB  {digest[:16]}...")
    return entries


def build_and_hash_json():
    STAGE.mkdir(parents=True, exist_ok=True)
    entries = []
    for year, commit in JSON_COMMITS.items():
        out = STAGE / f"factbook-json-{year}.zip"
        # Build snapshot via git archive
        print(f"  building factbook-json-{year}.zip from commit {commit}...", flush=True)
        t0 = time.time()
        with open(out, "wb") as f:
            subprocess.run(
                ["git", "-C", str(JSON_REPO), "archive", "--format=zip", commit],
                stdout=f, check=True,
            )
        digest = sha256(out)
        size = out.stat().st_size
        # Full commit hash for the manifest
        full = subprocess.run(
            ["git", "-C", str(JSON_REPO), "rev-parse", commit],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        commit_date = subprocess.run(
            ["git", "-C", str(JSON_REPO), "log", "-1", "--format=%ad", "--date=short", commit],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        dt = time.time() - t0
        entries.append({
            "year": year,
            "era": "json",
            "filename": f"factbook-json-{year}.zip",
            "size_bytes": size,
            "sha256": digest,
            "upstream_repo": "https://github.com/factbook/cache.factbook.json",
            "upstream_commit": full,
            "upstream_commit_short": commit,
            "upstream_commit_date": commit_date,
            "parser_script": "etl/reload_json_years.py",
            "produced_db_rows": True,
            "notes": f"git archive --format=zip {commit}",
        })
        print(f"    {size/1e6:6.1f} MB  {digest[:16]}...  ({dt:.1f}s)")
    return entries


def main():
    print("L2 hash baseline starting...")
    print(f"  output: {MANIFEST}")
    print()
    print("=== HTML zips (work/) ===")
    html_entries = hash_html_zips()
    print()
    print("=== Plaintext (samples/text_samples/) ===")
    text_entries = hash_text()
    print()
    print("=== JSON snapshots (built from factbook-json-cache commits) ===")
    json_entries = build_and_hash_json()
    print()

    all_entries = html_entries + text_entries + json_entries
    total_bytes = sum(e["size_bytes"] for e in all_entries)
    manifest = {
        "version": "raw-sources-v1-draft",
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "canonical_db": "data/factbook.db (SQLite)",
        "total_files": len(all_entries),
        "total_bytes": total_bytes,
        "files": all_entries,
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"=== SUMMARY ===")
    print(f"  files: {len(all_entries)}")
    print(f"  total: {total_bytes/1e9:.2f} GB")
    print(f"  manifest written: {MANIFEST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
