"""Reproducible re-downloader for the CIA World Factbook Archive raw sources.

Re-fetches every raw input file the ETL ever consumed, from its original
upstream URL, into a local raw-sources/ tree mirroring the GitHub Release
asset layout.

Use this if:
  - the GitHub Release for raw-sources is unavailable
  - you want to verify our bundle byte-for-byte against fresh upstream pulls
  - you only need specific years and don't want the full 3 GB bundle

Usage:
  python fetch_raw_sources.py                   # fetch everything (~3 GB)
  python fetch_raw_sources.py --era html        # only the 2000-2020 HTML zips
  python fetch_raw_sources.py --era text        # only the 1990-2001 plaintext
  python fetch_raw_sources.py --era json        # only the 2021-2025 JSON
  python fetch_raw_sources.py --year 2010       # only one year
  python fetch_raw_sources.py --out /some/dir   # write into a different dir

Sources:
  HTML zips:    Wayback Machine (specific captures, fallback via CDX API)
  Plaintext:    Project Gutenberg (cache/epub/{id}/pg{id}.txt)
  1996 repair:  Wayback Machine (CIA ODCI 1997-05-28 capture)
  JSON:         github.com/factbook/cache.factbook.json (git archive @ commit)
"""
import argparse
import gzip
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import urllib.error
from pathlib import Path

OUT_DEFAULT = Path("raw-sources")

WAYBACK_TIMESTAMPS = {
    2000: "20210115043153", 2001: "20210115043222", 2002: "20210115043238",
    2003: "20210115043307", 2004: "20210115043330", 2005: "20210115043355",
    2006: "20210115043418", 2007: "20210115043445", 2008: "20201028120645",
    2009: "20210115043527", 2010: "20210115043556", 2011: "20210115043622",
    2012: "20201028120347", 2013: "20210115043720", 2014: "20210115043803",
    2015: "20201028121353", 2016: "20210115043915", 2017: "20210115043959",
    2018: "20210115044100", 2019: "20201028120752", 2020: "20210115044405",
}

GUTENBERG = {
    1990: 14, 1991: 25, 1992: 48, 1993: 87, 1994: 180, 1995: 571,
    1996: 27675, 1997: 1662, 1998: 2016, 1999: 27676, 2001: 27638,
}

JSON_COMMITS = {
    2021: "01df1072", 2022: "756fb110", 2023: "e87ac6fc",
    2024: "d5b7d4ca", 2025: "d8115495",
}

CIA_ORIGINAL_URL = (
    "https://web.archive.org/web/19970528151800id_/"
    "http://www.odci.gov:80/cia/publications/nsolo/wfb-96.txt.gz"
)
FACTBOOK_JSON_REPO = "https://github.com/factbook/cache.factbook.json.git"


def http_download(url, dest):
    """Stream-download a URL to dest. Print progress per MB."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "factbook-archive-fetcher/1.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
        print(f"    wrote {downloaded:,} bytes" + (f" / {total:,}" if total else ""))


def fetch_html_year(year, out_dir):
    if year not in WAYBACK_TIMESTAMPS:
        raise ValueError(f"No Wayback timestamp known for year {year}")
    ts = WAYBACK_TIMESTAMPS[year]
    url = (f"https://web.archive.org/web/{ts}id_/"
           f"https://www.cia.gov/the-world-factbook/about/archives/download/factbook-{year}.zip")
    dest = out_dir / "html" / f"factbook-{year}.zip"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  exists, skipping: {dest}")
        return
    http_download(url, dest)


def fetch_text_year(year, out_dir):
    if year not in GUTENBERG:
        raise ValueError(f"No Gutenberg ID known for year {year}")
    gid = GUTENBERG[year]
    url = f"https://www.gutenberg.org/cache/epub/{gid}/pg{gid}.txt"
    dest = out_dir / "text" / f"{year}.txt"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  exists, skipping: {dest}")
        return
    http_download(url, dest)


def fetch_1996_cia_original(out_dir):
    dest = out_dir / "text" / "1996_cia_original.txt"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  exists, skipping: {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        http_download(CIA_ORIGINAL_URL, Path(tmp_path))
        print(f"  decompressing to {dest}")
        with gzip.open(tmp_path, "rb") as gz, open(dest, "wb") as f:
            shutil.copyfileobj(gz, f)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def fetch_json_year(year, out_dir, repo_cache):
    if year not in JSON_COMMITS:
        raise ValueError(f"No commit known for year {year}")
    commit = JSON_COMMITS[year]
    # Ensure local clone exists
    if not (repo_cache / ".git").exists():
        print(f"  cloning {FACTBOOK_JSON_REPO} -> {repo_cache}")
        repo_cache.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", FACTBOOK_JSON_REPO, str(repo_cache)], check=True)
    dest = out_dir / "json" / f"factbook-json-{year}.zip"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  exists, skipping: {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  git archive @ {commit} -> {dest}")
    with open(dest, "wb") as f:
        subprocess.run(
            ["git", "-C", str(repo_cache), "archive", "--format=zip", commit],
            stdout=f, check=True,
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(OUT_DEFAULT), help="output directory (default: raw-sources/)")
    ap.add_argument("--era", choices=["html", "text", "json", "all"], default="all")
    ap.add_argument("--year", type=int, help="fetch only this year")
    args = ap.parse_args()

    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    repo_cache = out.parent / "factbook-json-cache"

    print(f"Output: {out}")
    print(f"Era filter: {args.era}")
    if args.year:
        print(f"Year filter: {args.year}")
    print()

    if args.era in ("all", "html"):
        print("=== HTML zips (Wayback) ===")
        years = [args.year] if args.year else sorted(WAYBACK_TIMESTAMPS)
        for y in years:
            if y in WAYBACK_TIMESTAMPS:
                try:
                    fetch_html_year(y, out)
                except Exception as e:
                    print(f"  ERROR year={y}: {e}", file=sys.stderr)
        print()

    if args.era in ("all", "text"):
        print("=== Plaintext (Project Gutenberg) ===")
        years = [args.year] if args.year else sorted(GUTENBERG)
        for y in years:
            if y in GUTENBERG:
                try:
                    fetch_text_year(y, out)
                except Exception as e:
                    print(f"  ERROR year={y}: {e}", file=sys.stderr)
        if not args.year or args.year == 1996:
            try:
                fetch_1996_cia_original(out)
            except Exception as e:
                print(f"  ERROR 1996 CIA original: {e}", file=sys.stderr)
        print()

    if args.era in ("all", "json"):
        print("=== JSON snapshots (factbook/cache.factbook.json) ===")
        years = [args.year] if args.year else sorted(JSON_COMMITS)
        for y in years:
            if y in JSON_COMMITS:
                try:
                    fetch_json_year(y, out, repo_cache)
                except Exception as e:
                    print(f"  ERROR year={y}: {e}", file=sys.stderr)
        print()

    print("Done. To verify integrity, see raw-sources/README.md.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
