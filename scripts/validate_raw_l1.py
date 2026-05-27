"""L1 sampling validation: 200 random FieldValues rows per year, verify
SourceFragment substring exists in the matching raw file.

Output: per-year pass rate. Run from the archive repo root.
"""
import sqlite3
import zipfile
import random
import subprocess
import re
import html
import sys
import time
from pathlib import Path

random.seed(42)
SAMPLE_N = 200

import os as _os
REPO = Path(__file__).resolve().parent.parent
DB = REPO / "data" / "factbook.db"
WORK = REPO / "work"
TEXT = REPO / "samples" / "text_samples"
JSON_REPO = Path(_os.environ.get("FACTBOOK_JSON_REPO", str(WORK / "factbook-json-cache")))

JSON_COMMITS = {
    2021: "01df1072",
    2022: "756fb110",
    2023: "e87ac6fc",
    2024: "d5b7d4ca",
    2025: "d8115495",
}

WS_RE = re.compile(r"\s+")


def normalize(s):
    return WS_RE.sub(" ", html.unescape(s)).strip()


def sample_year(con, year, n):
    cur = con.cursor()
    cur.execute(
        """
        SELECT fv.ValueID, c.Name, cf.FieldName, fv.SubField, fv.SourceFragment
        FROM FieldValues fv
        JOIN CountryFields cf ON fv.FieldID = cf.FieldID
        JOIN Countries c ON cf.CountryID = c.CountryID
        WHERE c.Year = ?
          AND fv.SourceFragment IS NOT NULL
          AND LENGTH(fv.SourceFragment) >= 5
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (year, n),
    )
    return cur.fetchall()


def load_text_haystack(year):
    p = TEXT / f"{year}.txt"
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        return normalize(f.read())


def load_html_haystack(year):
    z = WORK / f"factbook-{year}.zip"
    parts = []
    with zipfile.ZipFile(z) as zf:
        for name in zf.namelist():
            n = name.lower()
            if n.endswith(".html") or n.endswith(".htm"):
                with zf.open(name) as f:
                    parts.append(f.read().decode("utf-8", errors="replace"))
    return normalize(" ".join(parts))


def load_json_haystack(year):
    commit = JSON_COMMITS[year]
    r = subprocess.run(
        ["git", "-C", str(JSON_REPO), "ls-tree", "-r", "--name-only", commit],
        capture_output=True, text=True, check=True,
    )
    paths = [p for p in r.stdout.splitlines() if p.endswith(".json")]
    parts = []
    for p in paths:
        rr = subprocess.run(
            ["git", "-C", str(JSON_REPO), "show", f"{commit}:{p}"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        parts.append(rr.stdout)
    return normalize(" ".join(parts))


def check_year(con, year, source):
    rows = sample_year(con, year, SAMPLE_N)
    if not rows:
        return {"year": year, "source": source, "samples": 0, "passed": 0, "fail_examples": []}

    t0 = time.time()
    if source == "text":
        haystack = load_text_haystack(year)
    elif source == "html":
        haystack = load_html_haystack(year)
    elif source == "json":
        haystack = load_json_haystack(year)
    else:
        raise ValueError(source)
    load_s = time.time() - t0

    passed = 0
    fails = []
    for vid, cname, fname, sub, frag in rows:
        needle = normalize(frag)
        if len(needle) > 400:
            needle = needle[:400]
        if needle in haystack:
            passed += 1
        else:
            if len(fails) < 3:
                fails.append({
                    "ValueID": vid, "Country": cname, "Field": fname,
                    "Sub": sub, "Frag_first120": needle[:120],
                })

    return {
        "year": year, "source": source, "samples": len(rows),
        "passed": passed, "rate": passed / len(rows),
        "load_s": round(load_s, 1), "fail_examples": fails,
    }


def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT DISTINCT Year, Source FROM Countries ORDER BY Year")
    year_sources = cur.fetchall()

    print(f"L1 validation: {SAMPLE_N} samples per year across {len(year_sources)} years")
    print("=" * 80)
    print(f"{'Year':<6}{'Source':<8}{'Samples':<10}{'Passed':<10}{'Rate':<10}{'Load(s)':<10}")
    print("-" * 80)

    results = []
    for year, source in year_sources:
        r = check_year(con, year, source)
        results.append(r)
        rate = f"{r['rate']*100:.1f}%" if r["samples"] else "N/A"
        print(f"{year:<6}{source:<8}{r['samples']:<10}{r['passed']:<10}{rate:<10}{r.get('load_s', 0):<10}")

    print("-" * 80)
    total_samples = sum(r["samples"] for r in results)
    total_passed = sum(r["passed"] for r in results)
    rate = total_passed / total_samples if total_samples else 0
    print(f"OVERALL: {total_passed}/{total_samples} = {rate*100:.2f}%")
    print()

    # Show fail examples for any year with < 100%
    bad = [r for r in results if r["samples"] and r["passed"] < r["samples"]]
    if bad:
        print("YEARS WITH FAILURES:")
        for r in bad:
            print(f"\n  Year {r['year']} ({r['source']}): {r['passed']}/{r['samples']} passed")
            for f in r["fail_examples"]:
                print(f"    - {f['Country']} / {f['Field']} / sub={f['Sub']}")
                print(f"      needle: {f['Frag_first120']!r}")

    con.close()
    return 0 if rate >= 0.95 else 1


if __name__ == "__main__":
    sys.exit(main())
