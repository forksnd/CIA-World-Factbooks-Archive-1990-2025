"""L3b: same in-memory re-parse as L3, but diff against SQL Server's
CountryFields (instead of SQLite's). Provides independent provenance proof
against the second database.

Reuses the parser-import + repair logic from validate_raw_l3.py.
"""
import importlib.util
import os
import re
import subprocess
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path

import pyodbc

REPO = Path(__file__).resolve().parent.parent
WORK = REPO / "work"
TEXT = REPO / "samples" / "text_samples"
JSON_REPO = Path(os.environ.get("FACTBOOK_JSON_REPO", str(WORK / "factbook-json-cache")))
REPORT = REPO / "docs" / "L3B_REBUILD_REPORT.md"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;DATABASE=CIA_WorldFactbook;"
    "Trusted_Connection=yes;TrustServerCertificate=yes;"
)

JSON_COMMITS = {
    2021: "01df1072", 2022: "756fb110", 2023: "e87ac6fc",
    2024: "d5b7d4ca", 2025: "d8115495",
}

COUNTRIES_1996_REPAIR = {
    "Venezuela", "Armenia", "Greece", "Luxembourg", "Malta", "Monaco", "Tuvalu",
}


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ETL = REPO / "etl"
ba = load_module("build_archive", ETL / "build_archive.py")
gut = load_module("load_gutenberg_years", ETL / "load_gutenberg_years.py")
rj = load_module("reload_json_years", ETL / "reload_json_years.py")

WS_RE = re.compile(r"\s+")


def norm(s):
    if s is None:
        return ""
    return WS_RE.sub(" ", str(s)).strip()


# ============================================================
# Re-parse functions (same as L3)
# ============================================================

def reparse_gutenberg(year):
    path = TEXT / f"{year}.txt"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        raw = f.read()
    text = gut.strip_pg_wrapper(raw)
    fmt = gut.YEAR_FORMATS[year]
    dispatch = {
        "old": gut.parse_old_format, "tagged": gut.parse_tagged_format,
        "asterisk": gut.parse_asterisk_format, "atsign": gut.parse_atsign_format,
        "colon": gut.parse_colon_format, "atsign_bare": gut.parse_atsign_bare_format,
        "equals": gut.parse_equals_format,
    }
    countries = dispatch[fmt](text)
    records = []
    for cname, cats in countries:
        cname = cname[:200]
        for _, fields in cats:
            for fn, content in fields:
                records.append((cname, fn[:200], content))
    return records


def apply_1996_repair(records):
    keep = [r for r in records if r[0] not in COUNTRIES_1996_REPAIR]
    repair_mod = load_module("repair_1996_truncated", ETL / "repair_1996_truncated.py")
    path = TEXT / "1996_cia_original.txt"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    entries = repair_mod.parse_cia_original(text)
    for cname in COUNTRIES_1996_REPAIR:
        if cname in entries:
            for section, content in entries[cname]:
                keep.append((cname, section[:200], content))
    return keep


def reparse_html(year):
    z = WORK / f"factbook-{year}.zip"
    records = []
    with zipfile.ZipFile(z) as zf:
        all_files = zf.namelist()
        geos = sorted([f for f in all_files if "/geos/" in f and f.endswith(".html")])
        seen = set()
        skip = ["template", "print", "summary", "notes", "appendix", "index", "wfb"]
        unique = []
        for g in geos:
            base = os.path.basename(g)
            code = os.path.splitext(base)[0].lower()
            if len(code) > 5:
                continue
            if any(p in code for p in skip):
                continue
            if code in seen:
                continue
            seen.add(code)
            unique.append(g)
        for gf in unique:
            try:
                html = zf.read(gf).decode("utf-8", errors="replace")
                name, categories = ba.parse_country_html(html, year)
                if not name:
                    continue
                name = name[:200]
                for _, fields in categories:
                    for fn, content in fields:
                        records.append((name, fn[:200], content))
            except Exception as e:
                print(f"  HTML {year} {gf}: {e}", file=sys.stderr)
    return records


def reparse_json(year):
    commit = JSON_COMMITS[year]
    r = subprocess.run(
        ["git", "-C", str(JSON_REPO), "ls-tree", "-r", "--name-only", commit],
        capture_output=True, text=True, check=True,
    )
    paths = [p for p in r.stdout.splitlines() if p.endswith(".json")]
    records = []
    import json as _json
    for p in paths:
        rr = subprocess.run(
            ["git", "-C", str(JSON_REPO), "show", f"{commit}:{p}"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        try:
            data = _json.loads(rr.stdout)
        except Exception:
            continue
        name = (data.get("name") or "Unknown")[:200]
        for cat in data.get("categories", []):
            for field in cat.get("fields", []):
                content = rj.strip_html(field.get("content", field.get("value", "")))
                fname = (field.get("name") or "")[:200]
                records.append((name, fname, content))
    return records


# ============================================================
# Query SQL Server
# ============================================================

def sql_records(conn, year):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.Name, cf.FieldName, cf.Content
        FROM CountryFields cf
        JOIN Countries c ON cf.CountryID = c.CountryID
        WHERE c.Year = ?
        """,
        year,
    )
    return cur.fetchall()


# ============================================================
# Diff (same shape as L3)
# ============================================================

def diff_records(actual, expected):
    def keymap(records):
        d = defaultdict(list)
        for c, fn, content in records:
            d[(norm(c), norm(fn))].append(norm(content))
        return d

    a = keymap(actual)
    e = keymap(expected)
    a_keys = set(a)
    e_keys = set(e)

    matches = 0
    content_diffs = 0
    diff_examples = []
    for k in a_keys & e_keys:
        a_vals = sorted(a[k])
        e_vals = sorted(e[k])
        if a_vals == e_vals:
            matches += len(a_vals)
        else:
            content_diffs += max(len(a_vals), len(e_vals))
            if len(diff_examples) < 3:
                diff_examples.append({
                    "country": k[0], "field": k[1],
                    "reparse": (a_vals[0] if a_vals else "")[:200],
                    "db": (e_vals[0] if e_vals else "")[:200],
                })

    missing = e_keys - a_keys
    extra = a_keys - e_keys
    return {
        "matches": matches,
        "content_diffs": content_diffs,
        "missing_keys": len(missing),
        "extra_keys": len(extra),
        "diff_examples": diff_examples,
        "missing_examples": [{"country": k[0], "field": k[1]} for k in list(missing)[:3]],
        "extra_examples": [{"country": k[0], "field": k[1]} for k in list(extra)[:3]],
    }


def main():
    conn = pyodbc.connect(CONN_STR, timeout=10)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT Year, Source FROM Countries ORDER BY Year")
    year_sources = cur.fetchall()

    print("L3b in-memory re-parse vs SQL Server")
    print("=" * 90)
    print(f"{'Year':<6}{'Src':<6}{'Reparse':<10}{'SQL':<10}{'Matches':<10}{'CntDiff':<10}{'MissDB':<10}{'Extra':<10}{'Time':<8}")
    print("-" * 90)

    results = []
    for year, src in year_sources:
        t0 = time.time()
        if src == "text":
            actual = reparse_gutenberg(year)
            if year == 1996:
                actual = apply_1996_repair(actual)
        elif src == "html":
            actual = reparse_html(year)
        elif src == "json":
            actual = reparse_json(year)
        else:
            continue
        expected = sql_records(conn, year)
        dt = time.time() - t0
        d = diff_records(actual, expected)
        results.append({"year": year, "source": src,
                        "actual_n": len(actual), "db_n": len(expected),
                        "elapsed_s": round(dt, 1), **d})
        print(f"{year:<6}{src:<6}{len(actual):<10}{len(expected):<10}"
              f"{d['matches']:<10}{d['content_diffs']:<10}{d['missing_keys']:<10}{d['extra_keys']:<10}{dt:<8.1f}")

    print("-" * 90)
    tot_actual = sum(r["actual_n"] for r in results)
    tot_db = sum(r["db_n"] for r in results)
    tot_match = sum(r["matches"] for r in results)
    tot_diff = sum(r["content_diffs"] for r in results)
    tot_missing = sum(r["missing_keys"] for r in results)
    tot_extra = sum(r["extra_keys"] for r in results)
    print(f"TOTAL: reparse={tot_actual:,} sql={tot_db:,} matched_rows={tot_match:,} "
          f"content_diffs={tot_diff:,} missing_keys={tot_missing:,} extra_keys={tot_extra:,}")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("# L3b Rebuild Report (re-parse vs SQL Server)\n\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} UTC\n\n")
        f.write("## Totals\n\n")
        f.write("| Metric | Count |\n|---|---|\n")
        f.write(f"| Re-parsed records | {tot_actual:,} |\n")
        f.write(f"| SQL Server records | {tot_db:,} |\n")
        f.write(f"| Matched (same key, same content) | {tot_match:,} |\n")
        f.write(f"| Content diffs (same key, different content) | {tot_diff:,} |\n")
        f.write(f"| Missing in re-parse (SQL has, re-parse doesn't) | {tot_missing:,} |\n")
        f.write(f"| Extra in re-parse (re-parse has, SQL doesn't) | {tot_extra:,} |\n\n")
        f.write("## Per-year breakdown\n\n")
        f.write("| Year | Src | Reparse | SQL | Matches | ContentDiffs | MissingFromReparse | ExtraInReparse |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for r in results:
            f.write(f"| {r['year']} | {r['source']} | {r['actual_n']:,} | {r['db_n']:,} | "
                    f"{r['matches']:,} | {r['content_diffs']:,} | {r['missing_keys']:,} | {r['extra_keys']:,} |\n")
        f.write("\n## Example diffs (first 3 per year with mismatches)\n\n")
        for r in results:
            if r["content_diffs"] or r["missing_keys"] or r["extra_keys"]:
                f.write(f"### {r['year']} ({r['source']})\n\n")
                if r["diff_examples"]:
                    f.write("Content diffs:\n\n")
                    for ex in r["diff_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                        f.write(f"  - reparse: `{ex['reparse']!r}`\n")
                        f.write(f"  - sql:     `{ex['db']!r}`\n")
                if r["missing_examples"]:
                    f.write("\nMissing from re-parse:\n\n")
                    for ex in r["missing_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                if r["extra_examples"]:
                    f.write("\nExtra in re-parse:\n\n")
                    for ex in r["extra_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                f.write("\n")

    print(f"\nReport written: {REPORT}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
