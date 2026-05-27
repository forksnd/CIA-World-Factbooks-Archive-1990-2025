"""L3 full ETL rebuild and row-level diff (in-memory).

Strategy: import parser dispatch functions from each ETL script, re-parse every
raw file into in-memory (country, field, content) records, then diff against
factbook.db CountryFields year-by-year.

This validates that the raw files in raw-sources/ are exactly the inputs that
produced CountryFields in factbook.db (which is the SQLite canonical truth
declared in L0).

Outputs L3_REBUILD_REPORT.md.
"""
import importlib.util
import os
import re
import sqlite3
import subprocess
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB = REPO / "data" / "factbook.db"
WORK = REPO / "work"
TEXT = REPO / "samples" / "text_samples"
JSON_REPO = Path(os.environ.get("FACTBOOK_JSON_REPO", str(WORK / "factbook-json-cache")))
REPORT = REPO / "docs" / "L3_REBUILD_REPORT.md"

JSON_COMMITS = {
    2021: "01df1072", 2022: "756fb110", 2023: "e87ac6fc",
    2024: "d5b7d4ca", 2025: "d8115495",
}

# Truncated 1996 countries repaired from 1996_cia_original.txt
COUNTRIES_1996_REPAIR = {
    "Venezuela", "Armenia", "Greece", "Luxembourg",
    "Malta", "Monaco", "Tuvalu",
}


def load_module(name, path):
    """Import a Python file as a module without running its main()."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load the three ETL modules; their top-level code defines functions only
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
# RE-PARSE EACH YEAR FROM RAW INPUTS
# ============================================================

def reparse_gutenberg(year):
    """Returns list of (country_name, field_name, content)."""
    path = TEXT / f"{year}.txt"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        raw = f.read()
    text = gut.strip_pg_wrapper(raw)
    fmt = gut.YEAR_FORMATS[year]
    dispatch = {
        "old": gut.parse_old_format,
        "tagged": gut.parse_tagged_format,
        "asterisk": gut.parse_asterisk_format,
        "atsign": gut.parse_atsign_format,
        "colon": gut.parse_colon_format,
        "atsign_bare": gut.parse_atsign_bare_format,
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
    """Replace truncated 1996 countries with repaired versions from
    1996_cia_original.txt."""
    # Strip out truncated rows for the 7 countries
    keep = [r for r in records if r[0] not in COUNTRIES_1996_REPAIR]
    # Re-parse CIA original for those 7
    repair_mod = load_module("repair_1996_truncated", ETL / "repair_1996_truncated.py")
    path = TEXT / "1996_cia_original.txt"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    entries = repair_mod.parse_cia_original(text)
    added = 0
    for cname in COUNTRIES_1996_REPAIR:
        if cname in entries:
            for section, content in entries[cname]:
                # repair_1996_truncated stores section->joined text;
                # match the DB shape: FieldName=section, Content=text
                keep.append((cname, section[:200], content))
                added += 1
    return keep


def reparse_html(year):
    """Returns list of (country_name, field_name, content) from the HTML zip."""
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
    """Returns list of (country_name, field_name, content) from JSON commit."""
    commit = JSON_COMMITS[year]
    # Get tree of JSON files at that commit
    r = subprocess.run(
        ["git", "-C", str(JSON_REPO), "ls-tree", "-r", "--name-only", commit],
        capture_output=True, text=True, check=True,
    )
    paths = [p for p in r.stdout.splitlines() if p.endswith(".json")]
    records = []
    for p in paths:
        rr = subprocess.run(
            ["git", "-C", str(JSON_REPO), "show", f"{commit}:{p}"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        import json as _json
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
# QUERY EXPECTED RECORDS FROM factbook.db
# ============================================================

def db_records(con, year):
    """Returns list of (country_name, field_name, content) for the given year."""
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.Name, cf.FieldName, cf.Content
        FROM CountryFields cf
        JOIN Countries c ON cf.CountryID = c.CountryID
        WHERE c.Year = ?
        """,
        (year,),
    )
    return cur.fetchall()


# ============================================================
# DIFF
# ============================================================

def diff_records(actual, expected):
    """Return (matches, content_diffs, missing_in_actual, extra_in_actual).
    Match key = (country_name_norm, field_name_norm). Content compared
    after normalization."""
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
                    "reparse": a_vals[0][:200] if a_vals else "",
                    "db": e_vals[0][:200] if e_vals else "",
                })

    missing = e_keys - a_keys  # in DB, not re-parsed
    extra = a_keys - e_keys    # re-parsed, not in DB
    missing_examples = [{"country": k[0], "field": k[1]} for k in list(missing)[:3]]
    extra_examples = [{"country": k[0], "field": k[1]} for k in list(extra)[:3]]

    return {
        "matches": matches,
        "content_diffs": content_diffs,
        "missing_keys": len(missing),
        "extra_keys": len(extra),
        "diff_examples": diff_examples,
        "missing_examples": missing_examples,
        "extra_examples": extra_examples,
    }


# ============================================================
# MAIN
# ============================================================

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT DISTINCT Year, Source FROM Countries ORDER BY Year")
    year_sources = cur.fetchall()

    print("L3 in-memory rebuild + diff")
    print("=" * 90)
    print(f"{'Year':<6}{'Src':<6}{'Reparse':<10}{'DB':<10}{'Matches':<10}{'CntDiff':<10}{'MissDB':<10}{'Extra':<10}{'Time':<8}")
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
            print(f"  unknown source {src} for {year}", file=sys.stderr)
            continue
        expected = db_records(con, year)
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
    print(f"TOTAL: reparse={tot_actual:,} db={tot_db:,} "
          f"matched_rows={tot_match:,} content_diffs={tot_diff:,} "
          f"missing_keys={tot_missing:,} extra_keys={tot_extra:,}")

    # Write report markdown
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("# L3 Rebuild Report\n\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} UTC\n\n")
        f.write(f"## Totals\n\n")
        f.write(f"| Metric | Count |\n|---|---|\n")
        f.write(f"| Re-parsed records | {tot_actual:,} |\n")
        f.write(f"| DB records | {tot_db:,} |\n")
        f.write(f"| Matched (same key, same content) | {tot_match:,} |\n")
        f.write(f"| Content diffs (same key, different content) | {tot_diff:,} |\n")
        f.write(f"| Missing in re-parse (DB has, re-parse doesn't) | {tot_missing:,} |\n")
        f.write(f"| Extra in re-parse (re-parse has, DB doesn't) | {tot_extra:,} |\n\n")

        f.write("## Per-year breakdown\n\n")
        f.write("| Year | Src | Reparse | DB | Matches | ContentDiffs | MissingInDB-cmp | ExtraInReparse |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for r in results:
            f.write(f"| {r['year']} | {r['source']} | {r['actual_n']:,} | {r['db_n']:,} | "
                    f"{r['matches']:,} | {r['content_diffs']:,} | {r['missing_keys']:,} | {r['extra_keys']:,} |\n")

        # Examples of mismatches per year
        f.write("\n## Example diffs (first 3 per year)\n\n")
        for r in results:
            if r["content_diffs"] or r["missing_keys"] or r["extra_keys"]:
                f.write(f"### {r['year']} ({r['source']})\n\n")
                if r["diff_examples"]:
                    f.write("Content diffs:\n\n")
                    for ex in r["diff_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                        f.write(f"  - reparse: `{ex['reparse']!r}`\n")
                        f.write(f"  - db:      `{ex['db']!r}`\n")
                if r["missing_examples"]:
                    f.write("\nMissing from re-parse (in DB but not produced):\n\n")
                    for ex in r["missing_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                if r["extra_examples"]:
                    f.write("\nExtra in re-parse (produced but not in DB):\n\n")
                    for ex in r["extra_examples"]:
                        f.write(f"- `{ex['country']}` / `{ex['field']}`\n")
                f.write("\n")

    print(f"\nReport written: {REPORT}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
