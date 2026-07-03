#!/usr/bin/env python3
"""Patch a deployed factbook.db for the 1997/98 'Current issues' bug.

Fixes, in place, the three defects documented in
scripts/fix_1997_98_current_issues.py (which repairs the published dumps):
  1. Re-homes each 1997/98 entry-header note from the alphabetically
     preceding country to its true owner (e.g. Israel's occupied-territories
     disclaimer off of Ireland).
  2. Deletes the 1998 book back-matter (glossary/appendices/contact info)
     that was appended to Zimbabwe.
  3. Stops the 'Current issues' field name from mapping to
     'Environment - current issues', which surfaced the note on the
     Environment field-history pages.

Run on the Fly machine:   python3 patch_live_db_1997_98_notes.py /data/factbook.db
Then clear the webapp cache (/admin/clear-cache) or restart the machine.

Idempotent: matches by (year, country code, note text), not row IDs, and
skips anything already fixed.
"""
import sqlite3
import sys

# (year, host code, owner code, distinctive start of the note text).
# Prefixes are matched against newline-normalized content and kept short
# enough to precede the first hard line wrap of the 1997/98 text files.
MOVES = [
    (1997, "an", "ao", "Civil war has been the norm since"),
    (1997, "ar", "am", "Armenia"),
    (1997, "au", "aj", "Azerbaijan continues to be plagued"),
    (1997, "bl", "bk", "On 21 November 1995, in Dayton"),
    (1997, "bm", "by", "in a number of waves since October 1993"),
    (1997, "cj", "ct", "in 1996 the Central African Republic"),
    (1997, "ga", "gz", "The Israel-PLO Declaration of Principles"),
    (1997, "gz", "gg", "Beset by ethnic and civil strife"),
    (1997, "ei", "is", "The territories occupied by Israel"),
    (1997, "lg", "le", "Lebanon has made progress toward"),
    (1997, "lt", "li", "Years of civil strife have destroyed"),
    (1997, "rs", "rw", "following the outbreak of genocidal"),
    (1997, "sg", "yi", "Serbia and Montenegro have asserted"),
    (1997, "sy", "ti", "Tajikistan has experienced three"),
    (1997, "wf", "we", "The Israel-PLO Declaration of Principles"),
    (1998, "an", "ao", "Civil war has been the norm since"),
    (1998, "ar", "am", "Armenia"),
    (1998, "au", "aj", "Azerbaijan continues to be plagued"),
    (1998, "bl", "bk", "On 21 November 1995, in Dayton"),
    (1998, "bm", "by", "in a number of waves since October 1993"),
    (1998, "cj", "ct", "In 1996, the Central African Republic"),
    (1998, "ga", "gz", "The Israel-PLO Declaration of Principles"),
    (1998, "gz", "gg", "Beset by ethnic and civil strife"),
    (1998, "ho", "hk", "Pursuant to the agreement signed by China"),
    (1998, "ei", "is", "The territories occupied by Israel"),
    (1998, "lg", "le", "Lebanon has made progress toward"),
    (1998, "lt", "li", "The Abuja Peace Accords ended seven"),
    (1998, "sg", "yi", "Serbia and Montenegro have asserted"),
    (1998, "se", "sl", "On 25 May 1997, the democratically-elected"),
    (1998, "tw", "ti", "Tajikistan has experienced three"),
    (1998, "wf", "we", "The Israel-PLO Declaration of Principles"),
]
# Zimbabwe 1998: everything in its Transnational Issues category except its
# two real fields is leaked book back-matter.
ZIM_REAL_FIELDS = ("Disputes-international", "Illicit drugs")

# Newline-normalized content: \r stripped, \n -> space, doubled spaces
# collapsed, so prefixes match regardless of where the source hard-wrapped.
NORM = "replace(replace(replace(Content, char(13), ''), char(10), ' '), '  ', ' ')"


def country_id(cur, year, code):
    row = cur.execute(
        "SELECT CountryID FROM Countries WHERE Year = ? AND Code = ?",
        (year, code)).fetchone()
    if not row:
        sys.exit(f"no Countries row for year={year} code={code}")
    return row[0]


def intro_category(cur, cid):
    row = cur.execute(
        "SELECT CategoryID FROM CountryCategories "
        "WHERE CountryID = ? AND CategoryTitle = 'Introduction'",
        (cid,)).fetchone()
    if row:
        return row[0]
    new_id = cur.execute(
        "SELECT MAX(CategoryID) + 1 FROM CountryCategories").fetchone()[0]
    cur.execute(
        "INSERT INTO CountryCategories (CategoryID, CountryID, CategoryTitle) "
        "VALUES (?, ?, 'Introduction')", (new_id, cid))
    return new_id


def main(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    moved = skipped = missing = 0

    for year, host_code, owner_code, prefix in MOVES:
        host = country_id(cur, year, host_code)
        owner = country_id(cur, year, owner_code)
        like = prefix.replace("'", "''") + "%"
        rows = cur.execute(
            f"SELECT FieldID FROM CountryFields "
            f"WHERE CountryID = ? AND {NORM} LIKE ?", (host, like)).fetchall()
        if not rows:
            already = cur.execute(
                f"SELECT 1 FROM CountryFields "
                f"WHERE CountryID = ? AND {NORM} LIKE ?",
                (owner, like)).fetchone()
            if already:
                skipped += 1
            else:
                print(f"WARNING {year} {host_code}->{owner_code}: "
                      f"note not found on either country")
                missing += 1
            continue
        if len(rows) > 1:
            sys.exit(f"{year} {host_code}: prefix matched {len(rows)} rows "
                     f"— refusing to guess")
        cat = intro_category(cur, owner)
        cur.execute(
            "UPDATE CountryFields SET CountryID = ?, CategoryID = ?, "
            "FieldName = 'Current issues' WHERE FieldID = ?",
            (owner, cat, rows[0][0]))
        moved += 1

    zim = country_id(cur, 1998, "zi")
    ph = ",".join("?" for _ in ZIM_REAL_FIELDS)
    deleted = cur.execute(
        f"DELETE FROM CountryFields WHERE CountryID = ? AND CategoryID IN "
        f"(SELECT CategoryID FROM CountryCategories WHERE CountryID = ? "
        f" AND CategoryTitle = 'Transnational Issues') "
        f"AND FieldName NOT IN ({ph})",
        (zim, zim, *ZIM_REAL_FIELDS)).rowcount

    remapped = cur.execute(
        "UPDATE FieldNameMappings SET CanonicalName = 'Current issues' "
        "WHERE OriginalName = 'Current issues' "
        "AND CanonicalName LIKE 'Environment%'").rowcount

    conn.commit()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.OperationalError:
        pass
    conn.close()
    print(f"moved {moved} notes ({skipped} already fixed, {missing} missing), "
          f"deleted {deleted} Zimbabwe appendix rows, "
          f"fixed {remapped} field-name mappings")
    if missing:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/data/factbook.db")
