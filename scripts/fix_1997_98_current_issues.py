#!/usr/bin/env python3
"""Fix the 1997/98 'Current issues' off-by-one misattribution in the dumps.

The 1997 and 1998 text-edition parser attributed each country's entry-header
"Current issues" note to the country immediately preceding it in the file
(owner CountryID = host CountryID + 1 in every case), e.g. Israel's occupied-
territories disclaimer was stored under Ireland. The 1998 parser additionally
appended the book's entire back-matter (Notes and Definitions glossary,
Appendices E-H, CIA contact info) to Zimbabwe, the last entry.

This script edits the published dumps in place:
  - data/fields/country_fields_1997.sql.gz : re-home 15 notes
  - data/fields/country_fields_1998.sql.gz : re-home 16 notes, delete the
    176 appendix rows (FieldID 1626049-1626224) from Zimbabwe
  - data/categories.sql : add 29 'Introduction' categories for owners that
    lacked one (the notes' correct home)

Idempotent: running on already-fixed dumps makes no changes.
See also scripts/patch_live_db_1997_98_notes.py for the deployed-DB patch.
"""
import gzip
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIELDS = ROOT / "data" / "fields"
CATEGORIES_SQL = ROOT / "data" / "categories.sql"

# (FieldID, host CountryID, host CategoryID, owner CountryID, owner CategoryID)
MOVES_1997 = [
    (2205079, 20670, 364030, 20671, 375556),  # Andorra -> Angola
    (2205514, 20676, 364077, 20677, 375557),  # Argentina -> Armenia
    (2205956, 20682, 364123, 20683, 375558),  # Austria -> Azerbaijan
    (2207186, 20696, 364234, 20697, 375559),  # Bolivia -> Bosnia and Herzegovina
    (2208041, 20706, 364315, 20707, 375560),  # Burma -> Burundi
    (2208619, 20712, 364364, 20713, 364373),  # Cayman Is. -> Central African Republic
    (2212316, 20753, 364696, 20754, 364705),  # Gambia, The -> Gaza Strip
    (2212391, 20754, 364705, 20755, 375561),  # Gaza Strip -> Georgia
    (2214864, 20783, 364933, 20784, 375562),  # Ireland -> Israel
    (2216497, 20803, 365091, 20804, 375563),  # Latvia -> Lebanon
    (2216687, 20805, 365108, 20806, 375564),  # Lesotho -> Liberia
    (2222242, 20867, 365601, 20868, 375565),  # Russia -> Rwanda
    (2223167, 20877, 365682, 20878, 375566),  # Senegal -> Serbia and Montenegro
    (2224959, 20897, 365843, 20898, 375567),  # Syria -> Tajikistan
    (2227321, 20923, 366052, 20924, 375568),  # Wallis and Futuna -> West Bank
]
MOVES_1998 = [
    (1603172, 14319, 317263, 14320, 375569),  # Andorra -> Angola
    (1603609, 14325, 317303, 14326, 375570),  # Argentina -> Armenia
    (1604049, 14331, 317343, 14332, 375571),  # Austria -> Azerbaijan
    (1605279, 14345, 317441, 14346, 375572),  # Bolivia -> Bosnia and Herzegovina
    (1606136, 14355, 317511, 14356, 375573),  # Burma -> Burundi
    (1606712, 14361, 317553, 14362, 375574),  # Cayman Is. -> Central African Republic
    (1610401, 14402, 317840, 14403, 375575),  # Gambia, The -> Gaza Strip
    (1610475, 14403, 317847, 14404, 375576),  # Gaza Strip -> Georgia
    (1612119, 14422, 317980, 14423, 375577),  # Honduras -> Hong Kong
    (1612953, 14432, 318048, 14433, 375578),  # Ireland -> Israel
    (1614576, 14452, 318188, 14453, 375579),  # Latvia -> Lebanon
    (1614766, 14454, 318202, 14455, 375580),  # Lesotho -> Liberia
    (1621325, 14527, 318710, 14528, 375581),  # Senegal -> Serbia and Montenegro
    (1621518, 14529, 318724, 14530, 375582),  # Seychelles -> Sierra Leone
    (1623201, 14548, 318857, 14549, 375583),  # Taiwan -> Tajikistan
    (1625549, 14574, 319039, 14575, 375584),  # Wallis and Futuna -> West Bank
]
# Zimbabwe 1998 appendix/back-matter rows (glossary defs, appendices, contacts)
DELETE_1998 = range(1626049, 1626224 + 1)

# New 'Introduction' categories for owners that had none.
NEW_CATEGORIES = [
    (375556, 20671), (375557, 20677), (375558, 20683), (375559, 20697),
    (375560, 20707), (375561, 20755), (375562, 20784), (375563, 20804),
    (375564, 20806), (375565, 20868), (375566, 20878), (375567, 20898),
    (375568, 20924),
    (375569, 14320), (375570, 14326), (375571, 14332), (375572, 14346),
    (375573, 14356), (375574, 14362), (375575, 14403), (375576, 14404),
    (375577, 14423), (375578, 14433), (375579, 14453), (375580, 14455),
    (375581, 14528), (375582, 14530), (375583, 14549), (375584, 14575),
]

ROW_RE = re.compile(r"^  \((\d+), (\d+), (\d+), ")
BATCH = 1000
INSERT_LINE = ("INSERT INTO CountryFields "
               "(FieldID, CategoryID, CountryID, FieldName, Content)")


def rewrite_fields_dump(year, moves, delete_ids=()):
    path = FIELDS / f"country_fields_{year}.sql.gz"
    move_by_id = {m[0]: m for m in moves}
    delete_ids = set(delete_ids)
    rows, moved, deleted, already = [], 0, 0, 0

    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            m = ROW_RE.match(line)
            if not m:
                continue  # structural line; batches are rebuilt below
            fid, cat, cid = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if fid in delete_ids:
                deleted += 1
                continue
            if fid in move_by_id:
                _, host, host_cat, owner, owner_cat = move_by_id[fid]
                if (cat, cid) == (owner_cat, owner):
                    already += 1
                elif (cat, cid) == (host_cat, host):
                    line = ROW_RE.sub(f"  ({fid}, {owner_cat}, {owner}, ", line)
                    moved += 1
                else:
                    sys.exit(f"{year}: FieldID {fid} has unexpected "
                             f"(CategoryID, CountryID)=({cat}, {cid})")
            body = line.rstrip("\n").rstrip()
            rows.append(body[:-1] if body.endswith((",", ";")) else body)

    if moved == 0 and deleted == 0 and already == len(moves):
        print(f"{year}: already fixed ({already} notes in place) — no changes")
        return
    if moved + already != len(moves):
        sys.exit(f"{year}: expected {len(moves)} note rows, "
                 f"matched {moved + already}")
    if delete_ids and deleted not in (0, len(delete_ids)):
        sys.exit(f"{year}: expected {len(delete_ids)} appendix rows, "
                 f"deleted {deleted}")

    out = [f"-- CountryFields for year {year}: {len(rows):,} rows",
           "-- Exported from CIA_WorldFactbook archive", "",
           "SET IDENTITY_INSERT CountryFields ON;", "GO", ""]
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        out.append(INSERT_LINE)
        out.append("VALUES")
        out.extend(f"{r}," for r in chunk[:-1])
        out.append(f"{chunk[-1]};")
        out.append("GO")
        out.append("")
    out.append("SET IDENTITY_INSERT CountryFields OFF;")
    out.append("GO")
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(out) + "\n")
    print(f"{year}: moved {moved} notes, deleted {deleted} appendix rows, "
          f"{len(rows):,} rows total")


def add_categories():
    text = CATEGORIES_SQL.read_text(encoding="utf-8")
    todo = [(cat, cid) for cat, cid in NEW_CATEGORIES
            if f"({cat}, {cid}, " not in text]
    if not todo:
        print("categories.sql: already fixed — no changes")
        return
    if len(todo) != len(NEW_CATEGORIES):
        sys.exit("categories.sql: partially applied — refusing to guess")
    rows = ",\n".join(f"  ({cat}, {cid}, N'Introduction')" for cat, cid in todo)
    block = ("INSERT INTO CountryCategories (CategoryID, CountryID, CategoryTitle)\n"
             f"VALUES\n{rows};\nGO\n\n")
    marker = "SET IDENTITY_INSERT CountryCategories OFF;"
    if marker not in text:
        sys.exit("categories.sql: terminator not found")
    text = text.replace(marker, block + marker)
    n = int(re.search(r"-- CountryCategories: ([\d,]+) rows", text)
            .group(1).replace(",", ""))
    text = re.sub(r"-- CountryCategories: [\d,]+ rows",
                  f"-- CountryCategories: {n + len(todo):,} rows", text, count=1)
    CATEGORIES_SQL.write_text(text, encoding="utf-8")
    print(f"categories.sql: added {len(todo)} Introduction categories")


if __name__ == "__main__":
    rewrite_fields_dump(1997, MOVES_1997)
    rewrite_fields_dump(1998, MOVES_1998, DELETE_1998)
    add_categories()
    print("done")
