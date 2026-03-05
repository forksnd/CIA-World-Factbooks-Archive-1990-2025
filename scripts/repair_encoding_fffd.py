"""
Repair remaining U+FFFD (replacement character) encoding corruption in factbook.db.

These 37 fields across 2006-2017 have accented Latin characters (í, á, é, etc.)
that were corrupted during HTML parsing. This script replaces them with the correct
Unicode characters based on context and verified against CIA original sources.

Usage:
    python scripts/repair_encoding_fffd.py          # dry run
    python scripts/repair_encoding_fffd.py --apply  # apply to factbook.db
"""

import argparse
import sqlite3
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "factbook.db")
FVDB_PATH = os.path.join(PROJECT_ROOT, "data", "factbook_field_values.db")

FFFD = "\ufffd"

# ── Repair map: (FieldID, [(old_substring, new_substring), ...]) ──
# Each repair is a targeted substring replacement within one field.
# Context verified against CIA Factbook originals and adjacent clean years.

REPAIRS = {
    # 2006 Chile — Economy overview: "5½-year high"
    144986: [(f"5{FFFD}-year", "5\u00bd-year")],

    # 2006 Costa Rica — Disputes: "Río San Juan"
    145876: [(f"R{FFFD}o San Juan", "R\u00edo San Juan")],

    # 2007 Costa Rica — Disputes: "Río San Juan"
    174941: [(f"R{FFFD}o San Juan", "R\u00edo San Juan")],

    # 2008 Benin — Political parties: "Lazare SÉHOUÉTO"
    201357: [(f"S{FFFD}HOU{FFFD}TO", "S\u00c9HOU\u00c9TO")],

    # 2008 Bolivia — Disputes: Isla Suárez, Guajará-Mirim, Río Mamoré
    201164: [
        (f"Isla Su{FFFD}rez", "Isla Su\u00e1rez"),
        (f"Guajar{FFFD}-Mirim", "Guajar\u00e1-Mirim"),
        (f"R{FFFD}o Mamor{FFFD}", "R\u00edo Mamor\u00e9"),
    ],

    # 2008 Brazil — Disputes: Itaipú, Isla Suárez, Guajará-Mirim, Río Mamoré
    201853: [
        (f"Itaip{FFFD} Dam", "Itaip\u00fa Dam"),
        (f"Isla Su{FFFD}rez", "Isla Su\u00e1rez"),
        (f"Guajar{FFFD}-Mirim", "Guajar\u00e1-Mirim"),
        (f"R{FFFD}o Mamor{FFFD}", "R\u00edo Mamor\u00e9"),
    ],

    # 2008 Colombia — Disputes: "82°W meridian"
    204083: [(f"82{FFFD}W", "82\u00b0W")],

    # 2008 Costa Rica — Disputes: "Río San Juan"
    204351: [(f"R{FFFD}o San Juan", "R\u00edo San Juan")],

    # 2009 Benin — Political parties: "Lazare SÉHOUÉTO"
    232130: [(f"S{FFFD}HOU{FFFD}TO", "S\u00c9HOU\u00c9TO")],

    # 2009 DRC — Political groups: "Forces Armées de la République Démocratique"
    233921: [
        (f"Arm{FFFD}es", "Arm\u00e9es"),
        (f"R{FFFD}publique", "R\u00e9publique"),
        (f"D{FFFD}mocratique", "D\u00e9mocratique"),
    ],

    # 2009 Costa Rica — Disputes: "Río San Juan"
    235137: [(f"R{FFFD}o San Juan", "R\u00edo San Juan")],

    # 2010 Benin — Political parties: "Lazare SÉHOUÉTO"
    262924: [(f"S{FFFD}HOU{FFFD}TO", "S\u00c9HOU\u00c9TO")],

    # 2010 DRC — Political groups: "Forces Armées de la République Démocratique"
    264785: [
        (f"Arm{FFFD}es", "Arm\u00e9es"),
        (f"R{FFFD}publique", "R\u00e9publique"),
        (f"D{FFFD}mocratique", "D\u00e9mocratique"),
    ],

    # 2010 Nepal — Background: em dashes around "due in May 2011"
    279669: [
        (f"constitution {FFFD} due in May 2011 {FFFD} and",
         "constitution \u2014 due in May 2011 \u2014 and"),
    ],

    # 2010 Paraguay — Waterways: "Paraná river"
    280579: [(f"Paran{FFFD} river", "Paran\u00e1 river")],

    # 2015 Benin — Political parties: "Lazare SÉHOUÉTO"
    436726: [(f"S{FFFD}HOU{FFFD}TO", "S\u00c9HOU\u00c9TO")],

    # 2015 DRC — Political groups: "Forces Armées de la République Démocratique"
    438989: [
        (f"Arm{FFFD}es", "Arm\u00e9es"),
        (f"R{FFFD}publique", "R\u00e9publique"),
        (f"D{FFFD}mocratique", "D\u00e9mocratique"),
    ],

    # 2015 EU — Executive branch: "EC's external"
    442098: [(f"EC{FFFD}s external", "EC\u2019s external")],

    # 2015 Lesotho — Political groups: "Tsebo MATŠASA"
    452236: [(f"MAT{FFFD}ASA", "MAT\u0160ASA")],

    # 2015 Lithuania — Administrative divisions: Lithuanian diacritics
    451599: [
        (f"Ank{FFFD}ciai", "Ank\u0161\u010diai"),
        (f"Bir{FFFD}tono", "Bir\u0161tono"),
        (f"Bir{FFFD}ai", "Bir\u017eai"),
        (f"Elektr{FFFD}nai", "Elektr\u0117nai"),
        (f"Joni{FFFD}kis", "Joni\u0161kis"),
        (f"Kai{FFFD}iadorys", "Kai\u0161iadorys"),
        (f"Kupi{FFFD}kis", "Kupi\u0161kis"),
        (f"Ma{FFFD}eikiai", "Ma\u017eeikiai"),
        (f"Pag{FFFD}giai", "Pag\u0117giai"),
        (f"Paneve{FFFD}ys", "Paneve\u017eys"),
        (f"Radvili{FFFD}kis", "Radvili\u0161kis"),
        (f"Roki{FFFD}kis", "Roki\u0161kis"),
        # Š-prefixed Lithuanian names
        (f"{FFFD}akiai", "\u0160akiai"),
        (f"{FFFD}alcininkai", "\u0160alcininkai"),
        (f"{FFFD}iauliu Miestas", "\u0160iauliu Miestas"),
        (f"{FFFD}iauliai", "\u0160iauliai"),
        (f"{FFFD}ilale", "\u0160ilal\u0117"),
        (f"{FFFD}ilute", "\u0160ilut\u0117"),
        (f"{FFFD}irvintos", "\u0160irvintos"),
        (f"{FFFD}vencionys", "\u0160vencionys"),
        (f"Tel{FFFD}iai", "Tel\u0161iai"),
        (f"Vilkavi{FFFD}kis", "Vilkavi\u0161kis"),
    ],

    # 2015 Paraguay — Waterways: "Paraná River"
    457863: [(f"Paran{FFFD} River", "Paran\u00e1 River")],

    # 2015 Senegal — Political parties: quote before BOKK
    461401: [(f"{FFFD}BOKK GIS GIS", "\u201cBOKK GIS GIS")],

    # 2015 Thailand — Economy: "coup d'état" (curly right quote before FFFD)
    463802: [(f"coup d\u2019{FFFD}tat", "coup d\u2019\u00e9tat")],

    # 2015 UK — Economy: "£375 billion"
    466066: [(f"{FFFD}375 billion", "\u00a3375 billion")],

    # 2016 Burma — Imports: "products, edible oil" (missing comma)
    473416: [(f"products{FFFD} edible", "products, edible")],

    # 2016 Comoros — Demographic profile: "mélange"
    476629: [(f"m{FFFD}lange", "m\u00e9lange")],

    # 2016 DRC — Political groups: "Forces Armées de la République Démocratique"
    475801: [
        (f"Arm{FFFD}es", "Arm\u00e9es"),
        (f"R{FFFD}publique", "R\u00e9publique"),
        (f"D{FFFD}mocratique", "D\u00e9mocratique"),
    ],

    # 2016 Dominican Republic — Economy: "4.0% ± 1.0%"
    478566: [(f"4.0% {FFFD} 1.0%", "4.0% \u00b1 1.0%")],

    # 2016 Lesotho — Political groups: "Tsebo MATŠASA"
    489027: [(f"MAT{FFFD}ASA", "MAT\u0160ASA")],

    # 2016 Paraguay — Waterways: "Paraná River"
    494667: [(f"Paran{FFFD} River", "Paran\u00e1 River")],

    # 2017 Burma — Imports: "products; edible oil" (missing semicolon)
    510236: [(f"products{FFFD} edible", "products; edible")],

    # 2017 DRC — Political groups: "Forces Armées de la République Démocratique"
    512635: [
        (f"Arm{FFFD}es", "Arm\u00e9es"),
        (f"R{FFFD}publique", "R\u00e9publique"),
        (f"D{FFFD}mocratique", "D\u00e9mocratique"),
    ],

    # 2017 Jordan — Administrative divisions: "Al 'Asimah"
    523278: [(f"Al {FFFD}Asimah", "Al \u2018Asimah")],

    # 2017 Lesotho — Political groups: "Tsebo MATŠASA"
    525956: [(f"MAT{FFFD}ASA", "MAT\u0160ASA")],

    # 2017 Monaco — Elevation: "Chemin des Révoires"
    527794: [(f"R{FFFD}voires", "R\u00e9voires")],

    # 2017 Paraguay — Waterways: "Paraná River"
    531635: [(f"Paran{FFFD} River", "Paran\u00e1 River")],

    # 2017 Vietnam — Economy: "'second wave'"
    541370: [(f"{FFFD}second wave", "\u2018second wave")],
}


def repair_db(db_path, dry_run=True):
    """Apply encoding repairs to a database."""
    conn = sqlite3.connect(db_path)
    repaired = 0
    remaining_fffd = 0

    for field_id, replacements in REPAIRS.items():
        row = conn.execute(
            "SELECT Content FROM CountryFields WHERE FieldID = ?", (field_id,)
        ).fetchone()
        if not row:
            print(f"  WARNING: FieldID {field_id} not found in {os.path.basename(db_path)}")
            continue

        content = row[0]
        if FFFD not in content:
            continue

        new_content = content
        for old, new in replacements:
            if old in new_content:
                new_content = new_content.replace(old, new)

        if new_content != content:
            chars_fixed = content.count(FFFD) - new_content.count(FFFD)
            if FFFD in new_content:
                remaining_fffd += new_content.count(FFFD)
                print(f"  PARTIAL: FieldID {field_id} — fixed {chars_fixed}, "
                      f"{new_content.count(FFFD)} remaining")
            else:
                print(f"  FIXED:   FieldID {field_id} — {chars_fixed} chars repaired")

            if not dry_run:
                conn.execute(
                    "UPDATE CountryFields SET Content = ? WHERE FieldID = ?",
                    (new_content, field_id),
                )
            repaired += 1
        else:
            print(f"  SKIP:    FieldID {field_id} — no matching pattern")

    if not dry_run:
        conn.commit()

    # Verify
    still_bad = conn.execute(
        f'SELECT COUNT(*) FROM CountryFields WHERE Content LIKE "%{FFFD}%"'
    ).fetchone()[0]

    conn.close()
    return repaired, still_bad


def main():
    parser = argparse.ArgumentParser(description="Repair U+FFFD encoding in factbook databases")
    parser.add_argument("--apply", action="store_true", help="Apply repairs (default: dry run)")
    args = parser.parse_args()

    dry_run = not args.apply
    mode = "DRY RUN" if dry_run else "APPLYING"

    print(f"=== Encoding Repair ({mode}) ===\n")
    print(f"Repair map: {len(REPAIRS)} fields, "
          f"{sum(len(v) for v in REPAIRS.values())} replacements\n")

    for db_path in [DB_PATH, FVDB_PATH]:
        if not os.path.exists(db_path):
            continue
        print(f"--- {os.path.basename(db_path)} ---")
        repaired, still_bad = repair_db(db_path, dry_run)
        print(f"\n  Repaired: {repaired} fields")
        print(f"  Remaining U+FFFD fields: {still_bad}")
        print()

    if dry_run:
        print("Run with --apply to commit changes.")


if __name__ == "__main__":
    main()
