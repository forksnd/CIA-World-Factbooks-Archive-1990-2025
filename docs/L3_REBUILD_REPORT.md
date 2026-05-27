# L3 Rebuild Report

Generated: 2026-05-27T05:03:54Z UTC

## Totals

| Metric | Count |
|---|---|
| Re-parsed records | 1,071,051 |
| DB records | 1,071,489 |
| Matched (same key, same content) | 1,070,747 |
| Content diffs (same key, different content) | 261 |
| Missing in re-parse (DB has, re-parse doesn't) | 575 |
| Extra in re-parse (re-parse has, DB doesn't) | 41 |

## Per-year breakdown

| Year | Src | Reparse | DB | Matches | ContentDiffs | MissingInDB-cmp | ExtraInReparse |
|---|---|---|---|---|---|---|---|
| 1990 | text | 15,750 | 15,750 | 15,750 | 0 | 0 | 0 |
| 1991 | text | 14,903 | 14,903 | 14,903 | 0 | 0 | 0 |
| 1992 | text | 17,372 | 17,372 | 17,372 | 0 | 0 | 0 |
| 1993 | text | 18,509 | 18,509 | 18,509 | 0 | 0 | 0 |
| 1994 | text | 28,633 | 28,633 | 28,633 | 0 | 0 | 0 |
| 1995 | text | 19,599 | 19,599 | 19,599 | 0 | 0 | 0 |
| 1996 | text | 20,566 | 21,116 | 20,523 | 0 | 575 | 41 |
| 1997 | text | 23,405 | 23,405 | 23,405 | 0 | 0 | 0 |
| 1998 | text | 23,524 | 23,524 | 23,524 | 0 | 0 | 0 |
| 1999 | text | 25,178 | 25,178 | 25,178 | 0 | 0 | 0 |
| 2000 | html | 25,724 | 25,724 | 25,724 | 0 | 0 | 0 |
| 2001 | text | 27,281 | 27,281 | 27,281 | 0 | 0 | 0 |
| 2002 | html | 27,430 | 27,430 | 27,430 | 0 | 0 | 0 |
| 2003 | html | 28,676 | 28,676 | 28,676 | 0 | 0 | 0 |
| 2004 | html | 28,958 | 28,958 | 28,958 | 0 | 0 | 0 |
| 2005 | html | 28,728 | 28,728 | 28,728 | 0 | 0 | 0 |
| 2006 | html | 28,962 | 28,962 | 28,960 | 2 | 0 | 0 |
| 2007 | html | 29,103 | 29,103 | 29,102 | 1 | 0 | 0 |
| 2008 | html | 30,755 | 30,643 | 30,526 | 229 | 0 | 0 |
| 2009 | html | 30,818 | 30,818 | 30,815 | 3 | 0 | 0 |
| 2010 | html | 30,805 | 30,805 | 30,801 | 4 | 0 | 0 |
| 2011 | html | 33,635 | 33,635 | 33,635 | 0 | 0 | 0 |
| 2012 | html | 35,192 | 35,192 | 35,192 | 0 | 0 | 0 |
| 2013 | html | 36,731 | 36,731 | 36,731 | 0 | 0 | 0 |
| 2014 | html | 36,680 | 36,680 | 36,680 | 0 | 0 | 0 |
| 2015 | html | 36,870 | 36,870 | 36,861 | 9 | 0 | 0 |
| 2016 | html | 36,804 | 36,804 | 36,798 | 6 | 0 | 0 |
| 2017 | html | 37,046 | 37,046 | 37,039 | 7 | 0 | 0 |
| 2018 | html | 37,285 | 37,285 | 37,285 | 0 | 0 | 0 |
| 2019 | html | 37,394 | 37,394 | 37,394 | 0 | 0 | 0 |
| 2020 | html | 36,687 | 36,687 | 36,687 | 0 | 0 | 0 |
| 2021 | json | 39,714 | 39,714 | 39,714 | 0 | 0 | 0 |
| 2022 | json | 37,344 | 37,344 | 37,344 | 0 | 0 | 0 |
| 2023 | json | 37,558 | 37,558 | 37,558 | 0 | 0 | 0 |
| 2024 | json | 34,838 | 34,838 | 34,838 | 0 | 0 | 0 |
| 2025 | json | 32,594 | 32,594 | 32,594 | 0 | 0 | 0 |

## Example diffs (first 3 per year)

### 1996 (text)


Missing from re-parse (in DB but not produced):

- `Armenia` / `Religions`
- `Tuvalu` / `Political parties and leaders`
- `Luxembourg` / `Birth rate`

Extra in re-parse (produced but not in DB):

- `Greece` / `People`
- `Luxembourg` / `Transportation`
- `Armenia` / `Economy`

### 2006 (html)

Content diffs:

- `Chile` / `Economy - overview`
  - reparse: `"Chile has a market-oriented economy characterized by a high level of foreign trade. During the early 1990s, Chile's reputation as a role model for economic reform was strengthened when the democratic "`
  - db:      `"Chile has a market-oriented economy characterized by a high level of foreign trade. During the early 1990s, Chile's reputation as a role model for economic reform was strengthened when the democratic "`
- `Costa Rica` / `Disputes - international`
  - reparse: `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the R�o San Juan over which Nicaragua retains sovere'`
  - db:      `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the Río San Juan over which Nicaragua retains sovere'`

### 2007 (html)

Content diffs:

- `Costa Rica` / `Disputes - international`
  - reparse: `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the R�o San Juan over which Nicaragua retains sovere'`
  - db:      `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the Río San Juan over which Nicaragua retains sovere'`

### 2008 (html)

Content diffs:

- `Serbia` / `Area - comparative`
  - reparse: `'slightly smaller than South Carolina'`
  - db:      `'slightly smaller than South Carolina'`
- `Serbia` / `Land use`
  - reparse: `'arable land: NA | permanent crops: NA | other: NA'`
  - db:      `'arable land: NA | permanent crops: NA | other: NA'`
- `Serbia` / `Currency (code)`
  - reparse: `'Serbian dinar (RSD)'`
  - db:      `'Serbian dinar (RSD)'`

### 2009 (html)

Content diffs:

- `Costa Rica` / `Disputes - international`
  - reparse: `'the ICJ has given Costa Rica until January 2008 to reply and Nicaragua until July 2008 to rejoin before rendering its decision on the navigation, security, and commercial rights of Costa Rican vessels'`
  - db:      `'the ICJ has given Costa Rica until January 2008 to reply and Nicaragua until July 2008 to rejoin before rendering its decision on the navigation, security, and commercial rights of Costa Rican vessels'`
- `Congo, Democratic Republic of the` / `Political pressure groups and leaders`
  - reparse: `'MONUC - UN organization working with the government; FARDC (Forces Arm�es de la R�publique D�mocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
  - db:      `'MONUC - UN organization working with the government; FARDC (Forces Armées de la République Démocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
- `Benin` / `Political parties and leaders`
  - reparse: `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
  - db:      `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`

### 2010 (html)

Content diffs:

- `Congo, Democratic Republic of the` / `Political pressure groups and leaders`
  - reparse: `'MONUC - UN organization working with the government; FARDC (Forces Arm�es de la R�publique D�mocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
  - db:      `'MONUC - UN organization working with the government; FARDC (Forces Armées de la République Démocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
- `Nepal` / `Background`
  - reparse: `'In 1951, the Nepalese monarch ended the century-old system of rule by hereditary premiers and instituted a cabinet system of government. Reforms in 1990 established a multiparty democracy within the f'`
  - db:      `'In 1951, the Nepalese monarch ended the century-old system of rule by hereditary premiers and instituted a cabinet system of government. Reforms in 1990 established a multiparty democracy within the f'`
- `Paraguay` / `Waterways`
  - reparse: `'3,100 km (primarily on the Paraguay and Paran� river systems) (2010) | country comparison to the world: | 33'`
  - db:      `'3,100 km (primarily on the Paraguay and Paraná river systems) (2010) | country comparison to the world: | 33'`

### 2015 (html)

Content diffs:

- `Senegal` / `Political parties and leaders`
  - reparse: `'Alliance for the Republic-Yakaar [Macky SALL] | Alliance of Forces of Progress or AFP [Moustapha NIASSE] | And-Jef/African Party for Democracy and Socialism or AJ/PADS [Mamadou DIOP, Landing SAVANE] |'`
  - db:      `'Alliance for the Republic-Yakaar [Macky SALL] | Alliance of Forces of Progress or AFP [Moustapha NIASSE] | And-Jef/African Party for Democracy and Socialism or AJ/PADS [Mamadou DIOP, Landing SAVANE] |'`
- `European Union` / `Executive branch`
  - reparse: `'note: the High Representative of the Union for Foreign Affairs and Security Policy is the EC�s external representation and foreign policy making body; Frederica MOGHERINI (since 1 November 2014), is t'`
  - db:      `'note: the High Representative of the Union for Foreign Affairs and Security Policy is the EC’s external representation and foreign policy making body; Frederica MOGHERINI (since 1 November 2014), is t'`
- `Congo, Democratic Republic Of The` / `Political pressure groups and leaders`
  - reparse: `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Arm�es de la R�publique D�mocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`
  - db:      `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Armées de la République Démocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`

### 2016 (html)

Content diffs:

- `Dominican Republic` / `Economy - overview`
  - reparse: `"The Dominican Republic has long been viewed primarily as an exporter of sugar, coffee, and tobacco, but in recent years the service sector has overtaken agriculture as the economy's largest employer, "`
  - db:      `"The Dominican Republic has long been viewed primarily as an exporter of sugar, coffee, and tobacco, but in recent years the service sector has overtaken agriculture as the economy's largest employer, "`
- `Congo, Democratic Republic Of The` / `Political pressure groups and leaders`
  - reparse: `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Arm�es de la R�publique D�mocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`
  - db:      `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Armées de la République Démocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`
- `Comoros` / `Demographic profile`
  - reparse: `'Comoros’ population is a m�lange of Arabs, Persians, Indonesians, Africans, and Indians, and the much smaller number of Europeans that settled on the islands between the 8th and 19th centuries, when t'`
  - db:      `'Comoros’ population is a mélange of Arabs, Persians, Indonesians, Africans, and Indians, and the much smaller number of Europeans that settled on the islands between the 8th and 19th centuries, when t'`

### 2017 (html)

Content diffs:

- `Jordan` / `Administrative divisions`
  - reparse: `"12 governorates (muhafazat, singular - muhafazah); 'Ajlun, Al 'Aqabah, Al Balqa', Al Karak, Al Mafraq, Al �Asimah (Amman), At Tafilah, Az Zarqa', Irbid, Jarash, Ma'an, Madaba"`
  - db:      `"12 governorates (muhafazat, singular - muhafazah); 'Ajlun, Al 'Aqabah, Al Balqa', Al Karak, Al Mafraq, Al ‘Asimah (Amman), At Tafilah, Az Zarqa', Irbid, Jarash, Ma'an, Madaba"`
- `Congo, Democratic Republic Of The` / `Political pressure groups and leaders`
  - reparse: `'Allied Democratic Forces or ADF (anti-Ugandan Government rebel groups] | Army of the Democratic Republic of the Congo (Forces Arm�es de la R�publique D�mocratique du Congo) or FARDC | Forces Democrati'`
  - db:      `'Allied Democratic Forces or ADF (anti-Ugandan Government rebel groups] | Army of the Democratic Republic of the Congo (Forces Armées de la République Démocratique du Congo) or FARDC | Forces Democrati'`
- `Monaco` / `Elevation`
  - reparse: `'mean elevation: NA | elevation extremes: lowest point: Mediterranean Sea 0 m | highest point: Chemin des R�voires on Mont Agel 162 m'`
  - db:      `'mean elevation: NA | elevation extremes: lowest point: Mediterranean Sea 0 m | highest point: Chemin des Révoires on Mont Agel 162 m'`

