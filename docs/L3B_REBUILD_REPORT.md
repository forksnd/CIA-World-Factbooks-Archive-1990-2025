# L3b Rebuild Report (re-parse vs SQL Server)

Generated: 2026-05-27T05:15:49Z UTC

## Totals

| Metric | Count |
|---|---|
| Re-parsed records | 1,071,051 |
| SQL Server records | 1,071,601 |
| Matched (same key, same content) | 1,063,060 |
| Content diffs (same key, different content) | 7,948 |
| Missing in re-parse (SQL has, re-parse doesn't) | 575 |
| Extra in re-parse (re-parse has, SQL doesn't) | 41 |

## Per-year breakdown

| Year | Src | Reparse | SQL | Matches | ContentDiffs | MissingFromReparse | ExtraInReparse |
|---|---|---|---|---|---|---|---|
| 1990 | text | 15,750 | 15,750 | 15,190 | 560 | 0 | 0 |
| 1991 | text | 14,903 | 14,903 | 14,644 | 259 | 0 | 0 |
| 1992 | text | 17,372 | 17,372 | 17,071 | 301 | 0 | 0 |
| 1993 | text | 18,509 | 18,509 | 18,367 | 142 | 0 | 0 |
| 1994 | text | 28,633 | 28,633 | 24,563 | 4,070 | 0 | 0 |
| 1995 | text | 19,599 | 19,599 | 19,144 | 455 | 0 | 0 |
| 1996 | text | 20,566 | 21,116 | 20,053 | 470 | 575 | 41 |
| 1997 | text | 23,405 | 23,405 | 22,933 | 472 | 0 | 0 |
| 1998 | text | 23,524 | 23,524 | 23,071 | 453 | 0 | 0 |
| 1999 | text | 25,178 | 25,178 | 24,945 | 233 | 0 | 0 |
| 2000 | html | 25,724 | 25,724 | 25,724 | 0 | 0 | 0 |
| 2001 | text | 27,281 | 27,281 | 26,785 | 496 | 0 | 0 |
| 2002 | html | 27,430 | 27,430 | 27,430 | 0 | 0 | 0 |
| 2003 | html | 28,676 | 28,676 | 28,676 | 0 | 0 | 0 |
| 2004 | html | 28,958 | 28,958 | 28,958 | 0 | 0 | 0 |
| 2005 | html | 28,728 | 28,728 | 28,728 | 0 | 0 | 0 |
| 2006 | html | 28,962 | 28,962 | 28,960 | 2 | 0 | 0 |
| 2007 | html | 29,103 | 29,103 | 29,102 | 1 | 0 | 0 |
| 2008 | html | 30,755 | 30,755 | 30,750 | 5 | 0 | 0 |
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

## Example diffs (first 3 per year with mismatches)

### 1990 (text)

Content diffs:

- `Madagascar` / `Unemployment rate`
  - reparse: `'NA%'`
  - sql:     `'1.5% (1988)'`
- `Madagascar` / `Territorial sea`
  - reparse: `'12 nm'`
  - sql:     `'3 nm'`
- `Somalia` / `Birth rate`
  - reparse: `'47 births/1,000 population (1990)'`
  - sql:     `'18 births/1,000 population (1990)'`

### 1991 (text)

Content diffs:

- `Madagascar` / `Unemployment rate`
  - reparse: `'NA%'`
  - sql:     `'1.5% (1988)'`
- `Somalia` / `Birth rate`
  - reparse: `'46 births/1,000 population (1991)'`
  - sql:     `'17 births/1,000 population (1991)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'6.9 children born/woman (1991)'`
  - sql:     `'1.8 children born/woman (1991)'`

### 1992 (text)

Content diffs:

- `Madagascar` / `Unemployment rate`
  - reparse: `'NA%'`
  - sql:     `'1.5% (1988)'`
- `Pakistan` / `Elections`
  - reparse: `'National Assembly: | last held on 24 October 1990 (next to be held by NA October 1995); results - | percent of vote by party NA; seats - (217 total) IJI 107, PDA 45, MQM 15, | ANP 6, JUI 2, JWP 2, PNP'`
  - sql:     `'National Assembly: | last held on 24 October 1990 (next to be held by NA October 1995); results - | percent of vote by party NA; seats - (217 total) IJI 107, PDA 45, MQM 15, | ANP 6, JUI 2, JWP 2, PNP'`
- `Cocos Islands` / `Elections`
  - reparse: `'NA'`
  - sql:     `'President: | last held 27 May 1990 (next to be held May 1994); results - Cesar GAVIRIA | Trujillo (Liberal) 47%, Alvaro GOMEZ Hurtado (National Salvation Movement) | 24%, Antonio NAVARRO Wolff (M-19) '`

### 1993 (text)

Content diffs:

- `Madagascar` / `Unemployment rate`
  - reparse: `'NA%'`
  - sql:     `'1% (1992 est.)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'6.75 children born/woman (1993 est.)'`
  - sql:     `'1.8 children born/woman (1993 est.)'`
- `Canada` / `Defense expenditures`
  - reparse: `'exchange rate conversion - $11.3 billion, 2% of GDP (FY92/93)'`
  - sql:     `'exchange rate conversion - $NA, NA% of GDP'`

### 1994 (text)

Content diffs:

- `Madagascar` / `consumption per capita`
  - reparse: `'35 kWh (1991)'`
  - sql:     `'2,965 kWh (1992)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'6.68 children born/woman (1994 est.)'`
  - sql:     `'1.8 children born/woman (1994 est.)'`
- `Belarus` / `chief of mission`
  - reparse: `"(vacant); Charge d'Affaires George KROL"`
  - sql:     `"(vacant); Charge d'Affaires George KROL"`

### 1995 (text)

Content diffs:

- `Madagascar` / `Unemployment rate`
  - reparse: `'NA%'`
  - sql:     `'1% (1992 est.)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'6.62 children born/woman (1995 est.)'`
  - sql:     `'1.8 children born/woman (1995 est.)'`
- `Micronesia, Federated States Of` / `FAX`
  - reparse: `'[1] (202) 223-4391 | consulate(s) general: Honolulu and Tamuning (Guam)'`
  - sql:     `'[691] 320-2186'`

### 1996 (text)

Content diffs:

- `Madagascar` / `Total fertility rate`
  - reparse: `'5.89 children born/woman (1996 est.)'`
  - sql:     `'1.68 children born/woman (1996 est.)'`
- `Romania` / `FAX`
  - reparse: `'[1] (202) 232-4748 | consulate(s) general: Los Angeles and New York'`
  - sql:     `'[40] (1) 210 03 95 | branch office: Cluj-Napoca'`
- `Madagascar` / `Religions`
  - reparse: `'indigenous beliefs 52%, Christian 41%, Muslim 7%'`
  - sql:     `'Anglican, Roman Catholic, Methodist, Baptist, | Presbyterian, Society of Friends'`

Missing from re-parse:

- `Greece` / `Terrain`
- `Malta` / `Fiscal year`
- `Luxembourg` / `GDP`

Extra in re-parse:

- `Malta` / `Economy`
- `Monaco` / `Geography`
- `Armenia` / `People`

### 1997 (text)

Content diffs:

- `Canada` / `Airports - with paved runways`
  - reparse: `'total: 816 | over 3,047 m: 17 | 2,438 to 3,047 m: 15 | 1,524 to 2,437 m : 138 | 914 to 1,523 m: 229 | under 914 m: 417 (1996 est.)'`
  - sql:     `'total: 6 | over 3,047 m: 1 | 914 to 1,523 m: 5 (1996 est.)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'5.83 children born/woman (1997 est.)'`
  - sql:     `'1.67 children born/woman (1997 est.)'`
- `Algeria` / `FAX`
  - reparse: `'[1] (202) 667-2174'`
  - sql:     `'[213] (2) 69-39-79'`

### 1998 (text)

Content diffs:

- `Madagascar` / `Total fertility rate`
  - reparse: `'5.76 children born/woman (1998 est.)'`
  - sql:     `'1.67 children born/woman (1998 est.)'`
- `Algeria` / `FAX`
  - reparse: `'[1] (202) 667-2174'`
  - sql:     `'[213] (2) 69-39-79'`
- `Congo, Republic of the` / `FAX`
  - reparse: `'[1] (202) 726-1860'`
  - sql:     `'[242] 83 63 38 | note: the embassy is temporarily collocated with the US Embassy in the | Democratic Republic of the Congo (US Embassy Kinshasa, 310 Avenue des | Aviateurs, Kinshasa)'`

### 1999 (text)

Content diffs:

- `Madagascar` / `Total fertility rate`
  - reparse: `'5.7 children born/woman (1999 est.)'`
  - sql:     `'1.67 children born/woman (1999 est.)'`
- `Canada` / `Military expenditures--percent of GDP`
  - reparse: `'1.2% (FY97/98)'`
  - sql:     `'1.8% (1996)'`
- `Madagascar` / `Religions`
  - reparse: `'indigenous beliefs 52%, Christian 41%, Muslim 7%'`
  - sql:     `'Anglican, Roman Catholic, Methodist, Baptist, | Presbyterian, Society of Friends'`

### 2001 (text)

Content diffs:

- `Canada` / `Airports - with paved runways`
  - reparse: `'total: 517 over 3,047 m: 18 2,438 to 3,047 m: 15 1,524 to 2,437 m: 151 914 to 1,523 m: 244 under 914 m: 89 (2000 est.)'`
  - sql:     `'total: 8 over 3,047 m: 1 914 to 1,523 m: 7 (2000)'`
- `Madagascar` / `Total fertility rate`
  - reparse: `'5.8 children born/woman (2001 est.)'`
  - sql:     `'1.65 children born/woman (2001 est.)'`
- `Algeria` / `FAX`
  - reparse: `'[1] (202) 667-2174'`
  - sql:     `'[213] (21) 69-39-79'`

### 2006 (html)

Content diffs:

- `Costa Rica` / `Disputes - international`
  - reparse: `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the R�o San Juan over which Nicaragua retains sovere'`
  - sql:     `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the Río San Juan over which Nicaragua retains sovere'`
- `Chile` / `Economy - overview`
  - reparse: `"Chile has a market-oriented economy characterized by a high level of foreign trade. During the early 1990s, Chile's reputation as a role model for economic reform was strengthened when the democratic "`
  - sql:     `"Chile has a market-oriented economy characterized by a high level of foreign trade. During the early 1990s, Chile's reputation as a role model for economic reform was strengthened when the democratic "`

### 2007 (html)

Content diffs:

- `Costa Rica` / `Disputes - international`
  - reparse: `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the R�o San Juan over which Nicaragua retains sovere'`
  - sql:     `'in September 2005, Costa Rica took its case before the ICJ to advocate the navigation, security, and commercial rights of Costa Rican vessels using the Río San Juan over which Nicaragua retains sovere'`

### 2008 (html)

Content diffs:

- `Brazil` / `Disputes - international`
  - reparse: `'unruly region at convergence of Argentina-Brazil-Paraguay borders is locus of money laundering, smuggling, arms and illegal narcotics trafficking, and fundraising for extremist organizations; uncontes'`
  - sql:     `'unruly region at convergence of Argentina-Brazil-Paraguay borders is locus of money laundering, smuggling, arms and illegal narcotics trafficking, and fundraising for extremist organizations; uncontes'`
- `Benin` / `Political parties and leaders`
  - reparse: `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
  - sql:     `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
- `Colombia` / `Disputes - international`
  - reparse: `'in December 2007, ICJ allocates San Andres, Providencia, and Santa Catalina islands to Colombia under 1928 Treaty but does not rule on 82�W meridian as maritime boundary with Nicaragua; managed disput'`
  - sql:     `'in December 2007, ICJ allocates San Andres, Providencia, and Santa Catalina islands to Colombia under 1928 Treaty but does not rule on 82°W meridian as maritime boundary with Nicaragua; managed disput'`

### 2009 (html)

Content diffs:

- `Benin` / `Political parties and leaders`
  - reparse: `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
  - sql:     `'Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
- `Congo, Democratic Republic of the` / `Political pressure groups and leaders`
  - reparse: `'MONUC - UN organization working with the government; FARDC (Forces Arm�es de la R�publique D�mocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
  - sql:     `'MONUC - UN organization working with the government; FARDC (Forces Armées de la République Démocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
- `Costa Rica` / `Disputes - international`
  - reparse: `'the ICJ has given Costa Rica until January 2008 to reply and Nicaragua until July 2008 to rejoin before rendering its decision on the navigation, security, and commercial rights of Costa Rican vessels'`
  - sql:     `'the ICJ has given Costa Rica until January 2008 to reply and Nicaragua until July 2008 to rejoin before rendering its decision on the navigation, security, and commercial rights of Costa Rican vessels'`

### 2010 (html)

Content diffs:

- `Paraguay` / `Waterways`
  - reparse: `'3,100 km (primarily on the Paraguay and Paran� river systems) (2010) | country comparison to the world: | 33'`
  - sql:     `'3,100 km (primarily on the Paraguay and Paraná river systems) (2010) | country comparison to the world: | 33'`
- `Benin` / `Political parties and leaders`
  - reparse: `'African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
  - sql:     `'African Movement for Democracy and Progress or MADEP [Sefou FAGBOHOUN]; Alliance for Dynamic Democracy or ADD; Alliance of Progress Forces or AFP; Benin Renaissance or RB [Rosine SOGLO]; Democratic Re'`
- `Congo, Democratic Republic of the` / `Political pressure groups and leaders`
  - reparse: `'MONUC - UN organization working with the government; FARDC (Forces Arm�es de la R�publique D�mocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`
  - sql:     `'MONUC - UN organization working with the government; FARDC (Forces Armées de la République Démocratique du Congo) - Army of the Democratic Republic of the Congo which commits atrocities on citizens; F'`

### 2015 (html)

Content diffs:

- `European Union` / `Executive branch`
  - reparse: `'note: the High Representative of the Union for Foreign Affairs and Security Policy is the EC�s external representation and foreign policy making body; Frederica MOGHERINI (since 1 November 2014), is t'`
  - sql:     `'note: the High Representative of the Union for Foreign Affairs and Security Policy is the EC’s external representation and foreign policy making body; Frederica MOGHERINI (since 1 November 2014), is t'`
- `Lithuania` / `Administrative divisions`
  - reparse: `'60 municipalities (savivaldybe, singular - savivaldybe); Akmene, Alytaus Miestas, Alytus, Ank�ciai, Bir�tono, Bir�ai, Druskininkai, Elektr�nai, Ignalina, Jonava, Joni�kis, Jurbarkas, Kai�iadorys, Kalv'`
  - sql:     `'60 municipalities (savivaldybe, singular - savivaldybe); Akmene, Alytaus Miestas, Alytus, Ankščiai, Birštono, Biržai, Druskininkai, Elektrėnai, Ignalina, Jonava, Joniškis, Jurbarkas, Kaišiadorys, Kalv'`
- `Senegal` / `Political parties and leaders`
  - reparse: `'Alliance for the Republic-Yakaar [Macky SALL] | Alliance of Forces of Progress or AFP [Moustapha NIASSE] | And-Jef/African Party for Democracy and Socialism or AJ/PADS [Mamadou DIOP, Landing SAVANE] |'`
  - sql:     `'Alliance for the Republic-Yakaar [Macky SALL] | Alliance of Forces of Progress or AFP [Moustapha NIASSE] | And-Jef/African Party for Democracy and Socialism or AJ/PADS [Mamadou DIOP, Landing SAVANE] |'`

### 2016 (html)

Content diffs:

- `Paraguay` / `Waterways`
  - reparse: `'3,100 km (primarily on the Paraguay and Paran� River systems) (2012) | country comparison to the world: 32'`
  - sql:     `'3,100 km (primarily on the Paraguay and Paraná River systems) (2012) | country comparison to the world: 32'`
- `Comoros` / `Demographic profile`
  - reparse: `'Comoros’ population is a m�lange of Arabs, Persians, Indonesians, Africans, and Indians, and the much smaller number of Europeans that settled on the islands between the 8th and 19th centuries, when t'`
  - sql:     `'Comoros’ population is a mélange of Arabs, Persians, Indonesians, Africans, and Indians, and the much smaller number of Europeans that settled on the islands between the 8th and 19th centuries, when t'`
- `Congo, Democratic Republic Of The` / `Political pressure groups and leaders`
  - reparse: `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Arm�es de la R�publique D�mocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`
  - sql:     `'Allied Democratic Forces or ADF (anti-Ugandan government rebel groups] | Forces Armées de la République Démocratique du Congor (Army of the Democratic Republic of the Congo) or FARDC | Forces Democrat'`

### 2017 (html)

Content diffs:

- `Paraguay` / `Waterways`
  - reparse: `'3,100 km (primarily on the Paraguay and Paran� River systems) (2012) | country comparison to the world: 32'`
  - sql:     `'3,100 km (primarily on the Paraguay and Paraná River systems) (2012) | country comparison to the world: 32'`
- `Jordan` / `Administrative divisions`
  - reparse: `"12 governorates (muhafazat, singular - muhafazah); 'Ajlun, Al 'Aqabah, Al Balqa', Al Karak, Al Mafraq, Al �Asimah (Amman), At Tafilah, Az Zarqa', Irbid, Jarash, Ma'an, Madaba"`
  - sql:     `"12 governorates (muhafazat, singular - muhafazah); 'Ajlun, Al 'Aqabah, Al Balqa', Al Karak, Al Mafraq, Al ‘Asimah (Amman), At Tafilah, Az Zarqa', Irbid, Jarash, Ma'an, Madaba"`
- `Congo, Democratic Republic Of The` / `Political pressure groups and leaders`
  - reparse: `'Allied Democratic Forces or ADF (anti-Ugandan Government rebel groups] | Army of the Democratic Republic of the Congo (Forces Arm�es de la R�publique D�mocratique du Congo) or FARDC | Forces Democrati'`
  - sql:     `'Allied Democratic Forces or ADF (anti-Ugandan Government rebel groups] | Army of the Democratic Republic of the Congo (Forces Armées de la République Démocratique du Congo) or FARDC | Forces Democrati'`

