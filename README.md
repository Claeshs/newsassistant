# Newsassistant

Et lille Python-projekt til en personlig nyhedsassistent, som:

1. henter nye artikler fra RSS-feeds
2. gemmer dem i en SQLite-database
3. beriger artikler med fuld tekst, når det er muligt
4. eksporterer de seneste artikler til en fil, som en LLM kan kuratere og opsummere

## Formål

Projektet er tænkt som en personlig nyhedspipeline, hvor RSS bruges til discovery, SQLite bruges til lager, og en LLM bruges til at udvælge og opsummere de vigtigste historier inden for et bestemt fokusområde.

## Filer

`fetch_feeds.py`
Henter RSS-elementer fra `feeds.json` og gemmer nye artikler i databasen.

`fetch_article_content.py`
Finder artikler i databasen, som mangler fuldtekst, downloader artikelsiden og forsøger at udtrække hovedindholdet som markdown.

`store_and_latest.py`
Projektets datalag. Opretter schema, normaliserer URL'er, undgår dubletter, upserter artikler og eksporterer seneste artikler som `latest.md` eller `latest.json`.

`summarize.py`
Sender de seneste artikler til Gemini og gemmer en kurateret opsummering som markdown.

`feeds.json`
Liste over RSS-kilder og hvor mange elementer der hentes fra hver.

`articles.db`
SQLite-database med gemte artikler.

`latest.json` / `latest.md`
Eksporterede artikler fra de seneste X timer. `latest.json` er det bedste format til videre LLM-behandling.

`requirements.txt`
Projektets Python-afhængigheder.

## Pipeline

Anbefalet rækkefølge:

1. Hent nye RSS-elementer:

```bash
python fetch_feeds.py
```

2. Hent fuld artikeltekst for artikler, der mangler indhold:

```bash
python fetch_article_content.py --hours 48 --limit 20
```

3. Eksportér de seneste artikler til JSON:

```bash
python store_and_latest.py json --out latest.json --hours 48
```

4. Lav en opsummering med Gemini:

```bash
python summarize.py --input latest.json --out briefing.md
```

Bemærk: `summarize.py` læser tekst fra fil og sender den videre til Gemini. Den kan bruges med både markdown og JSON som input, men JSON er mere velegnet som mellemformat.

## Opsætning

Installer afhængigheder:

```bash
pip install -r requirements.txt
```

Sæt Gemini-nøglen:

Windows CMD:

```bat
set GEMINI_API_KEY=din_api_nøgle_her
```

PowerShell:

```powershell
$env:GEMINI_API_KEY="din_api_nøgle_her"
```

macOS / Linux:

```bash
export GEMINI_API_KEY="din_api_nøgle_her"
```

## Arkitekturvalg

- RSS bruges kun til discovery og metadata.
- Fuld artikeltekst hentes i et separat trin, så scraping-logik holdes adskilt fra datalaget.
- SQLite er valgt som enkelt og lokalt lager.
- URL-normalisering og hash bruges til at undgå dubletter.
- `latest.json` er tænkt som hovedinput til en LLM.

## Kendte begrænsninger

- Nogle websites kan blokere scraping, være bag login eller have paywall.
- Artikeludtræk er heuristisk og virker ikke lige godt på alle kilder.
- `summarize.py` er i dag koblet til Gemini specifikt.
- Eksisterende gamle data i databasen eller `latest.json` kan stadig indeholde tidligere encoding-fejl, indtil de er regenereret.

## Næste oplagte forbedringer

- Lade `summarize.py` være eksplicit optimeret til `latest.json`.
- Tilføje scoring eller filtrering før LLM-trinnet.
- Gemme kørselshistorik eller logs for hver pipeline-kørsel.
- Tilføje tests for URL-normalisering, eksport og deduplikering.
