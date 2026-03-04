# mirascrape

Valencia property scraper. Collects listings from multiple Spanish real-estate portals and syncs them to Supabase.

## Supported sources

| Source | `--source` value | Notes |
|---|---|---|
| Idealista | `idealista` | HTML scraper, requires Playwright |
| Spain Real Estate | `spain-real-estate` | HTML scraper |
| Pisos.com | `pisos-com` | HTML + JSON-LD scraper, supports `enrich` |
| Fotocasa | `fotocasa` | Private JSON API — no enrichment needed, all data in list response |

## CLI commands

### `scrape` — collect listings to a JSONL file

```bash
uv run python -m src.cli scrape --source fotocasa --listing-type sale
uv run python -m src.cli scrape --source pisos-com --listing-type rent --max-pages 10
```

Options:
- `--source` — scraper to use (see table above)
- `--listing-type` — `sale`, `rent`, or `new-building`
- `--max-pages` — stop after N pages (default: unlimited)
- `--enrich` — fetch detail pages for extra data (pisos-com only)
- `--output` — output directory (default: `data/`)
- `-v` — verbose/debug logging

### `enrich` — fetch detail pages for unenriched properties

Resumes safely — re-run on the same file to continue where you left off.

```bash
uv run python -m src.cli enrich data/pisos-com_sale_20250101_120000.jsonl
uv run python -m src.cli enrich data/pisos-com_sale_20250101_120000.jsonl --batch-size 30
```

Options:
- `--batch-size N` / `-b N` — stop after enriching N properties (0 = no limit)
- `-v` — verbose/debug logging

### `sync` — upsert a JSONL file to Supabase

```bash
uv run python -m src.cli sync data/fotocasa_sale_20250101_120000.jsonl
```

### `run` — scrape + sync in one step

```bash
uv run python -m src.cli run --source fotocasa --listing-type sale
uv run python -m src.cli run --source pisos-com --listing-type rent --name my_run.jsonl
```

Options: same as `scrape`, plus `--name` to override the output filename.

## Recommended workflow (pisos-com)

```bash
# 1. Collect basic data
uv run python -m src.cli scrape --source pisos-com --listing-type sale

# 2. Enrich in batches (avoids ban risk from sustained requests)
uv run python -m src.cli enrich data/pisos-com_sale_*.jsonl --batch-size 30
# Wait a while, then re-run to continue — the file is updated after each property

# 3. Upload (can run even if enrichment is incomplete)
uv run python -m src.cli sync data/pisos-com_sale_*.jsonl
```

## Fotocasa scraper notes

- Uses the private JSON API at `https://web.gw.fotocasa.es/v1/search/ads` (POST)
- All listing data (description, images, lat/lng, features) is in the list response — `enriched` is set to `True` by default
- Defaults to Valencia province; location, property types, and coordinates are configurable in `FotocasaScraper`
- Unknown feature IDs and enum codes are logged immediately with an example listing URL, and summarised at the end of the run — check the summary to expand the lookup tables in `src/scrapers/fotocasa.py`
- Rate-limited: backs off 60 s on HTTP 429

## Project structure

```
src/
  cli.py              # Typer CLI entry point
  models.py           # Property Pydantic model
  db.py               # Supabase upsert helpers
  config.py           # Settings (env vars)
  scrapers/
    base.py           # BaseScraper ABC
    idealista.py
    spain_real_estate.py
    pisos_com.py      # HTML + JSON-LD, image URL normalisation (h700-wp)
    fotocasa.py       # JSON API scraper
tests/
data/                 # Output JSONL files (git-ignored)
```
