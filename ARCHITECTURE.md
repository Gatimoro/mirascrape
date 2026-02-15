# Mirascraper Architecture

Private developer reference. Do not publish.

## Project Structure

```
mirascraper/
├── src/
│   ├── __init__.py
│   ├── cli.py              # Typer CLI — entry point for all commands
│   ├── config.py            # Settings loaded from .env (Supabase creds, delays)
│   ├── db.py                # Supabase upsert logic
│   ├── models.py            # Property Pydantic model (single data shape)
│   └── scrapers/
│       ├── __init__.py
│       ├── base.py          # BaseScraper ABC — Patchright browser lifecycle, delays
│       └── idealista.py     # IdealistaScraper — AJAX + HTML scraping for idealista.com
├── tests/
│   ├── test_parser.py       # 33 unit tests (parsing only, no network)
│   └── fixtures/            # Optional saved HTML files for testing
├── data/                    # Output directory (JSONL files, gitignored)
├── pyproject.toml           # Dependencies, scripts, pytest config
└── .env                     # SUPABASE_URL + SUPABASE_KEY (not committed)
```

## Data Flow

```
CLI command
    │
    ▼
IdealistaScraper.scrape()
    │
    ├─ 1. _ensure_browser()  → launches headless Chromium via Patchright (lazy, once)
    ├─ 2. page.goto(warmup)  → navigates to listing page, DataDome challenge auto-solved
    ├─ 3. _fetch_json()      → in-browser fetch() to AJAX endpoint (same cookies/TLS)
    │      │
    │      └─ on failure ──► _scrape_html_pages() → HTML fallback
    │                              │
    │                              └─ _fetch() → page.goto() per detail page
    ├─ 4. parse_ajax_ad()    → builds Property from each JSON ad object
    └─ 5. (optional) _enrich_from_detail_pages() → fills in description, coords, images
    │
    ▼
List[Property]
    │
    ├──► Saved to data/<source>_<type>_<timestamp>.jsonl
    └──► (if `run` command) upsert_properties() → Supabase
```

## How the Pieces Connect

### `cli.py` → scrapers
The CLI imports the scraper lazily (`from src.scrapers.idealista import IdealistaScraper`)
inside each command function. This avoids loading heavy dependencies at import time.
It creates the scraper as a context manager (`with IdealistaScraper() as scraper`)
which auto-closes the browser on exit.

### `BaseScraper` → `IdealistaScraper`
`BaseScraper` (ABC) provides:
- **`_ensure_browser()`** — lazily launches a headless Chromium instance via Patchright (a patched Playwright that avoids CDP detection). Creates a browser context with `locale="es-ES"` and shared headers. The browser is **not** launched in `__init__()` so that tests (which only use parsing methods) don't require Chromium installed.
- **`self._page`** — the Patchright `Page` object used for all navigation and in-browser fetches
- **`_delay_sync()`** — random sleep between requests (configured via `settings`)
- **`close()`** — closes context, browser, and Patchright (no-op if browser was never launched)
- Abstract methods `scrape()` and `parse_detail_page()` that subclasses must implement

`IdealistaScraper` inherits all of this and adds Idealista-specific logic.

### `Property` model
Single Pydantic model used everywhere. Auto-generates `id` as `{source}-{source_id}`.
The same shape is used for JSONL output and Supabase upserts.

### `config.py`
Uses `pydantic-settings` to load from `.env`. Key settings:
- `SUPABASE_URL`, `SUPABASE_KEY` — database connection
- `REQUEST_DELAY_MIN/MAX` — random delay range between requests (default 2-5s)

## How to Add a New Scraper

### 1. Create the scraper file

Create `src/scrapers/fotocasa.py` (or whatever site):

```python
import logging
from src.models import Property
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class FotocasaScraper(BaseScraper):

    def scrape(self, listing_type: str, max_pages: int) -> list[Property]:
        # Your scraping logic here
        # Use self._ensure_browser() to launch the browser (lazy, once)
        # Use self._page.goto(url) to navigate and self._page.content() for HTML
        # Use self._page.evaluate() for in-browser fetch() calls
        # Use self._delay_sync() between requests
        # Return a list of Property objects
        ...

    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        # Parse a single listing page into a Property
        ...
```

### 2. Wire it into the CLI

In `src/cli.py`, update the `scrape` and `run` commands:

```python
if source == "idealista":
    from src.scrapers.idealista import IdealistaScraper
    scraper_cls = IdealistaScraper
elif source == "fotocasa":
    from src.scrapers.fotocasa import FotocasaScraper
    scraper_cls = FotocasaScraper
else:
    typer.echo(f"Unknown source: {source}")
    raise typer.Exit(1)

with scraper_cls() as scraper:
    properties = scraper.scrape(...)
```

### 3. Add tests

Create `tests/test_fotocasa.py`. Test parsing logic only (no network calls).
Instantiate the scraper, call its static/parsing methods with hardcoded HTML strings.
Look at `tests/test_parser.py` for the pattern.

### 4. That's it

The `Property` model, JSONL saving, and Supabase sync all work automatically
because every scraper returns `list[Property]`.

## Understanding Console Logs

Logging uses Python's `logging` module. Format: `HH:MM:SS [LEVEL] module: message`

### Normal successful run
```
14:30:01 [INFO] src.scrapers.base: Launching browser
14:30:03 [INFO] src.scrapers.idealista: Warming up browser session: https://www.idealista.com/venta-viviendas/valencia-valencia/
14:30:08 [INFO] src.scrapers.idealista: Fetching AJAX listings: https://www.idealista.com/es/ajax/...
14:30:09 [INFO] src.scrapers.idealista: AJAX returned 30 listings
14:30:09 [INFO] src.scrapers.idealista: Parsed 30 properties from AJAX data
Saved 30 properties to data/idealista_sale_20260215_143005.jsonl
```

### AJAX fails, falls back to HTML
```
14:30:04 [ERROR] src.scrapers.idealista: AJAX request failed: HTTP 403
14:30:04 [WARNING] src.scrapers.idealista: Falling back to HTML list pages
14:30:05 [INFO] src.scrapers.idealista: Fetching list page 1: https://www.idealista.com/venta-viviendas/...
14:30:06 [INFO] src.scrapers.idealista: Page 1: 30 listings (total pages: 50)
```

### Detail page errors during enrichment
```
14:30:10 [ERROR] src.scrapers.idealista: Detail 12345 failed: HTTP 403
14:30:15 [ERROR] src.scrapers.idealista: Detail 67890 transport error: ConnectionTimeout
14:30:20 [WARNING] src.scrapers.idealista: Skipping enrichment for 11111: 429 Too Many Requests
```

### Debug mode (`--verbose` / `-v`)
Adds extra output like delay timings:
```
14:30:01 [DEBUG] src.scrapers.base: Sleeping 3.2s between requests
```

## CLI Commands

```bash
# Scrape and save to JSONL
uv run python -m src.cli scrape --listing-type sale --max-pages 1

# Scrape with detail page enrichment (slower, more data)
uv run python -m src.cli scrape --listing-type rent --enrich

# Scrape with debug output
uv run python -m src.cli scrape --listing-type sale -v

# Sync a JSONL file to Supabase
uv run python -m src.cli sync data/idealista_sale_20260215_143005.jsonl

# Scrape + sync in one step
uv run python -m src.cli run --listing-type sale
```

## Anti-Bot Defenses

Idealista uses **DataDome**, which combines TLS fingerprinting (JA3), JavaScript
challenges, and cookie validation. DataDome ties cookies to the TLS fingerprint
that created them, so cookie transfer between different HTTP clients fails.

The scraper handles this with a **full browser approach**:

1. **Patchright** — a patched version of Playwright that fixes CDP (Chrome DevTools Protocol) leaks (`Runtime.enable`). This makes the headless browser indistinguishable from a real user's Chrome. Standard Playwright gets detected by DataDome via CDP inspection.
2. **Browser warmup** — `_page.goto(warmup_url)` navigates to a listing page; any DataDome JS challenge is solved automatically by the real browser engine
3. **In-browser `fetch()`** — `_fetch_json()` uses `page.evaluate()` to run `fetch()` inside the browser context, ensuring the same cookies, TLS fingerprint, and session are used for AJAX calls
4. **Page navigation** — `_fetch()` uses `page.goto()` + `page.content()` for HTML pages
5. **Random delays** — 2-5 seconds between requests (configurable in `.env`)
6. **Retry with backoff** — `tenacity` retries on 403/429/503 with exponential backoff

If you still get 403s after these measures, possible next steps:
- Try `headless=False` for debugging to see what the browser shows
- Add stealth args: `--disable-blink-features=AutomationControlled`
- Consider residential proxy rotation
- Consider commercial services (ScraperAPI, ZenRows)

## Running Tests

```bash
uv run pytest              # all tests
uv run pytest -v           # verbose test names
uv run pytest -k "ajax"    # only tests matching "ajax"
```

Tests are pure parsing tests — no network, no mocking needed. They test the
static methods that parse HTML/JS/JSON into structured data.
