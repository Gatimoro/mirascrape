from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from src.models import Property
from src.scrapers.base import BaseScraper

app = typer.Typer(help="Mirascraper - Valencia property scraper")
logger = logging.getLogger(__name__)


def _get_scraper(source: str) -> BaseScraper:
    """Return the right scraper instance for the given source."""
    if source == "idealista":
        from src.scrapers.idealista import IdealistaScraper
        return IdealistaScraper()
    if source == "spain-real-estate":
        from src.scrapers.spain_real_estate import SpainRealEstateScraper
        return SpainRealEstateScraper()
    typer.echo(f"Unknown source: {source}. Available: idealista, spain-real-estate")
    raise typer.Exit(code=1)


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=level,
    )
    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


@app.command()
def scrape(
    source: str = typer.Option("idealista", help="Scraper source"),
    listing_type: str = typer.Option("sale", help="sale, rent, or new-building"),
    max_pages: int = typer.Option(9999, help="Max list pages per tab"),
    enrich: bool = typer.Option(False, help="Fetch detail pages for extra data (slower)"),
    output: Path = typer.Option(Path("data"), help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show debug logs"),
) -> None:
    """Scrape property listings and save as JSONL."""
    _setup_logging(verbose)
    output.mkdir(parents=True, exist_ok=True)

    with _get_scraper(source) as scraper:
        properties = scraper.scrape(
            listing_type=listing_type,
            max_pages=max_pages,
            enrich=enrich,
        )

    if not properties:
        typer.echo("No properties found.")
        raise typer.Exit(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output / f"{source}_{listing_type}_{timestamp}.jsonl"

    with open(filename, "w", encoding="utf-8") as f:
        for prop in properties:
            f.write(prop.model_dump_json() + "\n")

    typer.echo(f"Saved {len(properties)} properties to {filename}")


@app.command()
def sync(
    input_file: Path = typer.Argument(..., help="JSONL file to sync"),
) -> None:
    """Read a JSONL file and upsert to Supabase."""
    from src.db import upsert_properties, upsert_translations

    if not input_file.exists():
        typer.echo(f"File not found: {input_file}")
        raise typer.Exit(1)

    properties: list[Property] = []
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                properties.append(Property.model_validate_json(line))

    typer.echo(f"Loaded {len(properties)} properties from {input_file}")

    count = upsert_properties(properties)
    typer.echo(f"Upserted {count} properties to Supabase.")

    tr_count = upsert_translations(properties)
    typer.echo(f"Upserted {tr_count} translations to Supabase.")


@app.command()
def run(
    source: str = typer.Option("idealista", help="Scraper source"),
    listing_type: str = typer.Option("sale", help="sale, rent, or new-building"),
    max_pages: int = typer.Option(9999, help="Max list pages per tab"),
    enrich: bool = typer.Option(False, help="Fetch detail pages for extra data (slower)"),
    output: Path = typer.Option(Path("data"), help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show debug logs"),
    name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="Override output filename"
    )
) -> None:
    """Scrape + sync in one step."""
    _setup_logging(verbose)
    from src.db import upsert_properties, upsert_translations

    output.mkdir(parents=True, exist_ok=True)

    with _get_scraper(source) as scraper:
        properties = scraper.scrape(
            listing_type=listing_type,
            max_pages=max_pages,
            enrich=enrich,
        )

    if not properties:
        typer.echo("No properties found.")
        raise typer.Exit(0)

    # Save JSONL
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if name:
        base = Path(name)
        if base.suffix != ".jsonl":
            base = base.with_suffix(".jsonl")
        filename = output / base
    else:
        filename = output / f"{source}_{listing_type}_{timestamp}.jsonl"

    with open(filename, "w", encoding="utf-8") as f:
        for prop in properties:
            f.write(prop.model_dump_json() + "\n")

    typer.echo(f"Saved {len(properties)} properties to {filename}")

    # Sync to Supabase
    count = upsert_properties(properties)
    typer.echo(f"Upserted {count} properties to Supabase.")

    tr_count = upsert_translations(properties)
    typer.echo(f"Upserted {tr_count} translations to Supabase.")


def _save_jsonl(path: Path, properties: list[Property]) -> None:
    """Write properties to path atomically (write temp → rename)."""
    import os
    tmp = path.with_suffix(".jsonl.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for p in properties:
            f.write(p.model_dump_json() + "\n")
    os.replace(tmp, path)


@app.command()
def enrich(
    input_file: Path = typer.Argument(..., help="JSONL file to enrich in place"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show debug logs"),
) -> None:
    """Fetch detail pages for unenriched properties, updating the file in place.

    Each successfully enriched property is written back immediately, so the
    command is safely resumable — just re-run it on the same file.
    """
    _setup_logging(verbose)

    if not input_file.exists():
        typer.echo(f"File not found: {input_file}")
        raise typer.Exit(1)

    properties: list[Property] = []
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                properties.append(Property.model_validate_json(line))

    if not properties:
        typer.echo("No properties in file.")
        raise typer.Exit(0)

    source = properties[0].source
    scraper_instance = _get_scraper(source)

    if not hasattr(scraper_instance, "enrich_property"):
        typer.echo(f"Scraper '{source}' does not support per-property enrichment.")
        raise typer.Exit(1)

    pending = sum(1 for p in properties if not p.enriched)
    typer.echo(f"Loaded {len(properties)} properties ({pending} to enrich).")

    total = len(properties)
    n_enriched = n_skipped = n_failed = 0

    with scraper_instance as scraper:
        for i, prop in enumerate(properties):
            if prop.enriched:
                n_skipped += 1
                continue

            result = scraper.enrich_property(prop)
            properties[i] = result

            if result.enriched:
                n_enriched += 1
                typer.echo(f"[{n_enriched + n_failed}/{pending}] Enriched {prop.source_id}")
            else:
                n_failed += 1
                typer.echo(f"[{n_enriched + n_failed}/{pending}] Failed   {prop.source_id}", err=True)

            # Save progress after every property so the file is always resumable
            _save_jsonl(input_file, properties)

    typer.echo(f"Done: {n_enriched} enriched, {n_skipped} already done, {n_failed} failed")


if __name__ == "__main__":
    app()
