from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import typer

from src.models import Property

app = typer.Typer(help="Mirascraper - Valencia property scraper")


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
    max_pages: int = typer.Option(2, help="Max list pages (HTML fallback only)"),
    enrich: bool = typer.Option(False, help="Fetch detail pages for extra data (slower)"),
    output: Path = typer.Option(Path("data"), help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show debug logs"),
) -> None:
    """Scrape property listings and save as JSONL."""
    _setup_logging(verbose)
    if source != "idealista":
        typer.echo(f"Unknown source: {source}")
        raise typer.Exit(1)

    from src.scrapers.idealista import IdealistaScraper

    output.mkdir(parents=True, exist_ok=True)

    with IdealistaScraper() as scraper:
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
    from src.db import upsert_properties

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


@app.command()
def run(
    source: str = typer.Option("idealista", help="Scraper source"),
    listing_type: str = typer.Option("sale", help="sale, rent, or new-building"),
    max_pages: int = typer.Option(2, help="Max list pages (HTML fallback only)"),
    enrich: bool = typer.Option(False, help="Fetch detail pages for extra data (slower)"),
    output: Path = typer.Option(Path("data"), help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show debug logs"),
) -> None:
    """Scrape + sync in one step."""
    _setup_logging(verbose)
    from src.scrapers.idealista import IdealistaScraper
    from src.db import upsert_properties

    output.mkdir(parents=True, exist_ok=True)

    with IdealistaScraper() as scraper:
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
    filename = output / f"{source}_{listing_type}_{timestamp}.jsonl"

    with open(filename, "w", encoding="utf-8") as f:
        for prop in properties:
            f.write(prop.model_dump_json() + "\n")

    typer.echo(f"Saved {len(properties)} properties to {filename}")

    # Sync to Supabase
    count = upsert_properties(properties)
    typer.echo(f"Upserted {count} properties to Supabase.")


if __name__ == "__main__":
    app()
