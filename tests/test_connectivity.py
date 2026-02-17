"""Smoke tests for spain-real.estate connectivity.

Run with: uv run pytest tests/test_connectivity.py -v -s
"""

from __future__ import annotations

import pytest

from src.scrapers.spain_real_estate import SpainRealEstateScraper


@pytest.fixture
def scraper():
    s = SpainRealEstateScraper()
    yield s
    s.close()


class TestConnectivity:
    """Live connectivity tests — require network access."""

    def test_httpx_fetch_index(self, scraper):
        """Verify httpx can fetch the homepage."""
        html = scraper._fetch_page("https://spain-real.estate/")
        assert "Spain-Real.Estate" in html
        assert len(html) > 1000
        print(f"  httpx OK — fetched {len(html)} bytes")

    def test_httpx_fetch_list_page(self, scraper):
        """Verify httpx can fetch a list page and we can parse it."""
        url = scraper.build_list_url(
            listing_type="sale", tab="apartment", page=1,
            region="Valencian Community", region_id=4120,
        )
        html = scraper._fetch_page(url)
        items = scraper.parse_list_page(html)
        total = scraper.parse_total_count(html)
        assert len(items) > 0, "No items parsed from list page"
        assert total > 0, "Could not parse total count"
        print(f"  httpx list OK — {len(items)} items, {total} total listings")

    def test_browser_fetch_index(self, scraper):
        """Verify Patchright browser can load the homepage."""
        html = scraper._fetch_with_browser("https://spain-real.estate/")
        assert "Spain-Real.Estate" in html
        assert len(html) > 1000
        print(f"  browser OK — fetched {len(html)} bytes")
