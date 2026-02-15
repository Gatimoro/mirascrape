from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod

from src.config import settings
from src.models import Property

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Raised when a page fetch returns a non-OK status."""

    def __init__(self, status_code: int, url: str) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code} for {url}")


class BaseScraper(ABC):
    HEADERS = {
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    def __init__(self) -> None:
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def _launch_browser(self) -> None:
        from patchright.sync_api import sync_playwright

        logger.info("Launching browser")
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=False)
        self._context = self._browser.new_context(
            locale="es-ES",
            extra_http_headers=self.HEADERS,
        )
        self._page = self._context.new_page()

    def _ensure_browser(self) -> None:
        if self._page is None:
            self._launch_browser()

    def close(self) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        self._page = None
        self._context = None
        self._browser = None
        self._pw = None

    def __enter__(self) -> BaseScraper:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _delay_sync(self) -> None:
        delay = random.uniform(settings.REQUEST_DELAY_MIN, settings.REQUEST_DELAY_MAX)
        logger.debug("Sleeping %.1fs between requests", delay)
        time.sleep(delay)

    @abstractmethod
    def scrape(
        self, listing_type: str, max_pages: int
    ) -> list[Property]:
        ...

    @abstractmethod
    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        ...
