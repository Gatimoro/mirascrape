"""Scraper for habitaclia.com – Spanish SSR property portal."""

from __future__ import annotations

import json
import logging
import random
import re
import time
from urllib.parse import urlsplit, urlunsplit

from selectolax.parser import HTMLParser

from src.models import Property
from src.scrapers.base import BaseScraper, FetchError

logger = logging.getLogger(__name__)

BASE_URL = "https://www.habitaclia.com"

# Progressive waits on 403: 1 min, 3 min, 5 min, 10 min
_RATE_LIMIT_WAITS = [60, 180, 300, 600]


class UserStop(Exception):
    """Raised when the user requests a graceful stop during rate-limit recovery."""

# data-propertysubtype → sub_category
SUBTYPE_MAP: dict[str, str] = {
    "FLAT": "apartment",
    "APARTMENT": "apartment",
    "STUDIO": "apartment",
    "DUPLEX": "apartment",
    "PENTHOUSE": "apartment",
    "LOFT": "apartment",
    "HOUSE": "house",
    "CHALET": "house",
    "TERRACED_HOUSE": "house",
    "DETACHED_HOUSE": "house",
    "FARMHOUSE": "house",
    "PLOT": "plot",
    "LAND": "plot",
    "COMMERCIAL": "commerce",
    "OFFICE": "commerce",
    "STORE": "commerce",
    "WAREHOUSE": "commerce",
    "INDUSTRIAL": "commerce",
    "GARAGE": "commerce",
}

# URL property-type slug → sub_category (fallback when attribute absent)
_URL_SUBTYPE_MAP: dict[str, str] = {
    "piso": "apartment",
    "duplex": "apartment",
    "atico": "apartment",
    "estudio": "apartment",
    "apartamento": "apartment",
    "casa": "house",
    "chalet": "house",
    "villa": "house",
    "finca": "house",
    "local": "commerce",
    "oficina": "commerce",
    "nave": "commerce",
    "garaje": "commerce",
    "terreno": "plot",
    "solar": "plot",
}

# listing_type → URL operation prefix
_OPERATION_SLUG: dict[str, str] = {
    "rent": "alquiler",
    "sale": "viviendas",
}

# article headings whose <li> items are structural specs (skip as features)
_DISTRIBUTION_HEADINGS = {"Distribución", "Distribuci\xf3n", "Distribucion"}

# Regex to detect floor from "Planta número 2"
_FLOOR_RE = re.compile(r"[Pp]lanta\s+n[úu]mero\s+(\d+)")

# Regex to detect numeric specs so we skip them in the distribution section
_NUMERIC_SPEC_RE = re.compile(r"\d+\s*(?:hab|ba[ñn]|m[²2])", re.IGNORECASE)


def _normalise_image_url(url: str) -> str:
    """Return the XL-size variant of a habitaclia image URL with https scheme.

    Input formats:
        //images.habimg.com/imgh/{id}/{slug}_{uuid}.jpg   (no size suffix)
        //images.habimg.com/imgh/{id}/{slug}_{uuid}G.jpg  (gallery / medium)
        //images.habimg.com/imgh/{id}/{slug}_{uuid}P.jpg  (preview / small)
    Output:
        https://images.habimg.com/imgh/{id}/{slug}_{uuid}XL.jpg
    """
    if url.startswith("//"):
        url = "https:" + url
    url = re.sub(r"(?:G|P|XL)?\.jpg$", "XL.jpg", url)
    return url


def _subtype_from_url(url: str) -> str | None:
    """Infer sub_category from the property-type slug in a listing URL.

    Example: /alquiler-piso-el_carme-valencia-i123.htm → 'apartment'
    """
    m = re.search(r"/(?:alquiler|venta(?:-\w+)?)-([^-/]+)-", url)
    if m:
        return _URL_SUBTYPE_MAP.get(m.group(1).lower())
    return None


def _strip_tracking(url: str) -> str:
    """Remove query parameters from a listing URL."""
    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc, p.path, "", ""))


class HabitacliaScraper(BaseScraper):
    """Scraper for habitaclia.com using patchright + selectolax.

    The listing pages provide: ID, URL, price, title, location, size,
    bedrooms, and 1 thumbnail per property.  Run the CLI ``enrich``
    command on the saved JSONL file to fetch each detail page and add
    coordinates, the full image gallery, description, and features.
    """

    def _launch_browser(self) -> None:
        """Launch a persistent browser profile so Imperva trust tokens survive across runs."""
        from pathlib import Path
        from patchright.sync_api import sync_playwright

        profile_dir = Path.home() / ".cache" / "mirascrape" / "habitaclia"
        profile_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Launching browser with persistent profile: %s", profile_dir)

        self._pw = sync_playwright().start()
        self._context = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            locale="es-ES",
            viewport={"width": 1366, "height": 768},
            extra_http_headers=self.HEADERS,
        )
        # launch_persistent_context returns a BrowserContext directly; no separate browser object
        self._browser = None
        self._page = self._context.new_page()

    def _accept_cookies(self) -> None:
        """Click the cookie consent accept button if visible."""
        selectors = [
            "button:has-text('Aceptar todo')",
            "button:has-text('Aceptar')",
            "button:has-text('Acceptar')",
            "#onetrust-accept-btn-handler",
            "[id*='accept'][class*='button']",
        ]
        for sel in selectors:
            try:
                btn = self._page.locator(sel).first
                if btn.is_visible(timeout=2000):
                    btn.click()
                    logger.debug("Accepted cookie consent (%s)", sel)
                    self._page.wait_for_timeout(600)
                    return
            except Exception:
                continue

    def _scroll_page(self) -> None:
        """Scroll the page gradually to produce human-like viewport events."""
        self._page.evaluate("""
            () => new Promise(resolve => {
                let pos = 0;
                const target = document.body.scrollHeight * 0.65;
                const step = () => {
                    pos += Math.random() * 120 + 40;
                    window.scrollTo(0, pos);
                    if (pos < target) setTimeout(step, 80 + Math.random() * 60);
                    else resolve();
                };
                step();
            })
        """)
        self._page.wait_for_timeout(400)

    @staticmethod
    def _is_blocked(html: str) -> bool:
        """Return True if Imperva served a JS challenge page instead of real content."""
        return "Pardon Our Interruption" in html or "reese84" in html[:2000]

    def _wait_for_page_load(self) -> str:
        """Post-navigation helper: pause, accept cookies, scroll, return HTML."""
        self._page.wait_for_load_state("domcontentloaded")
        self._page.wait_for_timeout(500 + random.randint(0, 1000))
        self._accept_cookies()
        self._scroll_page()
        return self._page.content()

    def _fetch_page(self, url: str) -> str:
        """Navigate to url via goto(), retrying on block/403 with progressive waits.

        Waits 1 → 3 → 5 → 10 minutes on successive blocks, then prompts the
        user.  Typing "stop" raises UserStop so the caller can save progress;
        anything else waits another 10 minutes and retries indefinitely.
        """
        self._ensure_browser()
        attempt = 0
        while True:
            resp = self._page.goto(url, wait_until="domcontentloaded")
            content = self._wait_for_page_load()
            status = resp.status if resp else 200

            if status == 403 or self._is_blocked(content):
                if attempt < len(_RATE_LIMIT_WAITS):
                    wait = _RATE_LIMIT_WAITS[attempt]
                    logger.warning(
                        "Blocked (attempt %d/%d) — waiting %d min before retry",
                        attempt + 1, len(_RATE_LIMIT_WAITS), wait // 60,
                    )
                    time.sleep(wait)
                    attempt += 1
                    continue

                # All waits exhausted — hand control back to the user
                print(
                    '\nStill blocked after all retries.'
                    '\nType "stop" to save and exit, or press Enter to wait 10 min and retry: ',
                    end="", flush=True,
                )
                try:
                    reply = input()
                except EOFError:
                    raise UserStop()
                if reply.strip().lower() == "stop":
                    raise UserStop()
                logger.info("Continuing — waiting 10 min before next attempt")
                time.sleep(600)
                continue

            if status >= 400:
                raise FetchError(status, url)

            return content

    def _click_next_page(self) -> str | None:
        """Click the 'Siguiente' pagination link and return the new page's HTML.

        Simulates a real user scrolling to the pagination controls and clicking
        through to the next page instead of navigating directly via goto().
        Returns None when there is no next-page link (last page reached).
        Raises FetchError if Imperva blocks the resulting page.
        """
        next_a = self._page.locator("div.pagination li.next a")
        try:
            if not next_a.is_visible(timeout=1500):
                return None
        except Exception:
            return None

        # Scroll the pagination into view and pause like a human reading the page
        next_a.scroll_into_view_if_needed()
        self._page.wait_for_timeout(400 + random.randint(0, 600))

        with self._page.expect_navigation(wait_until="domcontentloaded"):
            next_a.click()

        content = self._wait_for_page_load()
        if self._is_blocked(content):
            raise FetchError(403, self._page.url)
        return content

    def _click_through_gallery(self) -> list[str]:
        """Click through the image wide-view gallery for human simulation.

        Extracts all non-video images from the DOM gallery (already present on
        page load), then opens the wide-view slider and clicks through every
        image to produce realistic interaction events.  Videos are ignored.
        Returns a list of normalised XL image URLs sorted by display order.
        """
        # Extract all images from the DOM — they are already rendered
        items: list[tuple[int, str]] = []
        for el in self._page.locator(
            "#js-gallery .ficha_foto[data-wide-type='img']"
        ).all():
            try:
                order = int(el.get_attribute("data-wide-order") or "999")
                src = el.locator("img").first.get_attribute("src") or ""
                if src:
                    items.append((order, _normalise_image_url(src)))
            except Exception:
                continue

        images = [url for _, url in sorted(items)]

        if not images:
            return images

        # Open wide-view by clicking the first image, then click through all
        try:
            first_link = self._page.locator(
                "#js-gallery .ficha_foto[data-wide-type='img'] a"
            ).first
            if first_link.is_visible(timeout=2000):
                first_link.click()
                self._page.wait_for_timeout(700 + random.randint(0, 300))

                next_btn = self._page.locator(
                    "button.wide-nav-btn:not(.wide-nav-btn-left)"
                )
                for _ in range(len(images) - 1):
                    try:
                        if next_btn.is_visible(timeout=700):
                            next_btn.click()
                            self._page.wait_for_timeout(250 + random.randint(0, 200))
                        else:
                            break
                    except Exception:
                        break

                self._page.keyboard.press("Escape")
                self._page.wait_for_timeout(400)
        except Exception as exc:
            logger.debug("Gallery click-through stopped early: %s", exc)

        return images

    # ── URL construction ──────────────────────────────────────────────

    @staticmethod
    def build_list_url(
        listing_type: str,
        location: str = "valencia",
        page: int = 1,
    ) -> str:
        """Build a paginated habitaclia listing URL.

        Examples:
            rent  page 1:  https://www.habitaclia.com/alquiler-valencia.htm
            rent  page 3:  https://www.habitaclia.com/alquiler-valencia-2.htm
            sale  page 1:  https://www.habitaclia.com/viviendas-valencia.htm
        """
        op = _OPERATION_SLUG.get(listing_type, "alquiler")
        if page == 1:
            return f"{BASE_URL}/{op}-{location}.htm"
        return f"{BASE_URL}/{op}-{location}-{page - 1}.htm"

    # ── Price parsing ─────────────────────────────────────────────────

    @staticmethod
    def parse_price(text: str | None) -> float | None:
        """Parse a Spanish-formatted price string like '1.200 €'.

        Dots are thousands separators; commas are decimal separators.
        """
        if not text:
            return None
        cleaned = re.sub(r"[€\s]", "", text)
        cleaned = re.sub(r"/\w+", "", cleaned)  # strip /mes, /m², etc.
        if not cleaned:
            return None
        if "," in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except ValueError:
            logger.debug("Could not parse price: %r", text)
            return None

    # ── List page parsing ─────────────────────────────────────────────

    @staticmethod
    def parse_last_page(html: str) -> int:
        """Return the total page count from the pagination links."""
        tree = HTMLParser(html)
        last = 1
        for a in tree.css("div.pagination ul li a"):
            href = a.attributes.get("href", "")
            # Pages 2+ use a numeric suffix: alquiler-valencia-1.htm = page 2
            m = re.search(r"-(\d+)\.htm$", href)
            if m:
                last = max(last, int(m.group(1)) + 1)
        return last

    @staticmethod
    def parse_list_page(html: str) -> list[dict]:
        """Parse all listing articles from a habitaclia search-results page."""
        tree = HTMLParser(html)
        items: list[dict] = []

        for article in tree.css("article.js-list-item"):
            source_id = article.attributes.get("data-id", "").strip()
            if not source_id:
                continue

            data: dict = {"source_id": source_id}

            # Canonical URL (tracking params stripped)
            href = article.attributes.get("data-href", "")
            if href:
                data["source_url"] = _strip_tracking(href)

            # Transaction type
            tx = article.attributes.get("data-transaction", "RENT")
            data["listing_type"] = "rent" if tx == "RENT" else "sale"

            # Sub-category — attribute first, URL slug as fallback
            subtype = article.attributes.get("data-propertysubtype", "")
            data["sub_category"] = (
                SUBTYPE_MAP.get(subtype.upper()) or _subtype_from_url(href)
            )

            # Title
            title_el = article.css_first("h3.list-item-title a")
            if title_el:
                data["title"] = title_el.text(strip=True)

            # Location — "Valencia - El Carme"
            loc_el = article.css_first("p.list-item-location span")
            if loc_el:
                parts = [p.strip() for p in loc_el.text(strip=True).split(" - ", 1)]
                data["municipality"] = parts[0] if parts else None
                data["neighborhood"] = parts[1] if len(parts) > 1 else None

            # Price, size, bedrooms from the notify button's data attributes
            # (clean integers — no Spanish number-formatting ambiguity)
            notify_btn = article.css_first("button.js-notify")
            if notify_btn:
                pvp = notify_btn.attributes.get("data-pvp", "")
                if pvp:
                    try:
                        data["price"] = float(pvp)
                    except ValueError:
                        pass
                sup = notify_btn.attributes.get("data-sup", "")
                if sup:
                    try:
                        data["size"] = float(sup)
                    except ValueError:
                        pass
                hab = notify_btn.attributes.get("data-hab", "")
                if hab:
                    try:
                        data["bedrooms"] = int(hab)
                    except ValueError:
                        pass

            # Price fallback from itemprop span (when notify button absent)
            if "price" not in data:
                price_el = article.css_first("span[itemprop='price']")
                if price_el:
                    data["price"] = HabitacliaScraper.parse_price(
                        price_el.text(strip=True)
                    )

            # Bathrooms — not in data attributes; parsed from the feature paragraph
            feat_el = article.css_first("p.list-item-feature")
            if feat_el:
                feat_text = feat_el.text(strip=True)
                m = re.search(r"(\d+)\s*ba[ñn]", feat_text, re.IGNORECASE)
                if m:
                    data["bathrooms"] = int(m.group(1))

            # First image (listing has one; enrichment adds the full gallery)
            img_el = article.css_first("div.list-gallery-image img")
            if img_el:
                src = img_el.attributes.get("src", "")
                if src:
                    data["image"] = _normalise_image_url(src)

            items.append(data)

        return items

    # ── Property builder ──────────────────────────────────────────────

    @staticmethod
    def build_property(item: dict, listing_type: str | None = None) -> Property:
        """Assemble a Property from a parsed item dict."""
        source_id = str(item.get("source_id", ""))
        lt = item.get("listing_type") or listing_type or "rent"
        title = item.get("title") or f"Listing {source_id}"

        specs: dict = {}
        if item.get("size") is not None:
            specs["size"] = item["size"]
        if item.get("bedrooms") is not None:
            specs["bedrooms"] = item["bedrooms"]
        if item.get("bathrooms") is not None:
            specs["bathrooms"] = item["bathrooms"]
        specs.update(item.get("specs", {}))

        images: list[str] = []
        if item.get("image"):
            images.append(item["image"])
        images.extend(img for img in item.get("images", []) if img not in images)

        return Property(
            listing_type=lt,
            sub_category=item.get("sub_category"),
            title=title,
            description=item.get("description"),
            price=item.get("price"),
            rent_period="month" if lt == "rent" else None,
            location=item.get("municipality"),
            municipality=item.get("municipality"),
            neighborhood=item.get("neighborhood"),
            postal_code=item.get("postal_code"),
            province="Valencia",
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            images=images,
            specs=specs,
            features=item.get("features", []),
            source="habitaclia",
            source_id=source_id,
            source_url=item.get("source_url"),
        )

    # ── Detail page parsing ───────────────────────────────────────────

    @staticmethod
    def _parse_detail_data(html: str) -> dict:
        """Parse a habitaclia detail page into a raw data dict.

        Extracts coordinates from the embedded GMapsConfig JSON, all images
        from the WideMediaDTO JSON array, description, specs (size/bedrooms/
        bathrooms/floor), features, and neighbourhood.
        """
        tree = HTMLParser(html)
        data: dict = {}

        # ── Coordinates from embedded GMapsConfig JSON ────────────────
        # Pattern inside a JSON.parse("...") string: "VGPSLat":39.48,"VGPSLon":-0.38
        m = re.search(
            r'\\"VGPSLat\\"\s*:\s*([\d.]+).*?\\"VGPSLon\\"\s*:\s*(-?[\d.]+)',
            html, re.DOTALL,
        )
        if m:
            try:
                data["latitude"] = float(m.group(1))
                data["longitude"] = float(m.group(2))
            except ValueError:
                pass

        # ── Full image gallery from WideMediaDTO.image JSON.parse ─────
        # The value is a JS string (outer double-quotes, inner \"-escaped),
        # so we use json.loads twice: first to decode the JS string escapes,
        # then to parse the resulting JSON array.
        img_m = re.search(
            r'var\s+WideMediaDTO\b.*?\bimage\s*:\s*JSON\.parse\("((?:[^"\\]|\\.)*)"\)',
            html, re.DOTALL,
        )
        if img_m:
            try:
                img_json_str = json.loads('"' + img_m.group(1) + '"')
                imgs = json.loads(img_json_str)
                sorted_imgs = sorted(imgs, key=lambda x: x.get("Orden", 999))
                data["images"] = [
                    _normalise_image_url(img["URLXL"])
                    for img in sorted_imgs
                    if img.get("URLXL")
                ]
            except (json.JSONDecodeError, ValueError, KeyError, TypeError):
                logger.debug("Could not parse WideMediaDTO.image")

        # ── Description ───────────────────────────────────────────────
        desc_el = tree.css_first("p#js-detail-description")
        if desc_el:
            text = desc_el.text(strip=True)
            if text:
                data["description"] = text

        # ── Specs from the feature-container (size, bedrooms, bathrooms) ──
        # Each <li class="feature"> has a <strong> with the numeric value
        # followed by the unit text — select structurally, not by pattern.
        specs: dict = {}
        for li in tree.css("ul.feature-container li.feature"):
            strong = li.css_first("strong")
            if not strong:
                continue
            val_str = strong.text(strip=True)
            rest = li.text(strip=True)[len(val_str):].strip()
            try:
                if "m" in rest and ("2" in rest or "²" in rest):
                    specs["size"] = round(float(val_str.replace(",", ".")), 2)
                elif "hab" in rest.lower():
                    specs["bedrooms"] = int(val_str)
                elif "ba" in rest.lower():
                    specs["bathrooms"] = int(val_str)
            except ValueError:
                pass

        # ── Features and floor from characteristic article sections ───
        features: list[str] = []
        for article in tree.css("article.has-aside"):
            h3 = article.css_first("h3")
            if not h3:
                continue
            heading = h3.text(strip=True)
            is_distribution = heading in _DISTRIBUTION_HEADINGS

            for li in article.css("ul li"):
                # Skip the energy-rating block (has nested div structure)
                if li.css_first(".energy-rating") or li.css_first(".detail-rating"):
                    continue
                text = li.text(strip=True)
                if not text:
                    continue

                if is_distribution:
                    # Size, bedrooms, bathrooms already in specs — only add
                    # non-numeric items like "Cocina tipo office"
                    if not _NUMERIC_SPEC_RE.search(text):
                        features.append(text)
                    continue

                floor_match = _FLOOR_RE.search(text)
                if floor_match:
                    try:
                        specs["floor"] = int(floor_match.group(1))
                    except ValueError:
                        pass
                    continue

                features.append(text)

        if specs:
            data["specs"] = specs
        if features:
            data["features"] = features

        # ── Neighbourhood / postal code from pageViewedEventData JSON ─
        pved_m = re.search(
            r'pageViewedEventData\s*:\s*JSON\.parse\("((?:[^"\\]|\\.)*)"\)',
            html,
        )
        if pved_m:
            try:
                pved_str = json.loads('"' + pved_m.group(1) + '"')
                pved = json.loads(pved_str)
                if pved.get("neighbourhood"):
                    data["neighborhood"] = pved["neighbourhood"]
                postal = pved.get("postal_code", "")
                if postal and postal != "00000":
                    data["postal_code"] = postal
            except (json.JSONDecodeError, ValueError, TypeError):
                logger.debug("Could not parse pageViewedEventData")

        return data

    # ── BaseScraper ABC ───────────────────────────────────────────────

    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        detail = self._parse_detail_data(html)
        if not detail:
            return None
        tree = HTMLParser(html)
        canon = tree.css_first("link[rel='canonical']")
        canonical_url = canon.attributes.get("href", "") if canon else ""
        lt = "rent" if "/alquiler-" in canonical_url else "sale"
        item = {**detail, "source_id": ad_id, "source_url": canonical_url or None}
        return self.build_property(item, listing_type=lt)

    # ── Enrichment ────────────────────────────────────────────────────

    def enrich_property(self, prop: Property) -> Property:
        """Fetch the detail page and return a fully enriched copy of the property."""
        if not prop.source_url:
            logger.debug("No source_url for %s, skipping", prop.source_id)
            return prop

        self._delay_sync()
        try:
            html = self._fetch_page(prop.source_url)
        except UserStop:
            raise  # propagate so the CLI enrich loop can save progress
        except FetchError as e:
            logger.warning("Failed to enrich %s: %s", prop.source_id, e)
            return prop

        detail = self._parse_detail_data(html)

        # Click through the gallery (human simulation + captures any lazy-loaded images).
        # Use gallery images if they outnumber what the JS variable provided.
        gallery_images = self._click_through_gallery()
        if len(gallery_images) > len(detail.get("images", [])):
            detail["images"] = gallery_images

        merged_specs = {**prop.specs, **detail.get("specs", {})}
        merged_features = list(dict.fromkeys(prop.features + detail.get("features", [])))
        updates: dict = {
            "description": detail.get("description") or prop.description,
            "images": detail.get("images") or prop.images,
            "specs": merged_specs,
            "features": merged_features,
            "enriched": True,
        }
        if detail.get("latitude") is not None:
            updates["latitude"] = detail["latitude"]
        if detail.get("longitude") is not None:
            updates["longitude"] = detail["longitude"]
        if detail.get("neighborhood"):
            updates["neighborhood"] = detail["neighborhood"]
        if detail.get("postal_code"):
            updates["postal_code"] = detail["postal_code"]

        return prop.model_copy(update=updates)

    # ── Main orchestration ────────────────────────────────────────────

    def scrape(
        self,
        listing_type: str = "rent",
        max_pages: int = 9999,
        enrich: bool = False,
        location: str = "valencia",
    ) -> list[Property]:
        """Scrape property listings from habitaclia.com.

        Args:
            listing_type: "sale" or "rent"
            max_pages: Maximum list pages to fetch (default: all)
            enrich: Unused — run ``uv run python -m src.cli enrich <file>`` instead
            location: URL location slug (default: "valencia")
        """
        all_properties: list[Property] = []
        seen_ids: set[str] = set()
        total_pages = max_pages

        self._stop_requested = False
        self._install_sigint_handler()
        try:
            # Warmup: visit homepage so Imperva can complete its JS handshake
            # and store the reese84 trust token before we start hitting list pages.
            logger.info("Warming up session: %s", BASE_URL)
            try:
                self._fetch_page(BASE_URL + "/")
                self._delay_sync()
            except (FetchError, UserStop):
                pass

            # ── Page 1: navigate via goto() ───────────────────────────
            first_url = self.build_list_url(listing_type, location, page=1)
            logger.info("Fetching page 1: %s", first_url)
            try:
                html = self._fetch_page(first_url)
            except UserStop:
                logger.info("Stopped by user — saving %d properties", len(all_properties))
                return all_properties
            except FetchError as e:
                logger.error("Failed to fetch page 1: %s", e)
                return all_properties

            last = self.parse_last_page(html)
            total_pages = min(max_pages, last)
            logger.info(
                "listing_type=%s location=%s: %d pages",
                listing_type, location, total_pages,
            )

            page = 1
            while True:
                items = self.parse_list_page(html)
                if not items:
                    logger.info("No items on page %d, stopping", page)
                    break

                page_new = 0
                for item in items:
                    sid = item.get("source_id", "")
                    if not sid or sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    try:
                        prop = self.build_property(item, listing_type)
                        all_properties.append(prop)
                        page_new += 1
                    except Exception as e:
                        logger.warning(
                            "Failed to build property %s: %s", sid, e, exc_info=True
                        )

                logger.info(
                    "Page %d/%d: +%d new (total %d)",
                    page, total_pages, page_new, len(all_properties),
                )

                if page >= total_pages:
                    break

                # ── Pages 2+: simulate human clicking "Siguiente" ─────
                self._delay_sync()
                if not self._check_and_pause():
                    logger.info(
                        "Scraping stopped by user after %d properties",
                        len(all_properties),
                    )
                    return all_properties

                try:
                    html = self._click_next_page()
                except UserStop:
                    logger.info("Stopped by user — saving %d properties", len(all_properties))
                    return all_properties
                except FetchError as e:
                    logger.error("Blocked navigating to page %d: %s", page + 1, e)
                    break

                if html is None:
                    logger.info("No 'Siguiente' link found after page %d", page)
                    break

                page += 1

        finally:
            self._uninstall_sigint_handler()

        logger.info("Total: %d unique properties scraped", len(all_properties))
        return all_properties
