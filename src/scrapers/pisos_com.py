"""Scraper for pisos.com – Spain's SSR property portal."""

from __future__ import annotations

import html as _html
import json
import logging
import math
import re
import time
import unicodedata

import httpx
from selectolax.parser import HTMLParser

from src.config import settings
from src.models import Property
from src.scrapers.base import BaseScraper, FetchError

logger = logging.getLogger(__name__)

BASE_URL = "https://www.pisos.com"
ITEMS_PER_PAGE = 30


# List-page type slug → sub_category
TYPE_TO_SUB_CATEGORY: dict[str, str] = {
    "pisos": "apartment",
    "casas": "house",
    "locales_comerciales": "commerce",
    "terrenos": "plot",
}

# Individual listing URL type slug → sub_category
_URL_TYPE_MAP: dict[str, str] = {
    "piso": "apartment",
    "atico": "apartment",
    "piso_duplex": "apartment",
    "estudio": "apartment",
    "apartamento": "apartment",
    "casa_unifamiliar": "house",
    "chalet": "house",
    "chalet_adosado": "house",
    "finca_rustica": "house",
    "local_comercial": "commerce",
    "nave_industrial": "commerce",
    "oficina": "commerce",
    "terreno": "plot",
    "solar": "plot",
}

# Location slug for Valencia province
LOCATION_SLUG = "valencia"


class PisosComScraper(BaseScraper):
    """Scraper for pisos.com using httpx + selectolax."""

    def __init__(self) -> None:
        super().__init__()
        self._http: httpx.Client | None = None

    def _ensure_http(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "es-ES,es;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                },
                follow_redirects=True,
                timeout=30.0,
            )
        return self._http

    def close(self) -> None:
        if self._http is not None:
            self._http.close()
            self._http = None
        super().close()

    RATE_LIMIT_COOLDOWN = 60  # seconds to wait on 403

    def _fetch_page(self, url: str) -> str:
        client = self._ensure_http()
        for attempt in range(settings.MAX_RETRIES):
            resp = client.get(url)
            if resp.status_code == 403:
                wait = self.RATE_LIMIT_COOLDOWN * (attempt + 1)
                logger.warning(
                    "Got 403 (attempt %d/%d), cooling down %ds",
                    attempt + 1, settings.MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                raise FetchError(resp.status_code, url)
            return resp.text
        raise FetchError(403, url)

    # ── URL construction ──────────────────────────────────────────────

    @staticmethod
    def build_list_url(
        listing_type: str,
        type_slug: str,
        location_slug: str = LOCATION_SLUG,
        page: int = 1,
    ) -> str:
        """Build a paginated list-page URL.

        Examples:
            sale page 1:  https://www.pisos.com/venta/pisos-valencia/
            sale page 2:  https://www.pisos.com/venta/pisos-valencia/2/
            rent page 1:  https://www.pisos.com/alquiler/pisos-valencia/
        """
        prefix = "alquiler" if listing_type == "rent" else "venta"
        if page > 1:
            return f"{BASE_URL}/{prefix}/{type_slug}-{location_slug}/{page}/"
        return f"{BASE_URL}/{prefix}/{type_slug}-{location_slug}/"

    # ── Price parsing ─────────────────────────────────────────────────

    @staticmethod
    def parse_price(text: str | None) -> float | None:
        """Parse Spanish-format price like '133.000 €' or '1.500 €/mes'.

        In Spanish number formatting, dots are thousands separators and
        commas are decimal separators.
        """
        if not text:
            return None
        cleaned = text.replace("€", "")
        cleaned = re.sub(r"/\w+", "", cleaned)  # strip /mes, /sem, etc.
        cleaned = cleaned.strip()
        if not cleaned:
            return None
        if "," in cleaned:
            # comma = decimal separator; dots = thousands
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # dots = thousands separators only
            cleaned = cleaned.replace(".", "")
        cleaned = cleaned.strip()
        try:
            return float(cleaned)
        except ValueError:
            logger.debug("Could not parse price: %r", text)
            return None

    # ── Characteristics parsing ───────────────────────────────────────

    @staticmethod
    def _parse_chars(chars: list[str]) -> dict:
        """Parse characteristic strings like '4 habs.', '2 baños', '100 m²'.

        Returns a specs dict with integer bedrooms, bathrooms, size.
        """
        specs: dict = {}
        for char in chars:
            c = char.strip()
            m = re.match(r"(\d+)\s*hab", c, re.IGNORECASE)
            if m:
                specs["bedrooms"] = int(m.group(1))
                continue
            m = re.match(r"(\d+)\s*ba[ñn]", c, re.IGNORECASE)
            if m:
                specs["bathrooms"] = int(m.group(1))
                continue
            m = re.match(r"([\d.,]+)\s*m[²2]", c, re.IGNORECASE)
            if m:
                size_str = m.group(1).replace(".", "").replace(",", ".")
                try:
                    specs["size"] = round(float(size_str), 2)
                except ValueError:
                    pass
        return specs

    # ── Image URL normalisation ───────────────────────────────────────

    @staticmethod
    def _normalise_image_url(url: str) -> str:
        """Rewrite any fotos.imghs.net image to h700-wp (HD) format.

        All pisos.com image formats share the same path after the format segment:
            fch-wp/1055/307/photo.jpg  →  h700-wp/1055/307/photo.jpg
        """
        return re.sub(r"(https?://fotos\.imghs\.net)/[^/]+/", r"\1/h700-wp/", url)

    # ── Sub-category ──────────────────────────────────────────────────

    @staticmethod
    def guess_sub_category_from_url(url: str) -> str | None:
        """Infer sub_category from an individual listing href.

        Examples:
            /comprar/piso-sueca-123/ → "apartment"
            /comprar/chalet-benidorm-456/ → "house"
        """
        m = re.search(r"/(?:comprar|alquiler)/([^-/]+)", url)
        if m:
            return _URL_TYPE_MAP.get(m.group(1))
        return None

    # ── Province normalisation ────────────────────────────────────────

    @staticmethod
    def _normalise_province(raw: str | None) -> str | None:
        if not raw:
            return None
        # Strip accents for robust matching ("Valècia" → "valencia", "Castellón" → "castellon")
        p = unicodedata.normalize("NFD", raw.lower())
        p = "".join(c for c in p if unicodedata.category(c) != "Mn")
        if "valenci" in p:
            return "Valencia"
        if "alicant" in p or "alacant" in p:
            return "Alicante"
        if "castell" in p:
            return "Castellón"
        return raw

    # ── List page parsing ─────────────────────────────────────────────

    @staticmethod
    def parse_total_count(html: str) -> int:
        """Extract total listing count from 'N de M resultados'."""
        tree = HTMLParser(html)
        for el in tree.css("span.pagination__counter"):
            m = re.search(r"de\s+([\d.]+)\s+resultados", el.text())
            if m:
                return int(m.group(1).replace(".", "").replace(",", ""))
        return 0

    @staticmethod
    def parse_next_page_url(html: str) -> str | None:
        """Return the next-page absolute URL, or None on the last page."""
        tree = HTMLParser(html)
        nxt = tree.css_first("div.pagination__next a")
        if nxt:
            href = nxt.attributes.get("href", "")
            if href:
                return href if href.startswith("http") else BASE_URL + href
        return None

    @staticmethod
    def parse_list_page(html: str) -> list[dict]:
        """Parse all ad-preview listings from a list page HTML."""
        tree = HTMLParser(html)
        items: list[dict] = []

        for div in tree.css("div.ad-preview"):
            source_id = div.attributes.get("id", "")
            if not source_id:
                continue

            data: dict = {"source_id": source_id}

            # Listing URL
            href = div.attributes.get("data-lnk-href", "")
            if href:
                data["source_url"] = (
                    href if href.startswith("http") else BASE_URL + href
                )
                data["sub_category"] = PisosComScraper.guess_sub_category_from_url(href)

            # Image — first carousel slide has a direct src; lazy slides use data-src
            first_img = div.css_first("div.carousel__main-photo img")
            if first_img:
                src = first_img.attributes.get("src") or first_img.attributes.get("data-src", "")
                if src and "default_nophoto" not in src:
                    data["image"] = PisosComScraper._normalise_image_url(src)

            # Price — skip "A consultar", extract rent period from nested span
            price_el = div.css_first("span.ad-preview__price")
            if price_el:
                price_text = price_el.text(strip=True)
                if "consultar" in price_text.lower():
                    continue
                data["price_text"] = price_text
                period_span = price_el.css_first("span")
                if period_span:
                    period = period_span.text(strip=True).lower()
                    if "mes" in period:
                        data["rent_period"] = "month"
                    elif "sem" in period:
                        data["rent_period"] = "week"

            # Title
            title_el = div.css_first("a.ad-preview__title")
            if title_el:
                data["title"] = title_el.text(strip=True)

            # Subtitle — may be "Sueca" or "Playa de Gandia (Gandia)"
            subtitle_el = div.css_first("p.ad-preview__subtitle")
            if subtitle_el:
                data["subtitle"] = subtitle_el.text(strip=True)

            # Characteristics
            chars = [el.text(strip=True) for el in div.css("p.ad-preview__char")]
            if chars:
                data["chars"] = chars

            # Short description
            desc_el = div.css_first("p.ad-preview__description")
            if desc_el:
                data["description"] = desc_el.text(strip=True)

            # Schema.org JSON-LD — provides geo coordinates, address, and image
            ld_script = div.css_first('script[type="application/ld+json"]')
            if ld_script:
                try:
                    ld = json.loads(ld_script.text())
                    geo = ld.get("geo", {})
                    lat = geo.get("latitude")
                    lng = geo.get("longitude")
                    if lat and lng:
                        data["latitude"] = float(lat)
                        data["longitude"] = float(lng)
                    address = ld.get("address", {})
                    locality = address.get("addressLocality")
                    region = address.get("addressRegion")
                    if locality:
                        data["address_locality"] = _html.unescape(locality)
                    if region:
                        data["address_region"] = _html.unescape(region)
                    # JSON-LD image as fallback if carousel img not found
                    if "image" not in data:
                        ld_img = ld.get("image") or ld.get("photo", {}).get("contentUrl")
                        if ld_img:
                            data["image"] = PisosComScraper._normalise_image_url(ld_img)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

            items.append(data)

        return items

    # ── Property builder ──────────────────────────────────────────────

    @staticmethod
    def build_property(
        item: dict,
        listing_type: str,
        type_sub_category: str | None = None,
    ) -> Property:
        """Assemble a Property from a parsed item dict."""
        source_id = str(item.get("source_id", ""))
        title = item.get("title") or f"Listing {source_id}"
        price = PisosComScraper.parse_price(item.get("price_text"))

        # Sub-category: from individual URL slug, fall back to type-level slug
        sub_cat = item.get("sub_category") or type_sub_category

        specs = PisosComScraper._parse_chars(item.get("chars", []))

        # Municipality: prefer JSON-LD addressLocality (clean), fall back to subtitle
        municipality = item.get("address_locality") or item.get("subtitle")

        province = PisosComScraper._normalise_province(item.get("address_region"))

        images = []
        if item.get("image"):
            images.append(item["image"])

        return Property(
            listing_type=listing_type,
            sub_category=sub_cat,
            title=title,
            description=item.get("description"),
            price=price,
            rent_period=item.get("rent_period"),
            location=municipality,
            municipality=municipality,
            province=province,
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            images=images,
            specs=specs,
            source="pisos-com",
            source_id=source_id,
            source_url=item.get("source_url"),
        )

    # ── Detail page parsing ───────────────────────────────────────────

    @staticmethod
    def _parse_detail_data(html: str) -> dict:
        """Parse a pisos.com property detail page into a data dict."""
        from urllib.parse import parse_qs

        tree = HTMLParser(html)
        data: dict = {}

        # Price from data attribute
        el = tree.css_first("[data-ad-price]")
        if el:
            try:
                data["price"] = float(el.attributes["data-ad-price"])
            except (KeyError, ValueError):
                pass

        # Rent period from price value span (e.g. "1.350 €<span>/mes</span>")
        price_val_el = tree.css_first(".price__value")
        if price_val_el:
            period_span = price_val_el.css_first("span")
            if period_span:
                period = period_span.text(strip=True).lower()
                if "mes" in period:
                    data["rent_period"] = "month"
                elif "sem" in period:
                    data["rent_period"] = "week"

        # Title
        h1 = tree.css_first("h1")
        if h1:
            data["title"] = h1.text(strip=True)

        # Full description
        desc_el = tree.css_first(".description__content")
        if desc_el:
            text = desc_el.text(strip=True)
            if text:
                data["description"] = text

        # Coordinates from data-params query string
        loc_el = tree.css_first("[data-params]")
        if loc_el:
            params = parse_qs(loc_el.attributes.get("data-params", ""))
            try:
                data["latitude"] = float(params["latitude"][0])
                data["longitude"] = float(params["longitude"][0])
            except (KeyError, ValueError, IndexError):
                pass

        # Images — masonry grid; normalize all fotos.imghs.net URLs to h700-wp (HD)
        # Skip no-photo placeholder items and default_nophoto URLs
        images: list[str] = []
        for item in tree.css(".masonry__item"):
            if "masonry__item--no-photos" in (item.attributes.get("class") or ""):
                continue
            for img_el in item.css("img"):
                src = img_el.attributes.get("src") or img_el.attributes.get("data-src", "")
                if src and "default_nophoto" not in src:
                    norm = PisosComScraper._normalise_image_url(src)
                    if norm not in images:
                        images.append(norm)

        # Fallback: any carousel slide image (e.g. pages with no masonry)
        if not images:
            for img_el in tree.css(".carousel__slide img"):
                src = img_el.attributes.get("src", "")
                if src and "default_nophoto" not in src:
                    norm = PisosComScraper._normalise_image_url(src)
                    if norm not in images:
                        images.append(norm)

        if images:
            data["images"] = images

        # Specs (label+value) and features (boolean flags, label only)
        raw_specs: dict = {}
        char_strings: list[str] = []
        features: list[str] = []
        for feat in tree.css(".features__feature"):
            lbl_el = feat.css_first(".features__label")
            val_el = feat.css_first(".features__value")
            if not lbl_el:
                continue
            label = lbl_el.text(strip=True).rstrip(":").strip()
            if not label:
                continue
            if val_el:
                value = val_el.text(strip=True)
                if value:
                    raw_specs[label] = value
                    char_strings.append(value)
            else:
                # Boolean amenity flag — no value element
                features.append(label)

        parsed = PisosComScraper._parse_chars(char_strings)
        if raw_specs or parsed:
            data["specs"] = {**raw_specs, **parsed}
        if features:
            data["features"] = features

        return data

    # ── BaseScraper ABC ───────────────────────────────────────────────

    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        detail = self._parse_detail_data(html)
        if not detail:
            return None
        listing_type = "rent" if "/alquiler/" in (detail.get("source_url") or "") else "sale"
        return self.build_property({**detail, "source_id": ad_id}, listing_type=listing_type)

    # ── Enrichment ────────────────────────────────────────────────────

    def enrich_property(self, prop: Property) -> Property:
        """Fetch detail page for an existing Property and return an enriched copy.

        Returns the original property unchanged if source_url is missing or fetch fails.
        """
        if not prop.source_url:
            logger.debug("No source_url for %s, skipping", prop.source_id)
            return prop

        self._delay_sync()
        try:
            html = self._fetch_page(prop.source_url)
        except FetchError as e:
            logger.warning("Failed to enrich %s: %s", prop.source_id, e)
            return prop

        detail = self._parse_detail_data(html)

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
        if detail.get("rent_period") is not None:
            updates["rent_period"] = detail["rent_period"]

        return prop.model_copy(update=updates)

    # ── Main orchestration ────────────────────────────────────────────

    def scrape(
        self,
        listing_type: str = "sale",
        max_pages: int = 9999,
        enrich: bool = False,
        type_slugs: list[str] | None = None,
        location_slug: str = LOCATION_SLUG,
    ) -> list[Property]:
        """Scrape property listings for multiple type slugs.

        Args:
            listing_type: "sale" or "rent"
            max_pages: Max pages per type slug
            enrich: Unused (list pages already contain coordinates and images)
            type_slugs: Property type slugs to scrape (default: all 4)
            location_slug: Location segment in the URL (default: "valencia")
        """
        if type_slugs is None:
            type_slugs = list(TYPE_TO_SUB_CATEGORY.keys())

        all_properties: list[Property] = []
        seen_ids: set[str] = set()

        self._stop_requested = False
        self._install_sigint_handler()
        try:
            for type_slug in type_slugs:
                sub_category = TYPE_TO_SUB_CATEGORY.get(type_slug)
                logger.info("Scraping type=%s listing_type=%s", type_slug, listing_type)
                page = 1
                total_pages = max_pages  # refined after page 1

                while page <= min(max_pages, total_pages):
                    url = self.build_list_url(
                        listing_type=listing_type,
                        type_slug=type_slug,
                        location_slug=location_slug,
                        page=page,
                    )
                    logger.debug("Fetching page %d: %s", page, url)

                    try:
                        html = self._fetch_page(url)
                    except FetchError as e:
                        logger.error("Failed to fetch page %d: %s", page, e)
                        break

                    items = self.parse_list_page(html)
                    if not items:
                        logger.info("No items on page %d, stopping type=%s", page, type_slug)
                        break

                    if page == 1:
                        total_count = self.parse_total_count(html)
                        if total_count:
                            total_pages = math.ceil(total_count / ITEMS_PER_PAGE)
                            logger.info(
                                "Type %s: %d listings across %d pages",
                                type_slug, total_count, total_pages,
                            )

                    for item in items:
                        sid = item.get("source_id", "")
                        if sid in seen_ids:
                            continue
                        seen_ids.add(sid)
                        prop = self.build_property(item, listing_type, sub_category)
                        all_properties.append(prop)

                    logger.info("Page %d: parsed %d items", page, len(items))
                    page += 1
                    self._delay_sync()
                    if not self._check_and_pause():
                        logger.info("Scraping stopped by user after %d properties", len(all_properties))
                        return all_properties
        finally:
            self._uninstall_sigint_handler()

        logger.info("Total: %d unique properties scraped", len(all_properties))
        return all_properties
