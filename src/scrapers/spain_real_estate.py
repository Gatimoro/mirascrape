"""Scraper for spain-real.estate – a PHP server-side-rendered portal."""

from __future__ import annotations

import json
import logging
import math
import re
import time
from urllib.parse import urlencode

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.models import Property, Translation
from src.scrapers.base import BaseScraper, FetchError

logger = logging.getLogger(__name__)

BASE_URL = "https://spain-real.estate"

# Tabs and their mapping to sub_category
TAB_SUB_CATEGORY: dict[str, str | None] = {
    "apartment": "apartment",
    "villa": "house",
    "commercial": "commerce",
    "land": "plot",
}

ITEMS_PER_PAGE = 24

# Province lookup for Valencian Community cities
CITY_TO_PROVINCE: dict[str, str] = {
    "valencia": "Valencia",
    "alicante": "Alicante",
    "castellón": "Castellón",
    "castellon": "Castellón",
    "benidorm": "Alicante",
    "torrevieja": "Alicante",
    "calpe": "Alicante",
    "denia": "Alicante",
    "altea": "Alicante",
    "orihuela": "Alicante",
    "elche": "Alicante",
    "gandia": "Valencia",
    "sagunto": "Valencia",
    "xàtiva": "Valencia",
    "jávea": "Alicante",
    "javea": "Alicante",
    "villajoyosa": "Alicante",
    "guardamar del segura": "Alicante",
    "pilar de la horadada": "Alicante",
    "benicàssim": "Castellón",
    "peñíscola": "Castellón",
    "vinaròs": "Castellón",
}


class SpainRealEstateScraper(BaseScraper):
    """Scraper for spain-real.estate using httpx with browser fallback."""

    def __init__(self) -> None:
        super().__init__()
        self._http: httpx.Client | None = None
        self._use_browser = False

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
                    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
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

    RATE_LIMIT_COOLDOWN = 60  # seconds to wait on 403 before retrying

    def _fetch_page(self, url: str, accept_language: str | None = None) -> str:
        """Fetch a page with rate-limit cooldown on 403.

        Args:
            accept_language: Override Accept-Language header for this request only.
                             Use for translation pages to ensure the site serves the
                             correct locale (e.g. "es,es-ES;q=0.9").
        """
        if self._use_browser:
            return self._fetch_with_browser(url)

        client = self._ensure_http()
        req_headers = {"Accept-Language": accept_language} if accept_language else {}
        for attempt in range(settings.MAX_RETRIES):
            resp = client.get(url, headers=req_headers)
            if resp.status_code == 403:
                wait = self.RATE_LIMIT_COOLDOWN * (attempt + 1)
                logger.warning(
                    "Got 403 (attempt %d/%d), cooling down %ds before retry",
                    attempt + 1, settings.MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                raise FetchError(resp.status_code, url)
            return resp.text

        # All retries hit 403 — try browser as last resort
        logger.warning("All httpx retries got 403, switching to browser fallback")
        self._use_browser = True
        return self._fetch_with_browser(url)

    def _fetch_with_browser(self, url: str) -> str:
        self._ensure_browser()
        assert self._page is not None
        resp = self._page.goto(url, wait_until="domcontentloaded")
        if resp and resp.status >= 400:
            raise FetchError(resp.status, url)
        return self._page.content()

    # ── URL construction ─────────────────────────────────────────────

    @staticmethod
    def build_list_url(
        listing_type: str = "sale",
        tab: str = "apartment",
        page: int = 1,
        region: str = "Valencian Community",
        region_id: int = 4120,
    ) -> str:
        """Build a list-page URL for spain-real.estate."""
        base = f"{BASE_URL}/rent/" if listing_type == "rent" else f"{BASE_URL}/property/"

        params: dict[str, str | int] = {"tab": tab}
        if region:
            params["region"] = region
        if region_id:
            params["prj_region[]"] = region_id
        if page > 1:
            params["n"] = page

        return f"{base}?{urlencode(params)}"

    # ── Price parsing ────────────────────────────────────────────────

    @staticmethod
    def parse_price(text: str | None) -> float | None:
        """Parse price text like '€ 181 000' or '€ 1 500 monthly'."""
        if not text:
            return None
        # Remove currency symbols
        cleaned = text.replace("\u20ac", "").replace("€", "")
        # Strip period words (no \b — may be glued like "977monthly")
        cleaned = re.sub(r"(monthly|yearly|weekly)", "", cleaned, flags=re.IGNORECASE)
        # Remove all whitespace variants (regular, non-breaking, etc.)
        cleaned = cleaned.replace("\xa0", "").replace("\u00a0", "").replace(" ", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            logger.debug("Could not parse price: %r", text)
            return None

    # ── Sub-category guessing ────────────────────────────────────────

    @staticmethod
    def guess_sub_category(title: str) -> str | None:
        """Map English terms in the title to model sub_category literals."""
        t = title.lower()
        if any(w in t for w in ("apartment", "flat", "penthouse", "duplex", "studio", "townhouse")):
            return "apartment"
        if any(w in t for w in ("villa", "house", "chalet", "bungalow", "finca")):
            return "house"
        if any(w in t for w in ("commercial", "office", "shop", "hotel", "business", "restaurant")):
            return "commerce"
        if any(w in t for w in ("land", "plot")):
            return "plot"
        return None

    # ── Location extraction ──────────────────────────────────────────

    @staticmethod
    def extract_location_from_title(title: str) -> dict[str, str | None]:
        """Extract city and province from 'Type in City, Spain' pattern."""
        result: dict[str, str | None] = {"municipality": None, "province": None}
        m = re.search(r"\bin\s+(.+?)(?:,\s*Spain|\s+No\.)", title, re.IGNORECASE)
        if not m:
            return result
        # "Benidorm, Alicante" or just "Valencia"
        parts = [p.strip() for p in m.group(1).split(",") if p.strip()]
        if parts:
            city = parts[0]
            result["municipality"] = city
            # Check province lookup
            province = CITY_TO_PROVINCE.get(city.lower())
            if province:
                result["province"] = province
            elif len(parts) > 1:
                result["province"] = parts[1]
        return result

    # ── List page parsing ────────────────────────────────────────────

    @staticmethod
    def parse_total_count(html: str) -> int:
        """Extract total listings count from the page."""
        tree = HTMLParser(html)
        # Try total_counter: "25 - 48 out of 15089"
        counter = tree.css_first("div.total_counter")
        if counter:
            m = re.search(r"out of\s+([\d,]+)", counter.text())
            if m:
                return int(m.group(1).replace(",", ""))
        # Fallback: objects_list totals: "15089 properties"
        totals = tree.css_first("div.objects_list.totals span")
        if totals:
            m = re.search(r"([\d,]+)", totals.text())
            if m:
                return int(m.group(1).replace(",", ""))
        return 0

    @staticmethod
    def parse_list_page(html: str) -> list[dict]:
        """Parse all listing items from a list page HTML."""
        tree = HTMLParser(html)
        items: list[dict] = []

        for li in tree.css("div.objects-list ul > li[data-object]"):
            data: dict = {}
            obj_id = li.attributes.get("data-object")
            if not obj_id:
                continue
            data["source_id"] = str(obj_id)

            # Title + URL
            title_a = li.css_first("div.title a")
            if title_a:
                data["title"] = title_a.text(strip=True)
                href = title_a.attributes.get("href", "")
                if href and not href.startswith("http"):
                    href = BASE_URL + href
                data["source_url"] = href

            # Price - first <span> inside div.price > span
            price_div = li.css_first("div.price")
            if price_div:
                # Skip sold-out listings
                sold = price_div.css_first("span.small")
                if sold and "sold" in sold.text(strip=True).lower():
                    continue

                spans = price_div.css("span")
                for span in spans:
                    txt = span.text(strip=True)
                    if "\u20ac" in txt or "€" in txt:
                        data["price_text"] = txt
                        break
                # Check if rental
                rent_period = li.css_first("span.rent-period")
                if rent_period:
                    data["is_rental"] = True

            # Params
            for key in ("rooms", "bedrooms", "bathrooms"):
                el = li.css_first(f"span.{key} b")
                if el:
                    data[key] = el.text(strip=True)
            area_el = li.css_first("span.area b")
            if area_el:
                data["area"] = area_el.text(strip=True)

            # Excerpt
            excerpt = li.css_first("div.excerpt")
            if excerpt:
                data["excerpt"] = excerpt.text(strip=True)
                # Remove trailing "Details" link text
                if data["excerpt"].endswith("Details"):
                    data["excerpt"] = data["excerpt"][:-7].rstrip(". ")

            # Thumbnail
            thumb = li.css_first("img.thumb")
            if thumb:
                data["thumbnail"] = thumb.attributes.get("src", "")

            items.append(data)

        return items

    @staticmethod
    def parse_last_page(html: str) -> int:
        """Extract the last page number from pagination."""
        tree = HTMLParser(html)
        max_page = 1
        for li in tree.css("ul.pagination li"):
            a = li.css_first("a")
            text = a.text(strip=True) if a else li.text(strip=True)
            try:
                page_num = int(text)
                max_page = max(max_page, page_num)
            except ValueError:
                continue
        return max_page

    # ── Detail page parsing ──────────────────────────────────────────

    @staticmethod
    def _parse_detail_data(html: str, source_id: str) -> dict:
        """Parse a property detail page for extra data (coordinates, images, features)."""
        tree = HTMLParser(html)
        data: dict = {}

        # Coordinates from OBJECT_MAP_DATA: {"lat_lng":[{"lat":"41.58","lng":"2.29",...}]}
        m = re.search(r"OBJECT_MAP_DATA\s*=\s*(\{.*?\});", html, re.DOTALL)
        if m:
            try:
                map_data = json.loads(m.group(1))
                # The key is "lat_lng" as a string; iterate first entry's list
                for _key, entries in map_data.items():
                    if isinstance(entries, list) and entries:
                        entry = entries[0]
                        lat = entry.get("lat")
                        lng = entry.get("lng")
                        if lat and lng:
                            data["latitude"] = float(lat)
                            data["longitude"] = float(lng)
                        break
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
        # Fallback: schema.org meta tags
        if "latitude" not in data:
            lat_el = tree.css_first('meta[itemprop="latitude"]')
            lng_el = tree.css_first('meta[itemprop="longitude"]')
            if lat_el and lng_el:
                try:
                    data["latitude"] = float(lat_el.attributes.get("content", ""))
                    data["longitude"] = float(lng_el.attributes.get("content", ""))
                except ValueError:
                    pass

        # Price from schema.org (clean integer, no parsing needed)
        price_meta = tree.css_first('meta[itemprop="price"]')
        if price_meta:
            try:
                data["detail_price"] = float(price_meta.attributes.get("content", ""))
            except ValueError:
                pass

        # Gallery images — full-res from data-real attribute
        images: list[str] = []
        for img in tree.css("#gallery_container .thumbs img"):
            src = (
                img.attributes.get("data-real")
                or img.attributes.get("data-big")
                or img.attributes.get("src", "")
            )
            if src and src not in images:
                images.append(src)
        # Fallback: main image
        if not images:
            main_img = tree.css_first(".main_image img")
            if main_img:
                src = main_img.attributes.get("src", "")
                if src:
                    images.append(src)
        if images:
            data["images"] = images

        # Features from all feature lists (wrapped and non-wrapped)
        features: list[str] = []
        for li_el in tree.css(".features ul li"):
            text = li_el.text(strip=True)
            if text:
                features.append(text)
        if features:
            data["features"] = features

        # Description from article div
        desc_el = tree.css_first('div.article[itemprop="description"], div.article')
        if desc_el:
            # Remove the heading element before extracting text
            h2 = desc_el.css_first("h2")
            if h2:
                h2.decompose()
            text = desc_el.text(strip=True)
            if text:
                data["description"] = text

        # Specs from right sidebar parameters
        specs: dict[str, str] = {}
        for div in tree.css(".right_block.parameters .params > div"):
            name_el = div.css_first("span.name")
            value_el = div.css_first("span.value")
            if name_el and value_el:
                key = name_el.text(strip=True)
                value = value_el.text(strip=True)
                if key and value:
                    specs[key] = value
        # Also grab schema.org room counts
        for prop_name, spec_key in [
            ("numberOfRooms", "rooms"),
            ("numberOfBedrooms", "bedrooms"),
            ("numberOfBathroomsTotal", "bathrooms"),
        ]:
            el = tree.css_first(f'meta[itemprop="{prop_name}"]')
            if el and spec_key not in specs:
                val = el.attributes.get("content", "")
                if val:
                    specs[spec_key] = val
        if specs:
            data["specs"] = specs

        # Extract hreflang URLs for translations
        translations: dict[str, str] = {}
        for link in tree.css('link[rel="alternate"][hreflang]'):
            lang = link.attributes.get("hreflang", "")
            href = link.attributes.get("href", "")
            if lang and href and lang not in ("x-default",):
                translations[lang] = href
        if translations:
            data["_translation_urls"] = translations

        return data

    @staticmethod
    def _parse_translation(html: str) -> dict[str, str | list[str]]:
        """Extract title, description, and features from a translated detail page."""
        tree = HTMLParser(html)
        result: dict[str, str | list[str]] = {}

        title_el = tree.css_first("h1")
        if title_el:
            result["title"] = title_el.text(strip=True)

        desc_el = tree.css_first('div.article[itemprop="description"], div.article')
        if desc_el:
            h2 = desc_el.css_first("h2")
            if h2:
                h2.decompose()
            text = desc_el.text(strip=True)
            if text:
                result["description"] = text

        features: list[str] = []
        for li_el in tree.css(".features ul li"):
            text = li_el.text(strip=True)
            if text:
                features.append(text)
        if features:
            result["features"] = features

        return result

    # ── Specs normalization ──────────────────────────────────────────

    @staticmethod
    def normalize_specs(specs: dict) -> dict:
        """Convert area/string specs to size/int specs expected by the app."""
        result: dict = {}

        # Rename 'area' → 'size', extract leading number, cast to int
        area = specs.get("area") or specs.get("size")
        if area is not None:
            m = re.match(r"[\d.]+", str(area).strip())
            if m:
                result["size"] = int(float(m.group()))

        for key in ("bedrooms", "bathrooms"):
            val = specs.get(key)
            if val is not None:
                try:
                    result[key] = int(val)
                except (ValueError, TypeError):
                    pass

        # Preserve any other keys
        for k, v in specs.items():
            if k not in ("area", "size", "bedrooms", "bathrooms"):
                result[k] = v

        return result

    # ── Property builder ─────────────────────────────────────────────

    @staticmethod
    def build_property(
        item: dict,
        listing_type: str,
        tab: str,
    ) -> Property:
        """Assemble a Property from a parsed list-page item dict."""
        # EN title used for sub_category guessing and location extraction
        en_title = item.get("title", f"Listing {item.get('source_id', 'unknown')}")
        # Prefer clean price from detail page schema.org, fall back to list-page text
        price = item.get("detail_price") or SpainRealEstateScraper.parse_price(item.get("price_text"))

        # Sub-category: from tab first, then from EN title
        sub_cat = TAB_SUB_CATEGORY.get(tab)
        if sub_cat is None:
            sub_cat = SpainRealEstateScraper.guess_sub_category(en_title)

        # Location (always from EN title which has "in City, Spain" pattern)
        loc = SpainRealEstateScraper.extract_location_from_title(en_title)

        # Primary title/description: prefer ES translation, fall back to EN
        es_data = item.get("_es", {})
        title = es_data.get("title") or en_title
        description = (
            es_data.get("description")
            or item.get("description")
            or item.get("excerpt")
        )

        # If rental detected from price, override listing_type
        actual_listing_type = listing_type
        if item.get("is_rental"):
            actual_listing_type = "rent"

        # Specs — collect raw then normalize
        raw_specs: dict = {}
        for key in ("rooms", "bedrooms", "bathrooms", "area"):
            if key in item:
                raw_specs[key] = item[key]
        # Merge any detail-page specs
        if "specs" in item:
            raw_specs.update(item["specs"])
        specs = SpainRealEstateScraper.normalize_specs(raw_specs)

        images = []
        if item.get("thumbnail"):
            images.append(item["thumbnail"])
        if item.get("images"):
            # Detail images supersede thumbnail
            images = item["images"]

        source_id = str(item.get("source_id", ""))
        property_id = f"spain-real-estate-{source_id}"

        # Build translations
        translations: list[Translation] = []
        for tr_data in item.get("_translations", []):
            translations.append(
                Translation(
                    property_id=property_id,
                    locale=tr_data["locale"],
                    title=tr_data.get("title"),
                    description=tr_data.get("description"),
                    features=tr_data.get("features"),
                )
            )

        return Property(
            listing_type=actual_listing_type,
            sub_category=sub_cat,
            title=title,
            description=description,
            price=price,
            location=loc.get("municipality"),
            municipality=loc.get("municipality"),
            province=loc.get("province"),
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            images=images,
            specs=specs,
            features=item.get("features", []),
            source="spain-real-estate",
            source_id=source_id,
            source_url=item.get("source_url"),
            translations=translations,
        )

    # ── Enrichment helpers ───────────────────────────────────────────

    def _enrich_item(self, item: dict) -> dict:
        """Fetch detail page + ES/RU translations for one item dict.

        Mutates and returns item. Raises FetchError if the detail page fetch fails.
        """
        sid = item["source_id"]
        detail_html = self._fetch_page(item["source_url"])
        detail_data = self._parse_detail_data(detail_html, sid)
        item.update(detail_data)

        en_tr = self._parse_translation(detail_html)
        if en_tr.get("title") or en_tr.get("description"):
            item.setdefault("_translations", []).append({"locale": "en", **en_tr})

        tr_urls = detail_data.get("_translation_urls", {})

        es_url = tr_urls.get("es")
        if es_url:
            self._delay_sync()
            try:
                es_html = self._fetch_page(es_url, accept_language="es,es-ES;q=0.9")
                es_tr = self._parse_translation(es_html)
                if es_tr.get("title") or es_tr.get("description"):
                    item["_es"] = es_tr
            except FetchError:
                logger.debug("Failed to fetch ES for %s", sid)

        ru_url = tr_urls.get("ru")
        if ru_url:
            self._delay_sync()
            try:
                ru_html = self._fetch_page(ru_url, accept_language="ru,ru-RU;q=0.9")
                ru_tr = self._parse_translation(ru_html)
                if ru_tr.get("title") or ru_tr.get("description"):
                    item.setdefault("_translations", []).append({"locale": "ru", **ru_tr})
            except FetchError:
                logger.debug("Failed to fetch RU for %s", sid)

        return item

    _SUBCATEGORY_TO_TAB: dict[str, str] = {
        "apartment": "apartment",
        "house": "villa",
        "commerce": "commercial",
        "plot": "land",
    }

    def enrich_property(self, prop: Property) -> Property:
        """Fetch detail data for an existing Property and return an enriched copy.

        Returns the original property unchanged if source_url is missing or the
        detail page fetch fails — the caller must only mark enriched=True when this
        returns a different object (check result.enriched).
        """
        if not prop.source_url:
            logger.debug("No source_url for %s, skipping", prop.source_id)
            return prop

        en_title = next(
            (t.title for t in prop.translations if t.locale == "en" and t.title),
            prop.title,
        )
        item: dict = {
            "source_id": prop.source_id,
            "title": en_title,
            "source_url": prop.source_url,
            "detail_price": prop.price,
            "is_rental": prop.listing_type == "rent",
        }

        self._delay_sync()
        try:
            item = self._enrich_item(item)
        except FetchError as e:
            logger.warning("Failed to enrich %s: %s", prop.source_id, e)
            return prop

        tab = self._SUBCATEGORY_TO_TAB.get(prop.sub_category or "", "apartment")
        result = self.build_property(item, listing_type=prop.listing_type, tab=tab)
        result.enriched = True
        return result

    # ── Main orchestration ───────────────────────────────────────────

    def scrape(
        self,
        listing_type: str = "sale",
        max_pages: int = 9999,
        enrich: bool = False,
        tabs: list[str] | None = None,
        region: str = "Valencian Community",
        region_id: int = 4120,
    ) -> list[Property]:
        """Scrape listings across property-type tabs.

        Args:
            listing_type: "sale" or "rent"
            max_pages: Max pages per tab
            enrich: Fetch detail pages for coordinates/images/features
            tabs: Property types to scrape (default: all 4)
            region: Region name filter
            region_id: Region ID for prj_region[] param
        """
        if tabs is None:
            tabs = list(TAB_SUB_CATEGORY.keys())

        all_properties: list[Property] = []
        seen_ids: set[str] = set()

        for tab in tabs:
            logger.info("Scraping tab=%s listing_type=%s", tab, listing_type)
            page = 1
            tab_total = 0
            total_pages = max_pages  # will be refined after page 1

            while page <= min(max_pages, total_pages):
                url = self.build_list_url(
                    listing_type=listing_type,
                    tab=tab,
                    page=page,
                    region=region,
                    region_id=region_id,
                )
                logger.debug("Fetching page %d: %s", page, url)

                try:
                    html = self._fetch_page(url)
                except FetchError as e:
                    logger.error("Failed to fetch page %d: %s", page, e)
                    break

                items = self.parse_list_page(html)
                if not items:
                    logger.info("No items on page %d, stopping tab=%s", page, tab)
                    break

                # On first page, determine total pages
                if page == 1:
                    total_count = self.parse_total_count(html)
                    if total_count:
                        total_pages = math.ceil(total_count / ITEMS_PER_PAGE)
                        logger.info(
                            "Tab %s: %d listings across %d pages",
                            tab, total_count, total_pages,
                        )
                    else:
                        total_pages = self.parse_last_page(html)
                        logger.info("Tab %s: last page = %d", tab, total_pages)

                for item in items:
                    sid = item.get("source_id", "")
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)

                    if enrich and item.get("source_url"):
                        self._delay_sync()
                        try:
                            item = self._enrich_item(item)
                        except FetchError as e:
                            logger.warning("Failed to enrich %s: %s", sid, e)

                    prop = self.build_property(item, listing_type, tab)
                    all_properties.append(prop)
                    tab_total += 1

                logger.info("Page %d: parsed %d items (tab total: %d)", page, len(items), tab_total)
                page += 1
                self._delay_sync()

        logger.info("Total: %d unique properties scraped", len(all_properties))
        return all_properties

    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        """Required by BaseScraper ABC."""
        data = self._parse_detail_data(html, ad_id)
        if not data:
            return None
        data["source_id"] = ad_id
        return self.build_property(data, listing_type="sale", tab="apartment")
