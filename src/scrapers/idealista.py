from __future__ import annotations

import json
import logging
import re

from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from src.config import settings
from src.models import Property
from src.scrapers.base import BaseScraper, FetchError

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, FetchError):
        return exc.status_code in (403, 429, 503)
    return False


class IdealistaScraper(BaseScraper):
    BASE_URL = "https://www.idealista.com"

    # AJAX endpoints for listing discovery
    AJAX_URL_TEMPLATES = {
        "sale": "{base}/es/ajax/listing/georeach/venta-viviendas/valencia-valencia",
        "rent": "{base}/es/ajax/listing/georeach/alquiler-viviendas/valencia-valencia",
        "new-building": "{base}/es/ajax/listing/georeach/valencia-valencia",
    }

    # HTML list page fallbacks
    LIST_URL_TEMPLATES = {
        "sale": "{base}/venta-viviendas/valencia-valencia/pagina-{page}.htm",
        "rent": "{base}/alquiler-viviendas/valencia-valencia/pagina-{page}.htm",
    }

    DETAIL_URL_TEMPLATE = "{base}/inmueble/{ad_id}/"

    # ── JS extraction helpers ──────────────────────────────────────────

    @staticmethod
    def _extract_js_var(html: str, var_name: str) -> str | None:
        """Extract a JS variable assignment from <script> blocks.

        Handles patterns like:
            var myVar = { ... };
            myVar = { ... };
        """
        # Try: var varName = { ... }; (greedy to capture nested braces)
        pattern = rf"(?:var\s+)?{re.escape(var_name)}\s*=\s*(\{{[^;]*\}});?"
        match = re.search(pattern, html, re.DOTALL)
        if match:
            return match.group(1)

        # Try array form: var varName = [ ... ];
        pattern = rf"(?:var\s+)?{re.escape(var_name)}\s*=\s*(\[[^;]*\]);?"
        match = re.search(pattern, html, re.DOTALL)
        if match:
            return match.group(1)

        return None

    @staticmethod
    def _js_to_json(js_str: str) -> str:
        """Convert a JS object literal to valid JSON.

        Handles: unquoted keys, single quotes, trailing commas, undefined/null.
        """
        s = js_str

        # Replace single quotes with double quotes (but not inside already-double-quoted strings)
        s = s.replace("'", '"')

        # Remove JS comments
        s = re.sub(r"//.*?$", "", s, flags=re.MULTILINE)
        s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)

        # Replace undefined with null
        s = re.sub(r"\bundefined\b", "null", s)

        # Quote unquoted keys: { key: ... } -> { "key": ... }
        s = re.sub(
            r'(?<=[{,])\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:',
            r' "\1":',
            s,
        )

        # Remove trailing commas before } or ]
        s = re.sub(r",\s*([}\]])", r"\1", s)

        return s

    # ── Feature / spec parsing ─────────────────────────────────────────

    @staticmethod
    def _parse_features(features_list: list[dict]) -> dict[str, str]:
        """Parse headerFeatures / features arrays into a flat specs dict.

        Each item typically has { "label": [...] } with text snippets.
        """
        specs: dict[str, str] = {}
        for item in features_list:
            labels = item.get("label") or item.get("labels") or []
            if isinstance(labels, list):
                for label_item in labels:
                    text = label_item if isinstance(label_item, str) else str(label_item)
                    text = text.strip()
                    if not text:
                        continue
                    if ":" in text:
                        k, v = text.split(":", 1)
                        specs[k.strip()] = v.strip()
                    else:
                        specs[text] = "true"
            elif isinstance(labels, str):
                specs[labels.strip()] = "true"
        return specs

    @staticmethod
    def _parse_detail_features(html: str) -> tuple[dict[str, str], list[str]]:
        """Parse .details-property_features HTML into specs dict + features list."""
        tree = HTMLParser(html)
        specs: dict[str, str] = {}
        features: list[str] = []

        container = tree.css_first(".details-property_features")
        if not container:
            return specs, features

        for li in container.css("li"):
            text = li.text(strip=True)
            if not text:
                continue
            if ":" in text:
                k, v = text.split(":", 1)
                specs[k.strip()] = v.strip()
            else:
                features.append(text)

        return specs, features

    @staticmethod
    def _extract_location(html: str) -> dict[str, str | None]:
        """Extract location info from .header-map-list elements."""
        tree = HTMLParser(html)
        loc: dict[str, str | None] = {
            "location": None,
            "province": None,
            "municipality": None,
            "neighborhood": None,
            "postal_code": None,
        }

        header_map = tree.css_first("#headerMap")
        if not header_map:
            return loc

        items = header_map.css("li")
        texts = [li.text(strip=True) for li in items if li.text(strip=True)]

        if texts:
            loc["location"] = ", ".join(texts)

        # Typical structure: [neighborhood, municipality, province]
        # or [street, neighborhood, municipality, province]
        for text in texts:
            # Postal code pattern
            pc_match = re.search(r"\b(\d{5})\b", text)
            if pc_match:
                loc["postal_code"] = pc_match.group(1)

        if len(texts) >= 3:
            loc["neighborhood"] = texts[-3]
            loc["municipality"] = texts[-2]
            loc["province"] = texts[-1]
        elif len(texts) == 2:
            loc["municipality"] = texts[-2]
            loc["province"] = texts[-1]
        elif len(texts) == 1:
            loc["municipality"] = texts[0]

        return loc

    @staticmethod
    def _extract_description(html: str) -> str | None:
        """Extract description text from detail page."""
        tree = HTMLParser(html)
        comment = tree.css_first(".comment")
        if not comment:
            comment = tree.css_first("#details-content .adCommentsLanguage")
        if comment:
            text = comment.text(strip=True)
            return text if text else None
        return None

    @staticmethod
    def _guess_sub_category(title: str, specs: dict) -> str | None:
        """Guess sub_category from title and specs."""
        title_lower = title.lower()
        if any(w in title_lower for w in ("piso", "apartamento", "ático", "atico", "estudio", "dúplex", "duplex")):
            return "apartment"
        if any(w in title_lower for w in ("casa", "chalet", "villa", "adosado", "pareado", "finca")):
            return "house"
        if any(w in title_lower for w in ("local", "oficina", "nave", "comercial")):
            return "commerce"
        if any(w in title_lower for w in ("terreno", "parcela", "solar")):
            return "plot"
        return None

    # ── AJAX response parsing ──────────────────────────────────────────

    @staticmethod
    def _parse_feature_strings(features: list[str]) -> dict[str, str]:
        """Parse compact feature strings from AJAX response.

        Examples: "2 hab.", "97 m²", "Planta 2ª Con ascensor"
        """
        specs: dict[str, str] = {}
        for feat in features:
            feat = feat.strip()
            if not feat:
                continue

            # Rooms: "2 hab." or "1 hab."
            m = re.match(r"(\d+)\s*hab\.", feat)
            if m:
                specs["habitaciones"] = m.group(1)
                continue

            # Area: "97 m²"
            m = re.match(r"(\d+)\s*m²", feat)
            if m:
                specs["superficie"] = f"{m.group(1)} m²"
                continue

            # Floor + elevator: "Planta 2ª Con ascensor" / "Bajo ext. Sin ascensor"
            if "planta" in feat.lower() or "bajo" in feat.lower():
                specs["planta"] = feat
                if "con ascensor" in feat.lower():
                    specs["ascensor"] = "true"
                elif "sin ascensor" in feat.lower():
                    specs["ascensor"] = "false"
                continue

            # Anything else goes in as-is
            specs[feat] = "true"

        return specs

    def parse_ajax_ad(self, ad: dict, listing_type: str = "sale") -> Property:
        """Build a Property from a single AJAX ad object."""
        ad_id = str(ad["adId"])
        address = ad.get("address", "")
        price = ad.get("price")
        features_raw = ad.get("features", [])
        thumbnail = ad.get("thumbnails", {}).get("thumbnail", "")
        detail_url = ad.get("detailUrl", "")

        # Parse feature strings into structured specs
        specs = self._parse_feature_strings(features_raw)

        # Build title from address
        title = address if address else f"Listing {ad_id}"

        # Sub-category from title
        sub_category = self._guess_sub_category(title, specs)

        # Images: use thumbnail as single image
        images = [thumbnail] if thumbnail else []

        # Source URL
        source_url = f"{self.BASE_URL}{detail_url}" if detail_url else f"{self.BASE_URL}/inmueble/{ad_id}/"

        # Check if this is new-building from ribbons
        ribbons = ad.get("ribbons", [])
        if any("obra nueva" in r.lower() for r in ribbons):
            listing_type = "new-building"

        return Property(
            listing_type=listing_type,
            sub_category=sub_category,
            title=title,
            price=float(price) if price else None,
            location=address,
            municipality="Valencia",
            province="Valencia",
            images=images,
            specs=specs,
            features=features_raw,
            source="idealista",
            source_id=ad_id,
            source_url=source_url,
        )

    # ── Detail page parsing ────────────────────────────────────────────

    def parse_detail_page(
        self,
        html: str,
        ad_id: str,
        listing_type: str = "sale",
    ) -> Property | None:
        """Parse a single Idealista detail page into a Property."""
        tree = HTMLParser(html)

        # ── Price from mortgagesConfig.initialPrice ──
        price: float | None = None
        mortgages_raw = self._extract_js_var(html, "mortgagesConfig")
        if mortgages_raw:
            try:
                mortgages = json.loads(self._js_to_json(mortgages_raw))
                price = float(mortgages.get("initialPrice", 0)) or None
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        # Fallback: try price from HTML
        if price is None:
            price_el = tree.css_first(".info-data-price .txt-bold")
            if price_el:
                price_text = re.sub(r"[^\d.,]", "", price_el.text(strip=True))
                price_text = price_text.replace(".", "").replace(",", ".")
                try:
                    price = float(price_text)
                except ValueError:
                    pass

        # ── Title ──
        title = ""
        ad_detail_raw = self._extract_js_var(html, "adDetail")
        if ad_detail_raw:
            try:
                ad_detail = json.loads(self._js_to_json(ad_detail_raw))
                title = ad_detail.get("headerTitle", "")
            except (json.JSONDecodeError, ValueError):
                pass

        if not title:
            h1 = tree.css_first("h1")
            if h1:
                title = h1.text(strip=True)

        if not title:
            title = f"Listing {ad_id}"

        # ── Images from adMultimediasInfo ──
        images: list[str] = []
        multimedia_raw = self._extract_js_var(html, "adMultimediasInfo")
        if multimedia_raw:
            try:
                multimedia = json.loads(self._js_to_json(multimedia_raw))
                gallery = multimedia.get("fullScreenGalleryPics", [])
                for pic in gallery:
                    if pic.get("isPlan"):
                        continue
                    url = pic.get("src") or pic.get("url") or ""
                    if url:
                        images.append(url)
                    if len(images) >= 10:
                        break
            except (json.JSONDecodeError, ValueError):
                pass

        # ── Specs from JS then HTML ──
        specs: dict[str, str] = {}
        features: list[str] = []

        # Try adMultimediasInfo.features first
        if multimedia_raw:
            try:
                multimedia = json.loads(self._js_to_json(multimedia_raw))
                js_features = multimedia.get("features", [])
                if js_features:
                    specs.update(self._parse_features(js_features))
            except (json.JSONDecodeError, ValueError):
                pass

        # Try adDetail.headerFeatures
        if ad_detail_raw:
            try:
                ad_detail = json.loads(self._js_to_json(ad_detail_raw))
                header_features = ad_detail.get("headerFeatures", [])
                if header_features:
                    specs.update(self._parse_features(header_features))
            except (json.JSONDecodeError, ValueError):
                pass

        # HTML features (supplements JS data)
        html_specs, html_features = self._parse_detail_features(html)
        for k, v in html_specs.items():
            if k not in specs:
                specs[k] = v
        features.extend(html_features)

        # ── Location ──
        loc = self._extract_location(html)

        # ── Description ──
        description = self._extract_description(html)

        # ── Coordinates from map ──
        latitude: float | None = None
        longitude: float | None = None
        map_el = tree.css_first("#mapWrapper, .map-container, [data-latitude]")
        if map_el:
            lat_str = map_el.attributes.get("data-latitude", "")
            lon_str = map_el.attributes.get("data-longitude", "")
            try:
                latitude = float(lat_str) if lat_str else None
                longitude = float(lon_str) if lon_str else None
            except ValueError:
                pass

        # Also try from JS
        if latitude is None:
            lat_match = re.search(r'"latitude"\s*:\s*([\d.]+)', html)
            lon_match = re.search(r'"longitude"\s*:\s*([\d.-]+)', html)
            if lat_match and lon_match:
                try:
                    latitude = float(lat_match.group(1))
                    longitude = float(lon_match.group(1))
                except ValueError:
                    pass

        # ── Sub-category ──
        sub_category = self._guess_sub_category(title, specs)

        source_url = f"{self.BASE_URL}/inmueble/{ad_id}/"

        return Property(
            listing_type=listing_type,
            sub_category=sub_category,
            title=title,
            description=description,
            price=price,
            location=loc["location"],
            province=loc["province"],
            municipality=loc["municipality"],
            neighborhood=loc["neighborhood"],
            postal_code=loc["postal_code"],
            latitude=latitude,
            longitude=longitude,
            images=images,
            specs=specs,
            features=features,
            source="idealista",
            source_id=ad_id,
            source_url=source_url,
        )

    # ── List page parsing ──────────────────────────────────────────────

    def get_listing_ids_from_list_page(
        self, html: str
    ) -> tuple[list[str], int]:
        """Extract listing IDs and total pages from a list page.

        Returns (list_of_ad_ids, total_pages).
        """
        ids: list[str] = []
        total_pages = 1

        # Extract from utag_data JS object
        utag_raw = self._extract_js_var(html, "utag_data")
        if utag_raw:
            try:
                utag = json.loads(self._js_to_json(utag_raw))
                # adIds can be a comma-separated string or list
                ad_ids = utag.get("adIds", "")
                if isinstance(ad_ids, str) and ad_ids:
                    ids = [x.strip() for x in ad_ids.split(",") if x.strip()]
                elif isinstance(ad_ids, list):
                    ids = [str(x) for x in ad_ids]
            except (json.JSONDecodeError, ValueError):
                pass

        # Fallback: parse article elements
        if not ids:
            tree = HTMLParser(html)
            for article in tree.css("article.item"):
                data_id = article.attributes.get("data-adid", "")
                if not data_id:
                    # Try extracting from the link
                    link = article.css_first("a.item-link")
                    if link:
                        href = link.attributes.get("href", "")
                        match = re.search(r"/inmueble/(\d+)/", href)
                        if match:
                            data_id = match.group(1)
                if data_id:
                    ids.append(data_id)

        # Total pages from pagination
        tree = HTMLParser(html)
        pagination = tree.css(".pagination-list li a")
        for link in pagination:
            text = link.text(strip=True)
            try:
                page_num = int(text)
                total_pages = max(total_pages, page_num)
            except ValueError:
                continue

        return ids, total_pages

    # ── HTTP with retries ──────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch(self, url: str) -> str:
        self._ensure_browser()
        resp = self._page.goto(url, wait_until="domcontentloaded")
        if resp and resp.status >= 400:
            raise FetchError(resp.status, url)
        return self._page.content()

    # ── AJAX fetch (in-browser) ──────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_json(self, url: str, referer: str | None = None) -> dict:
        self._ensure_browser()
        try:
            result = self._page.evaluate(
                """async ([url, referer]) => {
                    const headers = {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                    };
                    if (referer) headers['Referer'] = referer;
                    const resp = await fetch(url, { headers });
                    if (!resp.ok) throw new Error('HTTP_' + resp.status);
                    return await resp.json();
                }""",
                [url, referer],
            )
        except Exception as e:
            match = re.search(r"HTTP_(\d+)", str(e))
            if match:
                raise FetchError(int(match.group(1)), url) from e
            raise
        return result

    # ── Main scrape orchestration ──────────────────────────────────────

    def scrape(
        self,
        listing_type: str = "sale",
        max_pages: int = 2,
        enrich: bool = False,
    ) -> list[Property]:
        """Scrape listings via AJAX endpoint. Returns list of Property.

        Args:
            listing_type: "sale", "rent", or "new-building"
            max_pages: Not used for AJAX (single response), kept for CLI compat.
            enrich: If True, also fetch detail pages for extra data (description,
                    coordinates, full images). Much slower due to per-listing requests.
        """
        # ── Step 1: Warm up session to get cookies ──
        warmup_pages = {
            "sale": f"{self.BASE_URL}/venta-viviendas/valencia-valencia/",
            "rent": f"{self.BASE_URL}/alquiler-viviendas/valencia-valencia/",
            "new-building": f"{self.BASE_URL}/obra-nueva/valencia-valencia/",
        }
        warmup_url = warmup_pages.get(
            listing_type,
            f"{self.BASE_URL}/venta-viviendas/valencia-valencia/",
        )
        self._ensure_browser()
        logger.info("Warming up browser session: %s", warmup_url)
        self._page.goto(warmup_url, wait_until="networkidle")
        self._page.wait_for_timeout(3000)
        self._delay_sync()

        # ── Step 2: Fetch listings from AJAX endpoint ──
        ajax_template = self.AJAX_URL_TEMPLATES.get(listing_type)
        if not ajax_template:
            raise ValueError(
                f"Unknown listing type: {listing_type}. "
                f"Use: {', '.join(self.AJAX_URL_TEMPLATES)}"
            )

        ajax_url = ajax_template.format(base=self.BASE_URL)
        logger.info("Fetching AJAX listings: %s", ajax_url)

        try:
            data = self._fetch_json(ajax_url, referer=warmup_url)
        except Exception as e:
            logger.error("AJAX request failed: %s", e)

            # Fallback to HTML list pages if AJAX fails
            if listing_type in self.LIST_URL_TEMPLATES:
                logger.warning("Falling back to HTML list pages")
                return self._scrape_html_pages(listing_type, max_pages, enrich)
            return []

        body = data.get("body", data)
        ads = body.get("ads", [])
        logger.info("AJAX returned %d listings", len(ads))

        if not ads:
            # Fallback to HTML list pages
            if listing_type in self.LIST_URL_TEMPLATES:
                logger.warning("No ads in AJAX response, falling back to HTML list pages")
                return self._scrape_html_pages(listing_type, max_pages, enrich)
            return []

        # ── Step 3: Build properties from AJAX data ──
        properties: list[Property] = []
        for ad in ads:
            try:
                prop = self.parse_ajax_ad(ad, listing_type)
                properties.append(prop)
            except Exception as e:
                logger.error("Error parsing ad %s: %s", ad.get("adId", "?"), e)

        logger.info("Parsed %d properties from AJAX data", len(properties))

        # ── Step 3: Optionally enrich with detail pages ──
        if enrich:
            properties = self._enrich_from_detail_pages(properties)

        return properties

    def _scrape_html_pages(
        self,
        listing_type: str,
        max_pages: int,
        enrich: bool,
    ) -> list[Property]:
        """Fallback: scrape from HTML list pages + detail pages."""
        url_template = self.LIST_URL_TEMPLATES.get(listing_type)
        if not url_template:
            return []

        all_ids: list[str] = []
        total_pages = 1

        for page in range(1, max_pages + 1):
            if page > total_pages and page > 1:
                break

            url = url_template.format(base=self.BASE_URL, page=page)
            logger.info("Fetching list page %d: %s", page, url)

            try:
                html = self._fetch(url)
            except FetchError as e:
                logger.error("List page %d failed: HTTP %d", page, e.status_code)
                break

            ids, total = self.get_listing_ids_from_list_page(html)
            if page == 1:
                total_pages = total
            all_ids.extend(ids)

            logger.info("Page %d: %d listings (total pages: %d)", page, len(ids), total_pages)

            if page < max_pages:
                self._delay_sync()

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique_ids: list[str] = []
        for ad_id in all_ids:
            if ad_id not in seen:
                seen.add(ad_id)
                unique_ids.append(ad_id)

        if not unique_ids:
            return []

        logger.info("Fetching %d detail pages", len(unique_ids))

        properties: list[Property] = []
        for i, ad_id in enumerate(unique_ids, 1):
            detail_url = self.DETAIL_URL_TEMPLATE.format(base=self.BASE_URL, ad_id=ad_id)
            logger.info("Detail (%d/%d): %s", i, len(unique_ids), detail_url)

            try:
                detail_html = self._fetch(detail_url)
            except FetchError as e:
                logger.error("Detail %s failed: HTTP %d", ad_id, e.status_code)
                self._delay_sync()
                continue
            except Exception as e:
                logger.error("Detail %s error: %s", ad_id, e)
                self._delay_sync()
                continue

            prop = self.parse_detail_page(detail_html, ad_id, listing_type)
            if prop:
                properties.append(prop)

            self._delay_sync()

        return properties

    def _enrich_from_detail_pages(self, properties: list[Property]) -> list[Property]:
        """Enrich existing properties with data from detail pages."""
        logger.info("Enriching %d properties from detail pages", len(properties))

        for i, prop in enumerate(properties, 1):
            detail_url = self.DETAIL_URL_TEMPLATE.format(
                base=self.BASE_URL, ad_id=prop.source_id
            )
            logger.info("Enrich (%d/%d): %s", i, len(properties), detail_url)

            try:
                detail_html = self._fetch(detail_url)
            except Exception as e:
                logger.warning("Skipping enrichment for %s: %s", prop.source_id, e)
                self._delay_sync()
                continue

            detail_prop = self.parse_detail_page(
                detail_html, prop.source_id, prop.listing_type
            )
            if detail_prop:
                # Merge: detail page data fills in gaps
                if not prop.description and detail_prop.description:
                    prop.description = detail_prop.description
                if not prop.latitude and detail_prop.latitude:
                    prop.latitude = detail_prop.latitude
                    prop.longitude = detail_prop.longitude
                if not prop.neighborhood and detail_prop.neighborhood:
                    prop.neighborhood = detail_prop.neighborhood
                if not prop.postal_code and detail_prop.postal_code:
                    prop.postal_code = detail_prop.postal_code
                if len(detail_prop.images) > len(prop.images):
                    prop.images = detail_prop.images
                # Merge specs (detail page may have more)
                for k, v in detail_prop.specs.items():
                    if k not in prop.specs:
                        prop.specs[k] = v
                # Use detail title if AJAX title was just an address
                if detail_prop.title and detail_prop.title != f"Listing {prop.source_id}":
                    prop.title = detail_prop.title
                    prop.sub_category = self._guess_sub_category(prop.title, prop.specs)

            self._delay_sync()

        return properties
