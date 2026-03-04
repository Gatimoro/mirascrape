"""Scraper for fotocasa.es – Spanish property portal (JSON search API)."""

from __future__ import annotations

import logging
import math
import re
import time
from collections import defaultdict

import httpx

from src.config import settings
from src.models import Property
from src.scrapers.base import BaseScraper, FetchError

logger = logging.getLogger(__name__)

API_URL = "https://web.gw.fotocasa.es/v1/search/ads"
BASE_URL = "https://www.fotocasa.es"
ITEMS_PER_PAGE = 30

# Matches ALL_CAPS_WITH_UNDERSCORES identifiers (fotocasa internal codes)
_CAPS_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")


def _humanise(raw: str) -> str:
    """'INTERMEDIATE_FLOOR' → 'Intermediate floor'; other strings pass through."""
    if _CAPS_RE.match(raw):
        return raw.replace("_", " ").capitalize()
    return raw


# ── Lookup tables ──────────────────────────────────────────────────────────────

# propertyType code → (sub_category, URL slug)
PROPERTY_TYPE_MAP: dict[str, tuple[str, str]] = {
    "1": ("house",     "casa"),
    "2": ("apartment", "vivienda"),
    "3": ("commerce",  "local-comercial"),
    "4": ("commerce",  "garaje"),
    "5": ("plot",      "terreno"),
    "6": ("commerce",  "oficina"),
    "7": ("commerce",  "trastero"),
    "8": ("apartment", "alquiler-vacacional"),
}

FLOOR_MAP: dict[str, str] = {
    "SO": "Basement",
    "SS": "Semi-basement",
    "ST": "Semi-basement",
    "BJ": "Ground floor",
    "PB": "Ground floor",
    "EN": "Mezzanine",
    "AT": "Top floor",
}

ORIENTATION_MAP: dict[str, str] = {
    "1": "North",
    "2": "South",
    "3": "East",
    "4": "West",
    "5": "Northeast",
    "6": "Northwest",
    "7": "Southeast",
    "8": "Southwest",
}

ANTIQUITY_MAP: dict[str, str] = {
    "1": "New construction",
    "2": "Less than 5 years",
    "3": "5–10 years",
    "4": "10–20 years",
    "5": "20–30 years",
    "6": "More than 30 years",
    "7": "Second hand",
    "8": "70 to 100 years",
    "9": "Over 100 years",
}

CONSERVATION_MAP: dict[str, str] = {
    "1": "New",
    "2": "Good condition",
    "3": "Needs renovation",
    "4": "Renovated",
    "8": "Very good condition",
}

# Feature ID → human-readable label
# Unknown IDs are logged and accumulated; add new ones here as they appear
FEATURE_MAP: dict[str, str] = {
    "1":   "Elevator",
    "2":   "Parking",
    "3":   "Swimming pool",
    "4":   "Garden",
    "5":   "Air conditioning",
    "6":   "Furnished",
    "7":   "Terrace",
    "8":   "Storage room",
    "9":   "Doorman",
    "10":  "Storage room",
    "11":  "Gym",
    "12":  "Security system",
    "13":  "Terrace",
    "14":  "Balcony",
    "15":  "Fitted kitchen",
    "16":  "Laundry room",
    "17":  "Furnished",
    "18":  "Garden",
    "19":  "Built-in wardrobes",
    "20":  "Gym",
    "21":  "Communal heating",
    "22":  "Individual heating",
    "23":  "Central heating",
    "24":  "Electric heating",
    "25":  "Adapted for disabilities",
    "26":  "Communal garage",
    "27":  "Private garage",
    "28":  "Communal pool",
    "29":  "Private pool",
    "30":  "Double glazing",
    "31":  "Laundry room",
    "32":  "Built-in wardrobes",
    "33":  "Communal areas",
    "34":  "Green areas",
    "35":  "Utility room",
    "36":  "Underground parking",
    "37":  "Communal heating",
    "38":  "Natural gas heating",
    "39":  "Diesel heating",
    "40":  "Electric heating",
    "41":  "Solar energy",
    "42":  "Heat recovery",
    "43":  "Underfloor heating",
    "44":  "Gas heating",
    "45":  "Communal pool",
    "46":  "Private pool",
    "52":  "Parquet flooring",
    "56":  "Wine cellar",
    "60":  "Pets allowed",
    "61":  "Communal pool",
    "62":  "Private pool",
    "63":  "Communal garden",
    "64":  "Private garden",
    "65":  "Balcony",
    "66":  "Terrace",
    "67":  "Patio",
    "68":  "Solarium",
    "69":  "Communal garage",
    "70":  "Private garage",
    "77":  "Heating",
    "79":  "Children's play area",
    "81":  "Communal area",
    "83":  "Appliances included",
    "84":  "Internet connection",
    "109": "En suite bathroom",
    "122": "Alarm system",
    "124": "Guest bathroom",
    "126": "Designer furniture",
    "127": "Laundry/ironing room",
    "128": "Covered porch",
    "129": "Cinema room",
    "130": "Heat pump",
    "131": "Aerothermal",
    "132": "Energy certificate A",
    "133": "Energy certificate B",
    "134": "Energy certificate C",
    "135": "Energy certificate D",
    "136": "Energy certificate E",
    "137": "Energy certificate F",
    "138": "Energy certificate G",
}

_SUB_CATEGORY_LABELS: dict[str, str] = {
    "apartment": "Apartment",
    "house":     "House",
    "commerce":  "Commercial property",
    "plot":      "Plot",
}


class FotocasaScraper(BaseScraper):
    """Scraper for fotocasa.es using its private JSON search API.

    All listing data (description, images, coordinates, features) is returned
    by the list endpoint — no detail-page enrichment is required.
    """

    DEFAULT_COMBINED_LOCATION = "724,19,46,0,0,0,0,0,0"  # Valencia province
    DEFAULT_LATITUDE = 39.4699
    DEFAULT_LONGITUDE = -0.375811
    # Default property types: houses (1), apartments (2), plots (5)
    DEFAULT_PROPERTY_TYPES = [1, 2, 5]
    RATE_LIMIT_COOLDOWN = 60

    def __init__(self) -> None:
        super().__init__()
        self._http: httpx.Client | None = None
        # Tracks unknown values: field → {raw_value → (fallback_label, example_url)}
        self._unknown: defaultdict[str, dict[str, tuple[str, str]]] = defaultdict(dict)
        self._current_url: str = ""  # set per-item so warnings include an example link

    def _ensure_http(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Origin": BASE_URL,
                    "Referer": BASE_URL + "/",
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

    # ── Unknown-value tracking ─────────────────────────────────────────────────

    def _lookup(
        self,
        mapping: dict[str, str],
        key: str | None,
        field: str,
    ) -> str | None:
        """Look up a code in a mapping table.

        Falls back to ``_humanise(key)`` for CAPS_UNDERSCORE strings, or the
        raw value otherwise. Unknown values are logged immediately and collected
        for a summary report at the end of the scrape.
        """
        if key is None:
            return None
        s = str(key)
        if s in mapping:
            return mapping[s]
        fallback = _humanise(s)
        if s not in self._unknown[field]:
            logger.warning(
                "Unknown %s: %r  →  using %r  |  %s", field, s, fallback, self._current_url
            )
        self._unknown[field][s] = (fallback, self._current_url)
        return fallback

    def _log_unknowns(self) -> None:
        """Print a summary of all unknown values encountered during the run."""
        if not self._unknown:
            return
        lines = ["─" * 60, "UNKNOWN FIELD VALUES — add these to the mapping tables:"]
        for field, values in sorted(self._unknown.items()):
            for raw, (human, url) in sorted(values.items()):
                lines.append(f"  {field}: {raw!r}  →  {human!r}  |  {url}")
        lines.append("─" * 60)
        logger.warning("\n".join(lines))

    # ── Network ────────────────────────────────────────────────────────────────

    def _post(self, payload: dict) -> dict:
        """POST to the search API; retries on 403 with cooldown."""
        client = self._ensure_http()
        for attempt in range(settings.MAX_RETRIES):
            resp = client.post(API_URL, json=payload)
            if resp.status_code == 403:
                wait = self.RATE_LIMIT_COOLDOWN * (attempt + 1)
                logger.warning(
                    "Got 403 (attempt %d/%d), cooling down %ds",
                    attempt + 1, settings.MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                raise FetchError(resp.status_code, API_URL)
            return resp.json()
        raise FetchError(403, API_URL)

    # ── Item parsing ───────────────────────────────────────────────────────────

    def _parse_item(self, item: dict, listing_type: str) -> Property:
        prop_id = str(item.get("propertyId", ""))
        tx_type = str(item.get("transactionType", "1"))

        # Price
        transaction = item.get("transaction", {})
        price = transaction.get("price")

        # Property type → sub_category + URL slug
        prop_type = str(item.get("propertyType", "2"))
        type_info = PROPERTY_TYPE_MAP.get(prop_type)
        if type_info is None:
            fallback = _humanise(prop_type)
            if prop_type not in self._unknown["property_type"]:
                logger.warning("Unknown property_type: %r  |  %s", prop_type, self._current_url)
            self._unknown["property_type"][prop_type] = (fallback, self._current_url)
            type_info = ("apartment", "vivienda")
        sub_category, type_slug = type_info

        # Source URL (constructed from known parts; redirects to canonical listing)
        tx_slug = "comprar" if tx_type == "1" else "alquiler"
        source_url = (
            f"{BASE_URL}/es/{tx_slug}/{type_slug}"
            f"/valencia-provincia/todas-las-zonas/{prop_id}/d"
        )
        self._current_url = source_url  # used by _lookup warnings

        # Location
        loc = item.get("location", {})
        municipality = (loc.get("level5Name") or "").strip() or None
        neighborhood = (loc.get("level8Name") or loc.get("level7Name") or "").strip() or None
        try:
            lat = float(loc["latitude"])
            lng = float(loc["longitude"])
        except (KeyError, ValueError, TypeError):
            lat = lng = None

        # Specs
        specs: dict = {}
        if item.get("surface"):
            specs["size"] = round(float(item["surface"]), 2)
        if item.get("rooms") is not None:
            specs["bedrooms"] = int(item["rooms"])
        if item.get("baths") is not None:
            specs["bathrooms"] = int(item["baths"])

        # Floor (numeric or named code like BJ, EN, AT, or CAPS_UNDERSCORE)
        floor_raw = item.get("floor")
        if floor_raw is not None:
            floor_s = str(floor_raw)
            if floor_s.lstrip("-").isdigit():
                specs["floor"] = int(floor_s)
            else:
                floor_label = self._lookup(FLOOR_MAP, floor_s, "floor")
                if floor_label:
                    specs["floor"] = floor_label

        orientation = self._lookup(ORIENTATION_MAP, item.get("orientation"), "orientation")
        if orientation:
            specs["orientation"] = orientation

        antiquity = self._lookup(ANTIQUITY_MAP, item.get("antiquity"), "antiquity")
        if antiquity:
            specs["antiquity"] = antiquity

        conservation = self._lookup(CONSERVATION_MAP, item.get("conservationStatus"), "conservation_status")
        if conservation:
            specs["condition"] = conservation

        if item.get("zipCode"):
            specs["zip_code"] = item["zipCode"]

        # Images — multimedia type "2" = photo, in position order
        images = [
            m["url"]
            for m in sorted(item.get("multimedia", []), key=lambda x: x.get("position", 999))
            if str(m.get("type")) == "2" and m.get("url")
        ]

        # Features
        features: list[str] = []
        seen_features: set[str] = set()
        for feat in item.get("features", []):
            fid = str(feat.get("id", ""))
            if not fid:
                continue
            if fid in FEATURE_MAP:
                label = FEATURE_MAP[fid]
            else:
                fallback = f"Feature #{fid}"
                if fid not in self._unknown["feature_id"]:
                    logger.warning(
                        "Unknown feature_id: %r  →  %r  |  %s", fid, fallback, self._current_url
                    )
                self._unknown["feature_id"][fid] = (fallback, self._current_url)
                label = fallback
            if label not in seen_features:
                features.append(label)
                seen_features.add(label)

        # Title: generated from type + transaction + neighbourhood
        type_label = _SUB_CATEGORY_LABELS.get(sub_category, "Property")
        tx_word = "for sale" if listing_type == "sale" else "for rent"
        location_part = neighborhood or municipality
        title = f"{type_label} {tx_word}" + (f" in {location_part}" if location_part else "")

        return Property(
            listing_type=listing_type,
            sub_category=sub_category,
            title=title,
            description=item.get("description"),
            price=float(price) if price is not None else None,
            rent_period="month" if listing_type == "rent" else None,
            location=municipality,
            municipality=municipality,
            neighborhood=neighborhood,
            postal_code=item.get("zipCode"),
            province="Valencia",
            latitude=lat,
            longitude=lng,
            images=images,
            specs=specs,
            features=features,
            source="fotocasa",
            source_id=prop_id,
            source_url=source_url,
            enriched=True,  # API gives complete data; no detail fetch needed
        )

    # ── BaseScraper ABC ────────────────────────────────────────────────────────

    def parse_detail_page(self, html: str, ad_id: str) -> Property | None:
        # All data is in the list API response — detail pages are not needed
        return None

    def scrape(
        self,
        listing_type: str = "sale",
        max_pages: int = 9999,
        enrich: bool = False,
        property_types: list[int] | None = None,
        combined_location: str = DEFAULT_COMBINED_LOCATION,
        latitude: float = DEFAULT_LATITUDE,
        longitude: float = DEFAULT_LONGITUDE,
    ) -> list[Property]:
        """Scrape property listings from fotocasa.es.

        Args:
            listing_type: ``"sale"`` or ``"rent"``
            max_pages: Max pages per property type (30 listings each)
            enrich: Unused — the API returns complete data including description and images
            property_types: fotocasa propertyType codes (default: [1, 2, 5] = houses, apartments, plots)
            combined_location: fotocasa location code (default: Valencia province)
            latitude: Search centre latitude
            longitude: Search centre longitude
        """
        if property_types is None:
            property_types = list(self.DEFAULT_PROPERTY_TYPES)

        tx_type = 1 if listing_type == "sale" else 2
        all_properties: list[Property] = []
        seen_ids: set[str] = set()

        self._unknown.clear()
        self._stop_requested = False
        self._install_sigint_handler()
        try:
            for prop_type in property_types:
                logger.info(
                    "Scraping propertyType=%d listing_type=%s", prop_type, listing_type
                )
                total_pages = max_pages

                for page in range(1, max_pages + 1):
                    if page > total_pages:
                        break

                    payload = {
                        "combinedLocations": [combined_location],
                        "contracts": [],
                        "includePurchaseTypeFacets": True,
                        "isMap": False,
                        "latitude": latitude,
                        "longitude": longitude,
                        "pageNumber": page,
                        "propertyType": prop_type,
                        "size": ITEMS_PER_PAGE,
                        "sortOrderDesc": True,
                        "sortType": "scoring",
                        "transactionType": tx_type,
                        "userId": None,
                    }

                    try:
                        data = self._post(payload)
                    except FetchError as e:
                        logger.error(
                            "Failed to fetch page %d (type %d): %s", page, prop_type, e
                        )
                        break

                    if page == 1:
                        total_items = data.get("totalItems", 0)
                        total_pages = min(
                            max_pages,
                            math.ceil(total_items / ITEMS_PER_PAGE) if total_items else 1,
                        )
                        logger.info(
                            "Type %d: %d listings across %d pages",
                            prop_type, total_items, total_pages,
                        )

                    items = data.get("items", [])
                    if not items:
                        logger.info(
                            "No items on page %d (type %d), stopping", page, prop_type
                        )
                        break

                    page_new = 0
                    for item in items:
                        prop_id = str(item.get("propertyId", ""))
                        if not prop_id or prop_id in seen_ids:
                            continue
                        seen_ids.add(prop_id)
                        try:
                            prop = self._parse_item(item, listing_type)
                            all_properties.append(prop)
                            page_new += 1
                        except Exception as e:
                            logger.warning(
                                "Failed to parse item %s: %s", prop_id, e, exc_info=True
                            )

                    logger.info(
                        "Page %d/%d: +%d new (total %d)",
                        page, total_pages, page_new, len(all_properties),
                    )

                    if page < total_pages:
                        self._delay_sync()
                        if not self._check_and_pause():
                            logger.info(
                                "Scraping stopped by user after %d properties",
                                len(all_properties),
                            )
                            self._log_unknowns()
                            return all_properties
        finally:
            self._uninstall_sigint_handler()

        logger.info("Total: %d unique properties scraped", len(all_properties))
        self._log_unknowns()
        return all_properties
