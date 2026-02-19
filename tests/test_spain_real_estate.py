"""Tests for spain-real.estate HTML parsing logic."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.scrapers.spain_real_estate import SpainRealEstateScraper

S = SpainRealEstateScraper

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ── Price parsing ────────────────────────────────────────────────────


class TestParsePrice:
    def test_basic_eur(self):
        assert S.parse_price("\u20ac 181 000") == 181000.0

    def test_large_amount(self):
        assert S.parse_price("\u20ac 2 500 000") == 2500000.0

    def test_monthly(self):
        assert S.parse_price("\u20ac 1 500 monthly") == 1500.0

    def test_non_breaking_spaces(self):
        assert S.parse_price("\u20ac\xa0995\xa0000") == 995000.0

    def test_empty(self):
        assert S.parse_price("") is None
        assert S.parse_price(None) is None

    def test_euro_sign(self):
        assert S.parse_price("€ 370 000") == 370000.0

    def test_glued_monthly(self):
        assert S.parse_price("€ 1\xa0977monthly") == 1977.0


# ── Sub-category guessing ───────────────────────────────────────────


class TestGuessSubCategory:
    def test_apartment(self):
        assert S.guess_sub_category("Apartment in Valencia, Spain") == "apartment"

    def test_flat(self):
        assert S.guess_sub_category("Flat in Barcelona, Spain") == "apartment"

    def test_penthouse(self):
        assert S.guess_sub_category("Penthouse in Alicante, Spain") == "apartment"

    def test_villa(self):
        assert S.guess_sub_category("Villa in Benidorm, Spain") == "house"

    def test_commercial(self):
        assert S.guess_sub_category("Commercial property in Valencia") == "commerce"

    def test_land(self):
        assert S.guess_sub_category("Land plot in Denia, Spain") == "plot"

    def test_unknown(self):
        assert S.guess_sub_category("Property No. 12345") is None


# ── Location extraction ─────────────────────────────────────────────


class TestExtractLocation:
    def test_valencia(self):
        loc = S.extract_location_from_title("Apartment in Valencia, Spain 2 bedrooms, No. 12345")
        assert loc["municipality"] == "Valencia"
        assert loc["province"] == "Valencia"

    def test_alicante(self):
        loc = S.extract_location_from_title("Villa in Alicante, Spain No. 99999")
        assert loc["municipality"] == "Alicante"
        assert loc["province"] == "Alicante"

    def test_benidorm_maps_to_alicante(self):
        loc = S.extract_location_from_title("Apartment in Benidorm, Spain 3 bedrooms, No. 55555")
        assert loc["municipality"] == "Benidorm"
        assert loc["province"] == "Alicante"

    def test_no_match(self):
        loc = S.extract_location_from_title("Some weird title without pattern")
        assert loc["municipality"] is None
        assert loc["province"] is None


# ── List page parsing ───────────────────────────────────────────────


LIST_HTML = """
<div class="objects-list switchable g4 listview">
<ul>
    <li data-object="141031">
        <div class="image">
            <div class="price js-list-for-show"><span>\u20ac 995\xa0000</span></div>
            <a href="https://spain-real.estate/property/o141031/">
                <img class="thumb" src="https://storage1.spain-real.estate/img1.jpg"
                     alt="Apartment in Barcelona"/>
            </a>
        </div>
        <div class="info">
            <div class="title"><a href="https://spain-real.estate/property/o141031/">Apartment in Barcelona, Spain 4 bedrooms, No. 141031</a></div>
            <div class="params">
                <span class="rooms">Rooms: <b>5</b></span>
                <span class="bedrooms">Bedrooms: <b>4</b></span>
                <span class="bathrooms">Bathrooms: <b>4</b></span>
            </div>
            <div class="excerpt">A beautiful apartment in the heart of the city. <a href="/property/o141031/">Details</a></div>
        </div>
    </li>
    <li data-object="141034">
        <div class="image">
            <div class="price price-rent js-list-for-show">
                <span><span>\u20ac 12\xa0500</span><span class="rent-period">monthly</span></span>
            </div>
            <a href="/property/o141034/">
                <img class="thumb" src="https://storage1.spain-real.estate/img2.jpg"
                     alt="Apartment in Barcelona"/>
            </a>
        </div>
        <div class="info">
            <div class="title"><a href="/property/o141034/">Apartment in Barcelona, Spain 3 bedrooms, No. 141034</a></div>
            <div class="params">
                <span class="rooms">Rooms: <b>4</b></span>
                <span class="bedrooms">Bedrooms: <b>3</b></span>
                <span class="bathrooms">Bathrooms: <b>3</b></span>
                <span class="area">Living space: <b>248 m<sup>2</sup></b></span>
            </div>
            <div class="excerpt">Located in the heart of Sant Gervasi. <a href="/property/o141034/">Details</a></div>
        </div>
    </li>
</ul>
</div>
<div class="total_counter">25 - 48 out of 15089</div>
<ul class="pagination">
    <li class="prev"><a href="?n=1">&nbsp;</a></li>
    <li><a href="?n=1">1</a></li>
    <li class="current">2</li>
    <li><a href="?n=3">3</a></li>
    <li class="empty">...</li>
    <li><a href="?n=629">629</a></li>
    <li class="next"><a href="?n=3">&nbsp;</a></li>
</ul>
"""


class TestParseListPage:
    def test_basic_list(self):
        items = S.parse_list_page(LIST_HTML)
        assert len(items) == 2
        assert items[0]["source_id"] == "141031"
        assert items[1]["source_id"] == "141034"

    def test_first_item_data(self):
        items = S.parse_list_page(LIST_HTML)
        item = items[0]
        assert "Barcelona" in item["title"]
        assert item["source_url"] == "https://spain-real.estate/property/o141031/"
        assert item["rooms"] == "5"
        assert item["bedrooms"] == "4"
        assert item["bathrooms"] == "4"
        assert item["thumbnail"] == "https://storage1.spain-real.estate/img1.jpg"

    def test_rental_detected(self):
        items = S.parse_list_page(LIST_HTML)
        item = items[1]
        assert item.get("is_rental") is True
        assert item["area"] == "248 m2"

    def test_relative_url_prefixed(self):
        items = S.parse_list_page(LIST_HTML)
        assert items[1]["source_url"] == "https://spain-real.estate/property/o141034/"

    def test_empty_page(self):
        items = S.parse_list_page("<html><body><div>No listings</div></body></html>")
        assert items == []

    def test_sold_out_skipped(self):
        html = """
        <div class="objects-list switchable g4 listview"><ul>
            <li data-object="111">
                <div class="image">
                    <div class="price js-list-for-show"><span><span class="small">Sold out</span></span></div>
                </div>
                <div class="info">
                    <div class="title"><a href="/property/o111/">Sold item</a></div>
                </div>
            </li>
            <li data-object="222">
                <div class="image">
                    <div class="price js-list-for-show"><span>\u20ac 100\xa0000</span></div>
                </div>
                <div class="info">
                    <div class="title"><a href="/property/o222/">Available item</a></div>
                </div>
            </li>
        </ul></div>
        """
        items = S.parse_list_page(html)
        assert len(items) == 1
        assert items[0]["source_id"] == "222"


class TestParseTotalCount:
    def test_total_counter(self):
        assert S.parse_total_count(LIST_HTML) == 15089

    def test_totals_fallback(self):
        html = '<div class="objects_list totals"><span>15089 properties </span></div>'
        assert S.parse_total_count(html) == 15089

    def test_no_counter(self):
        assert S.parse_total_count("<html></html>") == 0


class TestParseLastPage:
    def test_pagination(self):
        assert S.parse_last_page(LIST_HTML) == 629

    def test_no_pagination(self):
        assert S.parse_last_page("<html></html>") == 1


# ── Detail page parsing ─────────────────────────────────────────────


class TestParseDetailPage:
    def test_coordinates_from_map_data(self):
        html = """
        <script>
        var OBJECT_MAP_DATA = {"41.583766_2.2898":[{"title":"Test","lat":"41.583766","lng":"2.2898","id":"12345"}]};
        </script>
        """
        data = S._parse_detail_data(html, "12345")
        assert data["latitude"] == pytest.approx(41.583766)
        assert data["longitude"] == pytest.approx(2.2898)

    def test_coordinates_from_schema(self):
        html = """
        <div itemprop="geo">
            <meta itemprop="latitude" content="39.4699">
            <meta itemprop="longitude" content="-0.3763">
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert data["latitude"] == pytest.approx(39.4699)
        assert data["longitude"] == pytest.approx(-0.3763)

    def test_price_from_schema(self):
        html = '<meta itemprop="price" content="698000">'
        data = S._parse_detail_data(html, "12345")
        assert data["detail_price"] == 698000.0

    def test_gallery_images(self):
        html = """
        <div id="gallery_container">
            <div class="thumbs"><div class="scroll"><ul>
                <li><img src="thumb1.jpg" data-real="full1.jpg" data-big="full1.jpg"></li>
                <li><img src="thumb2.jpg" data-real="full2.jpg" data-big="full2.jpg"></li>
            </ul></div></div>
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert data["images"] == ["full1.jpg", "full2.jpg"]

    def test_features_wrapped(self):
        html = """
        <div class="features wrapped">
            <h3>Features</h3>
            <ul>
                <li class="prj_o_300">Balcony</li>
                <li class="prj_o_27">Air Conditioners</li>
            </ul>
        </div>
        <div class="features wrapped">
            <h3>Indoor facilities</h3>
            <ul>
                <li class="prj_o_97">Elevator</li>
            </ul>
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert "Balcony" in data["features"]
        assert "Air Conditioners" in data["features"]
        assert "Elevator" in data["features"]
        assert len(data["features"]) == 3

    def test_features_non_wrapped(self):
        """Non-wrapped .features blocks (e.g. outdoor) must also be captured."""
        html = """
        <div class="features">
            <h3 class="title-tag">Outdoor features</h3>
            <ul>
                <li class="prj_o_160">Children's playground</li>
                <li class="prj_o_146">Swimming pool</li>
            </ul>
        </div>
        <div class="features wrapped">
            <h3 class="title-tag">Features</h3>
            <ul>
                <li class="prj_o_125">Terrace</li>
            </ul>
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert "Children's playground" in data["features"]
        assert "Swimming pool" in data["features"]
        assert "Terrace" in data["features"]
        assert len(data["features"]) == 3

    def test_description(self):
        html = """
        <div class="article" itemprop="description">
            <h2>Property description</h2>
            <p>A beautiful apartment near the beach.</p>
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert "beautiful apartment" in data["description"]
        assert not data["description"].startswith("Property description")

    def test_specs_from_sidebar(self):
        html = """
        <div class="right_block parameters">
            <div class="params">
                <div class="city"><span class="name">City</span><span class="value">Barcelona</span></div>
                <div class="tip"><span class="name">Type</span><span class="value">Apartment</span></div>
                <div class="rooms"><span class="name">Bedrooms</span><span class="value">4</span></div>
            </div>
        </div>
        """
        data = S._parse_detail_data(html, "12345")
        assert data["specs"]["City"] == "Barcelona"
        assert data["specs"]["Bedrooms"] == "4"

    def test_translation_urls(self):
        html = """
        <link rel="alternate" hreflang="en" href="https://spain-real.estate/property/o12345/">
        <link rel="alternate" hreflang="es" href="https://spain-real.estate/es/property/o12345/">
        <link rel="alternate" hreflang="ru" href="https://spain-real.estate/ru/property/o12345/">
        <link rel="alternate" hreflang="x-default" href="https://spain-real.estate/property/o12345/">
        """
        data = S._parse_detail_data(html, "12345")
        urls = data["_translation_urls"]
        assert urls["es"] == "https://spain-real.estate/es/property/o12345/"
        assert urls["ru"] == "https://spain-real.estate/ru/property/o12345/"
        assert "x-default" not in urls

    def test_minimal(self):
        data = S._parse_detail_data("<html><body></body></html>", "12345")
        assert "latitude" not in data
        assert "features" not in data
        assert "images" not in data


class TestParseTranslation:
    def test_spanish_translation(self):
        html = """
        <html><body>
        <h1>Apartamento en Barcelona, España 4 dormitorios</h1>
        <div class="article" itemprop="description">
            <h2>Descripción</h2>
            <p>Un hermoso apartamento cerca de la playa.</p>
        </div>
        </body></html>
        """
        tr = S._parse_translation(html)
        assert "Barcelona" in tr["title"]
        assert "hermoso" in tr["description"]
        assert not tr["description"].startswith("Descripción")

    def test_features_extracted(self):
        html = """
        <html><body>
        <h1>Test Property</h1>
        <div class="features wrapped">
            <ul>
                <li>Balcony</li>
                <li>Air Conditioning</li>
            </ul>
        </div>
        <div class="features wrapped">
            <ul>
                <li>Elevator</li>
            </ul>
        </div>
        </body></html>
        """
        tr = S._parse_translation(html)
        assert tr["features"] == ["Balcony", "Air Conditioning", "Elevator"]

    def test_en_translation_from_detail_page(self):
        html = """
        <html><body>
        <h1>Apartment in Valencia, Spain 2 bedrooms, No. 141278</h1>
        <div class="article" itemprop="description">
            <h2>Description</h2>
            <p>A lovely apartment with sea views.</p>
        </div>
        <div class="features wrapped">
            <ul>
                <li>Swimming Pool</li>
                <li>Parking</li>
            </ul>
        </div>
        </body></html>
        """
        tr = S._parse_translation(html)
        assert "Valencia" in tr["title"]
        assert "sea views" in tr["description"]
        assert tr["features"] == ["Swimming Pool", "Parking"]


# ── Property builder ────────────────────────────────────────────────


class TestNormalizeSpecs:
    def test_area_to_size(self):
        specs = S.normalize_specs({"area": "80 m2"})
        assert specs["size"] == 80
        assert "area" not in specs

    def test_numeric_strings(self):
        specs = S.normalize_specs({"bedrooms": "2", "bathrooms": "3"})
        assert specs["bedrooms"] == 2
        assert specs["bathrooms"] == 3

    def test_already_numeric(self):
        specs = S.normalize_specs({"size": 120, "bedrooms": 3})
        assert specs["size"] == 120
        assert specs["bedrooms"] == 3

    def test_preserves_other_keys(self):
        specs = S.normalize_specs({"area": "80 m2", "City": "Valencia", "Type": "Apartment"})
        assert specs["size"] == 80
        assert specs["City"] == "Valencia"
        assert specs["Type"] == "Apartment"

    def test_empty_specs(self):
        assert S.normalize_specs({}) == {}

    def test_area_with_decimal(self):
        specs = S.normalize_specs({"area": "120.5 m2"})
        assert specs["size"] == 120


class TestBuildProperty:
    def test_sale(self):
        item = {
            "source_id": "141031",
            "title": "Apartment in Valencia, Spain 2 bedrooms, No. 141031",
            "price_text": "\u20ac 370 000",
            "rooms": "3",
            "bedrooms": "2",
            "bathrooms": "2",
            "area": "80 m2",
            "thumbnail": "https://example.com/img.jpg",
            "source_url": "https://spain-real.estate/property/o141031/",
        }
        prop = S.build_property(item, listing_type="sale", tab="apartment")
        assert prop.source == "spain-real-estate"
        assert prop.source_id == "141031"
        assert prop.id == "spain-real-estate-141031"
        assert prop.listing_type == "sale"
        assert prop.sub_category == "apartment"
        assert prop.price == 370000.0
        assert prop.municipality == "Valencia"
        assert prop.province == "Valencia"
        assert len(prop.images) == 1
        # Specs are normalized: int values, area→size
        assert prop.specs["bedrooms"] == 2
        assert prop.specs["bathrooms"] == 2
        assert prop.specs["size"] == 80
        assert "area" not in prop.specs

    def test_es_title_as_primary(self):
        item = {
            "source_id": "141031",
            "title": "Apartment in Valencia, Spain No. 141031",
            "_es": {
                "title": "Apartamento en Valencia, España No. 141031",
                "description": "Un apartamento precioso.",
            },
        }
        prop = S.build_property(item, listing_type="sale", tab="apartment")
        # Primary title/description should be Spanish
        assert prop.title == "Apartamento en Valencia, España No. 141031"
        assert prop.description == "Un apartamento precioso."
        # Location is still extracted from EN title
        assert prop.municipality == "Valencia"
        assert prop.province == "Valencia"

    def test_fallback_to_en_when_no_es(self):
        item = {
            "source_id": "141031",
            "title": "Apartment in Valencia, Spain No. 141031",
            "description": "A lovely apartment.",
        }
        prop = S.build_property(item, listing_type="sale", tab="apartment")
        # Falls back to EN when no _es data
        assert prop.title == "Apartment in Valencia, Spain No. 141031"
        assert prop.description == "A lovely apartment."

    def test_rent_from_monthly(self):
        item = {
            "source_id": "141034",
            "title": "Apartment in Barcelona, Spain 3 bedrooms, No. 141034",
            "price_text": "\u20ac 12 500 monthly",
            "is_rental": True,
        }
        prop = S.build_property(item, listing_type="sale", tab="apartment")
        assert prop.listing_type == "rent"
        assert prop.price == 12500.0

    def test_sub_category_from_tab(self):
        item = {"source_id": "99", "title": "Some property"}
        prop = S.build_property(item, listing_type="sale", tab="villa")
        assert prop.sub_category == "house"

    def test_sub_category_fallback_to_title(self):
        item = {"source_id": "99", "title": "Land plot in Valencia, Spain No. 99"}
        prop = S.build_property(item, listing_type="sale", tab="unknown_tab")
        assert prop.sub_category == "plot"

    def test_translations_attached(self):
        item = {
            "source_id": "141278",
            "title": "Apartment in Valencia, Spain No. 141278",
            "source_url": "https://spain-real.estate/property/o141278/",
            "_translations": [
                {
                    "locale": "en",
                    "title": "Apartment in Valencia, Spain No. 141278",
                    "description": "A lovely apartment.",
                    "features": ["Pool", "Parking"],
                },
                {
                    "locale": "ru",
                    "title": "Квартира в Валенсии, Испания",
                    "description": "Прекрасная квартира.",
                    "features": ["Бассейн", "Парковка"],
                },
            ],
        }
        prop = S.build_property(item, listing_type="sale", tab="apartment")
        assert len(prop.translations) == 2
        en = prop.translations[0]
        assert en.locale == "en"
        assert en.property_id == "spain-real-estate-141278"
        assert en.title == "Apartment in Valencia, Spain No. 141278"
        assert en.features == ["Pool", "Parking"]
        ru = prop.translations[1]
        assert ru.locale == "ru"
        assert ru.property_id == "spain-real-estate-141278"
        assert "Валенсии" in ru.title


# ── enrich_property ─────────────────────────────────────────────────


class TestEnrichProperty:
    """Unit tests for enrich_property — HTTP calls are mocked."""

    def _make_prop(self, source_id="141031", sub_category="apartment", listing_type="sale"):
        from src.models import Property
        return Property(
            listing_type=listing_type,
            sub_category=sub_category,
            title="Apartment in Valencia, Spain No. 141031",
            price=370000.0,
            location="Valencia",
            municipality="Valencia",
            source="spain-real-estate",
            source_id=source_id,
            source_url=f"https://spain-real.estate/property/o{source_id}/",
        )

    def test_returns_enriched_flag_on_success(self, monkeypatch):
        scraper = S()

        detail_html = """
        <html><head>
        <link rel="alternate" hreflang="es" href="https://spain-real.estate/es/property/o141031/">
        </head><body>
        <h1>Apartment in Valencia, Spain No. 141031</h1>
        <meta itemprop="price" content="370000">
        <div class="article"><p>Great apartment.</p></div>
        </body></html>
        """
        es_html = """
        <html><body>
        <h1>Apartamento en Valencia, España</h1>
        <div class="article"><p>Gran apartamento.</p></div>
        </body></html>
        """

        def fake_fetch(url, accept_language=None):
            if "es/" in url:
                return es_html
            return detail_html

        monkeypatch.setattr(scraper, "_fetch_page", fake_fetch)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop()
        result = scraper.enrich_property(prop)

        assert result.enriched is True
        assert result.title == "Apartamento en Valencia, España"
        assert any(t.locale == "en" for t in result.translations)

    def test_returns_original_on_fetch_error(self, monkeypatch):
        from src.scrapers.base import FetchError
        scraper = S()

        def fake_fetch(url, accept_language=None):
            raise FetchError(403, url)

        monkeypatch.setattr(scraper, "_fetch_page", fake_fetch)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop()
        result = scraper.enrich_property(prop)

        assert result is prop
        assert result.enriched is False

    def test_skips_when_no_source_url(self):
        from src.models import Property
        scraper = S()
        prop = Property(
            listing_type="sale",
            title="No URL property",
            source="spain-real-estate",
            source_id="999",
            source_url=None,
        )
        result = scraper.enrich_property(prop)
        assert result is prop

    def test_subcategory_to_tab_mapping(self, monkeypatch):
        """enrich_property preserves sub_category via reverse tab lookup."""
        scraper = S()

        def fake_fetch(url, accept_language=None):
            return "<html><body><h1>Villa in Valencia</h1></body></html>"

        monkeypatch.setattr(scraper, "_fetch_page", fake_fetch)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop(sub_category="house")
        result = scraper.enrich_property(prop)

        assert result.enriched is True
        assert result.sub_category == "house"


# ── URL construction ────────────────────────────────────────────────


class TestBuildListUrl:
    def test_sale_page1(self):
        url = S.build_list_url(listing_type="sale", tab="apartment", page=1)
        assert "spain-real.estate/property/" in url
        assert "tab=apartment" in url
        assert "&n=" not in url

    def test_sale_page2(self):
        url = S.build_list_url(listing_type="sale", tab="apartment", page=2)
        assert "n=2" in url

    def test_rent(self):
        url = S.build_list_url(listing_type="rent", tab="apartment", page=1)
        assert "spain-real.estate/rent/" in url
        assert "tab=apartment" in url

    def test_region_params(self):
        url = S.build_list_url(region="Valencian Community", region_id=4120)
        assert "region=Valencian" in url
        assert "prj_region" in url
        assert "4120" in url


# ── Fixture integration tests ───────────────────────────────────────


class TestRealFixture:
    @pytest.mark.skipif(
        not (FIXTURES_DIR / "spain_real_estate_list.html").exists(),
        reason="No fixture file at tests/fixtures/spain_real_estate_list.html",
    )
    def test_real_list_page(self):
        html = (FIXTURES_DIR / "spain_real_estate_list.html").read_text(encoding="utf-8")
        items = S.parse_list_page(html)
        assert len(items) > 0
        for item in items:
            assert "source_id" in item
            assert "title" in item

    @pytest.mark.skipif(
        not (FIXTURES_DIR / "spain_real_estate_list_valencia.html").exists(),
        reason="No fixture file at tests/fixtures/spain_real_estate_list_valencia.html",
    )
    def test_real_valencia_list(self):
        html = (FIXTURES_DIR / "spain_real_estate_list_valencia.html").read_text(encoding="utf-8")
        items = S.parse_list_page(html)
        assert len(items) > 0
        total = S.parse_total_count(html)
        assert total > 0
