"""Tests for Idealista HTML/JS parsing logic."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.scrapers.idealista import IdealistaScraper

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def scraper():
    s = IdealistaScraper()
    yield s
    s.close()


# ── JS extraction tests ───────────────────────────────────────────────


class TestExtractJsVar:
    def test_simple_object(self, scraper):
        html = '<script>var mortgagesConfig = {"initialPrice": 150000};</script>'
        result = scraper._extract_js_var(html, "mortgagesConfig")
        assert result is not None
        assert "150000" in result

    def test_no_var_keyword(self, scraper):
        html = '<script>mortgagesConfig = {"initialPrice": 250000};</script>'
        result = scraper._extract_js_var(html, "mortgagesConfig")
        assert result is not None
        assert "250000" in result

    def test_multiline(self, scraper):
        html = """<script>
        var adDetail = {
            headerTitle: "Piso en venta en Valencia",
            rooms: 3
        };
        </script>"""
        result = scraper._extract_js_var(html, "adDetail")
        assert result is not None
        assert "Piso" in result

    def test_array_value(self, scraper):
        html = '<script>var items = [1, 2, 3];</script>'
        result = scraper._extract_js_var(html, "items")
        assert result is not None
        assert "1" in result

    def test_missing_var(self, scraper):
        html = "<script>var other = {};</script>"
        result = scraper._extract_js_var(html, "mortgagesConfig")
        assert result is None


class TestJsToJson:
    def test_unquoted_keys(self, scraper):
        js = '{name: "hello", count: 5}'
        result = json.loads(scraper._js_to_json(js))
        assert result == {"name": "hello", "count": 5}

    def test_single_quotes(self, scraper):
        js = "{'name': 'hello'}"
        result = json.loads(scraper._js_to_json(js))
        assert result == {"name": "hello"}

    def test_trailing_comma(self, scraper):
        js = '{"a": 1, "b": 2,}'
        result = json.loads(scraper._js_to_json(js))
        assert result == {"a": 1, "b": 2}

    def test_undefined_replacement(self, scraper):
        js = '{"a": undefined, "b": 1}'
        result = json.loads(scraper._js_to_json(js))
        assert result == {"a": None, "b": 1}

    def test_mixed(self, scraper):
        js = "{initialPrice: 150000, type: 'sale', extra: undefined,}"
        result = json.loads(scraper._js_to_json(js))
        assert result["initialPrice"] == 150000
        assert result["type"] == "sale"
        assert result["extra"] is None


# ── Feature parsing tests ─────────────────────────────────────────────


class TestParseFeatures:
    def test_label_list(self, scraper):
        features = [
            {"label": ["3 habitaciones", "2 baños"]},
            {"label": ["Superficie: 90 m²"]},
        ]
        result = scraper._parse_features(features)
        assert result["3 habitaciones"] == "true"
        assert result["Superficie"] == "90 m²"

    def test_empty_list(self, scraper):
        assert scraper._parse_features([]) == {}


class TestParseDetailFeatures:
    def test_html_features(self, scraper):
        html = """
        <div class="details-property_features">
            <ul>
                <li>Superficie: 90 m²</li>
                <li>Ascensor</li>
                <li>Aire acondicionado</li>
            </ul>
        </div>
        """
        specs, features = scraper._parse_detail_features(html)
        assert specs["Superficie"] == "90 m²"
        assert "Ascensor" in features
        assert "Aire acondicionado" in features


# ── Location extraction tests ─────────────────────────────────────────


class TestExtractLocation:
    def test_full_location(self, scraper):
        html = """
        <div id="headerMap">
            <ul class="header-map-list">
                <li>El Cabanyal</li>
                <li>Valencia</li>
                <li>Valencia</li>
            </ul>
        </div>
        """
        loc = scraper._extract_location(html)
        assert loc["neighborhood"] == "El Cabanyal"
        assert loc["municipality"] == "Valencia"
        assert loc["province"] == "Valencia"

    def test_postal_code(self, scraper):
        html = """
        <div id="headerMap">
            <ul class="header-map-list">
                <li>46001 Valencia</li>
                <li>Valencia</li>
            </ul>
        </div>
        """
        loc = scraper._extract_location(html)
        assert loc["postal_code"] == "46001"


# ── Description extraction ────────────────────────────────────────────


class TestExtractDescription:
    def test_comment_div(self, scraper):
        html = '<div class="comment"><p>Beautiful apartment in the center.</p></div>'
        result = scraper._extract_description(html)
        assert result == "Beautiful apartment in the center."

    def test_no_description(self, scraper):
        html = "<div>No description here</div>"
        result = scraper._extract_description(html)
        assert result is None


# ── Sub-category guessing ─────────────────────────────────────────────


class TestGuessSubCategory:
    def test_apartment(self, scraper):
        assert scraper._guess_sub_category("Piso en venta en Valencia", {}) == "apartment"

    def test_house(self, scraper):
        assert scraper._guess_sub_category("Chalet adosado en venta", {}) == "house"

    def test_commerce(self, scraper):
        assert scraper._guess_sub_category("Local comercial en alquiler", {}) == "commerce"

    def test_plot(self, scraper):
        assert scraper._guess_sub_category("Terreno en venta", {}) == "plot"

    def test_unknown(self, scraper):
        assert scraper._guess_sub_category("Propiedad en venta", {}) is None


# ── AJAX feature string parsing ───────────────────────────────────────


class TestParseFeatureStrings:
    def test_rooms(self, scraper):
        specs = scraper._parse_feature_strings(["2 hab."])
        assert specs["habitaciones"] == "2"

    def test_area(self, scraper):
        specs = scraper._parse_feature_strings(["97 m²"])
        assert specs["superficie"] == "97 m²"

    def test_floor_with_elevator(self, scraper):
        specs = scraper._parse_feature_strings(["Planta 2ª Con ascensor"])
        assert specs["planta"] == "Planta 2ª Con ascensor"
        assert specs["ascensor"] == "true"

    def test_floor_without_elevator(self, scraper):
        specs = scraper._parse_feature_strings(["Bajo ext. Sin ascensor"])
        assert specs["planta"] == "Bajo ext. Sin ascensor"
        assert specs["ascensor"] == "false"

    def test_full_feature_set(self, scraper):
        specs = scraper._parse_feature_strings([
            "2 hab.",
            "97 m²",
            "Planta 2ª Con ascensor",
        ])
        assert specs["habitaciones"] == "2"
        assert specs["superficie"] == "97 m²"
        assert specs["ascensor"] == "true"

    def test_empty(self, scraper):
        assert scraper._parse_feature_strings([]) == {}


class TestParseAjaxAd:
    def test_full_ad(self, scraper):
        ad = {
            "adId": 109639881,
            "thumbnails": {
                "thumbnail": "https://img4.idealista.com/blur/591_420_mq/0/id.pro.es.image.master/e7/ac/00/1354368005.jpg",
            },
            "ribbons": ["Obra nueva"],
            "address": "Piso en Calle Valencia, 3",
            "detailUrl": "/inmueble/109639881/",
            "price": 418000.0,
            "features": ["2 hab.", "97 m²", "Planta 2ª Con ascensor"],
        }
        prop = scraper.parse_ajax_ad(ad, "sale")
        assert prop.source_id == "109639881"
        assert prop.id == "idealista-109639881"
        assert prop.price == 418000.0
        assert prop.title == "Piso en Calle Valencia, 3"
        assert prop.sub_category == "apartment"
        assert prop.listing_type == "new-building"  # from "Obra nueva" ribbon
        assert prop.specs["habitaciones"] == "2"
        assert prop.specs["superficie"] == "97 m²"
        assert prop.municipality == "Valencia"
        assert len(prop.images) == 1
        assert prop.source_url == "https://www.idealista.com/inmueble/109639881/"

    def test_minimal_ad(self, scraper):
        ad = {
            "adId": 12345,
            "price": 100000.0,
            "features": [],
        }
        prop = scraper.parse_ajax_ad(ad, "rent")
        assert prop.source_id == "12345"
        assert prop.listing_type == "rent"
        assert prop.price == 100000.0
        assert prop.title == "Listing 12345"


# ── List page parsing ─────────────────────────────────────────────────


class TestListPageParsing:
    def test_utag_data_extraction(self, scraper):
        html = """
        <script>
            var utag_data = {
                adIds: "12345,67890,11111",
                page_number: "1"
            };
        </script>
        <div class="pagination-list">
            <ul><li><a href="/p-1">1</a></li><li><a href="/p-5">5</a></li></ul>
        </div>
        """
        ids, total_pages = scraper.get_listing_ids_from_list_page(html)
        assert ids == ["12345", "67890", "11111"]
        assert total_pages == 5

    def test_fallback_article_parsing(self, scraper):
        html = """
        <article class="item" data-adid="99999">
            <a class="item-link" href="/inmueble/99999/"></a>
        </article>
        <article class="item" data-adid="88888">
            <a class="item-link" href="/inmueble/88888/"></a>
        </article>
        """
        ids, total_pages = scraper.get_listing_ids_from_list_page(html)
        assert "99999" in ids
        assert "88888" in ids


# ── Full detail page parsing ──────────────────────────────────────────


class TestDetailPageParsing:
    def test_minimal_detail_page(self, scraper):
        html = """
        <html><head>
        <script>
            var mortgagesConfig = {initialPrice: 195000};
            var adDetail = {headerTitle: "Piso en venta en Ruzafa"};
        </script>
        </head><body>
        <div id="headerMap">
            <ul class="header-map-list">
                <li>Ruzafa</li>
                <li>Valencia</li>
                <li>Valencia</li>
            </ul>
        </div>
        <div class="comment"><p>Lovely flat near the market.</p></div>
        <div class="details-property_features">
            <ul>
                <li>Superficie: 85 m²</li>
                <li>Ascensor</li>
            </ul>
        </div>
        </body></html>
        """
        prop = scraper.parse_detail_page(html, "12345", "sale")
        assert prop is not None
        assert prop.id == "idealista-12345"
        assert prop.price == 195000.0
        assert prop.title == "Piso en venta en Ruzafa"
        assert prop.listing_type == "sale"
        assert prop.sub_category == "apartment"
        assert prop.neighborhood == "Ruzafa"
        assert prop.municipality == "Valencia"
        assert prop.description == "Lovely flat near the market."
        assert prop.specs.get("Superficie") == "85 m²"
        assert "Ascensor" in prop.features
        assert prop.source_url == "https://www.idealista.com/inmueble/12345/"

    @pytest.mark.skipif(
        not (FIXTURES_DIR / "detail.html").exists(),
        reason="No fixture file found at tests/fixtures/detail.html",
    )
    def test_real_fixture(self, scraper):
        """Test against a real saved HTML fixture (if available)."""
        html = (FIXTURES_DIR / "detail.html").read_text(encoding="utf-8")
        prop = scraper.parse_detail_page(html, "fixture_test", "sale")
        assert prop is not None
        assert prop.title
        assert prop.source_id == "fixture_test"
