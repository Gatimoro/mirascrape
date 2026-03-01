"""Tests for the pisos.com scraper."""

from __future__ import annotations

import pytest

from src.scrapers.pisos_com import PisosComScraper, BASE_URL

# ── Fixtures ──────────────────────────────────────────────────────────────────

LISTING_1 = """
<div id="60932948350.991284" class="ad-preview ad-preview--has-desc"
     data-lnk-href="/comprar/piso-sueca_centro_urbano-60932948350_991284/">
    <div class="carousel__container">
        <div class="carousel__slide">
            <div class="carousel__main-photo carousel__main-photo--as-img">
                <picture>
                    <img decoding="async"
                         src="https://fotos.imghs.net/mm-wp/991284/photo1.jpg"
                         alt="Piso en Carrer" width="380" height="285">
                </picture>
            </div>
        </div>
    </div>
    <div class="ad-preview__bottom">
        <div class="ad-preview__info">
            <div class="ad-preview__section">
                <span class="ad-preview__price">133.000 €</span>
            </div>
            <div class="ad-preview__section">
                <a href="/comprar/piso-sueca_centro_urbano-60932948350_991284/"
                   class="ad-preview__title">Piso en Carrer de l'Esculptor Beltr&#xE1;n, 58</a>
                <p class="p-sm ad-preview__subtitle">Sueca</p>
            </div>
            <div class="ad-preview__section">
                <div class="ad-preview__inline">
                    <p class="ad-preview__char p-sm">4 habs.</p>
                    <p class="ad-preview__char p-sm">1 ba&#xF1;o</p>
                    <p class="ad-preview__char p-sm">100 m&#xB2;</p>
                    <p class="ad-preview__char p-sm">1&#xAA; planta</p>
                </div>
            </div>
            <div class="ad-preview__section">
                <p class="ad-preview__description">Piso a la venta en sueca con muchas posibilidades...</p>
            </div>
        </div>
    </div>
    <script type="application/ld+json">
    {
        "@context": "https://schema.org/",
        "@type": "SingleFamilyResidence",
        "@id": "60932948350.991284",
        "image": "https://fotos.imghs.net/mm-wp/991284/photo1.jpg",
        "name": "Piso en Carrer de l\u0027Esculptor Beltr\u00e1n, 58",
        "address": {
            "@type": "PostalAddress",
            "addressLocality": "Sueca",
            "addressRegion": "Val\u00e8ncia"
        },
        "geo": {
            "@type": "GeoCoordinates",
            "latitude": "39.196747",
            "longitude": "-0.31120336"
        }
    }
    </script>
</div>
"""

LISTING_2 = """
<div id="61732572206.520494" class="ad-preview ad-preview--has-desc"
     data-lnk-href="/comprar/apartamento-playa_de_gandia-61732572206_520494/">
    <div class="carousel__main-photo carousel__main-photo--as-img">
        <picture>
            <img decoding="async"
                 src="https://fotos.imghs.net/mm-wp/520494/photo2.jpg"
                 alt="Apartamento" width="380" height="285">
        </picture>
    </div>
    <div class="ad-preview__bottom">
        <span class="ad-preview__price">173.000 €</span>
        <a href="/comprar/apartamento-playa_de_gandia-61732572206_520494/"
           class="ad-preview__title">Apartamento en calle Menorca, 27</a>
        <p class="p-sm ad-preview__subtitle">Playa de Gandia (Gandia)</p>
        <p class="ad-preview__char p-sm">2 habs.</p>
        <p class="ad-preview__char p-sm">1 ba&#xF1;o</p>
        <p class="ad-preview__char p-sm">68 m&#xB2;</p>
    </div>
    <script type="application/ld+json">
    {
        "@context": "https://schema.org/",
        "@type": "SingleFamilyResidence",
        "@id": "61732572206.520494",
        "image": "https://fotos.imghs.net/mm-wp/520494/photo2.jpg",
        "address": {
            "@type": "PostalAddress",
            "addressLocality": "Gandia",
            "addressRegion": "Val\u00e8ncia"
        },
        "geo": {
            "@type": "GeoCoordinates",
            "latitude": "39.01121",
            "longitude": "-0.17060804"
        }
    }
    </script>
</div>
"""

LIST_PAGE_HTML = f"""
<html>
<body>
{LISTING_1}
{LISTING_2}
<div class="pagination pager-fixed">
    <div class="pagination__content">
        <div class="pagination__next single">
            <a href="/venta/pisos-valencia/2/"><span>Siguiente</span></a>
        </div>
    </div>
    <div class="u-align-center">
        <h2 class="pagination__counter">Venta de pisos en Val&#xE8;ncia:</h2>
        <span class="pagination__counter"> 30 de 11.374 resultados</span>
    </div>
</div>
</body>
</html>
"""

LAST_PAGE_HTML = f"""
<html>
<body>
{LISTING_1}
<div class="pagination pager-fixed">
    <div class="pagination__content">
    </div>
    <div class="u-align-center">
        <span class="pagination__counter"> 30 de 11.374 resultados</span>
    </div>
</div>
</body>
</html>
"""

# ── parse_price ───────────────────────────────────────────────────────────────

class TestParsePrice:
    def test_integer_price_with_dot_thousands(self):
        assert PisosComScraper.parse_price("133.000 €") == 133000.0

    def test_price_with_per_month_suffix(self):
        assert PisosComScraper.parse_price("1.500 €/mes") == 1500.0

    def test_price_with_per_week_suffix(self):
        assert PisosComScraper.parse_price("900 €/sem") == 900.0

    def test_price_with_decimal_comma(self):
        assert PisosComScraper.parse_price("128.000,50 €") == 128000.50

    def test_price_no_thousands(self):
        assert PisosComScraper.parse_price("950 €") == 950.0

    def test_price_none(self):
        assert PisosComScraper.parse_price(None) is None

    def test_price_empty(self):
        assert PisosComScraper.parse_price("") is None

    def test_price_whitespace_around_value(self):
        assert PisosComScraper.parse_price("  128.000 €  ") == 128000.0


# ── _parse_chars ──────────────────────────────────────────────────────────────

class TestParseChars:
    def test_bedrooms(self):
        result = PisosComScraper._parse_chars(["4 habs."])
        assert result["bedrooms"] == 4

    def test_bathrooms(self):
        result = PisosComScraper._parse_chars(["2 baños"])
        assert result["bathrooms"] == 2

    def test_size_m2(self):
        result = PisosComScraper._parse_chars(["100 m²"])
        assert result["size"] == 100

    def test_size_m2_unicode(self):
        result = PisosComScraper._parse_chars(["68 m\u00b2"])
        assert result["size"] == 68

    def test_full_set(self):
        result = PisosComScraper._parse_chars(["4 habs.", "1 baño", "100 m²", "1ª planta"])
        assert result == {"bedrooms": 4, "bathrooms": 1, "size": 100}

    def test_empty_list(self):
        assert PisosComScraper._parse_chars([]) == {}

    def test_floor_string_ignored(self):
        result = PisosComScraper._parse_chars(["Bajo"])
        assert result == {}


# ── guess_sub_category_from_url ───────────────────────────────────────────────

class TestGuessSubCategory:
    def test_piso(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/piso-sueca-123/"
        ) == "apartment"

    def test_apartamento(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/apartamento-gandia-456/"
        ) == "apartment"

    def test_chalet(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/chalet-benidorm-789/"
        ) == "house"

    def test_local_comercial(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/local_comercial-valencia-321/"
        ) == "commerce"

    def test_terreno(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/terreno-requena-654/"
        ) == "plot"

    def test_unknown_type(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/comprar/garaje-valencia-999/"
        ) is None

    def test_rent_url(self):
        assert PisosComScraper.guess_sub_category_from_url(
            "/alquiler/piso-valencia-111/"
        ) == "apartment"


# ── build_list_url ────────────────────────────────────────────────────────────

class TestBuildListUrl:
    def test_sale_page1(self):
        url = PisosComScraper.build_list_url("sale", "pisos")
        assert url == "https://www.pisos.com/venta/pisos-valencia/"

    def test_sale_page2(self):
        url = PisosComScraper.build_list_url("sale", "pisos", page=2)
        assert url == "https://www.pisos.com/venta/pisos-valencia/2/"

    def test_rent_page1(self):
        url = PisosComScraper.build_list_url("rent", "pisos")
        assert url == "https://www.pisos.com/alquiler/pisos-valencia/"

    def test_custom_location(self):
        url = PisosComScraper.build_list_url("sale", "casas", location_slug="alicante")
        assert url == "https://www.pisos.com/venta/casas-alicante/"

    def test_commerce_type(self):
        url = PisosComScraper.build_list_url("sale", "locales_comerciales")
        assert url == "https://www.pisos.com/venta/locales_comerciales-valencia/"


# ── parse_total_count ─────────────────────────────────────────────────────────

class TestParseTotalCount:
    def test_standard_count(self):
        assert PisosComScraper.parse_total_count(LIST_PAGE_HTML) == 11374

    def test_no_counter(self):
        assert PisosComScraper.parse_total_count("<html><body></body></html>") == 0

    def test_small_count(self):
        html = '<span class="pagination__counter"> 5 de 45 resultados</span>'
        assert PisosComScraper.parse_total_count(html) == 45


# ── parse_next_page_url ───────────────────────────────────────────────────────

class TestParseNextPageUrl:
    def test_has_next_page(self):
        url = PisosComScraper.parse_next_page_url(LIST_PAGE_HTML)
        assert url == BASE_URL + "/venta/pisos-valencia/2/"

    def test_last_page_no_next(self):
        assert PisosComScraper.parse_next_page_url(LAST_PAGE_HTML) is None

    def test_no_pagination(self):
        assert PisosComScraper.parse_next_page_url("<html><body></body></html>") is None


# ── parse_list_page ───────────────────────────────────────────────────────────

class TestParseListPage:
    def setup_method(self):
        self.items = PisosComScraper.parse_list_page(LIST_PAGE_HTML)

    def test_parses_two_listings(self):
        assert len(self.items) == 2

    def test_source_ids(self):
        ids = [i["source_id"] for i in self.items]
        assert "60932948350.991284" in ids
        assert "61732572206.520494" in ids

    def test_first_listing_source_url(self):
        item = self.items[0]
        assert item["source_url"] == (
            BASE_URL + "/comprar/piso-sueca_centro_urbano-60932948350_991284/"
        )

    def test_first_listing_price_text(self):
        assert self.items[0]["price_text"] == "133.000 €"

    def test_first_listing_title(self):
        title = self.items[0]["title"]
        assert "Piso en Carrer" in title

    def test_first_listing_subtitle(self):
        assert self.items[0]["subtitle"] == "Sueca"

    def test_first_listing_chars(self):
        chars = self.items[0]["chars"]
        assert "4 habs." in chars
        assert "1 ba\u00f1o" in chars
        assert "100 m\u00b2" in chars

    def test_first_listing_description(self):
        assert "Piso a la venta" in self.items[0]["description"]

    def test_first_listing_geo(self):
        item = self.items[0]
        assert abs(item["latitude"] - 39.196747) < 0.0001
        assert abs(item["longitude"] - (-0.31120336)) < 0.0001

    def test_first_listing_address_locality(self):
        assert self.items[0]["address_locality"] == "Sueca"

    def test_first_listing_address_region(self):
        assert self.items[0]["address_region"] == "Val\u00e8ncia"

    def test_first_listing_image(self):
        assert "photo1.jpg" in self.items[0]["image"]

    def test_first_listing_sub_category(self):
        assert self.items[0]["sub_category"] == "apartment"

    def test_second_listing_sub_category(self):
        assert self.items[1]["sub_category"] == "apartment"

    def test_no_photo_placeholder_filtered(self):
        html = """
        <div id="12345.0" class="ad-preview" data-lnk-href="/comprar/piso-valencia-12345_0/">
            <div class="carousel__main-photo carousel__main-photo--as-img">
                <img src="https://statics.imghs.net/dist/img/default_nophoto.jpg">
            </div>
            <div class="ad-preview__bottom">
                <span class="ad-preview__price">100.000 €</span>
                <a href="/comprar/piso-valencia-12345_0/" class="ad-preview__title">Piso sin fotos</a>
            </div>
        </div>
        """
        items = PisosComScraper.parse_list_page(html)
        assert len(items) == 1
        assert "image" not in items[0]

    def test_rent_period_month_from_price_span(self):
        html = """
        <div id="11111.0" class="ad-preview" data-lnk-href="/alquilar/piso-valencia-11111_0/">
            <div class="ad-preview__bottom">
                <span class="ad-preview__price">8.000 €<span>/mes</span></span>
                <a href="/alquilar/piso-valencia-11111_0/" class="ad-preview__title">Nave en Massanassa</a>
            </div>
        </div>
        """
        items = PisosComScraper.parse_list_page(html)
        assert items[0]["rent_period"] == "month"
        assert items[0]["price_text"] == "8.000 €/mes"

    def test_rent_period_week_from_price_span(self):
        html = """
        <div id="22222.0" class="ad-preview" data-lnk-href="/alquilar/piso-valencia-22222_0/">
            <div class="ad-preview__bottom">
                <span class="ad-preview__price">500 €<span>/sem</span></span>
                <a href="/alquilar/piso-valencia-22222_0/" class="ad-preview__title">Estudio</a>
            </div>
        </div>
        """
        items = PisosComScraper.parse_list_page(html)
        assert items[0]["rent_period"] == "week"

    def test_a_consultar_skipped(self):
        html = """
        <div id="33333.0" class="ad-preview" data-lnk-href="/alquilar/nave-torrent-33333_0/">
            <div class="ad-preview__bottom">
                <span class="ad-preview__price">A consultar</span>
                <a href="/alquilar/nave-torrent-33333_0/" class="ad-preview__title">Nave sin precio</a>
            </div>
        </div>
        """
        items = PisosComScraper.parse_list_page(html)
        assert items == []

    def test_empty_page(self):
        assert PisosComScraper.parse_list_page("<html><body></body></html>") == []

    def test_div_without_id_skipped(self):
        html = '<div class="ad-preview"><span class="ad-preview__price">100 €</span></div>'
        assert PisosComScraper.parse_list_page(html) == []


# ── build_property ────────────────────────────────────────────────────────────

class TestBuildProperty:
    def setup_method(self):
        items = PisosComScraper.parse_list_page(LIST_PAGE_HTML)
        self.item = items[0]
        self.prop = PisosComScraper.build_property(self.item, "sale")

    def test_source(self):
        assert self.prop.source == "pisos-com"

    def test_source_id(self):
        assert self.prop.source_id == "60932948350.991284"

    def test_id_format(self):
        assert self.prop.id == "pisos-com-60932948350.991284"

    def test_listing_type(self):
        assert self.prop.listing_type == "sale"

    def test_price(self):
        assert self.prop.price == 133000.0

    def test_title(self):
        assert "Piso en Carrer" in self.prop.title

    def test_description(self):
        assert self.prop.description is not None

    def test_sub_category_from_url(self):
        assert self.prop.sub_category == "apartment"

    def test_specs_bedrooms(self):
        assert self.prop.specs["bedrooms"] == 4

    def test_specs_bathrooms(self):
        assert self.prop.specs["bathrooms"] == 1

    def test_specs_size(self):
        assert self.prop.specs["size"] == 100

    def test_municipality_from_json_ld(self):
        # Should prefer JSON-LD addressLocality over subtitle
        assert self.prop.municipality == "Sueca"

    def test_province_normalised(self):
        assert self.prop.province == "Valencia"

    def test_latitude(self):
        assert abs(self.prop.latitude - 39.196747) < 0.0001

    def test_longitude(self):
        assert abs(self.prop.longitude - (-0.31120336)) < 0.0001

    def test_images_list(self):
        assert len(self.prop.images) == 1
        assert "photo1.jpg" in self.prop.images[0]

    def test_source_url(self):
        assert self.prop.source_url is not None
        assert "60932948350_991284" in self.prop.source_url

    def test_type_sub_category_fallback(self):
        """When URL slug is unknown, fall back to the type-level sub_category."""
        item = {"source_id": "abc", "title": "Finca X"}
        prop = PisosComScraper.build_property(item, "sale", type_sub_category="house")
        assert prop.sub_category == "house"

    def test_rent_listing_type(self):
        prop = PisosComScraper.build_property(self.item, "rent")
        assert prop.listing_type == "rent"

    def test_rent_period_passed_through(self):
        item = {
            "source_id": "11111.0",
            "title": "Nave en Massanassa",
            "price_text": "8.000 €/mes",
            "rent_period": "month",
        }
        prop = PisosComScraper.build_property(item, "rent")
        assert prop.rent_period == "month"


# ── province normalisation ────────────────────────────────────────────────────

class TestNormaliseProvince:
    def test_valencian_spelling(self):
        assert PisosComScraper._normalise_province("Val\u00e8ncia") == "Valencia"

    def test_castellano_spelling(self):
        assert PisosComScraper._normalise_province("Valencia") == "Valencia"

    def test_alicante(self):
        assert PisosComScraper._normalise_province("Alicante") == "Alicante"

    def test_alacant(self):
        assert PisosComScraper._normalise_province("Alacant") == "Alicante"

    def test_castellon(self):
        assert PisosComScraper._normalise_province("Castell\u00f3n de la Plana") == "Castell\u00f3n"

    def test_none_input(self):
        assert PisosComScraper._normalise_province(None) is None

    def test_unknown_passthrough(self):
        assert PisosComScraper._normalise_province("Murcia") == "Murcia"

    def test_html_entity_valencian(self):
        # pisos.com JSON-LD sometimes contains raw HTML entities
        assert PisosComScraper._normalise_province("Val&#xE8;ncia") == "Val&#xE8;ncia"


# ── province HTML entity fix in parse_list_page ───────────────────────────────

LISTING_HTML_ENTITY = """
<div id="99999.000001" class="ad-preview"
     data-lnk-href="/comprar/piso-chiva-99999_000001/">
    <div class="ad-preview__bottom">
        <span class="ad-preview__price">200.000 €</span>
        <a href="/comprar/piso-chiva-99999_000001/" class="ad-preview__title">Piso en Chiva</a>
        <p class="p-sm ad-preview__subtitle">Chiva</p>
    </div>
    <script type="application/ld+json">
    {
        "@context": "https://schema.org/",
        "@type": "SingleFamilyResidence",
        "address": {
            "@type": "PostalAddress",
            "addressLocality": "Chiva",
            "addressRegion": "Val&#xE8;ncia"
        },
        "geo": {"@type": "GeoCoordinates", "latitude": "39.47", "longitude": "-0.70"}
    }
    </script>
</div>
"""


class TestHtmlEntityProvince:
    def test_entity_decoded_in_address_region(self):
        items = PisosComScraper.parse_list_page(LISTING_HTML_ENTITY)
        assert items[0]["address_region"] == "Val\u00e8ncia"

    def test_province_normalised_after_entity_decode(self):
        items = PisosComScraper.parse_list_page(LISTING_HTML_ENTITY)
        prop = PisosComScraper.build_property(items[0], "sale")
        assert prop.province == "Valencia"


# ── _normalise_image_url ──────────────────────────────────────────────────────

class TestNormaliseImageUrl:
    def test_fch_wp_to_h700(self):
        url = "https://fotos.imghs.net/fch-wp/1055/307/photo.jpg"
        assert PisosComScraper._normalise_image_url(url) == "https://fotos.imghs.net/h700-wp/1055/307/photo.jpg"

    def test_fchm_wp_to_h700(self):
        url = "https://fotos.imghs.net/fchm-wp/520494/photo2.jpg"
        assert PisosComScraper._normalise_image_url(url) == "https://fotos.imghs.net/h700-wp/520494/photo2.jpg"

    def test_mm_wp_to_h700(self):
        url = "https://fotos.imghs.net/mm-wp/991284/photo1.jpg"
        assert PisosComScraper._normalise_image_url(url) == "https://fotos.imghs.net/h700-wp/991284/photo1.jpg"

    def test_already_h700_unchanged(self):
        url = "https://fotos.imghs.net/h700-wp/1055/307/photo.jpg"
        assert PisosComScraper._normalise_image_url(url) == url

    def test_non_imghs_url_unchanged(self):
        url = "https://statics.imghs.net/dist/img/default_nophoto.jpg"
        assert PisosComScraper._normalise_image_url(url) == url

    def test_list_page_image_normalised(self):
        items = PisosComScraper.parse_list_page(LIST_PAGE_HTML)
        assert "h700-wp" in items[0]["image"]


# ── _parse_detail_data ────────────────────────────────────────────────────────

DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head><title>Terreno en venta</title></head>
<body>
<div class="details" data-ad-price="800000" data-ad-id="67580186902.105500"></div>
<h1>Terreno en venta en Capellanes</h1>
<div id="description_abc" class="description">
    <div class="description__content">Parcela rústica en venta en Quart de Poblet.</div>
</div>
<div class="location" data-params="latitude=39.4720477&amp;longitude=-0.5262189&amp;zoom=16"></div>
<div class="masonry">
    <div class="masonry__item">
        <img src="https://fotos.imghs.net/fch-wp/1055/307/photo1.jpg">
    </div>
    <div class="masonry__item">
        <img src="https://fotos.imghs.net/fchm-wp/1055/307/photo2.jpg">
    </div>
    <div class="masonry__item">
        <img src="" data-src="https://fotos.imghs.net/apps-wp/1055/307/photo3.jpg">
    </div>
</div>
<div class="features-container">
    <div class="features__feature">
        <span class="features__label">Superficie solar: </span>
        <span class="features__value">22.825 m²</span>
    </div>
    <div class="features__feature">
        <span class="features__label">Referencia: </span>
        <span class="features__value">1723</span>
    </div>
</div>
</body>
</html>
"""


class TestParseDetailData:
    def setup_method(self):
        self.data = PisosComScraper._parse_detail_data(DETAIL_HTML)

    def test_price(self):
        assert self.data["price"] == 800000.0

    def test_title(self):
        assert self.data["title"] == "Terreno en venta en Capellanes"

    def test_description(self):
        assert "Quart de Poblet" in self.data["description"]

    def test_latitude(self):
        assert abs(self.data["latitude"] - 39.4720477) < 0.0001

    def test_longitude(self):
        assert abs(self.data["longitude"] - (-0.5262189)) < 0.0001

    def test_images(self):
        assert len(self.data["images"]) == 3
        # All URLs normalized to h700-wp regardless of original format
        assert all("h700-wp" in url for url in self.data["images"])
        assert "photo1.jpg" in self.data["images"][0]
        assert "photo3.jpg" in self.data["images"][2]

    def test_specs_raw_label(self):
        assert self.data["specs"]["Superficie solar"] == "22.825 m²"
        assert self.data["specs"]["Referencia"] == "1723"

    def test_specs_size_parsed(self):
        assert self.data["specs"]["size"] == 22825.0

    def test_rent_period_month(self):
        html = """
        <html><body>
        <div class="price__value jsPriceValue">1.350 €<span>/mes</span></div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert data["rent_period"] == "month"

    def test_rent_period_week(self):
        html = """
        <html><body>
        <div class="price__value jsPriceValue">500 €<span>/sem</span></div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert data["rent_period"] == "week"

    def test_no_rent_period_for_sale(self):
        html = """
        <html><body>
        <div class="price__value jsPriceValue">250.000 €</div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert "rent_period" not in data

    def test_empty_html(self):
        data = PisosComScraper._parse_detail_data("<html><body></body></html>")
        assert "price" not in data
        assert "latitude" not in data
        assert "images" not in data

    def test_boolean_features_collected(self):
        html = """
        <html><body>
        <div class="features-container">
            <div class="features__feature">
                <span class="features__label">Piscina</span>
            </div>
            <div class="features__feature">
                <span class="features__label">Garaje</span>
            </div>
            <div class="features__feature">
                <span class="features__label">Superficie: </span>
                <span class="features__value">100 m²</span>
            </div>
        </div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert data["features"] == ["Piscina", "Garaje"]
        assert "Superficie" in data["specs"]

    def test_carousel_fallback(self):
        html = """
        <html><body>
        <div class="carousel__slide">
            <img src="https://fotos.imghs.net/fch-wp/999/photo1.jpg">
        </div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert data["images"] == ["https://fotos.imghs.net/h700-wp/999/photo1.jpg"]

    def test_no_photos_item_skipped(self):
        html = """
        <html><body>
        <div class="masonry">
            <div class="masonry__item masonry__item--no-photos">
                <div class="no-photo"><p>¡Vaya! Esta propiedad no tiene fotos</p></div>
            </div>
        </div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert "images" not in data

    def test_default_nophoto_url_filtered(self):
        html = """
        <html><body>
        <div class="masonry">
            <div class="masonry__item">
                <img src="https://statics.imghs.net/dist/img/default_nophoto.jpg">
            </div>
        </div>
        </body></html>
        """
        data = PisosComScraper._parse_detail_data(html)
        assert "images" not in data


# ── enrich_property ───────────────────────────────────────────────────────────

class TestEnrichProperty:
    def _make_prop(self, source_id="99999.000001", listing_type="sale"):
        from src.models import Property
        return Property(
            listing_type=listing_type,
            sub_category="plot",
            title="Terreno en Chiva",
            price=800000.0,
            location="Chiva",
            municipality="Chiva",
            province="Valencia",
            source="pisos-com",
            source_id=source_id,
            source_url="https://www.pisos.com/comprar/terreno-chiva-99999_000001/",
        )

    def test_returns_enriched_flag(self, monkeypatch):
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: DETAIL_HTML)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop()
        result = scraper.enrich_property(prop)

        assert result.enriched is True
        assert result is not prop

    def test_description_updated(self, monkeypatch):
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: DETAIL_HTML)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        result = scraper.enrich_property(self._make_prop())
        assert "Quart de Poblet" in result.description

    def test_images_updated(self, monkeypatch):
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: DETAIL_HTML)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        result = scraper.enrich_property(self._make_prop())
        assert len(result.images) == 3

    def test_coordinates_updated(self, monkeypatch):
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: DETAIL_HTML)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        result = scraper.enrich_property(self._make_prop())
        assert abs(result.latitude - 39.4720477) < 0.0001
        assert abs(result.longitude - (-0.5262189)) < 0.0001

    def test_specs_merged(self, monkeypatch):
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: DETAIL_HTML)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop()
        result = scraper.enrich_property(prop)
        assert result.specs["size"] == 22825.0

    def test_returns_original_on_fetch_error(self, monkeypatch):
        from src.scrapers.base import FetchError
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: (_ for _ in ()).throw(FetchError(403, url)))
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        prop = self._make_prop()
        result = scraper.enrich_property(prop)
        assert result is prop
        assert result.enriched is False

    def test_rent_period_propagated_from_detail(self, monkeypatch):
        detail_html = """
        <html><body>
        <div class="price__value jsPriceValue">1.350 €<span>/mes</span></div>
        <div class="description__content">Nave en alquiler.</div>
        </body></html>
        """
        scraper = PisosComScraper()
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: detail_html)
        monkeypatch.setattr(scraper, "_delay_sync", lambda: None)

        result = scraper.enrich_property(self._make_prop())
        assert result.rent_period == "month"

    def test_returns_original_when_no_source_url(self):
        from src.models import Property
        scraper = PisosComScraper()
        prop = Property(
            listing_type="sale", title="No URL", source="pisos-com",
            source_id="000", source_url=None,
        )
        result = scraper.enrich_property(prop)
        assert result is prop
