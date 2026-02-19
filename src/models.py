from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, model_validator


class Translation(BaseModel):
    property_id: str = ""
    locale: str
    title: str | None = None
    description: str | None = None
    features: list[str] | None = None


class Property(BaseModel):
    id: str = ""
    listing_type: Literal["sale", "rent", "new-building"]
    sub_category: Literal["apartment", "house", "commerce", "plot"] | None = None
    status: str = "available"
    title: str
    description: str | None = None
    price: float | None = None
    location: str | None = None
    region: str = "Comunidad Valenciana"
    province: str | None = None
    municipality: str | None = None
    neighborhood: str | None = None
    postal_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    images: list[str] = []
    specs: dict = {}
    features: list[str] = []
    source: str = "idealista"
    source_id: str
    source_url: str | None = None
    enriched: bool = False
    translations: list[Translation] = []

    @model_validator(mode="after")
    def set_id(self) -> Property:
        if not self.id:
            self.id = f"{self.source}-{self.source_id}"
        return self
