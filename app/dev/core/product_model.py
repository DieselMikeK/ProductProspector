from __future__ import annotations

import json
import re
from dataclasses import dataclass, field


PRODUCT_EXPORT_COLUMNS = [
    "sku",
    "product_url",
    "title",
    "description_html",
    "media_urls",
    "price",
    "map_price",
    "msrp_price",
    "jobber_price",
    "cost",
    "dealer_cost",
    "inventory",
    "barcode",
    "weight",
    "vendor",
    "type",
    "google_product_type",
    "category_code",
    "product_subtype",
    "core_charge_product_code",
    "mpn",
    "brand",
    "application",
    "collections",
    "tags",
    "metafields",
]


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _flatten_list_like_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""

    if text.startswith("[") and text.endswith("]"):
        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None

        if isinstance(parsed, list):
            values: list[str] = []
            seen: set[str] = set()
            for item in parsed:
                item_text = _clean_text(item)
                key = item_text.lower()
                if not item_text or key in seen:
                    continue
                seen.add(key)
                values.append(item_text)
            if len(values) == 1:
                return values[0]
            if values:
                return " | ".join(values)

        # Fallback for python-style list repr: ['value']
        single_match = re.match(r'^\[\s*[\'"](.+)[\'"]\s*\]$', text)
        if single_match:
            return _clean_text(single_match.group(1))

    return text


@dataclass
class Product:
    record_type: str = "Product"
    parent_has_variants: bool = False
    product_gid: str = ""
    product_id: str = ""
    variant_gid: str = ""
    variant_id: str = ""
    inventory_item_gid: str = ""
    inventory_item_id: str = ""
    variant_option_summary: str = ""
    variant_google_mpn: str = ""
    variant_enable_low_stock_message: str = ""
    variant_weight_unit: str = ""
    original_variant_weight_value: str = ""
    original_variant_weight_unit: str = ""
    title: str = ""
    description_html: str = ""
    media_urls: list[str] = field(default_factory=list)
    price: str = ""
    map_price: str = ""
    msrp_price: str = ""
    jobber_price: str = ""
    cost: str = ""
    dealer_cost: str = ""
    inventory: int = 3000000
    sku: str = ""
    product_url: str = ""
    barcode: str = ""
    weight: str = ""
    vendor: str = ""
    type: str = ""
    google_product_type: str = ""
    category_code: str = ""
    product_subtype: str = ""
    core_charge_product_code: str = ""
    mpn: str = ""
    brand: str = ""
    application: str = ""
    collections: str = ""
    tags: list[str] = field(default_factory=list)
    metafields: dict[str, str] = field(default_factory=dict)
    field_sources: dict[str, str] = field(default_factory=dict)
    field_status: dict[str, str] = field(default_factory=dict)
    scrape_status: str = ""
    scrape_fields_found: str = ""
    scrape_error: str = ""
    scrape_mismatch_error: str = ""
    media_folder: str = ""
    remove_recommended: bool = False
    remove_marked: bool = False
    remove_reason: str = ""
    excluded: bool = False
    exclusion_reason: str = ""

    def set_field(self, name: str, value: object, source: str) -> None:
        text = _clean_text(value)
        if not hasattr(self, name):
            return
        setattr(self, name, text)
        self.field_sources[name] = source
        self.field_status[name] = "ok" if text else "missing"

    def finalize_defaults(self) -> None:
        self.sku = _clean_text(self.sku).upper()
        # Shopify list-style text can surface as JSON arrays like ["Suspension & Steering"].
        # Flatten those to plain strings for review/push consistency.
        self.type = _flatten_list_like_text(self.type)
        self.google_product_type = _flatten_list_like_text(self.google_product_type)
        self.category_code = _flatten_list_like_text(self.category_code)
        self.product_subtype = _flatten_list_like_text(self.product_subtype)
        self.mpn = _clean_text(self.mpn) or self.sku
        self.brand = _clean_text(self.brand) or _clean_text(self.vendor)

    def to_row(self) -> dict[str, str]:
        return {
            "record_type": _clean_text(self.record_type) or "Product",
            "parent_has_variants": "yes" if bool(self.parent_has_variants) else "",
            "product_id": _clean_text(self.product_id),
            "variant_id": _clean_text(self.variant_id),
            "variant_option_summary": _clean_text(self.variant_option_summary),
            "excluded": "yes" if bool(self.excluded) else "",
            "exclusion_reason": _clean_text(self.exclusion_reason),
            "scrape_status": _clean_text(self.scrape_status),
            "scrape_fields_found": _clean_text(self.scrape_fields_found),
            "scrape_error": _clean_text(self.scrape_error),
            "scrape_mismatch_error": _clean_text(self.scrape_mismatch_error),
            "media_folder": _clean_text(self.media_folder),
            "remove_recommended": "yes" if bool(self.remove_recommended) else "",
            "remove_marked": "yes" if bool(self.remove_marked) else "",
            "remove_reason": _clean_text(self.remove_reason),
            "sku": _clean_text(self.sku),
            "product_url": _clean_text(self.product_url),
            "title": _clean_text(self.title),
            "description_html": _clean_text(self.description_html),
            "media_urls": " | ".join([item for item in self.media_urls if _clean_text(item)]),
            "price": _clean_text(self.price),
            "map_price": _clean_text(self.map_price),
            "msrp_price": _clean_text(self.msrp_price),
            "jobber_price": _clean_text(self.jobber_price),
            "cost": _clean_text(self.cost),
            "dealer_cost": _clean_text(self.dealer_cost),
            "inventory": str(self.inventory),
            "barcode": _clean_text(self.barcode),
            "weight": _clean_text(self.weight),
            "vendor": _clean_text(self.vendor),
            "type": _clean_text(self.type),
            "google_product_type": _clean_text(self.google_product_type),
            "category_code": _clean_text(self.category_code),
            "product_subtype": _clean_text(self.product_subtype),
            "core_charge_product_code": _clean_text(self.core_charge_product_code),
            "mpn": _clean_text(self.mpn),
            "brand": _clean_text(self.brand),
            "application": _clean_text(self.application),
            "collections": _clean_text(self.collections),
            "variant_google_mpn": _clean_text(self.variant_google_mpn),
            "variant_enable_low_stock_message": _clean_text(self.variant_enable_low_stock_message),
            "variant_weight_unit": _clean_text(self.variant_weight_unit),
            "tags": " | ".join([item for item in self.tags if _clean_text(item)]),
            "metafields": str(self.metafields) if self.metafields else "",
        }
