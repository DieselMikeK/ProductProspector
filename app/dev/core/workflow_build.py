from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from product_prospector.core.pricing_rules import (
    find_vendor_discount_file,
    load_vendor_discounts,
    resolve_discount_candidates,
)
from product_prospector.core.product_model import Product
from product_prospector.core.processing import normalize_sku
from product_prospector.core.session_state import MODE_NEW, MODE_UPDATE, AppSession
from product_prospector.core.vendor_profiles import resolve_vendor_profile
from product_prospector.core.vendor_normalization import normalize_vendor_name as normalize_vendor_from_rules


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_multi_value(raw: str) -> list[str]:
    text = _clean_text(raw)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    return [item.strip() for item in parts if item and item.strip()]


def _row_value(row: pd.Series, column_name: str) -> str:
    if not column_name:
        return ""
    if column_name not in row.index:
        return ""
    return _clean_text(row[column_name])


def _to_float(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _rows_from_session(session: AppSession) -> list[pd.Series]:
    rows: list[pd.Series] = []
    if session.vendor_df is not None and not session.vendor_df.empty:
        rows.extend([row for _, row in session.vendor_df.iterrows()])
    scope_skus = session.target_skus or session.pasted_skus
    if scope_skus:
        existing = {
            normalize_sku(_row_value(row, session.source_mapping.sku or "sku"))
            for row in rows
        }
        for sku in scope_skus:
            sku_norm = normalize_sku(sku)
            if not sku_norm or sku_norm in existing:
                continue
            rows.append(pd.Series({session.source_mapping.sku or "sku": sku_norm}))
            existing.add(sku_norm)
    return rows


@dataclass
class BuildStats:
    rows_considered: int = 0
    rows_built: int = 0
    rows_skipped_missing_sku: int = 0
    rows_skipped_no_shopify_match: int = 0


def build_existing_shopify_index(shopify_df: pd.DataFrame | None) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    if shopify_df is None or shopify_df.empty:
        return index
    for _, row in shopify_df.iterrows():
        sku = normalize_sku(row.get("sku", ""))
        if not sku:
            continue
        if sku in index:
            continue
        index[sku] = {
            "sku": sku,
            "title": _clean_text(row.get("title", "")),
            "description_html": _clean_text(row.get("description", "")),
            "application": _clean_text(row.get("fitment", "")),
            "type": _clean_text(row.get("product_type", "")),
            "vendor": _clean_text(row.get("vendor", "")),
            "barcode": _clean_text(row.get("barcode", "")),
        }
    return index


def _can_infer_cost_for_sku_rows(
    sku_rows: list[pd.Series],
    mapping,
    discounts_df: pd.DataFrame,
    required_root: Path | None = None,
) -> bool:
    if not sku_rows:
        return False
    if discounts_df is None or discounts_df.empty:
        return False

    vendor_value = ""
    title_value = ""
    type_value = ""
    price_value: float | None = None
    for row in sku_rows:
        if not vendor_value:
            vendor_value = _row_value(row, getattr(mapping, "vendor", ""))
        if not title_value:
            title_value = _row_value(row, getattr(mapping, "title", ""))
        if not type_value:
            type_value = _row_value(row, "type")
        if price_value is None:
            price_value = _to_float(_row_value(row, getattr(mapping, "price", "")))

    if price_value is None or not vendor_value:
        return False
    profile = resolve_vendor_profile(vendor_value, required_root=required_root)
    vendor_value = (
        (profile.discount_vendor_key if profile is not None else "")
        or normalize_vendor_from_rules(vendor_value, required_root=required_root)
        or vendor_value
    )

    options = resolve_discount_candidates(
        discounts_df=discounts_df,
        vendor_name=vendor_value,
        product_title=title_value,
        product_type=type_value,
    )
    return bool(options)


def detect_missing_required_fields(session: AppSession, required_root: Path | None = None) -> list[str]:
    required: list[str] = []
    if session.mode == MODE_UPDATE:
        selected = set(session.update_fields or [])
        if "title" in selected:
            required.append("title")
        if "price" in selected:
            required.append("price")
        if "cost" in selected:
            required.append("cost")
        if "description_html" in selected:
            required.append("description")
        if "media_urls" in selected:
            required.append("media")
        if "vendor" in selected:
            required.append("vendor")
        if "weight" in selected:
            required.append("weight")
        if "barcode" in selected:
            required.append("barcode")
        if "application" in selected:
            required.append("application")
        if (
            "type" in selected
            or "google_product_type" in selected
            or "category_code" in selected
            or "product_subtype" in selected
        ):
            required.append("title")
    else:
        required = ["title", "description", "media", "price", "cost", "vendor", "application"]

    dedup_required: list[str] = []
    seen_required: set[str] = set()
    for field in required:
        if field in seen_required:
            continue
        seen_required.add(field)
        dedup_required.append(field)
    required = dedup_required

    target_skus = collect_session_skus(session)
    if not target_skus:
        return required

    if session.vendor_df is None or session.vendor_df.empty:
        return required

    sku_column = _clean_text(session.source_mapping.sku)
    if not sku_column or sku_column not in session.vendor_df.columns:
        return required

    rows_by_sku: dict[str, list[pd.Series]] = {}
    for _, row in session.vendor_df.iterrows():
        sku_value = normalize_sku(_row_value(row, sku_column))
        if not sku_value:
            continue
        rows_by_sku.setdefault(sku_value, []).append(row)

    discounts_df = pd.DataFrame()
    if "cost" in required and required_root is not None:
        discount_file = find_vendor_discount_file(required_root)
        if discount_file is not None:
            try:
                discounts_df = load_vendor_discounts(discount_file)
            except Exception:
                discounts_df = pd.DataFrame()

    mapping = session.source_mapping
    missing: list[str] = []
    for field in required:
        mapped_column = getattr(mapping, field, "") if hasattr(mapping, field) else ""

        field_has_gap = False
        for sku in target_skus:
            sku_rows = rows_by_sku.get(sku, [])
            if not sku_rows:
                field_has_gap = True
                break

            if field == "cost":
                cost_mapped = bool(mapped_column and mapped_column in session.vendor_df.columns)
                sku_has_value = cost_mapped and any(_row_value(row, mapped_column) for row in sku_rows)
                if not sku_has_value:
                    sku_has_value = _can_infer_cost_for_sku_rows(
                        sku_rows=sku_rows,
                        mapping=mapping,
                        discounts_df=discounts_df,
                        required_root=required_root,
                    )
                if not sku_has_value:
                    field_has_gap = True
                    break
                continue

            if not mapped_column or mapped_column not in session.vendor_df.columns:
                field_has_gap = True
                break

            sku_has_value = any(_row_value(row, mapped_column) for row in sku_rows)
            if not sku_has_value:
                field_has_gap = True
                break
        if field_has_gap:
            missing.append(field)
    return missing


def _set_if_present(product: Product, field_name: str, raw_value: object, source: str) -> None:
    text = _clean_text(raw_value)
    if not text:
        return
    if field_name == "media_urls":
        product.media_urls = _split_multi_value(text)
        product.field_sources["media_urls"] = source
        product.field_status["media_urls"] = "ok" if product.media_urls else "missing"
        return
    product.set_field(field_name, text, source)


def build_products_from_session(
    session: AppSession,
    existing_shopify_index: dict[str, dict[str, str]] | None = None,
    scraped_records: dict[str, dict[str, str]] | None = None,
) -> tuple[list[Product], BuildStats]:
    mapping = session.source_mapping
    products: list[Product] = []
    stats = BuildStats()
    rows = _rows_from_session(session)
    stats.rows_considered = len(rows)
    existing_index = existing_shopify_index or {}
    scraped_index = scraped_records or {}

    for row in rows:
        sku = normalize_sku(_row_value(row, mapping.sku))
        if not sku:
            stats.rows_skipped_missing_sku += 1
            continue

        existing = existing_index.get(sku, {})
        if session.mode == MODE_UPDATE and not existing:
            stats.rows_skipped_no_shopify_match += 1
            continue

        product = Product()
        product.set_field("sku", sku, "input")

        if existing:
            _set_if_present(product, "title", existing.get("title", ""), "shopify")
            _set_if_present(product, "description_html", existing.get("description_html", ""), "shopify")
            _set_if_present(product, "application", existing.get("application", ""), "shopify")
            _set_if_present(product, "type", existing.get("type", ""), "shopify")
            _set_if_present(product, "vendor", existing.get("vendor", ""), "shopify")
            _set_if_present(product, "barcode", existing.get("barcode", ""), "shopify")

        spreadsheet_values = {
            "vendor": _row_value(row, mapping.vendor),
            "title": _row_value(row, mapping.title),
            "description_html": _row_value(row, mapping.description),
            "media_urls": _row_value(row, mapping.media),
            "price": _row_value(row, mapping.price),
            "cost": _row_value(row, mapping.cost),
            "barcode": _row_value(row, mapping.barcode),
            "weight": _row_value(row, mapping.weight),
            "application": _row_value(row, mapping.application),
        }

        if session.mode == MODE_NEW:
            for field_name, value in spreadsheet_values.items():
                _set_if_present(product, field_name, value, "spreadsheet")
        elif session.mode == MODE_UPDATE:
            selected = set(session.update_fields or [])
            if "category_code" in selected or "product_subtype" in selected or "google_product_type" in selected:
                selected.add("type")
            for field_name, value in spreadsheet_values.items():
                if field_name not in selected:
                    continue
                _set_if_present(product, field_name, value, "spreadsheet")
            product.field_sources["update_scope"] = ",".join(sorted(selected))

        scraped_values = scraped_index.get(sku, {})
        if scraped_values:
            if session.mode == MODE_NEW:
                for field_name in [
                    "vendor",
                    "title",
                    "description_html",
                    "media_urls",
                    "price",
                    "cost",
                    "barcode",
                    "weight",
                    "application",
                ]:
                    existing_value = _clean_text(getattr(product, field_name, ""))
                    if existing_value:
                        continue
                    _set_if_present(product, field_name, scraped_values.get(field_name, ""), "scraper")
            elif session.mode == MODE_UPDATE:
                selected = set(session.update_fields or [])
                for field_name in selected:
                    existing_value = _clean_text(getattr(product, field_name, ""))
                    if existing_value:
                        continue
                    _set_if_present(product, field_name, scraped_values.get(field_name, ""), "scraper")

        product.finalize_defaults()
        products.append(product)
        stats.rows_built += 1

    deduped: dict[str, Product] = {}
    for product in products:
        if product.sku and product.sku not in deduped:
            deduped[product.sku] = product
    return list(deduped.values()), stats


def products_to_dataframe(products: list[Product]) -> pd.DataFrame:
    if not products:
        return pd.DataFrame()
    return pd.DataFrame([product.to_row() for product in products])


def merge_mode_label(mode: str) -> str:
    if mode == MODE_NEW:
        return "Create New Product"
    if mode == MODE_UPDATE:
        return "Update Existing Product"
    return "Not Selected"


def collect_session_skus(session: AppSession) -> list[str]:
    scope_skus = session.target_skus or session.pasted_skus
    if scope_skus:
        values: list[str] = []
        seen: set[str] = set()
        for raw in scope_skus:
            sku = normalize_sku(raw)
            if not sku or sku in seen:
                continue
            seen.add(sku)
            values.append(sku)
        return values

    rows = _rows_from_session(session)
    sku_column = session.source_mapping.sku or "sku"
    values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        sku = normalize_sku(_row_value(row, sku_column))
        if not sku:
            continue
        if sku in seen:
            continue
        seen.add(sku)
        values.append(sku)
    return values
