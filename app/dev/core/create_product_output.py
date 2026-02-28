from __future__ import annotations

import pandas as pd


CREATE_OUTPUT_COLUMNS = [
    "sku",
    "title",
    "description",
    "vendor",
    "brand_metafield",
    "barcode",
    "price",
    "cost_per_item",
    "weight_lb",
    "inventory_available",
    "status",
    "media_urls",
    "product_type",
    "tags",
    "metafield:custom.application",
    "metafield:custom.google_product_type",
    "metafield:custom.product_subtype",
    "metafield:custom.category_codes_simplified",
    "variant_metafield:custom.enable_low_stock",
    "variant_metafield:google.mpn",
]


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_create_product_output(plan_df: pd.DataFrame, default_vendor: str = "") -> pd.DataFrame:
    if plan_df is None or plan_df.empty:
        return pd.DataFrame(columns=CREATE_OUTPUT_COLUMNS)

    create_rows = plan_df[plan_df.get("row_action", "") == "create"].copy()
    if create_rows.empty:
        return pd.DataFrame(columns=CREATE_OUTPUT_COLUMNS)

    output_rows: list[dict[str, str]] = []
    for _, row in create_rows.iterrows():
        sku = _text(row.get("sku", ""))
        title = _text(row.get("vendor_title", "")) or _text(row.get("proposed_title", ""))
        description = _text(row.get("vendor_description", ""))
        application = _text(row.get("vendor_fitment", "")) or _text(row.get("proposed_fitment", ""))
        vendor = _text(default_vendor)

        output_rows.append(
            {
                "sku": sku,
                "title": title,
                "description": description,
                "vendor": vendor,
                "brand_metafield": vendor,
                "barcode": "",
                "price": "",
                "cost_per_item": "",
                "weight_lb": "2",
                "inventory_available": "3000000",
                "status": "draft",
                "media_urls": "",
                "product_type": "",
                "tags": "",
                "metafield:custom.application": application,
                "metafield:custom.google_product_type": "",
                "metafield:custom.product_subtype": "",
                "metafield:custom.category_codes_simplified": "",
                "variant_metafield:custom.enable_low_stock": "true",
                "variant_metafield:google.mpn": sku,
            }
        )

    output = pd.DataFrame(output_rows)
    for column in CREATE_OUTPUT_COLUMNS:
        if column not in output.columns:
            output[column] = ""
    return output[CREATE_OUTPUT_COLUMNS]
