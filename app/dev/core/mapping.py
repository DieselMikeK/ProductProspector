from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from product_prospector.core.years import parse_years_from_text


@dataclass
class ColumnSuggestion:
    column: str | None
    score: float
    reason: str


FIELD_ALIASES: dict[str, list[str]] = {
    "sku": [
        "sku",
        "sky",
        "part number",
        "part #",
        "item number",
        "item #",
        "model number",
        "product number",
        "pn",
        "mpn",
        "manufacturer part",
    ],
    "title": ["title", "name", "product name", "item name", "short description"],
    "description": ["description", "long description", "details", "product description", "desc"],
    "fitment": ["fitment", "application", "apps", "vehicle", "compatibility", "fits", "fit"],
    "years": ["years", "year", "model years", "fitment years", "application years"],
    "price": ["price", "retail", "msrp", "list price"],
    "msrp_price": ["msrp", "retail", "retail price", "list price", "suggested retail"],
    "map_price": ["map", "minimum advertised price", "m a p", "m.a.p"],
    "jobber_price": ["jobber", "jobber price", "jobber net", "jobber cost"],
    "dealer_cost": ["dealer", "dealer price", "dealer cost", "dealer net", "dealer t1", "dealer t2"],
    "cost": ["cost", "dealer", "wholesale", "net price"],
    "map": ["map", "minimum advertised price"],
    "core_charge_product_code": ["core", "core charge", "corecharge", "core value"],
    "image_url": ["image", "image url", "image link", "picture", "photo", "img", "media"],
    "barcode": ["barcode", "upc", "ean", "gtin"],
    "product_number": ["product number", "part number", "manufacturer part", "mpn"],
}


def normalize_header(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _non_empty_values(series: pd.Series, max_values: int = 200) -> list[str]:
    values: list[str] = []
    for raw in series:
        if pd.isna(raw):
            continue
        text = str(raw).strip()
        if not text:
            continue
        values.append(text)
        if len(values) >= max_values:
            break
    return values


def _score_alias(header_normalized: str, field_name: str) -> tuple[float, str]:
    aliases = [normalize_header(alias) for alias in FIELD_ALIASES.get(field_name, [])]
    for alias in aliases:
        if header_normalized == alias:
            return 1.0, f"Header exactly matches '{alias}'."
    for alias in aliases:
        if alias and alias in header_normalized:
            return 0.8, f"Header contains alias '{alias}'."
    return 0.0, "No header alias match."


def _looks_like_currency(value: str) -> bool:
    return bool(re.search(r"^\$?\s*\d+(?:[\.,]\d{1,2})?$", value.strip()))


def _looks_like_barcode(value: str) -> bool:
    compact = re.sub(r"\D", "", value)
    return 8 <= len(compact) <= 14


def _looks_like_image_url(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered.startswith(("http://", "https://")):
        return False
    return any(ext in lowered for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif", "image", "img"])


def _score_values(field_name: str, values: list[str]) -> tuple[float, str]:
    if not values:
        return 0.0, "No non-empty samples."

    sample = values[:100]
    total = len(sample)
    ratio = 0.0

    if field_name == "years":
        hits = sum(1 for item in sample if parse_years_from_text(item))
        ratio = hits / total
        return ratio, f"{hits}/{total} values contain parseable years."

    if field_name in {"price", "cost", "map", "jobber_price", "msrp_price", "map_price", "dealer_cost"}:
        hits = sum(1 for item in sample if _looks_like_currency(item))
        ratio = hits / total
        return ratio, f"{hits}/{total} values look like currency."

    if field_name == "core_charge_product_code":
        hits = sum(
            1
            for item in sample
            if bool(re.search(r"(?i)core", item))
            or bool(re.search(r"\b[0-9]{2,5}\b", item.replace(",", "")))
        )
        ratio = hits / total
        return ratio, f"{hits}/{total} values look like core-charge values."

    if field_name == "barcode":
        hits = sum(1 for item in sample if _looks_like_barcode(item))
        ratio = hits / total
        return ratio, f"{hits}/{total} values look like barcode/UPC."

    if field_name == "image_url":
        hits = sum(1 for item in sample if _looks_like_image_url(item))
        ratio = hits / total
        return ratio, f"{hits}/{total} values look like image URLs."

    if field_name == "fitment":
        hints = ["ford", "chevy", "gmc", "dodge", "ram", "fits", "application", "year"]
        hits = sum(1 for item in sample if any(hint in item.lower() for hint in hints))
        ratio = hits / total
        return ratio, f"{hits}/{total} values contain fitment-like tokens."

    if field_name in {"title", "description"}:
        avg_length = sum(len(item) for item in sample) / total
        ratio = min(avg_length / 80.0, 1.0)
        return ratio, f"Average length {avg_length:.1f} characters."

    return 0.0, "No value heuristic for this field."


def suggest_column_for_field(
    df: pd.DataFrame,
    field_name: str,
    excluded_columns: Iterable[str] | None = None,
) -> ColumnSuggestion:
    excluded = set(excluded_columns or [])

    best_column: str | None = None
    best_score = -1.0
    best_reason = "No candidate columns."

    for column in df.columns:
        if column in excluded:
            continue

        header_norm = normalize_header(column)
        alias_score, alias_reason = _score_alias(header_norm, field_name)
        values = _non_empty_values(df[column])
        value_score, value_reason = _score_values(field_name, values)

        combined = (alias_score * 0.75) + (value_score * 0.25)
        reason = f"{alias_reason} {value_reason}"
        if combined > best_score:
            best_score = combined
            best_column = column
            best_reason = reason

    if best_score < 0.15:
        return ColumnSuggestion(column=None, score=max(best_score, 0.0), reason=best_reason)
    return ColumnSuggestion(column=best_column, score=best_score, reason=best_reason)


def suggest_columns(
    df: pd.DataFrame,
    fields: Iterable[str],
    excluded_columns: Iterable[str] | None = None,
) -> dict[str, ColumnSuggestion]:
    suggestions: dict[str, ColumnSuggestion] = {}
    for field in fields:
        suggestions[field] = suggest_column_for_field(df=df, field_name=field, excluded_columns=excluded_columns)
    return suggestions
