from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from product_prospector.core.years import apply_year_policy, format_years_compact, parse_years_from_many, replace_years_in_text


RUN_MODE_UPDATE = "Update Existing"
RUN_MODE_CREATE = "Create New"
RUN_MODE_UPSERT = "Upsert"


@dataclass
class PlanningConfig:
    run_mode: str
    year_policy: str
    vendor_sku_column: str
    vendor_title_column: str | None
    vendor_description_column: str | None
    vendor_fitment_column: str | None
    vendor_year_columns: list[str]
    shopify_sku_column: str | None
    shopify_title_column: str | None
    shopify_description_column: str | None
    shopify_fitment_column: str | None
    propose_title_year_update: bool
    only_rows_with_year_changes: bool


def normalize_sku(value: str | None) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip().upper()
    return " ".join(cleaned.split())


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _combine_unique(values: Iterable[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in values:
        text = _normalize_text(item)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return " | ".join(ordered)


def stitch_rows_by_sku(df: pd.DataFrame, sku_column: str, carry_down_sku: bool = True) -> pd.DataFrame:
    if sku_column not in df.columns:
        raise ValueError(f"SKU column '{sku_column}' not found in uploaded file.")

    working = df.copy()
    sku_series = working[sku_column].apply(normalize_sku).replace("", pd.NA)

    if carry_down_sku:
        working["_sku_anchor"] = sku_series.ffill()
    else:
        working["_sku_anchor"] = sku_series

    working = working[working["_sku_anchor"].notna()].copy()
    if working.empty:
        return working.drop(columns=["_sku_anchor"], errors="ignore")

    aggregated_rows: list[dict[str, str]] = []
    for sku_value, group in working.groupby("_sku_anchor", sort=False):
        row: dict[str, str] = {sku_column: str(sku_value)}
        for column in working.columns:
            if column == "_sku_anchor" or column == sku_column:
                continue
            row[column] = _combine_unique(group[column].tolist())
        row["_source_rows"] = str(len(group))
        aggregated_rows.append(row)

    result = pd.DataFrame(aggregated_rows)
    ordered_columns = [sku_column] + [c for c in result.columns if c != sku_column]
    return result[ordered_columns]


def _determine_row_action(run_mode: str, has_match: bool) -> str:
    if run_mode == RUN_MODE_UPDATE:
        return "update" if has_match else "skip"
    if run_mode == RUN_MODE_CREATE:
        return "skip" if has_match else "create"
    if run_mode == RUN_MODE_UPSERT:
        return "update" if has_match else "create"
    return "skip"


def _row_value(row: pd.Series | None, column: str | None) -> str:
    if row is None or column is None or column not in row.index:
        return ""
    return _normalize_text(row[column])


def _collect_values(row: pd.Series | None, columns: list[str]) -> list[str]:
    if row is None:
        return []
    values: list[str] = []
    for column in columns:
        if column in row.index:
            values.append(_normalize_text(row[column]))
    return values


def _match_index(df: pd.DataFrame | None, sku_column: str | None) -> dict[str, pd.Series]:
    if df is None or sku_column is None or sku_column not in df.columns:
        return {}

    index: dict[str, pd.Series] = {}
    for _, row in df.iterrows():
        sku_value = normalize_sku(row.get(sku_column, ""))
        if not sku_value:
            continue
        if sku_value not in index:
            index[sku_value] = row
    return index


def build_action_plan(
    vendor_df: pd.DataFrame,
    shopify_df: pd.DataFrame | None,
    config: PlanningConfig,
) -> pd.DataFrame:
    shopify_index = _match_index(shopify_df, config.shopify_sku_column)
    rows: list[dict[str, str]] = []

    for _, vendor_row in vendor_df.iterrows():
        sku_value = normalize_sku(vendor_row.get(config.vendor_sku_column, ""))
        if not sku_value:
            continue

        match_row = shopify_index.get(sku_value)
        has_match = match_row is not None
        row_action = _determine_row_action(config.run_mode, has_match)

        vendor_title = _row_value(vendor_row, config.vendor_title_column)
        vendor_description = _row_value(vendor_row, config.vendor_description_column)
        vendor_fitment = _row_value(vendor_row, config.vendor_fitment_column)
        vendor_year_values = _collect_values(vendor_row, config.vendor_year_columns)

        shopify_title = _row_value(match_row, config.shopify_title_column)
        shopify_description = _row_value(match_row, config.shopify_description_column)
        shopify_fitment = _row_value(match_row, config.shopify_fitment_column)

        incoming_years = parse_years_from_many(vendor_year_values + [vendor_fitment, vendor_title, vendor_description])
        current_years = parse_years_from_many([shopify_fitment, shopify_title, shopify_description])
        final_years = apply_year_policy(current=current_years, incoming=incoming_years, policy=config.year_policy)

        current_years_text = format_years_compact(current_years)
        incoming_years_text = format_years_compact(incoming_years)
        final_years_text = format_years_compact(final_years)
        year_change = final_years != current_years

        proposed_fitment = vendor_fitment or shopify_fitment
        if proposed_fitment and final_years_text and not incoming_years_text:
            proposed_fitment = replace_years_in_text(proposed_fitment, final_years)
        elif proposed_fitment and incoming_years_text:
            proposed_fitment = replace_years_in_text(proposed_fitment, final_years)

        proposed_title = shopify_title
        if config.propose_title_year_update and shopify_title and final_years_text:
            proposed_title = replace_years_in_text(shopify_title, final_years)

        rows.append(
            {
                "sku": sku_value,
                "row_action": row_action,
                "match_status": "matched" if has_match else "unmatched",
                "vendor_title": vendor_title,
                "vendor_description": vendor_description,
                "vendor_fitment": vendor_fitment,
                "vendor_years": incoming_years_text,
                "shopify_title_current": shopify_title,
                "shopify_description_current": shopify_description,
                "shopify_fitment_current": shopify_fitment,
                "shopify_years_current": current_years_text,
                "years_final": final_years_text,
                "years_changed": "yes" if year_change else "no",
                "proposed_title": proposed_title,
                "proposed_fitment": proposed_fitment,
                "source_row_count": _row_value(vendor_row, "_source_rows"),
            }
        )

    plan = pd.DataFrame(rows)
    if plan.empty:
        return plan

    if config.only_rows_with_year_changes:
        plan = plan[(plan["years_changed"] == "yes") | (plan["row_action"] == "create")].copy()

    return plan.reset_index(drop=True)

