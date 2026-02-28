from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

@dataclass
class DiscountMatch:
    vendor_label: str
    discount_percent: float
    score: float
    reason: str


def _norm_text(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tokens(value: str | None) -> list[str]:
    text = _norm_text(value)
    if not text:
        return []
    return [tok for tok in text.split(" ") if tok]


def _clean_cell(value: object) -> str:
    return str(value or "").strip()


def _to_percent(value: object) -> float | None:
    if value is None:
        return None
    text = _clean_cell(value)
    if not text:
        return None
    low = text.lower()
    if "n/a" in low or low == "na":
        return None
    compact = re.sub(r"\s+", "", text)
    if re.fullmatch(r"-?\d+(?:\.\d+)?%?", compact):
        numeric = float(compact.replace("%", ""))
        if numeric <= 0:
            return None
        return numeric
    numbers = [float(item) for item in re.findall(r"\d+(?:\.\d+)?", text)]
    if not numbers:
        return None
    positives = [num for num in numbers if num > 0]
    if not positives:
        return None
    return min(positives)


def find_vendor_discount_file(required_root: Path) -> Path | None:
    candidates = [
        required_root / "mappings" / "VendorDiscounts.csv",
        required_root / "mappings" / "vendor_discounts.csv",
        required_root / "mappings" / "VendorDiscounts.xlsx",
        required_root / "mappings" / "pricing" / "VendorDiscounts.csv",
        required_root / "mappings" / "pricing" / "vendor_discounts.csv",
        required_root / "mappings" / "pricing" / "VendorDiscounts.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _load_raw_rows(path: Path) -> list[list[str]]:
    suffix = path.suffix.lower()
    rows: list[list[str]] = []
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                rows.append([_clean_cell(item) for item in row])
        return rows
    if suffix in {".xlsx", ".xls"}:
        table = pd.read_excel(path, header=None, dtype=str, keep_default_na=False)
        for _, row in table.iterrows():
            rows.append([_clean_cell(item) for item in row.tolist()])
        return rows
    return rows


def _find_header_index(rows: list[list[str]]) -> int:
    for idx, row in enumerate(rows):
        first = _norm_text(row[0] if len(row) > 0 else "")
        second = _norm_text(row[1] if len(row) > 1 else "")
        if first == "manufacturer" and second == "supplier":
            return idx
    return -1


def load_vendor_discounts(path: Path) -> pd.DataFrame:
    rows = _load_raw_rows(path)
    if not rows:
        return pd.DataFrame(
            columns=[
                "vendor_block",
                "vendor_block_norm",
                "row_a",
                "row_b",
                "row_c",
                "row_d",
                "row_e",
                "is_star_row",
            ]
        )

    header_idx = _find_header_index(rows)
    if header_idx < 0:
        header_idx = 0

    records: list[dict[str, object]] = []
    current_vendor = ""
    current_vendor_norm = ""
    for row in rows[header_idx + 1 :]:
        a = _clean_cell(row[0] if len(row) > 0 else "")
        b = _clean_cell(row[1] if len(row) > 1 else "")
        c = _clean_cell(row[2] if len(row) > 2 else "")
        d = _clean_cell(row[3] if len(row) > 3 else "")
        e = _clean_cell(row[4] if len(row) > 4 else "")
        if not any([a, b, c, d, e]):
            continue

        is_star_row = a.startswith("*")
        if a and not is_star_row:
            current_vendor = a
            current_vendor_norm = _norm_text(a)

        if not current_vendor:
            continue

        records.append(
            {
                "vendor_block": current_vendor,
                "vendor_block_norm": current_vendor_norm,
                "row_a": a,
                "row_b": b,
                "row_c": c,
                "row_d": d,
                "row_e": e,
                "is_star_row": is_star_row,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=[
                "vendor_block",
                "vendor_block_norm",
                "row_a",
                "row_b",
                "row_c",
                "row_d",
                "row_e",
                "is_star_row",
            ]
        )
    return pd.DataFrame(records)


def _resolve_vendor_block(discounts_df: pd.DataFrame, vendor_name: str) -> tuple[str, pd.DataFrame]:
    vendor_norm = _norm_text(vendor_name)
    if not vendor_norm or discounts_df is None or discounts_df.empty:
        return "", pd.DataFrame()

    vendor_blocks = discounts_df[["vendor_block", "vendor_block_norm"]].drop_duplicates().copy()
    if vendor_blocks.empty:
        return "", pd.DataFrame()

    exact = vendor_blocks[vendor_blocks["vendor_block_norm"] == vendor_norm]
    if not exact.empty:
        block_name = str(exact.iloc[0]["vendor_block"])
        rows = discounts_df[discounts_df["vendor_block"] == block_name].copy()
        return block_name, rows

    vendor_tokens = set(_tokens(vendor_norm))
    best_name = ""
    best_score = 0.0
    for _, row in vendor_blocks.iterrows():
        block_name = str(row.get("vendor_block", ""))
        block_norm = str(row.get("vendor_block_norm", ""))
        if not block_norm:
            continue
        block_tokens = set(_tokens(block_norm))
        overlap = len(vendor_tokens.intersection(block_tokens))
        score = 0.0
        if vendor_norm in block_norm or block_norm in vendor_norm:
            score += 0.9
        if overlap:
            denom = max(len(vendor_tokens), 1)
            score += min(overlap / denom, 1.0) * 0.4
        if score > best_score:
            best_score = score
            best_name = block_name

    if best_score < 0.5 or not best_name:
        return "", pd.DataFrame()
    rows = discounts_df[discounts_df["vendor_block"] == best_name].copy()
    return best_name, rows


def _dedupe_options(matches: list[DiscountMatch]) -> list[DiscountMatch]:
    seen: set[tuple[str, float]] = set()
    out: list[DiscountMatch] = []
    for item in matches:
        key = (_norm_text(item.vendor_label), round(float(item.discount_percent), 4))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def resolve_discount_candidates(
    discounts_df: pd.DataFrame,
    vendor_name: str,
    product_title: str = "",
    product_type: str = "",
) -> list[DiscountMatch]:
    block_name, block_rows = _resolve_vendor_block(discounts_df, vendor_name)
    if not block_name or block_rows.empty:
        return []

    vendor_norm = _norm_text(block_name)
    vendor_tokens = set(_tokens(vendor_norm))
    context_tokens = set(_tokens(f"{product_title} {product_type}"))

    b_c_options: list[DiscountMatch] = []
    a_c_options: list[DiscountMatch] = []
    d_only_options: list[DiscountMatch] = []

    for _, row in block_rows.iterrows():
        row_a = _clean_cell(row.get("row_a", ""))
        row_b = _clean_cell(row.get("row_b", ""))
        row_c = _clean_cell(row.get("row_c", ""))
        row_d = _clean_cell(row.get("row_d", ""))

        c_percent = _to_percent(row_c)
        d_percent = _to_percent(row_d)

        if row_b and c_percent is not None:
            label = f"{row_b} - {c_percent:.2f}%"
            b_c_options.append(
                DiscountMatch(
                    vendor_label=label,
                    discount_percent=float(c_percent),
                    score=1.0,
                    reason="column_b_plus_c",
                )
            )
            continue

        if row_a.startswith("*") and c_percent is not None:
            subtype_tokens = [tok for tok in _tokens(row_a) if tok not in vendor_tokens]
            subtype_overlap = set(subtype_tokens).intersection(context_tokens)
            score = 0.75
            reason = "column_a_plus_c"
            if subtype_tokens and subtype_overlap:
                score += 0.25
                reason = "column_a_plus_c_with_context"
            elif subtype_tokens and not subtype_overlap:
                score -= 0.1
            label = f"{row_a} - {c_percent:.2f}%"
            a_c_options.append(
                DiscountMatch(
                    vendor_label=label,
                    discount_percent=float(c_percent),
                    score=score,
                    reason=reason,
                )
            )
            continue

        if d_percent is not None:
            source_label = row_b or row_a
            if not source_label:
                continue
            label = f"{source_label} - {d_percent:.2f}%"
            d_only_options.append(
                DiscountMatch(
                    vendor_label=label,
                    discount_percent=float(d_percent),
                    score=0.5,
                    reason="column_d_fallback",
                )
            )

    if b_c_options and a_c_options:
        preferred = a_c_options if len(a_c_options) > len(b_c_options) else b_c_options
        return _dedupe_options(preferred)

    if b_c_options:
        return _dedupe_options(b_c_options)

    if a_c_options:
        return _dedupe_options(a_c_options)

    out = _dedupe_options(d_only_options)
    return out


def choose_best_discount(
    discounts_df: pd.DataFrame,
    vendor_name: str,
    product_title: str = "",
    product_type: str = "",
) -> tuple[float | None, list[DiscountMatch], str]:
    matches = resolve_discount_candidates(
        discounts_df=discounts_df,
        vendor_name=vendor_name,
        product_title=product_title,
        product_type=product_type,
    )
    if not matches:
        return None, [], "no_vendor_match"
    ranked = sorted(matches, key=lambda item: item.score, reverse=True)
    if len(matches) == 1:
        return ranked[0].discount_percent, matches, "single_match"

    top = ranked[0]
    second = ranked[1]
    if top.score - second.score >= 0.2:
        return top.discount_percent, matches, "resolved_by_subtype"
    return None, matches, "ambiguous_vendor_match"


def calculate_cost_from_price(price: float, discount_percent: float) -> float:
    return float(price) * (1.0 - (float(discount_percent) / 100.0))
