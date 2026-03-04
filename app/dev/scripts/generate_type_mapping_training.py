from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "kit",
    "kits",
    "new",
    "oem",
    "length",
    "power",
    "stroke",
    "diesel",
    "truck",
    "high",
    "output",
    "performance",
    "series",
    "complete",
    "replacement",
    "upgrade",
    "heavy",
    "duty",
    "factory",
    "stock",
    "side",
    "front",
    "rear",
    "inner",
    "outer",
    "upper",
    "lower",
    "left",
    "right",
    "driver",
    "passenger",
    "drv",
    "pass",
}

FITMENT_TOKENS = {
    "ford",
    "gm",
    "gmc",
    "chevy",
    "chevrolet",
    "ram",
    "dodge",
    "jeep",
    "nissan",
    "duramax",
    "cummins",
    "powerstroke",
    "super",
    "duty",
}

GENERIC_PHRASES = {
    "replacement kit",
    "performance kit",
    "complete kit",
    "high output",
    "oem length",
}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_space(value: str) -> str:
    text = _clean_text(value).lower()
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"[^a-z0-9\s-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_token(token: str) -> str:
    text = _clean_text(token).lower()
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _is_year_token(token: str) -> bool:
    if not token:
        return False
    if re.fullmatch(r"\d{2}", token):
        return True
    if re.fullmatch(r"\d{4}", token):
        return True
    if re.fullmatch(r"\d{2,4}\.5", token):
        return True
    return False


def _is_engine_liter_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d\.\dl?", token))


def _tokenize_title(title: str, vendor: str) -> list[str]:
    source = _normalize_space(title)
    if not source:
        return []
    vendor_tokens = [_normalize_token(part) for part in _normalize_space(vendor).split() if _normalize_token(part)]
    tokens = [_normalize_token(part) for part in source.split() if _normalize_token(part)]
    if not tokens:
        return []

    # Remove vendor-leading tokens at the start.
    while tokens and vendor_tokens and tokens[0] in vendor_tokens:
        tokens = tokens[1:]

    # Stop phrase at first clear fitment/year sequence.
    cut_index = len(tokens)
    for idx, token in enumerate(tokens):
        if _is_year_token(token):
            cut_index = idx
            break
        if token in FITMENT_TOKENS and idx > 0:
            cut_index = idx
            break
    core = tokens[:cut_index] if cut_index > 0 else tokens
    core = [token for token in core if not _is_engine_liter_token(token)]
    return core


def _parse_category_codes_4(value: str) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    if not text.startswith("["):
        return [text]
    try:
        parsed = json.loads(text)
    except Exception:
        return [text]
    if not isinstance(parsed, list):
        return [text]
    output: list[str] = []
    for item in parsed:
        cleaned = _clean_text(item)
        if cleaned:
            output.append(cleaned)
    return output


def _preferred_category(type_value: str, category_codes_4: str) -> str:
    type_clean = _clean_text(type_value)
    codes = _parse_category_codes_4(category_codes_4)
    if type_clean:
        return type_clean
    return codes[0] if codes else ""


def _mapping_key(row: pd.Series) -> tuple[str, str, str, str]:
    type_value = _clean_text(row.get("type", ""))
    subtype = _clean_text(row.get("custom_product_subtype", ""))
    google = _clean_text(row.get("custom_google_product_type", ""))
    category = _preferred_category(type_value, _clean_text(row.get("custom_category_codes_4", "")))
    return type_value, subtype, google, category


def _is_phrase_usable(tokens: list[str]) -> bool:
    if len(tokens) < 2:
        return False
    phrase = " ".join(tokens)
    if phrase in GENERIC_PHRASES:
        return False
    alpha_tokens = [token for token in tokens if re.search(r"[a-z]", token)]
    if len(alpha_tokens) < 2:
        return False
    if all(token in STOP_WORDS for token in alpha_tokens):
        return False
    if all(token in FITMENT_TOKENS for token in alpha_tokens):
        return False
    # Require at least one non-stop, non-fitment anchor token.
    for token in alpha_tokens:
        if token in STOP_WORDS or token in FITMENT_TOKENS:
            continue
        if len(token) >= 4:
            return True
    return False


def _phrase_tokens(tokens: list[str], min_len: int = 2, max_len: int = 5) -> set[tuple[str, ...]]:
    output: set[tuple[str, ...]] = set()
    if not tokens:
        return output
    for n in range(min_len, min(max_len, len(tokens)) + 1):
        for start in range(0, len(tokens) - n + 1):
            chunk = tokens[start : start + n]
            if not _is_phrase_usable(chunk):
                continue
            output.add(tuple(chunk))
    return output


def _pattern_from_tokens(tokens: list[str]) -> str:
    escaped = [re.escape(token) for token in tokens]
    phrase = r"\s+".join(escaped)
    return rf"\b(?:{phrase})\b"


def _confidence_score(precision: float, support: int) -> float:
    # Precision weighted by support saturation.
    saturation = 1.0 - math.exp(-float(support) / 10.0)
    return round(precision * saturation, 4)


@dataclass
class PhraseStats:
    products: set[str] = field(default_factory=set)
    vendors: set[str] = field(default_factory=set)
    mapping_counts: Counter = field(default_factory=Counter)
    examples: list[str] = field(default_factory=list)

    def add(self, product_id: str, vendor: str, mapping_key: tuple[str, str, str, str], title: str) -> None:
        self.products.add(product_id)
        if vendor:
            self.vendors.add(vendor)
        self.mapping_counts[mapping_key] += 1
        if len(self.examples) < 3 and title not in self.examples:
            self.examples.append(title)


def _build_candidates(
    df: pd.DataFrame,
    min_support: int,
    min_precision: float,
) -> pd.DataFrame:
    phrase_index: dict[tuple[str, ...], PhraseStats] = defaultdict(PhraseStats)

    for _, row in df.iterrows():
        product_id = _clean_text(row.get("product_id", ""))
        title = _clean_text(row.get("product_title", ""))
        vendor = _clean_text(row.get("vendor", ""))
        if not product_id or not title:
            continue
        mapping = _mapping_key(row)
        if not any(mapping):
            continue

        tokens = _tokenize_title(title=title, vendor=vendor)
        for phrase in _phrase_tokens(tokens):
            phrase_index[phrase].add(
                product_id=product_id,
                vendor=vendor,
                mapping_key=mapping,
                title=title,
            )

    rows: list[dict[str, object]] = []
    for phrase_tokens, stats in phrase_index.items():
        support = len(stats.products)
        if support < min_support:
            continue
        dominant_mapping, dominant_count = stats.mapping_counts.most_common(1)[0]
        precision = float(dominant_count) / float(max(support, 1))
        if precision < min_precision:
            continue
        type_value, subtype, google, category = dominant_mapping
        if not type_value:
            continue

        phrase_text = " ".join(phrase_tokens)
        rows.append(
            {
                "alias_phrase": phrase_text,
                "pattern": _pattern_from_tokens(list(phrase_tokens)),
                "type": type_value,
                "category": category or type_value,
                "subtype": subtype,
                "google_leaf": google,
                "support_products": support,
                "dominant_count": int(dominant_count),
                "precision": round(precision, 4),
                "confidence": _confidence_score(precision, support),
                "vendor_count": len(stats.vendors),
                "mapping_variants": len(stats.mapping_counts),
                "token_count": len(phrase_tokens),
                "example_titles": " | ".join(stats.examples),
            }
        )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = out.sort_values(
        by=["confidence", "precision", "support_products", "token_count", "alias_phrase"],
        ascending=[False, False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)

    # Keep stronger phrases first; suppress weaker contained phrases with same final mapping.
    kept_rows: list[dict[str, object]] = []
    for row in out.to_dict(orient="records"):
        phrase = str(row["alias_phrase"])
        mapping_key = (
            str(row.get("type", "")),
            str(row.get("subtype", "")),
            str(row.get("google_leaf", "")),
            str(row.get("category", "")),
        )
        skip = False
        for kept in kept_rows:
            kept_phrase = str(kept["alias_phrase"])
            kept_key = (
                str(kept.get("type", "")),
                str(kept.get("subtype", "")),
                str(kept.get("google_leaf", "")),
                str(kept.get("category", "")),
            )
            if mapping_key != kept_key:
                continue
            if phrase == kept_phrase:
                skip = True
                break
            if phrase in kept_phrase and float(row["support_products"]) <= float(kept["support_products"]) * 1.35:
                skip = True
                break
        if not skip:
            kept_rows.append(row)

    result = pd.DataFrame(kept_rows)
    result = result.sort_values(
        by=["confidence", "precision", "support_products", "token_count", "alias_phrase"],
        ascending=[False, False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    result = result.drop(columns=["token_count"], errors="ignore")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate robust type-mapping training candidates from local catalog export.")
    parser.add_argument(
        "--input",
        default="app/config/product_taxonomy_master.csv",
        help="Path to product taxonomy master CSV.",
    )
    parser.add_argument(
        "--output",
        default="app/required/rules/type_mapping_training_candidates.csv",
        help="Output CSV path for candidate alias mappings.",
    )
    parser.add_argument("--min-support", type=int, default=6, help="Minimum unique products supporting an alias.")
    parser.add_argument("--min-precision", type=float, default=0.9, help="Minimum dominant mapping precision (0-1).")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}")
        return 1

    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    required_columns = {"product_id", "product_title", "vendor", "type", "custom_google_product_type", "custom_category_codes_4", "custom_product_subtype"}
    missing = sorted(required_columns - set(df.columns))
    if missing:
        print(f"ERROR: input missing required columns: {', '.join(missing)}")
        return 1

    candidates = _build_candidates(
        df=df,
        min_support=max(2, int(args.min_support)),
        min_precision=max(0.0, min(1.0, float(args.min_precision))),
    )
    if candidates.empty:
        print("No candidate mappings met the threshold filters.")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(output_path, index=False, encoding="utf-8")

    print(f"Input rows: {len(df)}")
    print(f"Candidate rows: {len(candidates)}")
    print(f"Saved: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
