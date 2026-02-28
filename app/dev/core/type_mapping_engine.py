from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from product_prospector.core.product_model import Product


TOKEN_STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "set",
    "new",
    "diesel",
    "truck",
}

WEAK_MATCH_TOKENS = {
    "engine",
    "motor",
    "vehicle",
    "component",
    "components",
    "accessory",
    "accessories",
    "related",
    "part",
    "parts",
    "upgrade",
    "upgrades",
}

PENALTY_IGNORE_TOKENS = {
    "related",
    "accessory",
    "accessories",
    "motor",
    "vehicle",
    "part",
    "parts",
    "component",
    "components",
    "upgrade",
    "upgrades",
}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_phrase(value: str) -> str:
    cleaned = _clean_text(value).lower()
    cleaned = re.sub(r"[^a-z0-9\s]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _tokenize(value: str) -> set[str]:
    text = _normalize_phrase(value)
    if not text:
        return set()
    tokens = [part for part in text.split() if len(part) >= 3]
    return {token for token in tokens if token not in TOKEN_STOP_WORDS}


def _read_raw_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, header=None, dtype=str, keep_default_na=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, header=None, dtype=str, keep_default_na=False)
    return pd.DataFrame()


@dataclass
class DppSubtype:
    name: str
    tokens: set[str]
    phrase: str


@dataclass
class DppCategory:
    name: str
    tokens: set[str]
    phrase: str
    subtypes: list[DppSubtype] = field(default_factory=list)


@dataclass
class DppMatch:
    category: str
    subtype: str


@dataclass(frozen=True)
class ExplicitHint:
    pattern: str
    category: str
    subtype: str = ""
    google_leaf: str = ""


@dataclass
class GoogleEntry:
    leaf: str
    path: str
    tokens: set[str]
    depth: int


class TypeCategoryMapper:
    def __init__(
        self,
        dpp_entries: list[DppCategory],
        google_entries: list[GoogleEntry],
        explicit_hints: list[ExplicitHint] | None = None,
    ) -> None:
        self.dpp_entries = dpp_entries
        self.google_entries = google_entries
        self.explicit_hints = explicit_hints or _default_explicit_hints()

    @classmethod
    def from_required_root(cls, required_root: Path) -> "TypeCategoryMapper":
        types_root = required_root / "types"
        rules_root = required_root / "rules"
        dpp_candidates = [
            types_root / "DPPProductTypes.csv",
            types_root / "DPPProductTypes.xlsx",
        ]
        google_candidates = [
            types_root / "GoogleProductTypes.csv",
            types_root / "GoogleProductTypes.xlsx",
            types_root / "GoogleProductType.csv",
            types_root / "GoogleProductType.xlsx",
        ]

        dpp_entries: list[DppCategory] = []
        google_entries: list[GoogleEntry] = []
        explicit_hints: list[ExplicitHint] = _load_explicit_hints(rules_root)

        for path in dpp_candidates:
            if not path.exists():
                continue
            dpp_entries = _load_dpp_entries(path)
            break

        for path in google_candidates:
            if not path.exists():
                continue
            google_entries = _load_google_entries(path)
            break

        return cls(
            dpp_entries=dpp_entries,
            google_entries=google_entries,
            explicit_hints=explicit_hints,
        )

    def apply(self, product: Product, allow_category_overwrite: bool) -> Product:
        primary_tokens, secondary_tokens, context_phrase = _build_context(product)

        explicit_hint = _match_explicit_hint(context_phrase, self.explicit_hints)
        if explicit_hint:
            if allow_category_overwrite or not product.category_code:
                product.category_code = explicit_hint.category
                product.field_sources["category_code"] = "type_rules_hint"
            if allow_category_overwrite or not product.type:
                product.type = explicit_hint.category
                product.field_sources["type"] = "type_rules_hint"
            if explicit_hint.subtype:
                if allow_category_overwrite or not product.product_subtype:
                    product.product_subtype = explicit_hint.subtype
                    product.field_sources["product_subtype"] = "type_rules_hint"
            elif allow_category_overwrite:
                product.product_subtype = ""
                product.field_sources["product_subtype"] = "type_rules_hint"
            if explicit_hint.google_leaf and (allow_category_overwrite or not product.google_product_type):
                product.google_product_type = explicit_hint.google_leaf
                product.field_sources["google_product_type"] = "type_rules_hint"

        dpp_match = None
        if not explicit_hint:
            dpp_match = _match_dpp(self.dpp_entries, primary_tokens, secondary_tokens, context_phrase)
        if dpp_match:
            if allow_category_overwrite or not product.category_code:
                product.category_code = dpp_match.category
                product.field_sources["category_code"] = "type_rules"
            if allow_category_overwrite or not product.type:
                product.type = dpp_match.category
                product.field_sources["type"] = "type_rules"
            if dpp_match.subtype:
                if allow_category_overwrite or not product.product_subtype:
                    product.product_subtype = dpp_match.subtype
                    product.field_sources["product_subtype"] = "type_rules"
            elif allow_category_overwrite:
                product.product_subtype = ""
                product.field_sources["product_subtype"] = "type_rules"

        if not explicit_hint or not explicit_hint.google_leaf:
            google_primary = set(primary_tokens)
            if dpp_match:
                google_primary.update(_tokenize(f"{dpp_match.category} {dpp_match.subtype}"))

            google_match = _match_google(
                self.google_entries,
                google_primary,
                secondary_tokens,
                context_phrase,
            )
            if google_match and (allow_category_overwrite or not product.google_product_type):
                product.google_product_type = google_match.leaf
                product.field_sources["google_product_type"] = "type_rules"

        if not product.mpn:
            product.mpn = product.sku
            product.field_sources["mpn"] = product.field_sources.get("mpn", "rule")
        if not product.brand:
            product.brand = product.vendor
            product.field_sources["brand"] = product.field_sources.get("brand", "rule")

        return product


def _build_context(product: Product) -> tuple[set[str], set[str], str]:
    primary_text = " ".join(
        [
            product.title,
            product.description_html,
            product.type,
            product.product_subtype,
            product.category_code,
        ]
    )
    secondary_text = " ".join([product.vendor, product.application])
    full_context = " ".join([primary_text, secondary_text])
    return _tokenize(primary_text), _tokenize(secondary_text), _normalize_phrase(full_context)


def _match_explicit_hint(context_phrase: str, hints: list[ExplicitHint]) -> ExplicitHint | None:
    if not context_phrase:
        return None
    for hint in hints:
        if re.search(hint.pattern, context_phrase, flags=re.IGNORECASE):
            return hint
    return None


def _default_explicit_hints() -> list[ExplicitHint]:
    return [
        ExplicitHint(
            pattern=r"\bpie\s*cut(?:s)?\b",
            category="Exhaust",
            subtype="Exhaust Kits",
            google_leaf="Motor Vehicle Exhaust",
        ),
        ExplicitHint(
            pattern=r"\b(cat[-\s]*back|turbo[-\s]*back|axle[-\s]*back|exhaust\s+system)\b",
            category="Exhaust",
            subtype="Exhaust Kits",
            google_leaf="Motor Vehicle Exhaust",
        ),
        ExplicitHint(
            pattern=r"\bexhaust\s+brake\b|\bengine\s+brake\b|\bpacbrake\b",
            category="Exhaust Brakes / Engine Brakes",
        ),
    ]


def _load_explicit_hints(rules_root: Path) -> list[ExplicitHint]:
    candidates = [
        rules_root / "type_mapping_hints.csv",
        rules_root / "type_mapping_hints.xlsx",
    ]
    table = pd.DataFrame()
    for path in candidates:
        if not path.exists():
            continue
        table = _read_raw_table(path)
        break

    if table.empty:
        return _default_explicit_hints()

    header_values = [_clean_text(value).lower() for value in table.iloc[0].tolist()]
    has_named_header = "pattern" in header_values and "category" in header_values
    if has_named_header:
        normalized_columns: list[str] = []
        for value in header_values:
            if value:
                normalized_columns.append(value.replace(" ", "_"))
            else:
                normalized_columns.append("")
        table = table.iloc[1:].copy()
        table.columns = normalized_columns
    else:
        table = table.copy()
        table.columns = [f"col_{idx}" for idx in range(len(table.columns))]

    def _value(row: pd.Series, keys: list[str]) -> str:
        for key in keys:
            if key in row.index:
                return _clean_text(row.get(key, ""))
        return ""

    def _enabled(value: str) -> bool:
        text = _clean_text(value).lower()
        if not text:
            return True
        return text not in {"0", "false", "no", "off", "disabled", "disable"}

    hints: list[ExplicitHint] = []
    for _, row in table.iterrows():
        pattern = _value(row, ["pattern", "col_0"])
        category = _value(row, ["category", "category_type", "type", "col_1"])
        subtype = _value(row, ["subtype", "product_subtype", "col_2"])
        google_leaf = _value(row, ["google_leaf", "google_product_type", "col_3"])
        enabled_value = _value(row, ["enabled", "active", "col_4"])
        if not _enabled(enabled_value):
            continue
        if not pattern or not category:
            continue
        hints.append(
            ExplicitHint(
                pattern=pattern,
                category=category,
                subtype=subtype,
                google_leaf=google_leaf,
            )
        )

    return hints or _default_explicit_hints()


def _load_dpp_entries(path: Path) -> list[DppCategory]:
    table = _read_raw_table(path)
    if table.empty:
        return []

    categories: list[DppCategory] = []
    category_by_key: dict[str, DppCategory] = {}
    current_category: str = ""

    for _, row in table.iterrows():
        col_a = _clean_text(row.get(0, ""))
        col_b = _clean_text(row.get(1, ""))

        if col_a.lower() == "el" and "product subtype" in col_b.lower():
            continue
        if not col_a and not col_b:
            continue

        if col_a:
            current_category = col_a
            key = _normalize_phrase(current_category)
            if key and key not in category_by_key:
                category = DppCategory(
                    name=current_category,
                    tokens=_tokenize(current_category),
                    phrase=_normalize_phrase(current_category),
                )
                category_by_key[key] = category
                categories.append(category)

        if not current_category:
            continue
        category_key = _normalize_phrase(current_category)
        category = category_by_key.get(category_key)
        if category is None:
            continue

        if not col_b or "product subtype" in col_b.lower():
            continue

        subtype_phrase = _normalize_phrase(col_b)
        if not subtype_phrase:
            continue
        if any(existing.phrase == subtype_phrase for existing in category.subtypes):
            continue
        tokens = _tokenize(f"{current_category} {col_b}") or _tokenize(col_b)
        category.subtypes.append(
            DppSubtype(
                name=col_b,
                tokens=tokens,
                phrase=subtype_phrase,
            )
        )

    return categories


def _load_google_entries(path: Path) -> list[GoogleEntry]:
    table = _read_raw_table(path)
    entries: list[GoogleEntry] = []
    if table.empty:
        return entries

    for _, row in table.iterrows():
        segments = []
        for idx in [3, 4, 5, 6]:
            if idx not in row.index:
                continue
            value = _clean_text(row.get(idx, ""))
            if value:
                segments.append(value)
        if not segments:
            continue
        leaf = segments[-1]
        path_value = " > ".join(segments)
        tokens = _tokenize(path_value)
        if not tokens:
            continue
        entries.append(
            GoogleEntry(
                leaf=leaf,
                path=path_value,
                tokens=tokens,
                depth=len(segments),
            )
        )
    return entries


def _match_dpp(
    entries: list[DppCategory],
    primary_tokens: set[str],
    secondary_tokens: set[str],
    context_phrase: str,
) -> DppMatch | None:
    best_category: DppCategory | None = None
    best_category_score = 0.0

    for category in entries:
        score, _ = _score_entry_tokens(category.tokens, primary_tokens, secondary_tokens)
        if category.phrase and category.phrase in context_phrase:
            score += 1.2

        best_subtype_hint = 0.0
        for subtype in category.subtypes:
            subtype_score, _ = _score_entry_tokens(subtype.tokens, primary_tokens, secondary_tokens)
            if subtype.phrase and subtype.phrase in context_phrase:
                subtype_score += 0.8
            if subtype_score > best_subtype_hint:
                best_subtype_hint = subtype_score
        if best_subtype_hint > 0:
            score += min(1.2, best_subtype_hint * 0.15)

        if score > best_category_score + 0.001:
            best_category = category
            best_category_score = score
            continue
        if (
            best_category is not None
            and abs(score - best_category_score) <= 0.001
            and len(category.tokens) < len(best_category.tokens)
        ):
            best_category = category
            best_category_score = score

    if best_category is None or best_category_score < 1.1:
        return None

    best_subtype_name = ""
    best_subtype_score = 0.0
    best_subtype_unique_score = 0.0
    best_subtype_phrase_match = False

    for subtype in best_category.subtypes:
        score, _ = _score_entry_tokens(subtype.tokens, primary_tokens, secondary_tokens)
        phrase_match = bool(subtype.phrase and subtype.phrase in context_phrase)
        if phrase_match:
            score += 1.2
        unique_tokens = subtype.tokens - best_category.tokens
        unique_score, _ = _score_entry_tokens(unique_tokens, primary_tokens, secondary_tokens)
        score += unique_score * 0.6
        if score > best_subtype_score:
            best_subtype_name = subtype.name
            best_subtype_score = score
            best_subtype_unique_score = unique_score
            best_subtype_phrase_match = phrase_match

    if not best_subtype_name:
        return DppMatch(category=best_category.name, subtype="")
    if best_subtype_score < 1.6 and not best_subtype_phrase_match:
        return DppMatch(category=best_category.name, subtype="")
    if best_subtype_unique_score < 0.9 and not best_subtype_phrase_match:
        return DppMatch(category=best_category.name, subtype="")

    return DppMatch(category=best_category.name, subtype=best_subtype_name)


def _match_google(
    entries: list[GoogleEntry],
    primary_tokens: set[str],
    secondary_tokens: set[str],
    context_phrase: str,
) -> GoogleEntry | None:
    best: GoogleEntry | None = None
    best_score = 0.0
    for entry in entries:
        score, _ = _score_entry_tokens(entry.tokens, primary_tokens, secondary_tokens)
        leaf_phrase = _normalize_phrase(entry.leaf)
        if leaf_phrase and leaf_phrase in context_phrase:
            score += 1.0
        if score > best_score + 0.001:
            best = entry
            best_score = score
            continue
        if best is not None and abs(score - best_score) <= 0.001 and entry.depth > best.depth:
            best = entry
            best_score = score
    if best is not None and best_score >= 1.25:
        return best
    for entry in entries:
        if entry.leaf.lower() == "motor vehicle parts":
            return entry
    return None


def _score_entry_tokens(
    entry_tokens: set[str],
    primary_tokens: set[str],
    secondary_tokens: set[str],
) -> tuple[float, int]:
    if not entry_tokens:
        return 0.0, 0

    score = 0.0
    matched = 0
    for token in entry_tokens:
        best = _best_token_score(token, primary_tokens, secondary_tokens)
        score += best
        if best >= 0.95:
            matched += 1

    missing_penalty_count = 0
    for token in entry_tokens:
        token_norm = _stem_token(token)
        if token_norm in PENALTY_IGNORE_TOKENS:
            continue
        if _best_token_score(token_norm, primary_tokens, secondary_tokens) < 0.2:
            missing_penalty_count += 1
    score -= 0.65 * missing_penalty_count
    return score, matched


def _best_token_score(
    entry_token: str,
    primary_tokens: set[str],
    secondary_tokens: set[str],
) -> float:
    et = _stem_token(entry_token)
    if not et:
        return 0.0

    weak_factor = 0.4 if et in WEAK_MATCH_TOKENS else 1.0
    best = 0.0

    for token in primary_tokens:
        ct = _stem_token(token)
        if not ct:
            continue
        score = 0.0
        if et == ct:
            score = 2.0
        elif len(et) >= 5 and len(ct) >= 5 and (et in ct or ct in et):
            score = 0.75
        score *= weak_factor
        if score > best:
            best = score

    for token in secondary_tokens:
        ct = _stem_token(token)
        if not ct:
            continue
        score = 0.0
        if et == ct:
            score = 1.1
        elif len(et) >= 5 and len(ct) >= 5 and (et in ct or ct in et):
            score = 0.4
        score *= weak_factor
        if score > best:
            best = score

    return best


def _stem_token(token: str) -> str:
    value = token.lower().strip()
    if value.endswith("ies") and len(value) > 4:
        return value[:-3] + "y"
    for suffix in ["ing", "ers", "er", "ed"]:
        if value.endswith(suffix) and len(value) - len(suffix) >= 3:
            return value[: -len(suffix)]
    if value.endswith("es") and len(value) > 4:
        if value.endswith(("ses", "xes", "zes", "ches", "shes")):
            return value[:-2]
        return value[:-1]
    if value.endswith("s") and len(value) > 3:
        return value[:-1]
    return value
