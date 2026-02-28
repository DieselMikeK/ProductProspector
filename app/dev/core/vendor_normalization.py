from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pandas as pd
from product_prospector.core.vendor_profiles import resolve_vendor_profile


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_output(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _split_alias_values(raw: str) -> list[str]:
    text = _clean_text(raw)
    if not text:
        return []
    parts = re.split(r"[|;\n,]+", text)
    return [_clean_text(item) for item in parts if _clean_text(item)]


def _acronym(value: str) -> str:
    text = _normalize_key(value)
    if not text:
        return ""
    words = [part for part in text.split(" ") if part and not part.isdigit()]
    if len(words) < 2:
        return ""
    letters = "".join(word[0] for word in words if word)
    return letters.lower()


def find_vendor_normalization_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "vendors.csv",
        required_root / "mappings" / "Vendors.csv",
        required_root / "mappings" / "vendors.xlsx",
        required_root / "rules" / "vendor_normalization.csv",
        required_root / "rules" / "VendorNormalization.csv",
        required_root / "rules" / "vendor_normalization.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    return pd.DataFrame()


@lru_cache(maxsize=16)
def _load_vendor_lookup_cached(path_text: str, mtime_ns: int, size_bytes: int) -> tuple[dict[str, str], dict[str, str]]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    table = _read_table(path)
    if table.empty:
        return {}, {}

    columns = list(table.columns)
    normalized_columns = [_normalize_key(str(column)).replace(" ", "_") for column in columns]
    vendor_col: str | None = None
    alias_cols: list[str] = []
    for idx, col_name in enumerate(normalized_columns):
        if vendor_col is None and col_name in {"vendor", "normalized_vendor", "canonical", "canonical_vendor", "name"}:
            vendor_col = columns[idx]
        if col_name in {"alias", "aliases", "aka", "alternate", "alternate_names", "alt_names"}:
            alias_cols.append(columns[idx])

    if vendor_col is None:
        vendor_col = columns[0]
    if not alias_cols and len(columns) > 1:
        alias_cols = columns[1:]

    alias_to_canonical: dict[str, str] = {}
    canonical_by_key: dict[str, str] = {}

    for _, row in table.iterrows():
        canonical_raw = _clean_text(row.get(vendor_col, ""))
        canonical = _normalize_output(canonical_raw)
        if not canonical:
            continue

        canonical_key = _normalize_key(canonical)
        if canonical_key:
            canonical_by_key[canonical_key] = canonical
            alias_to_canonical.setdefault(canonical_key, canonical)

        for alias_col in alias_cols:
            alias_cell = _clean_text(row.get(alias_col, ""))
            for alias_value in _split_alias_values(alias_cell):
                alias_key = _normalize_key(alias_value)
                if not alias_key:
                    continue
                alias_to_canonical.setdefault(alias_key, canonical)

    return alias_to_canonical, canonical_by_key


@lru_cache(maxsize=16)
def _load_vendor_title_lookup_cached(path_text: str, mtime_ns: int, size_bytes: int) -> dict[str, str]:
    _ = mtime_ns, size_bytes
    path = Path(path_text)
    table = _read_table(path)
    if table.empty:
        return {}

    columns = list(table.columns)
    normalized_columns = [_normalize_key(str(column)).replace(" ", "_") for column in columns]
    vendor_col: str | None = None
    alias_cols: list[str] = []
    title_col: str | None = None
    for idx, col_name in enumerate(normalized_columns):
        if vendor_col is None and col_name in {"vendor", "normalized_vendor", "canonical", "canonical_vendor", "name"}:
            vendor_col = columns[idx]
        if col_name in {"alias", "aliases", "aka", "alternate", "alternate_names", "alt_names"}:
            alias_cols.append(columns[idx])
        if title_col is None and col_name in {"title", "title_prefix", "display_name", "short_name", "brand_title"}:
            title_col = columns[idx]

    if vendor_col is None:
        vendor_col = columns[0]
    if not alias_cols and len(columns) > 1:
        alias_cols = columns[1:]
    if title_col is None:
        return {}

    out: dict[str, str] = {}
    for _, row in table.iterrows():
        canonical_raw = _clean_text(row.get(vendor_col, ""))
        canonical = _normalize_output(canonical_raw)
        if not canonical:
            continue
        title_value = _normalize_output(_clean_text(row.get(title_col, "")))
        if not title_value:
            continue

        canonical_key = _normalize_key(canonical)
        if canonical_key:
            out.setdefault(canonical_key, title_value)

        for alias_col in alias_cols:
            alias_cell = _clean_text(row.get(alias_col, ""))
            for alias_value in _split_alias_values(alias_cell):
                alias_key = _normalize_key(alias_value)
                if not alias_key:
                    continue
                out.setdefault(alias_key, title_value)

    return out


def load_vendor_lookup(required_root: Path | None) -> tuple[dict[str, str], dict[str, str]]:
    mapping_file = find_vendor_normalization_file(required_root)
    if mapping_file is None:
        return {}, {}

    try:
        stat = mapping_file.stat()
    except Exception:
        return {}, {}

    try:
        return _load_vendor_lookup_cached(
            str(mapping_file.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return {}, {}


def load_vendor_title_lookup(required_root: Path | None) -> dict[str, str]:
    mapping_file = find_vendor_normalization_file(required_root)
    if mapping_file is None:
        return {}
    try:
        stat = mapping_file.stat()
    except Exception:
        return {}
    try:
        return _load_vendor_title_lookup_cached(
            str(mapping_file.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return {}


def normalize_vendor_name(vendor_name: str, required_root: Path | None = None) -> str:
    value = _normalize_output(vendor_name)
    if not value:
        return ""

    profile = resolve_vendor_profile(value, required_root=required_root)
    if profile is not None and _normalize_output(profile.canonical_vendor):
        return _normalize_output(profile.canonical_vendor)

    alias_to_canonical, canonical_by_key = load_vendor_lookup(required_root)
    if not alias_to_canonical:
        return value

    direct_key = _normalize_key(value)
    if direct_key and direct_key in alias_to_canonical:
        return alias_to_canonical[direct_key]

    acronym_key = _acronym(value)
    if acronym_key and acronym_key in canonical_by_key:
        return canonical_by_key[acronym_key]

    return value


def resolve_vendor_title_name(vendor_name: str, required_root: Path | None = None) -> str:
    value = _normalize_output(vendor_name)
    if not value:
        return ""

    profile = resolve_vendor_profile(value, required_root=required_root)
    if profile is not None and _normalize_output(profile.title_prefix):
        return _normalize_output(profile.title_prefix)

    title_lookup = load_vendor_title_lookup(required_root)
    if not title_lookup:
        return ""

    direct_key = _normalize_key(value)
    if direct_key and direct_key in title_lookup:
        return title_lookup[direct_key]

    normalized_vendor = normalize_vendor_name(value, required_root=required_root)
    normalized_key = _normalize_key(normalized_vendor)
    if normalized_key and normalized_key in title_lookup:
        return title_lookup[normalized_key]

    acronym_key = _acronym(value)
    if acronym_key and acronym_key in title_lookup:
        return title_lookup[acronym_key]

    return ""
