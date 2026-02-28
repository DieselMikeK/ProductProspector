from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_aliases(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    return [_clean_text(item) for item in parts if _clean_text(item)]


def _acronym(value: str) -> str:
    text = _normalize_key(value)
    if not text:
        return ""
    words = [part for part in text.split(" ") if part and not part.isdigit()]
    if len(words) < 2:
        return ""
    return "".join(word[0] for word in words).lower()


def _coerce_metaobject_gid(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if re.match(r"^gid://shopify/Metaobject/\d+$", text):
        return text
    digits = re.sub(r"\D+", "", text)
    if digits:
        return f"gid://shopify/Metaobject/{digits}"
    return ""


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    return pd.DataFrame()


@dataclass(frozen=True)
class VendorProfile:
    canonical_vendor: str = ""
    aliases: str = ""
    shopify_vendor_value: str = ""
    brand_name: str = ""
    brand_gid: str = ""
    title_prefix: str = ""
    sku_prefix: str = ""
    discount_vendor_key: str = ""
    notes: str = ""


def find_vendor_profile_file(required_root: Path | None) -> Path | None:
    if required_root is None:
        return None
    candidates = [
        required_root / "mappings" / "VendorProfiles.csv",
        required_root / "mappings" / "vendor_profiles.csv",
        required_root / "mappings" / "VendorProfiles.xlsx",
        required_root / "mappings" / "vendor_profiles.xlsx",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16)
def _load_vendor_profiles_cached(
    path_text: str,
    mtime_ns: int,
    size_bytes: int,
) -> tuple[dict[str, VendorProfile], dict[str, VendorProfile]]:
    _ = mtime_ns, size_bytes
    table = _read_table(Path(path_text))
    if table.empty:
        return {}, {}

    columns = list(table.columns)
    normalized = [_normalize_key(str(column)).replace(" ", "_") for column in columns]
    column_map = {normalized[i]: columns[i] for i in range(len(columns))}

    canonical_col = ""
    for candidate in ("canonical_vendor", "vendor", "canonical", "normalized_vendor", "name"):
        if candidate in column_map:
            canonical_col = column_map[candidate]
            break
    if not canonical_col and columns:
        canonical_col = columns[0]

    aliases_col = ""
    for candidate in ("aliases", "alias", "aka", "alternate_names", "alt_names"):
        if candidate in column_map:
            aliases_col = column_map[candidate]
            break

    shopify_col = ""
    for candidate in ("shopify_vendor_value", "shopify_vendor", "vendor_value", "shopify_vendor_name"):
        if candidate in column_map:
            shopify_col = column_map[candidate]
            break

    brand_name_col = ""
    for candidate in ("brand_name", "brand"):
        if candidate in column_map:
            brand_name_col = column_map[candidate]
            break

    brand_gid_col = ""
    for candidate in ("brand_gid", "metaobject_gid", "brand_metaobject_gid", "gid"):
        if candidate in column_map:
            brand_gid_col = column_map[candidate]
            break

    title_prefix_col = column_map.get("title_prefix", "")
    sku_prefix_col = column_map.get("sku_prefix", "")
    discount_col = ""
    for candidate in ("discount_vendor_key", "discount_vendor", "discount_key", "vendor_discount_key"):
        if candidate in column_map:
            discount_col = column_map[candidate]
            break
    notes_col = column_map.get("notes", "")

    by_key: dict[str, VendorProfile] = {}
    by_acronym: dict[str, VendorProfile] = {}

    for _, row in table.iterrows():
        canonical_vendor = _clean_text(row.get(canonical_col, ""))
        if not canonical_vendor:
            continue
        profile = VendorProfile(
            canonical_vendor=canonical_vendor,
            aliases=_clean_text(row.get(aliases_col, "")) if aliases_col else "",
            shopify_vendor_value=_clean_text(row.get(shopify_col, "")) if shopify_col else "",
            brand_name=_clean_text(row.get(brand_name_col, "")) if brand_name_col else "",
            brand_gid=_coerce_metaobject_gid(_clean_text(row.get(brand_gid_col, ""))) if brand_gid_col else "",
            title_prefix=_clean_text(row.get(title_prefix_col, "")) if title_prefix_col else "",
            sku_prefix=_clean_text(row.get(sku_prefix_col, "")) if sku_prefix_col else "",
            discount_vendor_key=_clean_text(row.get(discount_col, "")) if discount_col else "",
            notes=_clean_text(row.get(notes_col, "")) if notes_col else "",
        )

        key_values: list[str] = [profile.canonical_vendor]
        if profile.shopify_vendor_value:
            key_values.append(profile.shopify_vendor_value)
        if profile.brand_name:
            key_values.append(profile.brand_name)
        key_values.extend(_split_aliases(profile.aliases))

        for value in key_values:
            key = _normalize_key(value)
            if not key:
                continue
            by_key.setdefault(key, profile)
            acronym = _acronym(value)
            if acronym:
                by_acronym.setdefault(acronym, profile)

    return by_key, by_acronym


def _load_profile_lookup(required_root: Path | None) -> tuple[dict[str, VendorProfile], dict[str, VendorProfile]]:
    path = find_vendor_profile_file(required_root)
    if path is None:
        return {}, {}
    try:
        stat = path.stat()
    except Exception:
        return {}, {}
    try:
        return _load_vendor_profiles_cached(
            str(path.resolve()),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            int(stat.st_size),
        )
    except Exception:
        return {}, {}


def resolve_vendor_profile(vendor_like: str, required_root: Path | None) -> VendorProfile | None:
    value = _clean_text(vendor_like)
    if not value:
        return None
    by_key, by_acronym = _load_profile_lookup(required_root)
    if not by_key:
        return None

    key = _normalize_key(value)
    if key and key in by_key:
        return by_key[key]

    acronym = _acronym(value)
    if acronym and acronym in by_acronym:
        return by_acronym[acronym]
    return None
