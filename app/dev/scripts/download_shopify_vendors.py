from __future__ import annotations

import argparse
from pathlib import Path
import sys
import re

import pandas as pd

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from product_prospector.core.config_store import (
    load_shopify_config,
    load_shopify_token,
    save_shopify_token,
)
from product_prospector.core.shopify_oauth import exchange_client_credentials_for_token
from product_prospector.core.shopify_vendor_catalog import (
    build_vendor_profile_template,
    default_vendor_catalog_path,
    default_vendor_profile_path,
    fetch_shopify_product_vendors,
    save_table,
)


def _resolve_required_root() -> Path:
    here = Path(__file__).resolve()
    dev_root = here.parents[1]
    runtime_app = dev_root.parent
    required_root = runtime_app / "required"
    required_root.mkdir(parents=True, exist_ok=True)
    return required_root


def _ensure_access_token():
    token = load_shopify_token()
    if token is not None and token.access_token:
        return token.access_token, None
    config = load_shopify_config()
    if config is None:
        return "", "Invalid app/config/shopify.json."
    if config.admin_api_access_token:
        return config.admin_api_access_token, None
    result = exchange_client_credentials_for_token(config)
    if not result.success:
        return "", result.error or "Could not get Shopify token."
    save_shopify_token(result.access_token, result.scope)
    return result.access_token, None


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


def _load_alias_seed_map(required_root: Path) -> dict[str, list[str]]:
    candidates = [
        required_root / "mappings" / "vendors.csv",
        required_root / "mappings" / "Vendors.csv",
        required_root / "mappings" / "vendors.xlsx",
    ]
    source_path = None
    for path in candidates:
        if path.exists():
            source_path = path
            break
    if source_path is None:
        return {}

    try:
        if source_path.suffix.lower() == ".csv":
            df = pd.read_csv(source_path, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(source_path, dtype=str, keep_default_na=False)
    except Exception:
        return {}
    if df.empty:
        return {}

    columns = list(df.columns)
    normalized = [_normalize_key(str(column)).replace(" ", "_") for column in columns]
    colmap = {normalized[i]: columns[i] for i in range(len(columns))}

    canonical_col = ""
    for candidate in ("vendor", "canonical_vendor", "canonical", "name"):
        if candidate in colmap:
            canonical_col = colmap[candidate]
            break
    if not canonical_col and columns:
        canonical_col = columns[0]

    aliases_col = ""
    for candidate in ("aliases", "alias", "aka", "alternate_names", "alt_names"):
        if candidate in colmap:
            aliases_col = colmap[candidate]
            break
    if not aliases_col and len(columns) > 1:
        aliases_col = columns[1]

    out: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        canonical = _clean_text(row.get(canonical_col, ""))
        if not canonical:
            continue
        alias_values = _split_aliases(row.get(aliases_col, "")) if aliases_col else []
        if not alias_values:
            continue
        key = _normalize_key(canonical)
        if not key:
            continue
        if key not in out:
            out[key] = []
        existing = {_normalize_key(item) for item in out[key]}
        for alias in alias_values:
            alias_key = _normalize_key(alias)
            if not alias_key or alias_key in existing:
                continue
            out[key].append(alias)
            existing.add(alias_key)
    return out


def _merge_vendor_profiles(existing_df: pd.DataFrame, fresh_df: pd.DataFrame) -> pd.DataFrame:
    if fresh_df is None or fresh_df.empty:
        return existing_df.copy()
    if existing_df is None or existing_df.empty:
        return fresh_df.copy()

    editable_columns = [
        "aliases",
        "shopify_vendor_value",
        "brand_name",
        "brand_gid",
        "title_prefix",
        "sku_prefix",
        "discount_vendor_key",
        "notes",
    ]

    out = fresh_df.copy()
    out["canonical_vendor"] = out.get("canonical_vendor", "").astype(str).str.strip()
    existing = existing_df.copy()
    existing["canonical_vendor"] = existing.get("canonical_vendor", "").astype(str).str.strip()

    existing_map: dict[str, dict[str, str]] = {}
    for _, row in existing.iterrows():
        key = str(row.get("canonical_vendor", "")).strip()
        if not key:
            continue
        if key in existing_map:
            continue
        existing_map[key] = {column: str(row.get(column, "")).strip() for column in existing.columns}

    for idx, row in out.iterrows():
        key = str(row.get("canonical_vendor", "")).strip()
        if not key:
            continue
        old = existing_map.get(key)
        if not old:
            continue
        for column in editable_columns:
            if column not in out.columns:
                out[column] = ""
            old_value = str(old.get(column, "")).strip()
            if old_value:
                out.at[idx, column] = old_value

    # Keep legacy/manual rows not currently in Shopify vendor list.
    out_keys = set(out["canonical_vendor"].astype(str).str.strip().tolist())
    legacy_rows = existing[~existing["canonical_vendor"].astype(str).str.strip().isin(out_keys)].copy()
    if not legacy_rows.empty:
        for column in out.columns:
            if column not in legacy_rows.columns:
                legacy_rows[column] = ""
        legacy_rows = legacy_rows[out.columns]
        out = pd.concat([out, legacy_rows], ignore_index=True)

    out = out.fillna("")
    out = out.drop_duplicates(subset=["canonical_vendor"], keep="first")
    out = out.sort_values(by=["canonical_vendor"], kind="stable").reset_index(drop=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Shopify product vendor values and generate a vendor profile template.")
    parser.add_argument(
        "--vendors-output",
        default="",
        help="Output path for raw Shopify vendor values (.csv/.xlsx). Default: app/required/mappings/ShopifyProductVendors.csv",
    )
    parser.add_argument(
        "--profiles-output",
        default="",
        help="Output path for vendor profile template (.csv/.xlsx). Default: app/required/mappings/VendorProfiles.csv",
    )
    args = parser.parse_args()

    config = load_shopify_config()
    if config is None:
        print("ERROR: Invalid app/config/shopify.json.")
        return 1

    access_token, token_error = _ensure_access_token()
    if token_error:
        print(f"ERROR: {token_error}")
        return 1

    required_root = _resolve_required_root()
    vendors_path = Path(args.vendors_output).expanduser().resolve() if args.vendors_output else default_vendor_catalog_path(required_root)
    profiles_path = Path(args.profiles_output).expanduser().resolve() if args.profiles_output else default_vendor_profile_path(required_root)
    if vendors_path.suffix.lower() not in {".csv", ".xlsx"}:
        vendors_path = vendors_path.with_suffix(".csv")
    if profiles_path.suffix.lower() not in {".csv", ".xlsx"}:
        profiles_path = profiles_path.with_suffix(".csv")

    def on_progress(page: int, count: int) -> None:
        print(f"Reading Shopify product vendors... page {page}, unique vendors {count}")

    vendor_df, error = fetch_shopify_product_vendors(
        config=config,
        access_token=access_token,
        progress_callback=on_progress,
    )
    if error:
        print(f"ERROR: {error}")
        return 1

    profile_df = build_vendor_profile_template(vendor_df, required_root=required_root)
    if profiles_path.exists():
        try:
            existing_profile_df = pd.read_csv(profiles_path, dtype=str, keep_default_na=False)
        except Exception:
            existing_profile_df = pd.DataFrame()
        profile_df = _merge_vendor_profiles(existing_profile_df, profile_df)

    alias_seed_map = _load_alias_seed_map(required_root=required_root)
    if alias_seed_map and not profile_df.empty:
        if "aliases" not in profile_df.columns:
            profile_df["aliases"] = ""
        if "canonical_vendor" not in profile_df.columns:
            profile_df["canonical_vendor"] = ""
        for idx, row in profile_df.iterrows():
            current_aliases = _clean_text(row.get("aliases", ""))
            if current_aliases:
                continue
            canonical_vendor = _clean_text(row.get("canonical_vendor", ""))
            key = _normalize_key(canonical_vendor)
            aliases = alias_seed_map.get(key, [])
            if aliases:
                profile_df.at[idx, "aliases"] = " | ".join(aliases)

    vendor_count, vendor_save_error = save_table(vendor_df, vendors_path)
    if vendor_save_error:
        print(f"ERROR: Could not save vendor catalog: {vendor_save_error}")
        return 1

    profile_count, profile_save_error = save_table(profile_df, profiles_path)
    if profile_save_error:
        print(f"ERROR: Could not save vendor profile template: {profile_save_error}")
        return 1

    print(f"Done. Saved {vendor_count} vendor value(s) to:")
    print(str(vendors_path))
    print(f"Done. Saved {profile_count} vendor profile row(s) to:")
    print(str(profiles_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
