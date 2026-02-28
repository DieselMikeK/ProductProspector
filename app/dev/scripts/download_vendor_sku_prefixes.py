from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys

import pandas as pd


DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from product_prospector.core.config_store import load_shopify_config, load_shopify_token, save_shopify_token
from product_prospector.core.processing import normalize_sku
from product_prospector.core.shopify_catalog import fetch_shopify_catalog_dataframe
from product_prospector.core.shopify_oauth import exchange_client_credentials_for_token
from product_prospector.core.shopify_sku_cache import load_shopify_sku_cache, save_shopify_sku_cache


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def _extract_prefix(sku: str) -> str:
    value = normalize_sku(sku)
    if not value:
        return ""
    token = ""
    if "-" in value:
        token = value.split("-", 1)[0]
    elif "_" in value:
        token = value.split("_", 1)[0]
    if not token:
        return ""
    token = re.sub(r"[^A-Z0-9]+", "", token.upper())
    if not token:
        return ""
    if len(token) > 14:
        return ""
    return token


def _load_catalog_rows(refresh_cache: bool = False) -> tuple[pd.DataFrame, str | None]:
    cache_df = pd.DataFrame()
    if not refresh_cache:
        try:
            cache_df = load_shopify_sku_cache()
        except Exception:
            cache_df = pd.DataFrame()

    if cache_df is not None and not cache_df.empty and {"sku", "vendor"}.issubset(cache_df.columns):
        return cache_df.copy(), None

    config = load_shopify_config()
    if config is None:
        return pd.DataFrame(), "Invalid app/config/shopify.json."
    access_token, token_error = _ensure_access_token()
    if token_error:
        return pd.DataFrame(), token_error

    df, error = fetch_shopify_catalog_dataframe(config=config, access_token=access_token)
    if error:
        return pd.DataFrame(), error
    if df is None:
        df = pd.DataFrame()
    if not df.empty:
        save_shopify_sku_cache(df)
    return df, None


def _build_prefix_hints(catalog_df: pd.DataFrame) -> pd.DataFrame:
    if catalog_df is None or catalog_df.empty:
        return pd.DataFrame(
            columns=[
                "shopify_vendor",
                "total_skus",
                "prefixed_skus",
                "inferred_prefix",
                "inferred_count",
                "confidence_ratio",
                "candidate_prefixes",
                "sample_sku_1",
                "sample_sku_2",
                "sample_sku_3",
            ]
        )

    work = catalog_df.copy()
    if "vendor" not in work.columns or "sku" not in work.columns:
        return pd.DataFrame()

    work["vendor"] = work["vendor"].astype(str).map(_clean_text)
    work["sku"] = work["sku"].astype(str).map(normalize_sku)
    work = work[(work["vendor"] != "") & (work["sku"] != "")].copy()
    work = work.drop_duplicates(subset=["vendor", "sku"], keep="first")
    work["prefix"] = work["sku"].map(_extract_prefix)

    rows: list[dict[str, object]] = []
    for vendor_name, group in work.groupby("vendor", sort=True):
        sku_values = group["sku"].tolist()
        total_skus = len(sku_values)
        prefixed = group[group["prefix"] != ""]
        prefixed_skus = len(prefixed)

        inferred_prefix = ""
        inferred_count = 0
        confidence_ratio = 0.0
        candidate_prefixes = ""
        sample_skus: list[str] = []

        if prefixed_skus > 0:
            counts = (
                prefixed.groupby("prefix")["sku"]
                .nunique()
                .sort_values(ascending=False)
            )
            if not counts.empty:
                inferred_prefix = str(counts.index[0])
                inferred_count = int(counts.iloc[0])
                confidence_ratio = float(inferred_count) / float(total_skus) if total_skus else 0.0
                candidate_prefixes = " | ".join([f"{idx}:{int(val)}" for idx, val in counts.head(6).items()])
                sample_skus = (
                    prefixed[prefixed["prefix"] == inferred_prefix]["sku"]
                    .drop_duplicates()
                    .head(3)
                    .tolist()
                )

        row = {
            "shopify_vendor": vendor_name,
            "total_skus": int(total_skus),
            "prefixed_skus": int(prefixed_skus),
            "inferred_prefix": inferred_prefix,
            "inferred_count": int(inferred_count),
            "confidence_ratio": round(confidence_ratio, 4),
            "candidate_prefixes": candidate_prefixes,
            "sample_sku_1": sample_skus[0] if len(sample_skus) > 0 else "",
            "sample_sku_2": sample_skus[1] if len(sample_skus) > 1 else "",
            "sample_sku_3": sample_skus[2] if len(sample_skus) > 2 else "",
        }
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(by=["shopify_vendor"], kind="stable").reset_index(drop=True)
    return out


def _merge_prefixes_into_vendor_profiles(
    profiles_path: Path,
    hints_df: pd.DataFrame,
    min_confidence: float,
    min_count: int,
) -> tuple[int, int]:
    if not profiles_path.exists():
        return 0, 0
    try:
        profiles_df = pd.read_csv(profiles_path, dtype=str, keep_default_na=False)
    except Exception:
        return 0, 0
    if profiles_df.empty:
        return 0, 0

    for column in ("canonical_vendor", "shopify_vendor_value", "sku_prefix"):
        if column not in profiles_df.columns:
            profiles_df[column] = ""

    hint_map = {}
    for _, row in hints_df.iterrows():
        vendor_name = _clean_text(row.get("shopify_vendor", ""))
        if not vendor_name:
            continue
        hint_map[vendor_name] = row

    rows_checked = 0
    rows_updated = 0
    for idx, row in profiles_df.iterrows():
        shopify_vendor = _clean_text(row.get("shopify_vendor_value", ""))
        canonical_vendor = _clean_text(row.get("canonical_vendor", ""))
        key = shopify_vendor or canonical_vendor
        if not key:
            continue
        hint = hint_map.get(key)
        if hint is None:
            continue
        rows_checked += 1
        existing_prefix = _clean_text(row.get("sku_prefix", ""))
        if existing_prefix:
            continue
        inferred_prefix = _clean_text(hint.get("inferred_prefix", ""))
        inferred_count = int(float(str(hint.get("inferred_count", "0") or "0")))
        confidence = float(str(hint.get("confidence_ratio", "0") or "0"))
        if not inferred_prefix:
            continue
        if inferred_count < max(1, int(min_count)):
            continue
        if confidence < float(min_confidence):
            continue
        profiles_df.at[idx, "sku_prefix"] = inferred_prefix
        rows_updated += 1

    profiles_df.to_csv(profiles_path, index=False, encoding="utf-8-sig")
    return rows_checked, rows_updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Infer vendor SKU prefixes from Shopify catalog (read-only) and update VendorProfiles.")
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Force a fresh read-only Shopify catalog download before inferring prefixes.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.6,
        help="Minimum ratio (inferred_count / total_skus) to auto-fill VendorProfiles.sku_prefix (default 0.6).",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=3,
        help="Minimum inferred SKU count to auto-fill VendorProfiles.sku_prefix (default 3).",
    )
    args = parser.parse_args()

    required_root = _resolve_required_root()
    hints_path = required_root / "mappings" / "VendorSkuPrefixHints.csv"
    profiles_path = required_root / "mappings" / "VendorProfiles.csv"

    catalog_df, catalog_error = _load_catalog_rows(refresh_cache=bool(args.refresh_cache))
    if catalog_error:
        print(f"ERROR: {catalog_error}")
        return 1
    if catalog_df.empty:
        print("ERROR: No catalog rows available to infer SKU prefixes.")
        return 1

    hints_df = _build_prefix_hints(catalog_df)
    hints_path.parent.mkdir(parents=True, exist_ok=True)
    hints_df.to_csv(hints_path, index=False, encoding="utf-8-sig")

    checked, updated = _merge_prefixes_into_vendor_profiles(
        profiles_path=profiles_path,
        hints_df=hints_df,
        min_confidence=float(args.min_confidence),
        min_count=int(args.min_count),
    )
    print(f"Saved {len(hints_df)} vendor prefix hint row(s):")
    print(str(hints_path.resolve()))
    print(f"VendorProfiles rows checked: {checked}, sku_prefix auto-filled: {updated}")
    print(str(profiles_path.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
