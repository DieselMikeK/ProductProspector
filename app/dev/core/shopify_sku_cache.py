from __future__ import annotations

from pathlib import Path

import pandas as pd

from product_prospector.core.config_store import APP_BASE_DIR


CACHE_COLUMNS = ["sku", "title", "description", "fitment", "product_type", "vendor", "barcode"]
CACHE_PATH = APP_BASE_DIR / "config" / "shopify_sku_cache.csv"


def _empty_cache_df() -> pd.DataFrame:
    return pd.DataFrame(columns=CACHE_COLUMNS)


def load_shopify_sku_cache() -> pd.DataFrame:
    if not CACHE_PATH.exists():
        return _empty_cache_df()
    try:
        df = pd.read_csv(CACHE_PATH, dtype=str).fillna("")
    except Exception:
        return _empty_cache_df()

    for column in CACHE_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    df = df[CACHE_COLUMNS].copy()
    df["sku"] = df["sku"].astype(str).str.strip()
    df = df[df["sku"] != ""].copy()
    df = df.drop_duplicates(subset=["sku"], keep="first")
    return df.reset_index(drop=True)


def save_shopify_sku_cache(df: pd.DataFrame | None) -> int:
    if df is None or df.empty:
        return 0
    out = df.copy()
    for column in CACHE_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    out = out[CACHE_COLUMNS].copy()
    out["sku"] = out["sku"].astype(str).str.strip()
    out = out[out["sku"] != ""].copy()
    out = out.drop_duplicates(subset=["sku"], keep="first")
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(CACHE_PATH, index=False)
    return int(len(out))


def get_shopify_sku_cache_path() -> Path:
    return CACHE_PATH
