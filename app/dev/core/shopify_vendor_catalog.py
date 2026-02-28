from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

from product_prospector.core.config_store import ShopifyConfig
from product_prospector.core.shopify_brand_metaobjects import find_brand_metaobject_file, resolve_brand_metaobject_gid


DEFAULT_VENDOR_CATALOG_FILE = "ShopifyProductVendors.csv"
DEFAULT_VENDOR_PROFILE_FILE = "VendorProfiles.csv"


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _request_graphql_read_only(
    config: ShopifyConfig,
    access_token: str,
    query: str,
    variables: dict,
) -> tuple[dict | None, str | None]:
    # Hard safety guard: this module is read-only by design.
    query_text = _clean_text(query).lower()
    if "mutation" in query_text or "delete" in query_text or "update" in query_text:
        return None, "Read-only guard blocked non-query GraphQL operation."
    if not query_text.startswith("query"):
        return None, "Read-only guard requires GraphQL query operations only."

    url = f"https://{config.shop_domain}/admin/api/{config.api_version}/graphql.json"
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Shopify-Access-Token": access_token,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return None, f"Shopify HTTP {exc.code}: {detail}"
    except Exception as exc:
        return None, str(exc)

    try:
        parsed = json.loads(body)
    except Exception:
        return None, "Invalid JSON response from Shopify GraphQL."

    errors = parsed.get("errors") or []
    if errors:
        messages: list[str] = []
        for error in errors:
            if isinstance(error, dict):
                message = _clean_text(error.get("message", ""))
                if message:
                    messages.append(message)
        return None, "; ".join(messages) or "Shopify GraphQL returned errors."
    return parsed.get("data") or {}, None


_PRODUCT_VENDORS_QUERY = """
query ProductVendors($cursor: String) {
  productVendors(first: 250, after: $cursor) {
    edges {
      node
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def fetch_shopify_product_vendors(
    config: ShopifyConfig,
    access_token: str,
    max_pages: int = 200,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None]:
    cursor: str | None = None
    page = 0
    vendors: list[str] = []
    seen: set[str] = set()

    while page < max_pages:
        page += 1
        data, error = _request_graphql_read_only(
            config=config,
            access_token=access_token,
            query=_PRODUCT_VENDORS_QUERY,
            variables={"cursor": cursor},
        )
        if error:
            return pd.DataFrame(), error

        block = (data or {}).get("productVendors") or {}
        edges = block.get("edges") or []
        for edge in edges:
            vendor_name = _clean_text((edge or {}).get("node"))
            if not vendor_name:
                continue
            key = _normalize_key(vendor_name)
            if not key or key in seen:
                continue
            seen.add(key)
            vendors.append(vendor_name)

        if progress_callback is not None:
            try:
                progress_callback(page, len(vendors))
            except Exception:
                pass

        page_info = block.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

    vendors_sorted = sorted(vendors, key=lambda item: _normalize_key(item))
    df = pd.DataFrame({"shopify_vendor": vendors_sorted})
    return df, None


def default_vendor_catalog_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_VENDOR_CATALOG_FILE


def default_vendor_profile_path(required_root: Path) -> Path:
    return required_root / "mappings" / DEFAULT_VENDOR_PROFILE_FILE


def _load_brand_name_by_gid(required_root: Path | None) -> dict[str, str]:
    if required_root is None:
        return {}
    path = find_brand_metaobject_file(required_root=required_root)
    if path is None:
        return {}
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        elif suffix in {".xlsx", ".xls"}:
            df = pd.read_excel(path, dtype=str, keep_default_na=False)
        else:
            return {}
    except Exception:
        return {}
    if df.empty:
        return {}
    gid_col = ""
    for candidate in ("brand_gid", "gid", "metaobject_gid"):
        if candidate in df.columns:
            gid_col = candidate
            break
    if not gid_col:
        return {}
    name_col = ""
    for candidate in ("brand_name", "name", "display_name"):
        if candidate in df.columns:
            name_col = candidate
            break
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        gid = _clean_text(row.get(gid_col, ""))
        if not gid or gid in out:
            continue
        out[gid] = _clean_text(row.get(name_col, "")) if name_col else ""
    return out


def build_vendor_profile_template(vendor_df: pd.DataFrame, required_root: Path | None = None) -> pd.DataFrame:
    base = vendor_df.copy() if vendor_df is not None else pd.DataFrame()
    if "shopify_vendor" not in base.columns:
        base["shopify_vendor"] = ""
    base["shopify_vendor"] = base["shopify_vendor"].astype(str).map(_clean_text)
    base = base[base["shopify_vendor"] != ""].drop_duplicates(subset=["shopify_vendor"], keep="first")
    base = base.sort_values(by=["shopify_vendor"], kind="stable").reset_index(drop=True)

    brand_name_by_gid = _load_brand_name_by_gid(required_root=required_root)
    brand_gids: list[str] = []
    brand_names: list[str] = []
    for vendor_value in base["shopify_vendor"].tolist():
        gid = resolve_brand_metaobject_gid(vendor_value, required_root=required_root) if required_root is not None else ""
        brand_gids.append(gid)
        if gid:
            brand_names.append(brand_name_by_gid.get(gid, "") or vendor_value)
        else:
            brand_names.append("")

    profile = pd.DataFrame(
        {
            "canonical_vendor": base["shopify_vendor"],
            "aliases": "",
            "shopify_vendor_value": base["shopify_vendor"],
            "brand_name": brand_names,
            "brand_gid": brand_gids,
            "title_prefix": "",
            "sku_prefix": "",
            "discount_vendor_key": "",
            "notes": "",
        }
    )
    return profile


def save_table(df: pd.DataFrame, output_path: Path) -> tuple[int, str | None]:
    out = df.copy() if df is not None else pd.DataFrame()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if output_path.suffix.lower() == ".xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                out.to_excel(writer, index=False, sheet_name="data")
        else:
            out.to_csv(output_path, index=False, encoding="utf-8-sig")
    except Exception as exc:
        return 0, str(exc)
    return int(len(out)), None
