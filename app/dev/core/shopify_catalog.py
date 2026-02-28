from __future__ import annotations

import json
import urllib.error
import urllib.request

import pandas as pd

from product_prospector.core.config_store import ShopifyConfig


_CATALOG_QUERY = """
query Catalog($cursor: String) {
  products(first: 100, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        title
        description
        productType
        vendor
        metafield(namespace: "custom", key: "application") {
          value
        }
        variants(first: 100) {
          edges {
            node {
              sku
              barcode
            }
          }
        }
      }
    }
  }
}
"""

_VARIANT_BY_SKU_QUERY = """
query VariantsBySku($cursor: String, $search: String!) {
  productVariants(first: 100, after: $cursor, query: $search) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        sku
        barcode
        product {
          title
          description
          productType
          vendor
          metafield(namespace: "custom", key: "application") {
            value
          }
        }
      }
    }
  }
}
"""


def _request_graphql(config: ShopifyConfig, access_token: str, query: str, variables: dict) -> tuple[dict | None, str | None]:
    # Safety guard: catalog helpers are read-only and must remain query-only.
    query_text = str(query or "").strip().lower()
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
        for err in errors:
            if isinstance(err, dict):
                msg = str(err.get("message", "")).strip()
                if msg:
                    messages.append(msg)
        return None, "; ".join(messages) or "Shopify GraphQL returned errors."
    return parsed.get("data") or {}, None


def fetch_shopify_catalog_dataframe(
    config: ShopifyConfig,
    access_token: str,
    max_pages: int = 250,
    progress_callback=None,
) -> tuple[pd.DataFrame, str | None]:
    rows: list[dict[str, str]] = []
    cursor: str | None = None
    page_count = 0

    while page_count < max_pages:
        page_count += 1
        data, error = _request_graphql(config=config, access_token=access_token, query=_CATALOG_QUERY, variables={"cursor": cursor})
        if error:
            return pd.DataFrame(), error

        products = (data or {}).get("products") or {}
        edges = products.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            title = str(node.get("title", "")).strip()
            description = str(node.get("description", "")).strip()
            product_type = str(node.get("productType", "")).strip()
            vendor = str(node.get("vendor", "")).strip()
            metafield = node.get("metafield") or {}
            fitment = str((metafield or {}).get("value", "")).strip()

            variant_edges = ((node.get("variants") or {}).get("edges")) or []
            for variant_edge in variant_edges:
                variant = (variant_edge or {}).get("node") or {}
                sku = str(variant.get("sku", "")).strip()
                if not sku:
                    continue
                rows.append(
                    {
                        "sku": sku,
                        "title": title,
                        "description": description,
                        "fitment": fitment,
                        "product_type": product_type,
                        "vendor": vendor,
                        "barcode": str(variant.get("barcode", "")).strip(),
                    }
                )

        if progress_callback is not None:
            try:
                progress_callback(page_count, len(rows))
            except Exception:
                pass

        page_info = products.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

    if not rows:
        return pd.DataFrame(columns=["sku", "title", "description", "fitment", "product_type", "vendor", "barcode"]), None

    df = pd.DataFrame(rows)
    df["sku_norm"] = df["sku"].astype(str).str.strip().str.upper()
    df = df[df["sku_norm"] != ""].copy()
    df = df.drop_duplicates(subset=["sku_norm"], keep="first")
    df = df.drop(columns=["sku_norm"], errors="ignore")
    return df.reset_index(drop=True), None


def _normalize_sku(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().upper().split())


def _sku_search_query(skus: list[str]) -> str:
    tokens: list[str] = []
    for sku in skus:
        cleaned = str(sku).strip().replace('"', '\\"')
        if not cleaned:
            continue
        tokens.append(f'sku:"{cleaned}"')
    return " OR ".join(tokens)


def fetch_shopify_catalog_for_skus(
    config: ShopifyConfig,
    access_token: str,
    skus: list[str],
    batch_size: int = 25,
    max_pages_per_batch: int = 8,
) -> tuple[pd.DataFrame, str | None]:
    normalized_skus = [_normalize_sku(sku) for sku in skus if _normalize_sku(sku)]
    if not normalized_skus:
        return pd.DataFrame(columns=["sku", "title", "description", "fitment", "product_type", "vendor", "barcode"]), None

    rows: list[dict[str, str]] = []
    for start in range(0, len(normalized_skus), batch_size):
        sku_batch = normalized_skus[start : start + batch_size]
        search_query = _sku_search_query(sku_batch)
        if not search_query:
            continue
        cursor: str | None = None
        page_count = 0
        while page_count < max_pages_per_batch:
            page_count += 1
            data, error = _request_graphql(
                config=config,
                access_token=access_token,
                query=_VARIANT_BY_SKU_QUERY,
                variables={"cursor": cursor, "search": search_query},
            )
            if error:
                return pd.DataFrame(), error

            variants = (data or {}).get("productVariants") or {}
            edges = variants.get("edges") or []
            for edge in edges:
                node = (edge or {}).get("node") or {}
                sku = str(node.get("sku", "")).strip()
                if not sku:
                    continue
                product = node.get("product") or {}
                title = str(product.get("title", "")).strip()
                description = str(product.get("description", "")).strip()
                product_type = str(product.get("productType", "")).strip()
                vendor = str(product.get("vendor", "")).strip()
                metafield = product.get("metafield") or {}
                fitment = str((metafield or {}).get("value", "")).strip()
                rows.append(
                    {
                        "sku": sku,
                        "title": title,
                        "description": description,
                        "fitment": fitment,
                        "product_type": product_type,
                        "vendor": vendor,
                        "barcode": str(node.get("barcode", "")).strip(),
                    }
                )

            page_info = variants.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break

    if not rows:
        return pd.DataFrame(columns=["sku", "title", "description", "fitment", "product_type", "vendor", "barcode"]), None

    df = pd.DataFrame(rows)
    df["sku_norm"] = df["sku"].astype(str).str.strip().str.upper()
    df = df[df["sku_norm"] != ""].copy()
    df = df.drop_duplicates(subset=["sku_norm"], keep="first")
    df = df.drop(columns=["sku_norm"], errors="ignore")
    return df.reset_index(drop=True), None
