from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from product_prospector.core.config_store import ShopifyConfig
from product_prospector.core.processing import normalize_sku


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_numeric_id(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if text.isdigit():
        return text
    match = re.search(r"/(\d+)$", text)
    if match:
        return match.group(1)
    # Allows URL or free-form text with a product id token.
    match = re.search(r"\b(\d{8,})\b", text)
    if match:
        return match.group(1)
    return ""


def _to_gid(resource: str, numeric_id: str) -> str:
    rid = _extract_numeric_id(numeric_id)
    if not rid:
        return ""
    return f"gid://shopify/{resource}/{rid}"


def _request_graphql(
    config: ShopifyConfig,
    access_token: str,
    query: str,
    variables: dict,
    require_query_only: bool,
) -> tuple[dict | None, str | None]:
    query_text = _clean_text(query).lower()
    if require_query_only:
        if re.search(r"\bmutation\b", query_text):
            return None, "Read-only guard blocked non-query GraphQL operation."
        if not re.match(r"^\s*query\b", query_text):
            return None, "Read-only guard requires GraphQL query operations only."
    else:
        if not re.match(r"^\s*(query|mutation)\b", query_text):
            return None, "GraphQL operation must begin with query or mutation."

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


@dataclass
class VariantSnapshot:
    product_gid: str = ""
    product_id: str = ""
    product_title: str = ""
    product_description_html: str = ""
    product_type: str = ""
    product_vendor: str = ""
    product_application: str = ""
    product_collections: str = ""
    product_google_product_type: str = ""
    product_category_code: str = ""
    product_subtype: str = ""
    variant_gid: str = ""
    variant_id: str = ""
    variant_sku: str = ""
    variant_barcode: str = ""
    variant_price: str = ""
    variant_compare_at_price: str = ""
    variant_inventory_quantity: str = ""
    variant_inventory_policy: str = ""
    variant_taxable: str = ""
    variant_option_summary: str = ""
    variant_google_mpn: str = ""
    variant_enable_low_stock_message: str = ""
    inventory_item_gid: str = ""
    inventory_item_id: str = ""
    inventory_item_cost: str = ""
    variant_weight_value: str = ""
    variant_weight_unit: str = ""


def _normalize_weight_unit(value: object) -> str:
    unit = _clean_text(value).upper()
    if unit in {"POUNDS", "KILOGRAMS", "GRAMS", "OUNCES"}:
        return unit
    return "POUNDS"


def _weight_value_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    try:
        parsed = float(text)
    except Exception:
        return ""
    if abs(parsed - round(parsed)) < 0.000001:
        return str(int(round(parsed)))
    return f"{parsed:.4f}".rstrip("0").rstrip(".")


def _selected_options_summary(values: list[dict]) -> str:
    parts: list[str] = []
    for item in values or []:
        name = _clean_text((item or {}).get("name", ""))
        value = _clean_text((item or {}).get("value", ""))
        if not name and not value:
            continue
        if name and value:
            parts.append(f"{name}: {value}")
        else:
            parts.append(name or value)
    return " | ".join(parts)


def _collection_titles_csv(collections_block: dict) -> str:
    nodes = ((collections_block or {}).get("nodes")) or []
    ordered: list[str] = []
    seen: set[str] = set()
    for item in nodes:
        title = _clean_text((item or {}).get("title", ""))
        if not title:
            continue
        key = re.sub(r"\s+", " ", title).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(title)
    return ", ".join(ordered)


def _parse_variant_snapshot(node: dict, product_node: dict) -> VariantSnapshot:
    product_gid = _clean_text((product_node or {}).get("id", ""))
    product_app = _clean_text(((product_node or {}).get("metafield") or {}).get("value", ""))
    variant_gid = _clean_text((node or {}).get("id", ""))
    inventory_item = (node or {}).get("inventoryItem") or {}
    measurement = inventory_item.get("measurement") or {}
    weight = measurement.get("weight") or {}
    selected_options = (node or {}).get("selectedOptions") or []
    variant_mpn = _clean_text(((node or {}).get("mpnMetafield") or {}).get("value", ""))
    variant_low_stock = _clean_text(((node or {}).get("lowStockMetafield") or {}).get("value", ""))

    return VariantSnapshot(
        product_gid=product_gid,
        product_id=_extract_numeric_id(product_gid),
        product_title=_clean_text((product_node or {}).get("title", "")),
        product_description_html=_clean_text((product_node or {}).get("descriptionHtml", "")),
        product_type=_clean_text((product_node or {}).get("productType", "")),
        product_vendor=_clean_text((product_node or {}).get("vendor", "")),
        product_application=product_app,
        product_collections=_collection_titles_csv((product_node or {}).get("collections") or {}),
        product_google_product_type=_clean_text(((product_node or {}).get("googleProductTypeMetafield") or {}).get("value", "")),
        product_category_code=_clean_text(((product_node or {}).get("categoryCodesMetafield") or {}).get("value", "")),
        product_subtype=_clean_text(((product_node or {}).get("productSubtypeMetafield") or {}).get("value", "")),
        variant_gid=variant_gid,
        variant_id=_extract_numeric_id(variant_gid),
        variant_sku=normalize_sku(_clean_text((node or {}).get("sku", ""))),
        variant_barcode=_clean_text((node or {}).get("barcode", "")),
        variant_price=_clean_text((node or {}).get("price", "")),
        variant_compare_at_price=_clean_text((node or {}).get("compareAtPrice", "")),
        variant_inventory_quantity=_clean_text((node or {}).get("inventoryQuantity", "")),
        variant_inventory_policy=_clean_text((node or {}).get("inventoryPolicy", "")),
        variant_taxable=_clean_text((node or {}).get("taxable", "")),
        variant_option_summary=_selected_options_summary(selected_options),
        variant_google_mpn=variant_mpn,
        variant_enable_low_stock_message=variant_low_stock,
        inventory_item_gid=_clean_text(inventory_item.get("id", "")),
        inventory_item_id=_extract_numeric_id(inventory_item.get("id", "")),
        inventory_item_cost=_clean_text(inventory_item.get("cost", "")),
        variant_weight_value=_weight_value_text(weight.get("value", "")),
        variant_weight_unit=_normalize_weight_unit(weight.get("unit", "")),
    )


_PRODUCTS_BY_IDS_QUERY = """
query ProductsByIds($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Product {
      id
      title
      descriptionHtml
      productType
      vendor
      metafield(namespace: "custom", key: "application") {
        value
      }
      googleProductTypeMetafield: metafield(namespace: "custom", key: "google_product_type") {
        value
      }
      categoryCodesMetafield: metafield(namespace: "custom", key: "category_codes_4") {
        value
      }
      productSubtypeMetafield: metafield(namespace: "custom", key: "product_subtype") {
        value
      }
      collections(first: 250) {
        nodes {
          title
        }
      }
      variants(first: 250) {
        pageInfo {
          hasNextPage
        }
        edges {
          node {
            id
            sku
            barcode
            price
            compareAtPrice
            inventoryQuantity
            inventoryPolicy
            taxable
            selectedOptions {
              name
              value
            }
            mpnMetafield: metafield(namespace: "mm-google-shopping", key: "mpn") {
              value
            }
            lowStockMetafield: metafield(namespace: "custom", key: "enable_low_stock_message") {
              value
            }
            inventoryItem {
              id
              tracked
              requiresShipping
              measurement {
                weight {
                  value
                  unit
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


_VARIANTS_BY_SKU_QUERY = """
query VariantsBySku($cursor: String, $search: String!) {
  productVariants(first: 100, after: $cursor, query: $search) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        sku
        barcode
        price
        compareAtPrice
        inventoryQuantity
        inventoryPolicy
        taxable
        selectedOptions {
          name
          value
        }
        mpnMetafield: metafield(namespace: "mm-google-shopping", key: "mpn") {
          value
        }
        lowStockMetafield: metafield(namespace: "custom", key: "enable_low_stock_message") {
          value
        }
        inventoryItem {
          id
          tracked
          requiresShipping
          measurement {
            weight {
              value
              unit
            }
          }
        }
        product {
          id
          title
          descriptionHtml
          productType
          vendor
          metafield(namespace: "custom", key: "application") {
            value
          }
          googleProductTypeMetafield: metafield(namespace: "custom", key: "google_product_type") {
            value
          }
          categoryCodesMetafield: metafield(namespace: "custom", key: "category_codes_4") {
            value
          }
          productSubtypeMetafield: metafield(namespace: "custom", key: "product_subtype") {
            value
          }
          collections(first: 250) {
            nodes {
              title
            }
          }
        }
      }
    }
  }
}
"""


def _search_query_for_skus(skus: list[str]) -> str:
    tokens: list[str] = []
    for sku in skus:
        cleaned = _clean_text(sku).replace('"', '\\"')
        if not cleaned:
            continue
        tokens.append(f'sku:"{cleaned}"')
    return " OR ".join(tokens)


def fetch_variant_snapshots_by_product_ids(
    config: ShopifyConfig,
    access_token: str,
    product_ids: list[str],
    progress_callback=None,
) -> tuple[list[VariantSnapshot], list[str], str | None]:
    normalized_ids = list(dict.fromkeys(_extract_numeric_id(value) for value in product_ids if _extract_numeric_id(value)))
    if not normalized_ids:
        return [], [], None

    warnings: list[str] = []
    snapshots: list[VariantSnapshot] = []
    chunk_size = 40
    total_chunks = max(1, (len(normalized_ids) + chunk_size - 1) // chunk_size)
    for index, start in enumerate(range(0, len(normalized_ids), chunk_size), start=1):
        chunk = normalized_ids[start : start + chunk_size]
        gids = [_to_gid("Product", item) for item in chunk if _to_gid("Product", item)]
        if not gids:
            continue
        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_PRODUCTS_BY_IDS_QUERY,
            variables={"ids": gids},
            require_query_only=True,
        )
        if error:
            return [], warnings, error
        nodes = (data or {}).get("nodes") or []
        for node in nodes:
            product_node = node or {}
            product_gid = _clean_text(product_node.get("id", ""))
            if not product_gid:
                continue
            variants_block = product_node.get("variants") or {}
            if bool((variants_block.get("pageInfo") or {}).get("hasNextPage")):
                warnings.append(
                    f"{_extract_numeric_id(product_gid)}: product has more than 250 variants; loaded first 250 only."
                )
            variant_edges = variants_block.get("edges") or []
            for edge in variant_edges:
                variant_node = (edge or {}).get("node") or {}
                variant_gid = _clean_text(variant_node.get("id", ""))
                if not variant_gid:
                    continue
                snapshots.append(_parse_variant_snapshot(node=variant_node, product_node=product_node))
        if progress_callback is not None:
            try:
                progress_callback(index, total_chunks, len(snapshots))
            except Exception:
                pass

    deduped: dict[str, VariantSnapshot] = {}
    for snapshot in snapshots:
        key = _clean_text(snapshot.variant_gid)
        if not key:
            continue
        if key not in deduped:
            deduped[key] = snapshot
    ordered = list(deduped.values())
    ordered.sort(key=lambda item: (_clean_text(item.product_gid), _clean_text(item.variant_gid)))
    return ordered, warnings, None


def fetch_variant_snapshots_by_skus(
    config: ShopifyConfig,
    access_token: str,
    skus: list[str],
    progress_callback=None,
) -> tuple[list[VariantSnapshot], list[str], str | None]:
    normalized_skus = list(dict.fromkeys(normalize_sku(value) for value in skus if normalize_sku(value)))
    if not normalized_skus:
        return [], [], None

    warnings: list[str] = []
    snapshots: list[VariantSnapshot] = []
    batch_size = 25
    total_batches = max(1, (len(normalized_skus) + batch_size - 1) // batch_size)
    for batch_index, start in enumerate(range(0, len(normalized_skus), batch_size), start=1):
        sku_batch = normalized_skus[start : start + batch_size]
        search_query = _search_query_for_skus(sku_batch)
        if not search_query:
            continue
        cursor: str | None = None
        page = 0
        while True:
            page += 1
            data, error = _request_graphql(
                config=config,
                access_token=access_token,
                query=_VARIANTS_BY_SKU_QUERY,
                variables={"cursor": cursor, "search": search_query},
                require_query_only=True,
            )
            if error:
                return [], warnings, error
            block = (data or {}).get("productVariants") or {}
            edges = block.get("edges") or []
            for edge in edges:
                node = (edge or {}).get("node") or {}
                product_node = (node or {}).get("product") or {}
                variant_gid = _clean_text(node.get("id", ""))
                product_gid = _clean_text(product_node.get("id", ""))
                if not variant_gid or not product_gid:
                    continue
                snapshots.append(_parse_variant_snapshot(node=node, product_node=product_node))

            page_info = block.get("pageInfo") or {}
            has_next = bool(page_info.get("hasNextPage"))
            cursor = _clean_text(page_info.get("endCursor", "")) or None
            if not has_next or not cursor:
                break
        if progress_callback is not None:
            try:
                progress_callback(batch_index, total_batches, len(snapshots))
            except Exception:
                pass

    deduped: dict[str, VariantSnapshot] = {}
    for snapshot in snapshots:
        key = _clean_text(snapshot.variant_gid)
        if key and key not in deduped:
            deduped[key] = snapshot
    ordered = list(deduped.values())
    ordered.sort(key=lambda item: (_clean_text(item.product_gid), _clean_text(item.variant_gid)))
    return ordered, warnings, None


@dataclass
class VariantWeightUpdate:
    product_gid: str
    variant_gid: str
    weight_value: float
    weight_unit: str = "POUNDS"


@dataclass
class VariantWeightUpdateSummary:
    requested: int = 0
    updated_variant_ids: list[str] = field(default_factory=list)
    failed_by_variant_id: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ProductTagAddSummary:
    requested: int = 0
    tagged_product_ids: list[str] = field(default_factory=list)
    skipped_already_tagged_product_ids: list[str] = field(default_factory=list)
    failed_by_product_id: dict[str, str] = field(default_factory=dict)


_VARIANT_WEIGHT_BULK_MUTATION = """
mutation VariantWeightBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants {
      id
      sku
    }
    userErrors {
      field
      message
    }
  }
}
"""


_PRODUCT_TAGS_ADD_MUTATION = """
mutation AddProductTags($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node {
      id
    }
    userErrors {
      field
      message
    }
  }
}
"""


_PRODUCT_TAGS_BY_IDS_QUERY = """
query ProductTagsByIds($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Product {
      id
      tags
    }
  }
}
"""


def _chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        return [list(values)]
    output: list[list[str]] = []
    for start in range(0, len(values), chunk_size):
        output.append(list(values[start : start + chunk_size]))
    return output


def push_variant_weights_bulk(
    config: ShopifyConfig,
    access_token: str,
    updates: list[VariantWeightUpdate],
    progress_callback=None,
) -> VariantWeightUpdateSummary:
    summary = VariantWeightUpdateSummary(requested=len(updates))
    if not updates:
        return summary

    grouped: dict[str, list[VariantWeightUpdate]] = {}
    for item in updates:
        product_gid = _clean_text(item.product_gid)
        variant_gid = _clean_text(item.variant_gid)
        if not product_gid or not variant_gid:
            summary.failed_by_variant_id[variant_gid or "<missing_variant_gid>"] = "Missing product or variant id."
            continue
        grouped.setdefault(product_gid, []).append(item)

    done = 0
    total = len(updates)
    for product_gid, group in grouped.items():
        payload_variants: list[dict] = []
        variant_ids_in_payload: list[str] = []
        for item in group:
            unit = _normalize_weight_unit(item.weight_unit)
            payload_variants.append(
                {
                    "id": _clean_text(item.variant_gid),
                    "inventoryItem": {
                        "measurement": {
                            "weight": {
                                "value": float(item.weight_value),
                                "unit": unit,
                            }
                        }
                    },
                }
            )
            variant_ids_in_payload.append(_clean_text(item.variant_gid))

        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_VARIANT_WEIGHT_BULK_MUTATION,
            variables={
                "productId": product_gid,
                "variants": payload_variants,
            },
            require_query_only=False,
        )
        if error:
            for variant_gid in variant_ids_in_payload:
                summary.failed_by_variant_id[variant_gid] = error
                done += 1
                if progress_callback is not None:
                    try:
                        progress_callback(done, total, variant_gid)
                    except Exception:
                        pass
            continue

        block = (data or {}).get("productVariantsBulkUpdate") or {}
        user_errors = block.get("userErrors") or []
        updated_variants = block.get("productVariants") or []
        updated_ids = {_clean_text(item.get("id", "")) for item in updated_variants if _clean_text(item.get("id", ""))}
        for updated_id in sorted(updated_ids):
            summary.updated_variant_ids.append(updated_id)

        if user_errors:
            message_parts: list[str] = []
            for item in user_errors:
                message = _clean_text((item or {}).get("message", ""))
                if message:
                    message_parts.append(message)
            err_text = "; ".join(message_parts) or "Shopify returned userErrors."
            for variant_gid in variant_ids_in_payload:
                if variant_gid in updated_ids:
                    continue
                summary.failed_by_variant_id[variant_gid] = err_text

        for variant_gid in variant_ids_in_payload:
            done += 1
            if progress_callback is not None:
                try:
                    progress_callback(done, total, variant_gid)
                except Exception:
                    pass

    return summary


def add_tag_to_products(
    config: ShopifyConfig,
    access_token: str,
    product_gids: list[str],
    tag: str,
    progress_callback=None,
) -> ProductTagAddSummary:
    clean_tag = _clean_text(tag)
    clean_product_gids = list(dict.fromkeys(_clean_text(gid) for gid in (product_gids or []) if _clean_text(gid)))
    summary = ProductTagAddSummary(requested=len(clean_product_gids))
    if not clean_product_gids or not clean_tag:
        return summary

    tag_lookup = clean_tag.casefold()
    tags_by_product: dict[str, set[str]] = {}

    # Verify existing tags first so we only write when the operator tag is missing.
    for gid_chunk in _chunk_values(clean_product_gids, 50):
        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_PRODUCT_TAGS_BY_IDS_QUERY,
            variables={"ids": gid_chunk},
            require_query_only=True,
        )
        if error:
            for product_gid in gid_chunk:
                summary.failed_by_product_id[product_gid] = f"Could not verify existing product tags: {error}"
            continue

        nodes = (data or {}).get("nodes") or []
        node_by_gid: dict[str, dict] = {}
        for item in nodes:
            node = item or {}
            node_id = _clean_text(node.get("id", ""))
            if node_id:
                node_by_gid[node_id] = node

        for product_gid in gid_chunk:
            node = node_by_gid.get(product_gid)
            if node is None:
                summary.failed_by_product_id[product_gid] = "Product not found during tag verification."
                continue
            existing_tags = node.get("tags") or []
            normalized_tags = {_clean_text(item).casefold() for item in existing_tags if _clean_text(item)}
            tags_by_product[product_gid] = normalized_tags

    total = len(clean_product_gids)
    for index, product_gid in enumerate(clean_product_gids, start=1):
        existing_error = summary.failed_by_product_id.get(product_gid, "")
        if existing_error:
            if progress_callback is not None:
                try:
                    progress_callback(index, total, product_gid)
                except Exception:
                    pass
            continue

        product_tags = tags_by_product.get(product_gid, set())
        if tag_lookup in product_tags:
            summary.skipped_already_tagged_product_ids.append(product_gid)
            if progress_callback is not None:
                try:
                    progress_callback(index, total, product_gid)
                except Exception:
                    pass
            continue

        data, error = _request_graphql(
            config=config,
            access_token=access_token,
            query=_PRODUCT_TAGS_ADD_MUTATION,
            variables={
                "id": product_gid,
                "tags": [clean_tag],
            },
            require_query_only=False,
        )
        if error:
            summary.failed_by_product_id[product_gid] = error
            if progress_callback is not None:
                try:
                    progress_callback(index, total, product_gid)
                except Exception:
                    pass
            continue

        payload = (data or {}).get("tagsAdd") or {}
        user_errors = payload.get("userErrors") or []
        if user_errors:
            messages: list[str] = []
            for item in user_errors:
                message = _clean_text((item or {}).get("message", ""))
                if message:
                    messages.append(message)
            summary.failed_by_product_id[product_gid] = "; ".join(messages) or "Shopify returned userErrors."
        else:
            summary.tagged_product_ids.append(product_gid)

        if progress_callback is not None:
            try:
                progress_callback(index, total, product_gid)
            except Exception:
                pass

    return summary
