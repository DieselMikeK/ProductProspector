from __future__ import annotations

import base64
import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from product_prospector.core.config_store import ShopifyConfig
from product_prospector.core.processing import normalize_sku
from product_prospector.core.product_model import Product
from product_prospector.core.shopify_brand_metaobjects import resolve_brand_metaobject_gid
from product_prospector.core.shopify_collections import (
    resolve_collection_assignments,
    resolve_collection_assignments_from_titles,
)
from product_prospector.core.shopify_fitment_vehicle_metaobjects import resolve_fitment_vehicle_metaobject_gids
from product_prospector.core.vendor_profiles import resolve_vendor_profile
from product_prospector.core.vendor_normalization import normalize_vendor_name as normalize_vendor_from_rules


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_decimal_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return ""
    try:
        amount = float(cleaned)
    except Exception:
        return ""
    return f"{amount:.2f}"


def _to_int(value: object, default: int) -> int:
    text = _clean_text(value)
    if not text:
        return default
    try:
        return int(float(text))
    except Exception:
        return default


def _to_weight_lb(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return None
    try:
        parsed = float(cleaned)
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


def _split_multi_value(value: object) -> list[str]:
    items = re.split(r"[|,;\n]+", _clean_text(value))
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean_text(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(text)
    return output


def _normalize_media_urls(values: list[str] | object) -> list[str]:
    items: list[str]
    if isinstance(values, list):
        items = [str(item) for item in values]
    else:
        items = re.split(r"[|,\n]+", _clean_text(values))
    urls: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean_text(item)
        if not text:
            continue
        if not re.match(r"^https?://", text, flags=re.IGNORECASE):
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(text)
    return urls


def _safe_folder_name(value: str) -> str:
    text = normalize_sku(value)
    text = re.sub(r"[^A-Z0-9._-]+", "_", text)
    text = text.strip("._-")
    return text or "SKU"


def _strip_known_sku_prefix(sku: str, sku_prefix_hint: str = "") -> str:
    normalized_sku = normalize_sku(sku)
    prefix = normalize_sku(sku_prefix_hint)
    if not normalized_sku:
        return ""
    if not prefix:
        return normalized_sku
    for sep in ("-", "_"):
        token = f"{prefix}{sep}"
        if normalized_sku.startswith(token) and len(normalized_sku) > len(token):
            return normalized_sku[len(token) :]
    return normalized_sku


def _collect_local_images_for_sku(image_root: Path | None, sku: str, max_images: int = 20) -> list[Path]:
    if image_root is None:
        return []
    folder = image_root / _safe_folder_name(sku)
    if not folder.exists() or not folder.is_dir():
        return []
    allowed = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
    images = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() in allowed]
    images.sort(key=lambda item: item.name.lower())
    return images[:max_images]


def _collect_local_images_for_sku_candidates(
    image_root: Path | None,
    sku: str,
    sku_prefix_hint: str = "",
    max_images: int = 20,
) -> tuple[list[Path], list[str]]:
    normalized_sku = normalize_sku(sku)
    prefix_hint = normalize_sku(sku_prefix_hint)
    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        item = normalize_sku(value)
        if not item or item in seen:
            return
        seen.add(item)
        candidates.append(item)

    add(normalized_sku)

    if prefix_hint and normalized_sku.startswith(f"{prefix_hint}-"):
        add(normalized_sku[len(prefix_hint) + 1 :])
    if prefix_hint and normalized_sku.startswith(f"{prefix_hint}_"):
        add(normalized_sku[len(prefix_hint) + 1 :])

    if "-" in normalized_sku:
        add(normalized_sku.split("-", 1)[1])
    if "_" in normalized_sku:
        add(normalized_sku.split("_", 1)[1])

    generic_match = re.match(r"^[A-Z0-9]{2,12}[-_](.+)$", normalized_sku)
    if generic_match:
        add(generic_match.group(1))

    collected: list[Path] = []
    used_paths: set[str] = set()
    for candidate in candidates:
        files = _collect_local_images_for_sku(image_root=image_root, sku=candidate, max_images=max_images)
        for path in files:
            key = str(path.resolve())
            if key in used_paths:
                continue
            used_paths.add(key)
            collected.append(path)
            if len(collected) >= max_images:
                return collected, candidates

    return collected, candidates


def _extract_definition_type_from_error(error_text: str) -> str:
    text = _clean_text(error_text)
    if not text:
        return ""
    match = re.search(r"definition's type:\s*'([^']+)'", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _prepare_metafield_value(value: str, metafield_type: str) -> tuple[str | None, str | None]:
    text = _clean_text(value)
    kind = _clean_text(metafield_type).lower()
    if not text:
        return None, "empty value"

    if kind in {"single_line_text_field", "multi_line_text_field", "number_integer", "number_decimal", "date"}:
        return text, None

    if kind == "boolean":
        lowered = text.lower()
        if lowered in {"true", "1", "yes", "y"}:
            return "true", None
        if lowered in {"false", "0", "no", "n"}:
            return "false", None
        return None, "invalid boolean value"

    if kind.startswith("list."):
        values = _split_multi_value(text)
        if not values:
            return None, "empty list value"
        return json.dumps(values, ensure_ascii=False), None

    if kind == "metaobject_reference":
        if re.match(r"^gid://shopify/Metaobject/\d+$", text):
            return text, None
        return None, "requires metaobject gid (gid://shopify/Metaobject/<id>)"

    if kind in {"product_reference", "variant_reference", "file_reference", "page_reference"}:
        if text.startswith("gid://shopify/"):
            return text, None
        return None, "requires gid value"

    return text, None


def _request_rest_json(
    config: ShopifyConfig,
    access_token: str,
    method: str,
    path: str,
    payload: dict | None = None,
    timeout: int = 45,
    max_retries: int = 2,
) -> tuple[dict | None, str | None]:
    url = f"https://{config.shop_domain}/admin/api/{config.api_version}{path}"
    body = None
    headers = {
        "Accept": "application/json",
        "X-Shopify-Access-Token": access_token,
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    last_error = "Unknown Shopify REST request error."
    for attempt in range(max_retries + 1):
        request = urllib.request.Request(url=url, data=body, method=method.upper(), headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as exc:
            raw_error = exc.read().decode("utf-8", errors="ignore")
            if exc.code == 429 and attempt < max_retries:
                retry_after = 1.2
                try:
                    retry_after = float(exc.headers.get("Retry-After", "1.2"))
                except Exception:
                    retry_after = 1.2
                time.sleep(max(0.2, retry_after))
                last_error = f"Shopify HTTP 429: {raw_error}"
                continue
            return None, f"Shopify HTTP {exc.code}: {raw_error}"
        except Exception as exc:
            last_error = str(exc)
            if attempt < max_retries:
                time.sleep(0.5)
                continue
            return None, str(exc)

        if not raw:
            return {}, None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None, "Invalid JSON response from Shopify REST."
        return parsed, None

    return None, last_error


def _request_graphql_json(
    config: ShopifyConfig,
    access_token: str,
    query: str,
    variables: dict,
    timeout: int = 45,
) -> tuple[dict | None, str | None]:
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
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return None, f"Shopify HTTP {exc.code}: {detail}"
    except Exception as exc:
        return None, str(exc)

    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return None, "Invalid JSON response from Shopify GraphQL."

    errors = parsed.get("errors") or []
    if errors:
        messages: list[str] = []
        for err in errors:
            if isinstance(err, dict):
                msg = _clean_text(err.get("message", ""))
                if msg:
                    messages.append(msg)
        return None, "; ".join(messages) or "Shopify GraphQL returned errors."
    return parsed.get("data") or {}, None


_PUBLICATIONS_QUERY = """
query ListPublications($cursor: String) {
  publications(first: 100, after: $cursor) {
    edges {
      cursor
      node {
        id
        name
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

_PUBLISHABLE_PUBLISH_MUTATION = """
mutation PublishProductToPublications($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors {
      field
      message
    }
  }
}
"""


def _chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [values]
    chunks: list[list[str]] = []
    for index in range(0, len(values), size):
        chunks.append(values[index : index + size])
    return chunks


def _load_all_publications(
    config: ShopifyConfig,
    access_token: str,
) -> tuple[list[tuple[str, str]], str | None]:
    publications: list[tuple[str, str]] = []
    seen_ids: set[str] = set()
    cursor = ""

    while True:
        data, error = _request_graphql_json(
            config=config,
            access_token=access_token,
            query=_PUBLICATIONS_QUERY,
            variables={"cursor": cursor or None},
        )
        if error:
            return [], error

        block = (data or {}).get("publications") or {}
        edges = block.get("edges") or []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            publication_id = _clean_text(node.get("id", ""))
            if not publication_id or publication_id in seen_ids:
                continue
            seen_ids.add(publication_id)
            publication_name = _clean_text(node.get("name", "")) or publication_id
            publications.append((publication_id, publication_name))

        page_info = block.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _clean_text(page_info.get("endCursor", ""))
        if not cursor:
            break

    return publications, None


def _publish_product_to_publications(
    config: ShopifyConfig,
    access_token: str,
    product_gid: str,
    publication_ids: list[str],
) -> list[str]:
    errors: list[str] = []
    clean_product_gid = _clean_text(product_gid)
    if not clean_product_gid:
        return ["missing product gid"]
    targets = [_clean_text(item) for item in publication_ids if _clean_text(item)]
    if not targets:
        return []

    # Keep mutation payloads reasonably small for stability.
    for publication_chunk in _chunked(targets, 25):
        input_items = [{"publicationId": publication_id} for publication_id in publication_chunk]
        data, error = _request_graphql_json(
            config=config,
            access_token=access_token,
            query=_PUBLISHABLE_PUBLISH_MUTATION,
            variables={
                "id": clean_product_gid,
                "input": input_items,
            },
        )
        if error:
            errors.append(error)
            continue

        payload = (data or {}).get("publishablePublish") or {}
        user_errors = payload.get("userErrors") or []
        for user_error in user_errors:
            message = _clean_text((user_error or {}).get("message", ""))
            if message:
                errors.append(message)

    deduped: list[str] = []
    seen: set[str] = set()
    for item in errors:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(item)
    return deduped


_VARIANT_METAFIELD_DEFINITION_QUERY = """
query VariantMetafieldDefinition($namespace: String!, $key: String!) {
  metafieldDefinitions(first: 1, ownerType: PRODUCTVARIANT, namespace: $namespace, key: $key) {
    nodes {
      namespace
      key
      type { name }
      validations { name value }
    }
  }
}
"""


def _load_variant_metafield_definition(
    config: ShopifyConfig,
    access_token: str,
    namespace: str,
    key: str,
) -> tuple[str, list[str], str | None]:
    data, error = _request_graphql_json(
        config=config,
        access_token=access_token,
        query=_VARIANT_METAFIELD_DEFINITION_QUERY,
        variables={"namespace": _clean_text(namespace), "key": _clean_text(key)},
    )
    if error:
        return "", [], error
    nodes = (((data or {}).get("metafieldDefinitions") or {}).get("nodes")) or []
    if not nodes:
        return "", [], "definition not found"
    node = nodes[0] or {}
    type_name = _clean_text(((node.get("type") or {}).get("name")))
    validations = node.get("validations") or []
    choices: list[str] = []
    for rule in validations:
        if _clean_text((rule or {}).get("name", "")).lower() != "choices":
            continue
        raw = _clean_text((rule or {}).get("value", ""))
        if not raw:
            continue
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list):
                choices = [_clean_text(item) for item in decoded if _clean_text(item)]
            elif isinstance(decoded, str) and _clean_text(decoded):
                choices = [_clean_text(decoded)]
        except Exception:
            choices = [_clean_text(item) for item in re.split(r"[|,;\n]+", raw) if _clean_text(item)]
        break
    return type_name, choices, None


def _resolve_primary_location_id(config: ShopifyConfig, access_token: str) -> tuple[int | None, str | None]:
    data, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="GET",
        path="/locations.json?limit=1",
    )
    if error:
        return None, error
    locations = (data or {}).get("locations") or []
    if not locations:
        return None, "No Shopify locations found."
    location_id = locations[0].get("id")
    try:
        return int(location_id), None
    except Exception:
        return None, "Invalid Shopify location id."


def _build_product_payload(product: Product, vendor_override: str = "") -> dict:
    sku = normalize_sku(product.sku)
    title = _clean_text(product.title) or sku
    vendor_value = _clean_text(vendor_override) or _clean_text(product.vendor)
    variant: dict[str, object] = {
        "sku": sku,
        "barcode": _clean_text(product.barcode),
        "price": _to_decimal_text(product.price),
        "inventory_management": "shopify",
        # Always allow selling through zero inventory for this workflow.
        "inventory_policy": "continue",
    }
    weight_lb = _to_weight_lb(product.weight)
    if weight_lb is not None:
        variant["weight"] = weight_lb
        variant["weight_unit"] = "lb"

    payload: dict[str, object] = {
        "product": {
            "title": title,
            "body_html": _clean_text(product.description_html),
            "vendor": vendor_value,
            "product_type": _clean_text(product.type),
            "status": "draft",
            "variants": [variant],
        }
    }
    media_urls = _normalize_media_urls(product.media_urls)
    if media_urls:
        payload["product"]["images"] = [{"src": url} for url in media_urls]
    return payload


def _upload_product_image_from_file(
    config: ShopifyConfig,
    access_token: str,
    product_id: int,
    path: Path,
) -> str | None:
    try:
        body = path.read_bytes()
    except Exception as exc:
        return str(exc)
    if not body:
        return "empty image file"
    payload = {
        "image": {
            "attachment": base64.b64encode(body).decode("ascii"),
            "filename": path.name,
        }
    }
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path=f"/products/{product_id}/images.json",
        payload=payload,
    )
    return error


def _set_variant_cost(
    config: ShopifyConfig,
    access_token: str,
    inventory_item_id: int,
    cost_value: str,
) -> str | None:
    if not cost_value:
        return None
    payload = {"inventory_item": {"id": inventory_item_id, "cost": cost_value}}
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="PUT",
        path=f"/inventory_items/{inventory_item_id}.json",
        payload=payload,
    )
    return error


def _set_inventory_available(
    config: ShopifyConfig,
    access_token: str,
    location_id: int,
    inventory_item_id: int,
    available: int,
) -> str | None:
    payload = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": max(0, int(available)),
    }
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path="/inventory_levels/set.json",
        payload=payload,
    )
    return error


def _add_product_to_collection(
    config: ShopifyConfig,
    access_token: str,
    product_id: int,
    collection_id: int,
) -> str | None:
    payload = {
        "collect": {
            "product_id": int(product_id),
            "collection_id": int(collection_id),
        }
    }
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path="/collects.json",
        payload=payload,
    )
    if not error:
        return None
    if "already exists" in error.lower():
        return None
    return error


def _upsert_product_metafield(
    config: ShopifyConfig,
    access_token: str,
    product_id: int,
    namespace: str,
    key: str,
    value: str,
    metafield_type: str,
) -> str | None:
    text = _clean_text(value)
    if not text:
        return None

    encoded_value, encode_error = _prepare_metafield_value(text, metafield_type)
    if encode_error or encoded_value is None:
        return encode_error or "unsupported metafield value"

    payload = {
        "metafield": {
            "namespace": namespace,
            "key": key,
            "type": metafield_type,
            "value": encoded_value,
        }
    }
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path=f"/products/{product_id}/metafields.json",
        payload=payload,
    )
    if not error:
        return None

    expected_type = _extract_definition_type_from_error(error)
    if not expected_type or expected_type == metafield_type:
        return error

    retried_value, retry_encode_error = _prepare_metafield_value(text, expected_type)
    if retry_encode_error or retried_value is None:
        return f"definition expects {expected_type}; {retry_encode_error or 'value conversion failed'}"

    retry_payload = {
        "metafield": {
            "namespace": namespace,
            "key": key,
            "type": expected_type,
            "value": retried_value,
        }
    }
    _, retry_error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path=f"/products/{product_id}/metafields.json",
        payload=retry_payload,
    )
    return retry_error


def _upsert_variant_metafield(
    config: ShopifyConfig,
    access_token: str,
    variant_id: int,
    namespace: str,
    key: str,
    value: str,
    metafield_type: str,
) -> str | None:
    text = _clean_text(value)
    if not text:
        return None

    encoded_value, encode_error = _prepare_metafield_value(text, metafield_type)
    if encode_error or encoded_value is None:
        return encode_error or "unsupported metafield value"

    payload = {
        "metafield": {
            "namespace": namespace,
            "key": key,
            "type": metafield_type,
            "value": encoded_value,
        }
    }
    _, error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path=f"/variants/{variant_id}/metafields.json",
        payload=payload,
    )
    if not error:
        return None

    expected_type = _extract_definition_type_from_error(error)
    if not expected_type or expected_type == metafield_type:
        return error

    retried_value, retry_encode_error = _prepare_metafield_value(text, expected_type)
    if retry_encode_error or retried_value is None:
        return f"definition expects {expected_type}; {retry_encode_error or 'value conversion failed'}"

    retry_payload = {
        "metafield": {
            "namespace": namespace,
            "key": key,
            "type": expected_type,
            "value": retried_value,
        }
    }
    _, retry_error = _request_rest_json(
        config=config,
        access_token=access_token,
        method="POST",
        path=f"/variants/{variant_id}/metafields.json",
        payload=retry_payload,
    )
    return retry_error


@dataclass
class ShopifyDraftPushSummary:
    requested: int = 0
    created_skus: list[str] = field(default_factory=list)
    skipped_existing_skus: list[str] = field(default_factory=list)
    failed_by_sku: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def push_new_products_as_drafts(
    config: ShopifyConfig,
    access_token: str,
    products: list[Product],
    existing_skus: set[str] | None = None,
    include_images: bool = True,
    image_root: Path | None = None,
    required_root: Path | None = None,
    progress_callback=None,
) -> ShopifyDraftPushSummary:
    summary = ShopifyDraftPushSummary(requested=len(products))
    if not products:
        return summary

    publication_targets, publication_error = _load_all_publications(config=config, access_token=access_token)
    publication_ids = [item[0] for item in publication_targets if _clean_text(item[0])]
    publication_names = [item[1] for item in publication_targets if _clean_text(item[1])]
    publish_to_all_channels_enabled = bool(publication_ids)
    if publication_error:
        summary.warnings.append(f"Sales channel publication lookup failed ({publication_error}).")
    elif publication_ids:
        summary.warnings.append(
            f"Sales channel publication enabled for {len(publication_ids)} channel(s): {', '.join(publication_names[:10])}"
        )
    else:
        summary.warnings.append("No Shopify publications found; sales channels could not be assigned.")

    low_stock_type = "single_line_text_field"
    low_stock_value = "True"
    definition_type, definition_choices, definition_error = _load_variant_metafield_definition(
        config=config,
        access_token=access_token,
        namespace="custom",
        key="enable_low_stock_message",
    )
    if definition_type:
        low_stock_type = definition_type
    if definition_choices:
        preferred = ""
        for candidate in definition_choices:
            if _clean_text(candidate).lower() == "true":
                preferred = candidate
                break
        low_stock_value = _clean_text(preferred or definition_choices[0]) or low_stock_value
    if definition_error:
        summary.warnings.append(f"enable_low_stock_message definition lookup issue ({definition_error}); using fallback value.")

    existing_norm = {normalize_sku(value) for value in (existing_skus or set()) if normalize_sku(value)}
    location_id: int | None = None
    location_error: str | None = None
    collection_mapping_missing_noted = False

    for index, product in enumerate(products, start=1):
        sku = normalize_sku(product.sku)
        if progress_callback is not None:
            try:
                progress_callback(index - 1, len(products), sku)
            except Exception:
                pass

        if not sku:
            summary.failed_by_sku[f"row_{index}"] = "Missing SKU."
            continue
        if sku in existing_norm:
            summary.skipped_existing_skus.append(sku)
            continue

        title = _clean_text(product.title)
        if not title:
            summary.failed_by_sku[sku] = "Missing title."
            continue

        raw_vendor_value = _clean_text(product.vendor)
        normalized_vendor_value = normalize_vendor_from_rules(raw_vendor_value, required_root=required_root) or raw_vendor_value
        vendor_profile = resolve_vendor_profile(normalized_vendor_value or raw_vendor_value, required_root=required_root)
        profile_sku_prefix = _clean_text(vendor_profile.sku_prefix) if vendor_profile is not None else ""
        local_image_files: list[Path] = []
        local_image_candidates: list[str] = []
        if include_images:
            local_image_files, local_image_candidates = _collect_local_images_for_sku_candidates(
                image_root=image_root,
                sku=sku,
                sku_prefix_hint=profile_sku_prefix,
            )
        shopify_vendor_value = (
            _clean_text(vendor_profile.shopify_vendor_value) if vendor_profile is not None else ""
        ) or (
            _clean_text(vendor_profile.canonical_vendor) if vendor_profile is not None else ""
        ) or normalized_vendor_value or raw_vendor_value
        create_payload = _build_product_payload(product, vendor_override=shopify_vendor_value)
        if not include_images or local_image_files:
            product_payload = create_payload.get("product") or {}
            if isinstance(product_payload, dict) and "images" in product_payload:
                product_payload.pop("images", None)
        data, error = _request_rest_json(
            config=config,
            access_token=access_token,
            method="POST",
            path="/products.json",
            payload=create_payload,
        )
        if error:
            summary.failed_by_sku[sku] = error
            continue

        created_product = (data or {}).get("product") or {}
        product_id_raw = created_product.get("id")
        variants = created_product.get("variants") or []
        first_variant = variants[0] if variants else {}
        variant_id_raw = first_variant.get("id")
        inventory_item_id_raw = first_variant.get("inventory_item_id")

        try:
            product_id = int(product_id_raw)
        except Exception:
            summary.failed_by_sku[sku] = "Shopify create succeeded but product id was missing."
            continue

        manual_collections_text = _clean_text(getattr(product, "collections", ""))
        if manual_collections_text:
            collection_targets, collection_warnings = resolve_collection_assignments_from_titles(
                collections_text=manual_collections_text,
                required_root=required_root,
            )
        else:
            collection_targets, collection_warnings = resolve_collection_assignments(
                product_type=_clean_text(product.type),
                application_text=_clean_text(product.application),
                required_root=required_root,
                title_text=_clean_text(product.title),
                description_text=_clean_text(product.description_html),
            )
        for warning in collection_warnings:
            if "mapping file not found or empty" in warning.lower():
                if collection_mapping_missing_noted:
                    continue
                collection_mapping_missing_noted = True
            summary.warnings.append(f"{sku}: collections not assigned ({warning})")
        for target in collection_targets:
            collection_id_text = _clean_text(target.get("collection_id", ""))
            if not collection_id_text.isdigit():
                continue
            collection_title = _clean_text(target.get("collection_title", "")) or collection_id_text
            collection_error = _add_product_to_collection(
                config=config,
                access_token=access_token,
                product_id=product_id,
                collection_id=int(collection_id_text),
            )
            if collection_error:
                summary.warnings.append(f"{sku}: collection '{collection_title}' not assigned ({collection_error})")

        if publish_to_all_channels_enabled:
            product_gid = f"gid://shopify/Product/{product_id}"
            publish_errors = _publish_product_to_publications(
                config=config,
                access_token=access_token,
                product_gid=product_gid,
                publication_ids=publication_ids,
            )
            if publish_errors:
                combined = "; ".join(publish_errors)
                summary.warnings.append(f"{sku}: sales channels not fully assigned ({combined})")
                lowered = combined.lower()
                if "only active products" in lowered and "publish" in lowered:
                    summary.warnings.append(
                        "Sales channel publish requires ACTIVE products in this shop. "
                        "Auto channel assignment disabled for remaining draft products."
                    )
                    publish_to_all_channels_enabled = False

        variant_id: int | None
        try:
            variant_id = int(variant_id_raw)
        except Exception:
            variant_id = None

        inventory_item_id: int | None
        try:
            inventory_item_id = int(inventory_item_id_raw)
        except Exception:
            inventory_item_id = None

        cost_value = _to_decimal_text(product.cost)
        if inventory_item_id is not None and cost_value:
            cost_error = _set_variant_cost(
                config=config,
                access_token=access_token,
                inventory_item_id=inventory_item_id,
                cost_value=cost_value,
            )
            if cost_error:
                summary.warnings.append(f"{sku}: cost not set ({cost_error})")

        if inventory_item_id is not None:
            if location_id is None and location_error is None:
                location_id, location_error = _resolve_primary_location_id(config=config, access_token=access_token)
                if location_error:
                    summary.warnings.append(f"{sku}: inventory location not resolved ({location_error})")
            if location_id is not None:
                # Use review/mapped inventory value; default to 3,000,000 if blank/invalid.
                inventory_value = _to_int(product.inventory, 3_000_000)
                inventory_error = _set_inventory_available(
                    config=config,
                    access_token=access_token,
                    location_id=location_id,
                    inventory_item_id=inventory_item_id,
                    available=inventory_value,
                )
                if inventory_error:
                    summary.warnings.append(f"{sku}: inventory not set ({inventory_error})")

        profile_brand_gid = _clean_text(vendor_profile.brand_gid) if vendor_profile is not None else ""
        profile_brand_name = _clean_text(vendor_profile.brand_name) if vendor_profile is not None else ""
        brand_value = _clean_text(product.brand) or profile_brand_name or normalized_vendor_value or shopify_vendor_value
        brand_gid = profile_brand_gid or resolve_brand_metaobject_gid(brand_value, required_root=required_root)
        sku_no_prefix = _strip_known_sku_prefix(sku, profile_sku_prefix) or sku
        google_mpn_value = _strip_known_sku_prefix(_clean_text(product.mpn) or sku, profile_sku_prefix) or sku_no_prefix
        fitment_vehicle_gids, fitment_vehicle_warnings = resolve_fitment_vehicle_metaobject_gids(
            application_text=_clean_text(product.application),
            required_root=required_root,
            title_text=_clean_text(product.title),
            description_text=_clean_text(product.description_html),
        )
        for warning in fitment_vehicle_warnings:
            summary.warnings.append(f"{sku}: {warning}")
        fitment_vehicle_gid_text = " | ".join(fitment_vehicle_gids)

        metafields = [
            ("custom", "application", _clean_text(product.application), "single_line_text_field"),
            ("custom", "google_product_type", _clean_text(product.google_product_type), "single_line_text_field"),
            ("custom", "category_codes_4", _clean_text(product.category_code), "list.single_line_text_field"),
            ("custom", "product_subtype", _clean_text(product.product_subtype), "single_line_text_field"),
            (
                "custom",
                "core_charge_product_code",
                _clean_text(product.core_charge_product_code),
                "single_line_text_field",
            ),
            ("custom", "mpn", _clean_text(product.mpn) or sku, "single_line_text_field"),
            ("custom", "brand", brand_gid or brand_value, "metaobject_reference" if brand_gid else "single_line_text_field"),
            ("fitment", "vehicles", fitment_vehicle_gid_text, "list.metaobject_reference"),
        ]
        for namespace, key, value, metafield_type in metafields:
            metafield_error = _upsert_product_metafield(
                config=config,
                access_token=access_token,
                product_id=product_id,
                namespace=namespace,
                key=key,
                value=value,
                metafield_type=metafield_type,
            )
            if metafield_error:
                summary.warnings.append(f"{sku}: metafield {namespace}.{key} not set ({metafield_error})")

        variant_metafields = [
            ("mm-google-shopping", "mpn", google_mpn_value, "single_line_text_field"),
            ("custom", "enable_low_stock_message", low_stock_value, low_stock_type),
        ]
        if variant_id is None:
            summary.warnings.append(f"{sku}: variant metafields not set (missing variant id).")
        else:
            for namespace, key, value, metafield_type in variant_metafields:
                metafield_error = _upsert_variant_metafield(
                    config=config,
                    access_token=access_token,
                    variant_id=variant_id,
                    namespace=namespace,
                    key=key,
                    value=value,
                    metafield_type=metafield_type,
                )
                if metafield_error:
                    summary.warnings.append(f"{sku}: variant metafield {namespace}.{key} not set ({metafield_error})")

        if include_images and local_image_files:
            for file_path in local_image_files:
                image_error = _upload_product_image_from_file(
                    config=config,
                    access_token=access_token,
                    product_id=product_id,
                    path=file_path,
                )
                if image_error:
                    summary.warnings.append(f"{sku}: image {file_path.name} not uploaded ({image_error})")
        elif include_images:
            media_urls = _normalize_media_urls(product.media_urls)
            if not media_urls:
                attempted = ", ".join(local_image_candidates) if local_image_candidates else sku
                summary.warnings.append(
                    f"{sku}: no local images found for candidates [{attempted}] and no media URLs available"
                )

        summary.created_skus.append(sku)
        existing_norm.add(sku)

        # Keep write cadence conservative to reduce API burst errors.
        time.sleep(0.12)

    if progress_callback is not None:
        try:
            progress_callback(len(products), len(products), "")
        except Exception:
            pass
    return summary
