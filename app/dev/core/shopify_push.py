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

_PUBLISHABLE_UNPUBLISH_MUTATION = """
mutation UnpublishProductFromPublications($id: ID!, $input: [PublicationInput!]!) {
  publishableUnpublish(id: $id, input: $input) {
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


def _is_point_of_sale_publication(publication_name: object) -> bool:
    name = _clean_text(publication_name).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", name).strip()
    compact = re.sub(r"[^a-z0-9]+", "", name)
    return normalized in {"point of sale", "pos"} or compact in {"pointofsale", "pos"}


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


def _unpublish_product_from_publications(
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

    for publication_chunk in _chunked(targets, 25):
        input_items = [{"publicationId": publication_id} for publication_id in publication_chunk]
        data, error = _request_graphql_json(
            config=config,
            access_token=access_token,
            query=_PUBLISHABLE_UNPUBLISH_MUTATION,
            variables={
                "id": clean_product_gid,
                "input": input_items,
            },
        )
        if error:
            errors.append(error)
            continue

        payload = (data or {}).get("publishableUnpublish") or {}
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


def _build_product_payload(product: Product, vendor_override: str = "", operator_tag: str = "") -> dict:
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
    tag_values: list[str] = []
    seen_tags: set[str] = set()
    for raw_tag in list(getattr(product, "tags", []) or []):
        tag = _clean_text(raw_tag)
        key = tag.lower()
        if not tag or key in seen_tags:
            continue
        seen_tags.add(key)
        tag_values.append(tag)
    operator_value = _clean_text(operator_tag)
    if operator_value and operator_value.lower() not in seen_tags:
        tag_values.append(operator_value)
    if tag_values:
        payload["product"]["tags"] = ", ".join(tag_values)
    media_urls = _normalize_media_urls(product.media_urls)
    if media_urls:
        payload["product"]["images"] = [{"src": url} for url in media_urls]
    return payload


def _variant_option_value(product: Product) -> str:
    summary = _clean_text(getattr(product, "variant_option_summary", ""))
    if summary:
        first = summary.split("|", 1)[0].strip()
        if ":" in first:
            return _clean_text(first.split(":", 1)[1]) or first
        return first
    title = _clean_text(getattr(product, "title", ""))
    sku = normalize_sku(getattr(product, "sku", ""))
    return title or sku or "Variant"


def _build_variant_payload(product: Product, option_value: str = "") -> dict[str, object]:
    sku = normalize_sku(product.sku)
    variant: dict[str, object] = {
        "sku": sku,
        "barcode": _clean_text(product.barcode),
        "price": _to_decimal_text(product.price),
        "inventory_management": "shopify",
        "inventory_policy": "continue",
    }
    option_text = _clean_text(option_value)
    if option_text:
        variant["option1"] = option_text
    weight_lb = _to_weight_lb(product.weight)
    if weight_lb is not None:
        variant["weight"] = weight_lb
        variant["weight_unit"] = "lb"
    return variant


def _dedupe_option_values(variants: list[Product]) -> list[str]:
    values: list[str] = []
    counts: dict[str, int] = {}
    for variant in variants:
        value = _variant_option_value(variant)
        key = value.strip().lower()
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > 1:
            value = f"{value} {counts[key]}"
        values.append(value)
    return values


def _build_variant_product_payload(
    parent: Product,
    variants: list[Product],
    vendor_override: str = "",
    operator_tag: str = "",
) -> dict:
    title = _clean_text(parent.title) or _clean_text(variants[0].title) or normalize_sku(variants[0].sku)
    vendor_value = _clean_text(vendor_override) or _clean_text(parent.vendor)
    option_values = _dedupe_option_values(variants)
    payload_variants = [
        _build_variant_payload(variant, option_value=option_values[index])
        for index, variant in enumerate(variants)
    ]
    payload: dict[str, object] = {
        "product": {
            "title": title,
            "body_html": _clean_text(parent.description_html),
            "vendor": vendor_value,
            "product_type": _clean_text(parent.type),
            "status": "draft",
            "options": [{"name": "Type", "values": option_values}],
            "variants": payload_variants,
        }
    }
    tag_values: list[str] = []
    seen_tags: set[str] = set()
    for raw_tag in list(getattr(parent, "tags", []) or []):
        tag = _clean_text(raw_tag)
        key = tag.lower()
        if not tag or key in seen_tags:
            continue
        seen_tags.add(key)
        tag_values.append(tag)
    operator_value = _clean_text(operator_tag)
    if operator_value and operator_value.lower() not in seen_tags:
        tag_values.append(operator_value)
    if tag_values:
        payload["product"]["tags"] = ", ".join(tag_values)
    media_urls = _normalize_media_urls(parent.media_urls)
    if media_urls:
        payload["product"]["images"] = [{"src": url} for url in media_urls]
    return payload


def _first_nonempty_product_text(products: list[Product], field_name: str) -> str:
    for product in products:
        value = _clean_text(getattr(product, field_name, ""))
        if value:
            return value
    return ""


def _merge_unique_product_text(products: list[Product], field_name: str, separator: str = " | ") -> str:
    values: list[str] = []
    seen: set[str] = set()
    for product in products:
        raw_value = _clean_text(getattr(product, field_name, ""))
        if not raw_value:
            continue
        for part in re.split(r"[|\n]+", raw_value):
            value = _clean_text(part)
            key = re.sub(r"\s+", " ", value).strip().lower()
            if not value or key in seen:
                continue
            seen.add(key)
            values.append(value)
    return separator.join(values)


def _build_synthetic_parent_from_variants(variants: list[Product]) -> Product:
    parent = Product(
        record_type="Product",
        parent_has_variants=True,
        title=_first_nonempty_product_text(variants, "title"),
        description_html="",
        description_2="",
        vendor=_first_nonempty_product_text(variants, "vendor"),
        type=_first_nonempty_product_text(variants, "type"),
        google_product_type=_first_nonempty_product_text(variants, "google_product_type"),
        category_code=_first_nonempty_product_text(variants, "category_code"),
        product_subtype=_first_nonempty_product_text(variants, "product_subtype"),
        brand=_first_nonempty_product_text(variants, "brand"),
        application=_merge_unique_product_text(variants, "application"),
        collections=_merge_unique_product_text(variants, "collections", separator=", "),
        core_charge_product_code=_first_nonempty_product_text(variants, "core_charge_product_code"),
    )
    tags: list[str] = []
    seen_tags: set[str] = set()
    for variant in variants:
        for raw_tag in list(getattr(variant, "tags", []) or []):
            tag = _clean_text(raw_tag)
            key = tag.lower()
            if not tag or key in seen_tags:
                continue
            seen_tags.add(key)
            tags.append(tag)
    parent.tags = tags

    media_urls: list[str] = []
    seen_media: set[str] = set()
    for variant in variants:
        for raw_url in list(getattr(variant, "media_urls", []) or []):
            media_url = _clean_text(raw_url)
            key = media_url.lower()
            if not media_url or key in seen_media:
                continue
            seen_media.add(key)
            media_urls.append(media_url)
    parent.media_urls = media_urls
    parent.finalize_defaults()
    return parent


def _iter_create_groups(products: list[Product]) -> list[tuple[Product, list[Product]]]:
    groups: list[tuple[Product, list[Product]]] = []
    index = 0
    total = len(products)
    while index < total:
        product = products[index]
        record_type = _clean_text(getattr(product, "record_type", "")).lower()
        if record_type == "product" and bool(getattr(product, "parent_has_variants", False)):
            variants: list[Product] = []
            cursor = index + 1
            while cursor < total:
                candidate = products[cursor]
                candidate_type = _clean_text(getattr(candidate, "record_type", "")).lower()
                if candidate_type == "variant":
                    variants.append(candidate)
                    cursor += 1
                    continue
                break
            if variants:
                groups.append((product, variants))
                index = cursor
                continue
        if record_type == "variant":
            variants = [product]
            cursor = index + 1
            while cursor < total:
                candidate = products[cursor]
                candidate_type = _clean_text(getattr(candidate, "record_type", "")).lower()
                if candidate_type != "variant":
                    break
                variants.append(candidate)
                cursor += 1
            if len(variants) > 1:
                groups.append((_build_synthetic_parent_from_variants(variants), variants))
                index = cursor
                continue
            groups.append((product, variants))
        else:
            groups.append((product, [product]))
        index += 1
    return groups


def _upload_product_image_from_file(
    config: ShopifyConfig,
    access_token: str,
    product_id: int,
    path: Path,
    variant_ids: list[int] | None = None,
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
    if variant_ids:
        payload["image"]["variant_ids"] = [int(item) for item in variant_ids if item]
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
    operator_tag: str = "",
    progress_callback=None,
) -> ShopifyDraftPushSummary:
    summary = ShopifyDraftPushSummary(requested=len(products))
    if not products:
        return summary

    publication_targets, publication_error = _load_all_publications(config=config, access_token=access_token)
    publication_ids = [
        item[0]
        for item in publication_targets
        if _clean_text(item[0]) and not _is_point_of_sale_publication(item[1])
    ]
    point_of_sale_publication_ids = [
        item[0]
        for item in publication_targets
        if _clean_text(item[0]) and _is_point_of_sale_publication(item[1])
    ]
    excluded_publications = [
        item[1]
        for item in publication_targets
        if _clean_text(item[0]) and _is_point_of_sale_publication(item[1])
    ]
    publish_to_all_channels_enabled = bool(publication_ids)
    if publication_error:
        summary.warnings.append(f"Sales channel publication lookup failed ({publication_error}).")
    elif excluded_publications:
        summary.warnings.append("Point of Sale publication excluded from draft publish.")
    elif not publication_targets:
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

    create_groups = _iter_create_groups(products)
    summary.requested = len(create_groups)

    for index, (product, variant_products) in enumerate(create_groups, start=1):
        is_variant_group = len(variant_products) > 1 or bool(getattr(product, "parent_has_variants", False))
        variant_skus = [normalize_sku(item.sku) for item in variant_products if normalize_sku(item.sku)]
        sku = variant_skus[0] if variant_skus else normalize_sku(product.sku)
        if progress_callback is not None:
            try:
                progress_callback(index - 1, len(create_groups), sku)
            except Exception:
                pass

        if not sku and not variant_skus:
            summary.failed_by_sku[f"row_{index}"] = "Missing SKU."
            continue
        existing_variant_skus = [item for item in variant_skus if item in existing_norm]
        if existing_variant_skus:
            summary.skipped_existing_skus.extend(existing_variant_skus)
            if len(existing_variant_skus) == len(variant_skus):
                continue
            summary.failed_by_sku[sku or f"row_{index}"] = (
                "One or more selected variant SKUs already exist in Shopify; grouped product was not created."
            )
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
        variant_local_images: dict[str, list[Path]] = {}
        variant_local_candidates: dict[str, list[str]] = {}
        if include_images:
            if is_variant_group:
                seen_local_paths: set[str] = set()
                for variant_product in variant_products:
                    variant_sku = normalize_sku(variant_product.sku)
                    files, candidates = _collect_local_images_for_sku_candidates(
                        image_root=image_root,
                        sku=variant_sku,
                        sku_prefix_hint=profile_sku_prefix,
                    )
                    variant_local_images[variant_sku] = files
                    variant_local_candidates[variant_sku] = candidates
                    for file_path in files:
                        key = str(file_path.resolve())
                        if key in seen_local_paths:
                            continue
                        seen_local_paths.add(key)
                        local_image_files.append(file_path)
                local_image_candidates = [
                    candidate
                    for candidates in variant_local_candidates.values()
                    for candidate in candidates
                ]
            else:
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
        if is_variant_group:
            create_payload = _build_variant_product_payload(
                product,
                variant_products,
                vendor_override=shopify_vendor_value,
                operator_tag=operator_tag,
            )
        else:
            create_payload = _build_product_payload(
                product,
                vendor_override=shopify_vendor_value,
                operator_tag=operator_tag,
            )
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
        created_variants_by_sku: dict[str, dict] = {}
        for created_variant in variants:
            created_sku = normalize_sku((created_variant or {}).get("sku", ""))
            if created_sku:
                created_variants_by_sku[created_sku] = created_variant or {}

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

        if point_of_sale_publication_ids:
            product_gid = f"gid://shopify/Product/{product_id}"
            unpublish_errors = _unpublish_product_from_publications(
                config=config,
                access_token=access_token,
                product_gid=product_gid,
                publication_ids=point_of_sale_publication_ids,
            )
            if unpublish_errors:
                combined = "; ".join(unpublish_errors)
                summary.warnings.append(f"{sku}: Point of Sale not removed ({combined})")

        profile_brand_gid = _clean_text(vendor_profile.brand_gid) if vendor_profile is not None else ""
        profile_brand_name = _clean_text(vendor_profile.brand_name) if vendor_profile is not None else ""
        brand_value = _clean_text(product.brand) or profile_brand_name or normalized_vendor_value or shopify_vendor_value
        brand_gid = profile_brand_gid or resolve_brand_metaobject_gid(brand_value, required_root=required_root)
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
            ("custom", "prod_description_2", _clean_text(getattr(product, "description_2", "")), "multi_line_text_field"),
            ("custom", "ad_words_spend", _clean_text(getattr(product, "ad_words_spend", "")), "single_line_text_field"),
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

        for variant_product in variant_products:
            variant_sku = normalize_sku(variant_product.sku)
            created_variant = created_variants_by_sku.get(variant_sku)
            if not created_variant and len(variant_products) == 1:
                created_variant = first_variant
            variant_id_raw = (created_variant or {}).get("id")
            inventory_item_id_raw = (created_variant or {}).get("inventory_item_id")
            try:
                variant_id = int(variant_id_raw)
            except Exception:
                variant_id = None
            try:
                inventory_item_id = int(inventory_item_id_raw)
            except Exception:
                inventory_item_id = None

            cost_value = _to_decimal_text(variant_product.cost)
            if inventory_item_id is not None and cost_value:
                cost_error = _set_variant_cost(
                    config=config,
                    access_token=access_token,
                    inventory_item_id=inventory_item_id,
                    cost_value=cost_value,
                )
                if cost_error:
                    summary.warnings.append(f"{variant_sku}: cost not set ({cost_error})")

            if inventory_item_id is not None:
                if location_id is None and location_error is None:
                    location_id, location_error = _resolve_primary_location_id(config=config, access_token=access_token)
                    if location_error:
                        summary.warnings.append(f"{variant_sku}: inventory location not resolved ({location_error})")
                if location_id is not None:
                    inventory_value = _to_int(variant_product.inventory, 3_000_000)
                    inventory_error = _set_inventory_available(
                        config=config,
                        access_token=access_token,
                        location_id=location_id,
                        inventory_item_id=inventory_item_id,
                        available=inventory_value,
                    )
                    if inventory_error:
                        summary.warnings.append(f"{variant_sku}: inventory not set ({inventory_error})")

            sku_no_prefix = _strip_known_sku_prefix(variant_sku, profile_sku_prefix) or variant_sku
            variant_mpn_source = (
                _clean_text(getattr(variant_product, "variant_google_mpn", ""))
                or _clean_text(getattr(variant_product, "mpn", ""))
                or variant_sku
            )
            google_mpn_value = _strip_known_sku_prefix(variant_mpn_source, profile_sku_prefix) or sku_no_prefix
            variant_metafields = [
                ("mm-google-shopping", "mpn", google_mpn_value, "single_line_text_field"),
                ("custom", "enable_low_stock_message", low_stock_value, low_stock_type),
            ]
            if variant_id is None:
                summary.warnings.append(f"{variant_sku}: variant metafields not set (missing variant id).")
                continue
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
                    summary.warnings.append(f"{variant_sku}: variant metafield {namespace}.{key} not set ({metafield_error})")

        if include_images and local_image_files:
            uploaded_paths: set[str] = set()
            if is_variant_group:
                for variant_product in variant_products:
                    variant_sku = normalize_sku(variant_product.sku)
                    created_variant = created_variants_by_sku.get(variant_sku) or {}
                    try:
                        variant_image_id = int(created_variant.get("id"))
                    except Exception:
                        variant_image_id = 0
                    files = variant_local_images.get(variant_sku, [])
                    for file_index, file_path in enumerate(files):
                        path_key = str(file_path.resolve())
                        if path_key in uploaded_paths:
                            continue
                        uploaded_paths.add(path_key)
                        image_error = _upload_product_image_from_file(
                            config=config,
                            access_token=access_token,
                            product_id=product_id,
                            path=file_path,
                            variant_ids=[variant_image_id] if file_index == 0 and variant_image_id else None,
                        )
                        if image_error:
                            summary.warnings.append(f"{variant_sku}: image {file_path.name} not uploaded ({image_error})")
            else:
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

        created_sku_values = variant_skus or [sku]
        for created_sku in created_sku_values:
            if not created_sku:
                continue
            summary.created_skus.append(created_sku)
            existing_norm.add(created_sku)

        # Keep write cadence conservative to reduce API burst errors.
        time.sleep(0.12)

    if progress_callback is not None:
        try:
            progress_callback(len(create_groups), len(create_groups), "")
        except Exception:
            pass
    return summary
