from __future__ import annotations

import concurrent.futures
import csv
import http.cookiejar
import hashlib
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from io import BytesIO
import urllib.error
import urllib.parse
import urllib.request
from html import unescape
from pathlib import Path

from product_prospector.core.processing import normalize_sku
from product_prospector.core.vendor_resolver_registry import VendorResolverProfile, resolve_canonical_search_url


_REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
_REAL_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    if sys.platform == "darwin"
    else _REQUEST_USER_AGENT
)
_REQUEST_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
_REQUEST_ACCEPT_LANGUAGE = "en-US,en;q=0.9"

_HTTP_SESSION_LOCK = threading.Lock()
_HTTP_COOKIE_JARS_BY_HOST: dict[str, http.cookiejar.CookieJar] = {}
_HTTP_OPENERS_BY_HOST: dict[str, urllib.request.OpenerDirector] = {}
_HTTP_HOST_LOCKS_BY_HOST: dict[str, threading.Lock] = {}
_HTTP_HOST_BACKOFF_LOCK = threading.Lock()
_HTTP_HOST_NEXT_ALLOWED_AT: dict[str, float] = {}
_HTTP_HOST_BACKOFF_SECONDS: dict[str, float] = {}
_UNRESOLVED_VENDOR_CACHE_LOCK = threading.Lock()
_UNRESOLVED_VENDOR_CACHE_KEY = ""
_UNRESOLVED_VENDOR_CACHE_ROWS: list[dict[str, str]] = []
_BROWSER_DETAIL_CACHE_LOCK = threading.Lock()
_BROWSER_DETAIL_CACHE: dict[str, dict[str, str]] = {}
_BROWSER_DETAIL_SEMAPHORE = threading.Semaphore(1)


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _discovery_mapping_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[2] / "required" / "mappings" / "discovery" / filename


def _normalize_vendor_match_host(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    candidate = text
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    try:
        host = _clean_text(urllib.parse.urlparse(candidate).netloc).lower()
    except Exception:
        host = text.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _vendor_hosts_match(left: str, right: str) -> bool:
    left_host = _normalize_vendor_match_host(left)
    right_host = _normalize_vendor_match_host(right)
    if not left_host or not right_host:
        return False
    return (
        left_host == right_host
        or left_host.endswith(f".{right_host}")
        or right_host.endswith(f".{left_host}")
    )


def _load_unresolved_vendor_rows() -> list[dict[str, str]]:
    global _UNRESOLVED_VENDOR_CACHE_KEY, _UNRESOLVED_VENDOR_CACHE_ROWS

    path = _discovery_mapping_path("VendorDiscoveryUnresolvedWorklist.csv")
    try:
        stat = path.stat()
        cache_key = f"{path}:{stat.st_mtime_ns}:{stat.st_size}"
    except Exception:
        return []

    with _UNRESOLVED_VENDOR_CACHE_LOCK:
        if cache_key == _UNRESOLVED_VENDOR_CACHE_KEY:
            return list(_UNRESOLVED_VENDOR_CACHE_ROWS)

        rows: list[dict[str, str]] = []
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    website = _clean_text(row.get("official_website_url", ""))
                    if not website:
                        continue
                    rows.append({str(key): _clean_text(value) for key, value in row.items()})
        except Exception:
            rows = []

        _UNRESOLVED_VENDOR_CACHE_KEY = cache_key
        _UNRESOLVED_VENDOR_CACHE_ROWS = rows
        return list(rows)


def _match_unresolved_vendor(vendor_search_url: str) -> dict[str, str] | None:
    search_host = _normalize_vendor_match_host(vendor_search_url)
    if not search_host:
        return None
    for row in _load_unresolved_vendor_rows():
        website = row.get("official_website_url", "")
        if _vendor_hosts_match(search_host, website):
            return row
    return None


def _format_unresolved_vendor_error(row: dict[str, str]) -> str:
    vendor_name = _clean_text(row.get("display_name", "")) or _clean_text(row.get("vendor", "")) or "vendor"
    review_notes = _clean_text(row.get("review_notes", ""))
    blocking_hint = _clean_text(row.get("blocking_hint", ""))
    resolver_hint = _clean_text(row.get("resolver_hint", ""))
    parts = [f"Unresolved vendor search route: {vendor_name}."]
    if review_notes:
        parts.append(review_notes.rstrip(".") + ".")
    elif blocking_hint:
        parts.append(f"Known blocker: {blocking_hint}.")
    if resolver_hint and resolver_hint.lower() not in review_notes.lower():
        parts.append(f"Resolver hint: {resolver_hint}.")
    return " ".join([part for part in parts if part])


def _normalize_url(base_url: str, sku: str) -> str:
    url = _clean_text(base_url)
    if not url:
        return ""
    if "{sku}" in url.lower():
        return re.sub(r"\{sku\}", urllib.parse.quote(sku), url, flags=re.IGNORECASE)
    if url.endswith("/") or url.endswith("=") or url.endswith(":"):
        return f"{url}{urllib.parse.quote(sku)}"
    if "?" in url:
        sep = "" if url.endswith("?") or url.endswith("&") else "&"
        return f"{url}{sep}sku={urllib.parse.quote(sku)}"
    return f"{url.rstrip('/')}/{urllib.parse.quote(sku)}"


def _render_profile_template(template: str, **values: str) -> str:
    rendered = _clean_text(template)
    if not rendered:
        return ""
    for key, value in values.items():
        rendered = re.sub(
            rf"\{{{re.escape(_clean_text(key))}\}}",
            urllib.parse.quote(_clean_text(value), safe=""),
            rendered,
            flags=re.IGNORECASE,
        )
    return rendered


def _extract_nested_value(obj: object, dotted_path: str) -> object:
    current = obj
    for segment in [part for part in _clean_text(dotted_path).split(".") if part]:
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            return None
    return current


def _extract_profile_result_collection(payload: object, collection_name: str) -> list[object]:
    candidate = payload
    path = _clean_text(collection_name)
    if path:
        candidate = _extract_nested_value(payload, path)
    elif isinstance(payload, dict):
        for key in ["products", "items", "results", "hits"]:
            value = payload.get(key)
            if isinstance(value, list):
                candidate = value
                break
    if isinstance(candidate, list):
        return candidate
    return []


def _item_string_value(item: object, field_name: str) -> str:
    if not isinstance(item, dict):
        return ""
    return _clean_text(item.get(field_name, ""))


def _score_api_result_item(item: object, sku: str, profile: VendorResolverProfile) -> int:
    if not isinstance(item, dict):
        return -1

    compact_target = _compact_sku(sku).lower()
    result_sku = _item_string_value(item, profile.api_result_sku_field)
    normalized_result_sku = normalize_sku(result_sku)
    compact_result = _compact_sku(normalized_result_sku).lower()
    score = 0

    if compact_target and compact_result:
        if compact_target == compact_result:
            score += 500
        elif compact_target in compact_result or compact_result in compact_target:
            score += 220

    haystack = json.dumps(item, ensure_ascii=True)
    compact_haystack = re.sub(r"[^a-z0-9]", "", haystack.lower())
    if compact_target and compact_target in compact_haystack:
        score += 60

    for field_name in ["title", "name", "description", "url", "href", "productUrl", "product_url"]:
        value = _item_string_value(item, field_name)
        if value and _contains_compact_sku(value, sku):
            score += 50

    id_value = _item_string_value(item, profile.api_result_id_field)
    if id_value:
        score += 25
    return score


def _candidate_url_from_api_item(item: object, profile: VendorResolverProfile, sku: str) -> str:
    if not isinstance(item, dict):
        return ""

    id_value = _item_string_value(item, profile.api_result_id_field)
    if profile.product_url_template and id_value:
        return _render_profile_template(profile.product_url_template, id=id_value, sku=sku)

    for field_name in ["url", "href", "productUrl", "product_url", "link"]:
        value = _item_string_value(item, field_name)
        if value:
            if value.startswith("http://") or value.startswith("https://"):
                return value
            base_url = _clean_text(profile.search_entry_url) or _clean_text(profile.official_website_url)
            if base_url:
                return urllib.parse.urljoin(base_url, value)
    return ""


def _post_json(
    url: str,
    payload: object,
    *,
    referer: str = "",
    extra_headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> tuple[object | None, str | None]:
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    parsed = urllib.parse.urlparse(url)
    origin = ""
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
    headers = {
        "User-Agent": _REQUEST_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": _REQUEST_ACCEPT_LANGUAGE,
        "Content-Type": "application/json; charset=UTF-8",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if extra_headers:
        for key, value in extra_headers.items():
            if _clean_text(key) and _clean_text(value):
                headers[str(key)] = str(value)

    request = urllib.request.Request(url=url, data=data, method="POST", headers=headers)
    opener = _get_session_opener_for_host(url)
    try:
        if opener is not None:
            with opener.open(request, timeout=timeout) as response:
                body = response.read()
                content_encoding = _clean_text(response.headers.get("Content-Encoding", "")).lower()
        else:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
                content_encoding = _clean_text(response.headers.get("Content-Encoding", "")).lower()
    except urllib.error.HTTPError as exc:
        response_text = ""
        try:
            response_text = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            response_text = ""
        return None, f"HTTP {exc.code}: {response_text[:240] or 'request failed'}"
    except Exception as exc:
        return None, str(exc)

    if content_encoding == "gzip" or body[:2] == b"\x1f\x8b":
        try:
            body = gzip.decompress(body)
        except Exception:
            pass

    text = body.decode("utf-8", errors="ignore")
    try:
        return json.loads(text), None
    except Exception as exc:
        return None, f"Invalid JSON response: {exc}"


def _is_alliant_parts_search_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(_clean_text(url))
    host = _clean_text(parsed.netloc).lower()
    if host.startswith("www."):
        host = host[4:]
    return host == "parts.alliantpower.com" and "/search" in _clean_text(parsed.path).lower()


def _extract_spec_value(specifications: object, *names: str) -> str:
    wanted = {_clean_text(name).lower() for name in names if _clean_text(name)}
    if not isinstance(specifications, list):
        return ""
    for item in specifications:
        if not isinstance(item, dict):
            continue
        item_name = _clean_text(item.get("name", "")).lower()
        item_key = _clean_text(item.get("key", "")).lower()
        if item_name in wanted or item_key in wanted:
            return _clean_text(item.get("value", ""))
    return ""


def _extract_category_type(categories_paths: object) -> str:
    if not isinstance(categories_paths, list):
        return ""
    best = ""
    for path in categories_paths:
        if not isinstance(path, dict):
            continue
        categories = path.get("categories")
        if not isinstance(categories, list):
            continue
        names = [_clean_text(item.get("name", "")) for item in categories if isinstance(item, dict) and _clean_text(item.get("name", ""))]
        if len(names) >= 2:
            return names[-1]
        if names and not best:
            best = names[-1]
    return best


def _normalize_media_from_alliant(product: dict[str, object], page_url: str) -> str:
    media_values: list[str] = []
    image = product.get("image")
    if isinstance(image, dict):
        for key in ["large", "mediumLarge", "medium", "mediumSmall", "small"]:
            value = _clean_text(image.get(key, ""))
            if value:
                media_values.append(value)
    media = product.get("media")
    if isinstance(media, list):
        for item in media:
            if not isinstance(item, dict):
                continue
            for key in ["large", "mediumLarge", "medium", "mediumSmall", "small"]:
                value = _clean_text(item.get(key, ""))
                if value:
                    media_values.append(value)
    normalized = _normalize_media_values(media_values, page_url=page_url)
    return " | ".join(normalized) if normalized else ""


def _score_alliant_search_result(item: object, query_value: str) -> int:
    if not isinstance(item, dict):
        return -1
    score = 0
    title = _clean_text(item.get("title", ""))
    if _compact_sku(title) == _compact_sku(query_value):
        score += 600
    elif _contains_compact_sku(title, query_value):
        score += 280

    for value in item.get("crossReferences", []) if isinstance(item.get("crossReferences"), list) else []:
        cleaned = _clean_text(value)
        if not cleaned:
            continue
        if _compact_sku(cleaned) == _compact_sku(query_value):
            score += 550
            break
        if _contains_compact_sku(cleaned, query_value):
            score += 200

    url = _clean_text(item.get("url", ""))
    if _contains_compact_sku(url, query_value):
        score += 120

    barcode = _extract_spec_value(item.get("specifications"), "Item UPC/EAN Number", "UPC", "EAN", "GTIN", "Barcode")
    if barcode:
        score += 30
    return score


def _payload_from_alliant_product(
    search_product: dict[str, object],
    detail_product: dict[str, object] | None,
    detail_page: dict[str, object] | None,
    price_product: dict[str, object] | None,
    *,
    query_value: str,
    target_url: str,
    product_url: str,
) -> dict[str, str]:
    merged_product: dict[str, object] = {}
    if isinstance(search_product, dict):
        merged_product.update(search_product)
    if isinstance(detail_product, dict):
        merged_product.update({key: value for key, value in detail_product.items() if value is not None})

    output: dict[str, str] = {
        "source_url": product_url or target_url,
        "search_url": target_url,
        "product_url": product_url or target_url,
        "search_provider": "alliant_graph_search",
    }

    header_title = _clean_text(detail_page.get("header", "")) if isinstance(detail_page, dict) else ""
    product_title = _clean_text(merged_product.get("title", ""))
    output["title"] = header_title or product_title

    description_html = _clean_text(merged_product.get("description", ""))
    if description_html:
        output["description_html"] = description_html

    product_type = _extract_category_type(merged_product.get("categoriesPaths"))
    if product_type:
        output["type"] = product_type

    vendor = _extract_spec_value(merged_product.get("specifications"), "Brand") or "Alliant Power"
    if vendor:
        output["vendor"] = vendor

    barcode = _extract_spec_value(
        merged_product.get("specifications"),
        "Item UPC/EAN Number",
        "UPC",
        "EAN",
        "GTIN",
        "Barcode",
    )
    if barcode:
        output["barcode"] = barcode

    media_urls = _normalize_media_from_alliant(merged_product, page_url=product_url or target_url)
    if media_urls:
        output["media_urls"] = media_urls

    if isinstance(price_product, dict):
        price = _clean_text(price_product.get("price", ""))
        if price:
            output["price"] = price

    addon_fields = merged_product.get("addonFields")
    if isinstance(addon_fields, list):
        for field in addon_fields:
            if not isinstance(field, dict):
                continue
            caption = _clean_text(field.get("caption", "")).lower()
            name = _clean_text(field.get("name", "")).lower()
            value = _clean_text(field.get("value", ""))
            if value and ("product description" in caption or "description 2" in name):
                output.setdefault("application", value)
                break

    if query_value and query_value != normalize_sku(output.get("title", "")):
        output["search_term"] = query_value
    return output


def _scrape_single_sku_via_alliant_graph(
    sku: str,
    base_url: str,
    scrape_images: bool,
    search_term: str = "",
) -> tuple[str, dict[str, str], str | None]:
    del scrape_images  # Media URLs come from graph response regardless of image toggle.
    query_value = normalize_sku(search_term) or sku
    target_url = _normalize_url(base_url, query_value)
    if not target_url:
        return sku, {}, "Missing vendor search URL"

    graph_url = urllib.parse.urljoin(target_url, "/api/graph")
    graph_headers = {"x-languageid": "1033"}
    search_payload = {
        "variables": {
            "options": {"page": {"index": 0, "size": 10}, "keywords": query_value},
            "loadCategories": True,
            "searchRedirectTerm": query_value,
            "keywords": query_value,
        },
        "extensions": {
            "persistedQuery": {
                "version": "1",
                "sha256Hash": "66c8a55c9f243bff306ad2e2de35e5e482323d659c3046d09fe2d9ef80d08938",
            }
        },
    }
    search_response, search_error = _post_json(
        graph_url,
        search_payload,
        referer=target_url,
        extra_headers=graph_headers,
    )
    if search_error:
        return sku, {}, f"Alliant graph search failed: {search_error}"

    search_products = _extract_nested_value(search_response, "data.catalog.products.products")
    if not isinstance(search_products, list) or not search_products:
        return sku, {}, "Alliant graph search returned no products"

    ordered_products = sorted(search_products, key=lambda item: _score_alliant_search_result(item, query_value), reverse=True)
    best_product = ordered_products[0] if ordered_products else None
    if not isinstance(best_product, dict) or _score_alliant_search_result(best_product, query_value) < 150:
        return sku, {}, "Alliant graph search returned no confident product match"

    product_id = _clean_text(best_product.get("id", ""))
    product_url = _clean_text(best_product.get("url", ""))
    if product_url:
        product_url = urllib.parse.urljoin(target_url, product_url)

    detail_page = None
    detail_product = None
    if product_id:
        detail_payload = {
            "variables": {
                "productId": product_id.lower(),
                "specificationFilter": "FOR_DETAILS",
                "loadRelatedProductsCategories": True,
                "loadUom": True,
            },
            "extensions": {
                "persistedQuery": {
                    "version": "1",
                    "sha256Hash": "77153c302fff53be51c1f0d1e2f3580f4f0236fbfdcdd9aef0ad7f34474e37c9",
                }
            },
        }
        detail_referer = product_url or target_url
        detail_response, detail_error = _post_json(
            graph_url,
            detail_payload,
            referer=detail_referer,
            extra_headers=graph_headers,
        )
        if not detail_error:
            detail_page = _extract_nested_value(detail_response, "data.pages.product")
            detail_product = _extract_nested_value(detail_response, "data.pages.product.product")

    price_product = None
    if product_id:
        price_payload = {
            "variables": {
                "options": {"ids": [product_id], "uomId": None, "page": {"size": 1, "index": 0}}
            },
            "extensions": {
                "persistedQuery": {
                    "version": "1",
                    "sha256Hash": "5b553447d968e5f0ed5b59260f318dcf596a8fc2d1e604f2f5d829256f9f3eb6",
                }
            },
        }
        price_response, price_error = _post_json(
            graph_url,
            price_payload,
            referer=product_url or target_url,
            extra_headers=graph_headers,
        )
        if not price_error:
            products = _extract_nested_value(price_response, "data.catalog.products.products")
            if isinstance(products, list) and products:
                first = products[0]
                if isinstance(first, dict):
                    price_product = first

    merged = _payload_from_alliant_product(
        best_product,
        detail_product if isinstance(detail_product, dict) else None,
        detail_page if isinstance(detail_page, dict) else None,
        price_product if isinstance(price_product, dict) else None,
        query_value=query_value,
        target_url=target_url,
        product_url=product_url,
    )
    return sku, merged, None


def _extract_selector_href_hint(selector: str) -> tuple[str, str]:
    text = _clean_text(selector)
    if not text:
        return "", ""
    contains_match = re.search(r"""href\*=['"]([^'"]+)['"]""", text, flags=re.IGNORECASE)
    if contains_match:
        return "contains", _clean_text(contains_match.group(1))
    prefix_match = re.search(r"""href\^=['"]([^'"]+)['"]""", text, flags=re.IGNORECASE)
    if prefix_match:
        return "prefix", _clean_text(prefix_match.group(1))
    return "", ""


def _extract_profile_result_candidates(
    html: str,
    page_url: str,
    sku: str,
    profile: VendorResolverProfile,
) -> list[tuple[str, int]]:
    match_mode = _clean_text(profile.result_match_mode).lower()
    resolver_hint = _clean_text(profile.resolver_hint).lower()
    selector_mode, selector_value = _extract_selector_href_hint(profile.result_link_selector)

    result_html = html
    if not selector_value and "results_page_clickthrough" in resolver_hint:
        low_html = html.lower()
        for marker in ['id="itemsblock"', 'id="searchresults"', "returned the following results", 'class="searchpage"']:
            index = low_html.find(marker)
            if index >= 0:
                result_html = html[index:]
                break

    scored: dict[str, int] = {}
    anchor_candidates = _extract_anchor_link_candidates(result_html, page_url=page_url)
    for position, (href, anchor_text) in enumerate(anchor_candidates, start=1):
        if not _same_host_family(href, page_url=page_url):
            continue
        path = urllib.parse.urlparse(href).path.lower()
        if any(
            token in path
            for token in ["/search", "/cart", "/account", "/checkout", "/blog", "/blogs", "add_cart.asp", "view_cart.asp", "myaccount.asp", "crm.asp"]
        ):
            continue

        score = 0
        if selector_value:
            href_lower = href.lower()
            selector_lower = selector_value.lower()
            if selector_mode == "contains":
                if selector_lower not in href_lower:
                    continue
                score += 130
            elif selector_mode == "prefix":
                if not path.startswith(selector_lower.lower()):
                    continue
                score += 130

        if _contains_compact_sku(f"{href} {anchor_text}", sku):
            score += 190
        if "/i-" in path:
            score += 85
        if "/details" in path or "details?id=" in href.lower():
            score += 85
        if re.search(r"(?i)_p_\d+\.html$", path):
            score += 95
        if "first_result" in match_mode:
            score += max(0, 30 - min(position, 25))
        if score == 0 and "results_page_clickthrough" in resolver_hint:
            score += max(0, 18 - min(position, 18))

        if score <= 0:
            continue
        existing = scored.get(href, -1)
        if score > existing:
            scored[href] = score

    return sorted(scored.items(), key=lambda item: (-item[1], len(item[0])))


def _scrape_single_sku_via_json_api_id_detail(
    sku: str,
    profile: VendorResolverProfile,
    scrape_images: bool,
    search_term: str = "",
) -> tuple[str, dict[str, str], str | None]:
    query_value = normalize_sku(search_term) or sku
    api_template = _clean_text(profile.api_request_url_template) or _clean_text(profile.search_url_template)
    api_url = _render_profile_template(api_template, sku=query_value)
    if not api_url:
        return sku, {}, "Missing API search template"

    body, error = _fetch_html(api_url)
    if error:
        return sku, {}, f"{api_url} ({error})"

    try:
        payload = json.loads(_clean_text(body))
    except Exception as exc:
        return sku, {}, f"{api_url} (Invalid JSON response: {exc})"

    items = _extract_profile_result_collection(payload, profile.api_response_collection)
    if not items:
        return sku, {}, "JSON API search returned no results"

    ordered_items = sorted(items, key=lambda item: _score_api_result_item(item, query_value, profile), reverse=True)
    best_item = ordered_items[0] if ordered_items else None
    if best_item is None or _score_api_result_item(best_item, query_value, profile) < 80:
        return sku, {}, "JSON API search returned no confident SKU match"

    product_url = _candidate_url_from_api_item(best_item, profile, query_value)
    if not product_url:
        return sku, {}, "Could not derive product URL from JSON API result"

    product_html, product_error = _fetch_html(product_url)
    if product_error:
        return sku, {}, f"{product_url} ({product_error})"

    merged = _extract_page_payload(product_html, product_url, query_value, scrape_images=scrape_images)
    merged["search_url"] = api_url
    merged["product_url"] = product_url
    merged["source_url"] = product_url
    merged["search_provider"] = "resolver_json_api"
    if query_value and query_value != sku:
        merged["search_term"] = query_value
    return sku, merged, None


def _looks_like_bot_challenge(html: str) -> bool:
    text = _clean_text(html).lower()
    if not text:
        return False
    hard_signals = [
        "<title>just a moment",
        "<title>verifying your connection",
        "cf-browser-verification",
        "id=\"challenge-running\"",
        "id=\"challenge-error-text\"",
    ]
    if any(signal in text for signal in hard_signals):
        return True

    soft_signals = [
        "__cf_chl",
        "challenge-platform",
        "/cdn-cgi/challenge-platform",
    ]
    if not any(signal in text for signal in soft_signals):
        return False

    product_signals = [
        "application/ld+json",
        "\"@type\": \"product\"",
        "\"@type\":\"product\"",
        "add to cart",
        "specifications for",
        "product-main",
    ]
    return not any(signal in text for signal in product_signals)


def _fetch_html_with_curl(url: str, timeout: int = 30) -> tuple[str, str | None]:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return "", "curl unavailable"

    marker = "__CURL_HTTP_CODE__:"
    command = [
        curl_bin,
        "--http1.1",
        "--location",
        "--silent",
        "--show-error",
        "--compressed",
        "--max-time",
        str(max(5, int(timeout))),
        "-A",
        _REQUEST_USER_AGENT,
        "-H",
        f"Accept: {_REQUEST_ACCEPT}",
        "-H",
        f"Accept-Language: {_REQUEST_ACCEPT_LANGUAGE}",
        url,
        "-w",
        f"\n{marker}%{{http_code}}",
    ]

    run_kwargs: dict[str, object] = {
        "capture_output": True,
        "text": True,
        "encoding": "utf-8",
        "errors": "ignore",
        "timeout": max(10, int(timeout) + 5),
        "check": False,
    }
    if sys.platform == "win32":
        try:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            run_kwargs["startupinfo"] = startupinfo
            run_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000))
        except Exception:
            pass

    try:
        completed = subprocess.run(command, **run_kwargs)
    except Exception as exc:
        return "", str(exc)

    output_text = completed.stdout or ""
    if marker in output_text:
        body_text, _, status_text = output_text.rpartition(marker)
        try:
            status_code = int(status_text.strip() or "0")
        except Exception:
            status_code = 0
    else:
        body_text = output_text
        status_code = 0

    if completed.returncode != 0:
        error_text = _clean_text(completed.stderr)
        if status_code >= 400:
            return "", f"HTTP {status_code}"
        return "", error_text or f"curl exit {completed.returncode}"

    if status_code >= 400:
        return "", f"HTTP {status_code}"

    if not _clean_text(body_text):
        return "", "empty response body"

    return body_text, None


def _host_key_from_url(url: str) -> str:
    try:
        return _clean_text(urllib.parse.urlparse(url).netloc).lower()
    except Exception:
        return ""


def _sleep_for_host_backoff(url: str) -> None:
    host = _host_key_from_url(url)
    if not host:
        return
    while True:
        now = time.monotonic()
        with _HTTP_HOST_BACKOFF_LOCK:
            next_allowed = float(_HTTP_HOST_NEXT_ALLOWED_AT.get(host, 0.0))
        wait_seconds = next_allowed - now
        if wait_seconds <= 0:
            return
        time.sleep(min(wait_seconds, 1.25))


def _register_host_fetch_result(url: str, error_text: str | None) -> None:
    host = _host_key_from_url(url)
    if not host:
        return
    error = _clean_text(error_text).lower()
    now = time.monotonic()
    with _HTTP_HOST_BACKOFF_LOCK:
        current_backoff = float(_HTTP_HOST_BACKOFF_SECONDS.get(host, 0.0))
        if ("http 429" in error) or ("rate limit" in error) or ("too many requests" in error):
            if current_backoff <= 0:
                current_backoff = 0.75
            else:
                current_backoff = min(8.0, (current_backoff * 1.8) + 0.2)
            _HTTP_HOST_BACKOFF_SECONDS[host] = current_backoff
            _HTTP_HOST_NEXT_ALLOWED_AT[host] = max(
                float(_HTTP_HOST_NEXT_ALLOWED_AT.get(host, 0.0)),
                now + current_backoff,
            )
            return
        if ("bot challenge" in error) or ("http 403" in error):
            if current_backoff <= 0:
                current_backoff = 0.9
            else:
                current_backoff = min(7.0, (current_backoff * 1.5) + 0.15)
            _HTTP_HOST_BACKOFF_SECONDS[host] = current_backoff
            _HTTP_HOST_NEXT_ALLOWED_AT[host] = max(
                float(_HTTP_HOST_NEXT_ALLOWED_AT.get(host, 0.0)),
                now + current_backoff,
            )
            return
        if not error:
            if current_backoff > 0:
                reduced = max(0.0, current_backoff * 0.65)
                _HTTP_HOST_BACKOFF_SECONDS[host] = reduced
                if reduced <= 0.05:
                    _HTTP_HOST_NEXT_ALLOWED_AT.pop(host, None)
                else:
                    _HTTP_HOST_NEXT_ALLOWED_AT[host] = now + min(0.35, reduced * 0.25)


def _get_session_opener_for_host(url: str) -> urllib.request.OpenerDirector | None:
    host = _host_key_from_url(url)
    if not host:
        return None
    with _HTTP_SESSION_LOCK:
        opener = _HTTP_OPENERS_BY_HOST.get(host)
        if opener is not None:
            return opener
        jar = _HTTP_COOKIE_JARS_BY_HOST.get(host)
        if jar is None:
            jar = http.cookiejar.CookieJar()
            _HTTP_COOKIE_JARS_BY_HOST[host] = jar
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        _HTTP_OPENERS_BY_HOST[host] = opener
        return opener


def _get_host_lock_for_url(url: str) -> threading.Lock:
    host = _host_key_from_url(url) or "__default__"
    with _HTTP_SESSION_LOCK:
        lock = _HTTP_HOST_LOCKS_BY_HOST.get(host)
        if lock is None:
            lock = threading.Lock()
            _HTTP_HOST_LOCKS_BY_HOST[host] = lock
        return lock


def _fetch_html_with_cookie_session(url: str, timeout: int = 30) -> tuple[str, str | None]:
    opener = _get_session_opener_for_host(url)
    if opener is None:
        return "", "cookie session unavailable"
    parsed = urllib.parse.urlparse(url)
    host_root = ""
    if parsed.scheme and parsed.netloc:
        host_root = f"{parsed.scheme}://{parsed.netloc}/"

    host_lock = _get_host_lock_for_url(url)
    with host_lock:
        for attempt in range(2):
            request = urllib.request.Request(
                url=url,
                method="GET",
                headers={
                    "User-Agent": _REQUEST_USER_AGENT,
                    "Accept": _REQUEST_ACCEPT,
                    "Accept-Language": _REQUEST_ACCEPT_LANGUAGE,
                    "Referer": host_root or url,
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
            )
            try:
                with opener.open(request, timeout=timeout) as response:
                    body = response.read()
                    content_encoding = _clean_text(response.headers.get("Content-Encoding", "")).lower()
            except urllib.error.HTTPError as exc:
                response_text = ""
                try:
                    response_text = exc.read().decode("utf-8", errors="ignore")
                except Exception:
                    response_text = ""
                if exc.code == 429 and attempt == 0:
                    time.sleep(0.35)
                    continue
                if exc.code in {403, 429} and _clean_text(response_text):
                    # Keep cookies from challenge response and allow caller fallback.
                    return "", f"HTTP {exc.code}"
                return "", f"HTTP {exc.code}"
            except Exception as exc:
                return "", str(exc)

            if content_encoding == "gzip" or body[:2] == b"\x1f\x8b":
                try:
                    body = gzip.decompress(body)
                except Exception:
                    pass
            text = body.decode("utf-8", errors="ignore")
            if _looks_like_bot_challenge(text) and attempt == 0:
                time.sleep(0.35)
                continue
            if _looks_like_bot_challenge(text):
                return "", "Bot challenge page detected"
            return text, None

    return "", "empty response body"


def _fetch_html(url: str, timeout: int = 30) -> tuple[str, str | None]:
    _sleep_for_host_backoff(url)

    def _success(value: str) -> tuple[str, str | None]:
        _register_host_fetch_result(url, None)
        return value, None

    def _failure(error_text: str) -> tuple[str, str | None]:
        _register_host_fetch_result(url, error_text)
        return "", error_text

    # Prefer shared cookie-session fetches first; this reduces anti-bot false
    # positives on many vendor sites compared to stateless requests.
    session_text, session_error = _fetch_html_with_cookie_session(url, timeout=timeout)
    if not session_error and session_text and not _looks_like_bot_challenge(session_text):
        return _success(session_text)

    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={
            "User-Agent": _REQUEST_USER_AGENT,
            "Accept": _REQUEST_ACCEPT,
            "Accept-Language": _REQUEST_ACCEPT_LANGUAGE,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        response_text = ""
        try:
            response_text = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            response_text = ""
        if exc.code in {403, 429} or _looks_like_bot_challenge(response_text):
            session_text, session_error = _fetch_html_with_cookie_session(url, timeout=timeout)
            if not session_error and session_text and not _looks_like_bot_challenge(session_text):
                return _success(session_text)
            curl_text, curl_error = _fetch_html_with_curl(url, timeout=timeout)
            if not curl_error:
                return _success(curl_text)
            if exc.code == 429:
                # Final bounded cool-down retry for anti-bot burst windows.
                time.sleep(0.95)
                session_text, session_error = _fetch_html_with_cookie_session(url, timeout=timeout)
                if not session_error and session_text and not _looks_like_bot_challenge(session_text):
                    return _success(session_text)
        return _failure(f"HTTP {exc.code}")
    except Exception as exc:
        return _failure(str(exc))

    text = body.decode("utf-8", errors="ignore")
    if _looks_like_bot_challenge(text):
        session_text, session_error = _fetch_html_with_cookie_session(url, timeout=timeout)
        if not session_error and session_text and not _looks_like_bot_challenge(session_text):
            return _success(session_text)
        curl_text, curl_error = _fetch_html_with_curl(url, timeout=timeout)
        if not curl_error and not _looks_like_bot_challenge(curl_text):
            return _success(curl_text)
        time.sleep(0.75)
        session_text, session_error = _fetch_html_with_cookie_session(url, timeout=timeout)
        if not session_error and session_text and not _looks_like_bot_challenge(session_text):
            return _success(session_text)
        return _failure("Bot challenge page detected")
    return _success(text)


def _iter_nested_dicts(value: object) -> list[dict]:
    items: list[dict] = []
    if isinstance(value, dict):
        items.append(value)
        for child in value.values():
            items.extend(_iter_nested_dicts(child))
    elif isinstance(value, list):
        for child in value:
            items.extend(_iter_nested_dicts(child))
    return items


def _host_matches(url: str, host_value: str) -> bool:
    parsed_host = _normalize_host(urllib.parse.urlparse(_clean_text(url)).netloc)
    target_host = _normalize_host(host_value)
    if not parsed_host or not target_host:
        return False
    return (
        parsed_host == target_host
        or parsed_host.endswith(f".{target_host}")
        or target_host.endswith(f".{parsed_host}")
    )


def _is_xtreme_diesel_url(url: str) -> bool:
    return _host_matches(url, "xtremediesel.com")


def _real_chrome_executable_path() -> str:
    candidates: list[str] = []
    if sys.platform == "darwin":
        candidates.extend(
            [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                str(Path.home() / "Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            ]
        )
    elif sys.platform == "win32":
        local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
        program_files = Path(os.environ.get("ProgramFiles", "C:/Program Files"))
        program_files_x86 = Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)"))
        candidates.extend(
            [
                str(local_app_data / "Google/Chrome/Application/chrome.exe"),
                str(program_files / "Google/Chrome/Application/chrome.exe"),
                str(program_files_x86 / "Google/Chrome/Application/chrome.exe"),
                str(program_files / "Chromium/Application/chrome.exe"),
            ]
        )
    else:
        candidates.extend(
            [
                "/usr/bin/google-chrome",
                "/usr/bin/google-chrome-stable",
                "/usr/bin/chromium",
                "/usr/bin/chromium-browser",
            ]
        )

    for binary in [
        "google-chrome",
        "google-chrome-stable",
        "chrome",
        "chromium",
        "chromium-browser",
        "msedge",
        "microsoft-edge",
    ]:
        resolved = shutil.which(binary)
        if resolved:
            candidates.append(resolved)

    seen: set[str] = set()
    for candidate in candidates:
        cleaned = _clean_text(candidate)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        if Path(cleaned).exists():
            return cleaned
    return ""


def _score_browser_json_item(item: dict, sku: str, page_url: str = "") -> int:
    target = _compact_sku(sku)
    if not target:
        return 0

    score = 0
    exact_code = False
    partial_code = False
    code_values: list[str] = []
    for key in ["sku", "ss_sku", "mpn", "part_number", "partNumber", "product_code", "productCode", "id"]:
        code_values.extend(_iter_searchspring_field_values(item.get(key)))
    for value in code_values:
        compact = _compact_sku(value)
        if not compact:
            continue
        if compact == target:
            exact_code = True
            break
        if compact.endswith(target) or target.endswith(compact) or target in compact:
            partial_code = True

    if exact_code:
        score += 680
    elif partial_code:
        score += 240

    title_values = []
    for key in ["title", "name", "product_name", "productName", "short_desc", "shortDescription"]:
        title_values.extend(_iter_searchspring_field_values(item.get(key)))
    if any(_contains_compact_sku(value, sku) for value in title_values):
        score += 180

    link_values = []
    for key in ["url", "link", "product_url", "productUrl"]:
        link_values.extend(_iter_searchspring_field_values(item.get(key)))
    if any(_contains_compact_sku(value, sku) for value in link_values):
        score += 140
    if page_url and any(_clean_text(value) == _clean_text(page_url) for value in link_values):
        score += 90

    for field_name in ["gtin14", "gtin13", "gtin12", "gtin8", "gtin", "upc", "ean", "barcode"]:
        if any(
            _clean_text(value).lower() not in {"", "none", "nan", "null"}
            for value in _iter_searchspring_field_values(item.get(field_name))
        ):
            score += 200
            break

    if any(_iter_searchspring_field_values(item.get(key)) for key in ["price", "salePrice", "listPrice"]):
        score += 40

    return score


def _build_browser_json_payload(item: dict, page_url: str) -> dict[str, str]:
    payload: dict[str, str] = {}

    for key in ["title", "name", "product_name", "productName"]:
        value = _clean_text(unescape(next(iter(_iter_searchspring_field_values(item.get(key))), "")))
        if value:
            payload["title"] = value
            break

    for key in ["description_html", "description", "short_desc", "shortDescription", "body_html", "body"]:
        value = _clean_text(unescape(next(iter(_iter_searchspring_field_values(item.get(key))), "")))
        if value:
            payload["description_html"] = value
            break

    vendor = _clean_text(item.get("vendor", ""))
    if not vendor:
        brand = item.get("brand")
        if isinstance(brand, dict):
            vendor = _clean_text(brand.get("name", ""))
        else:
            vendor = _clean_text(next(iter(_iter_searchspring_field_values(brand)), ""))
    if not vendor:
        vendor = _clean_text(next(iter(_iter_searchspring_field_values(item.get("manufacturer_name"))), ""))
    if vendor:
        payload["vendor"] = vendor

    for key in ["type", "product_type", "category"]:
        value = _clean_text(next(iter(_iter_searchspring_field_values(item.get(key))), ""))
        if value:
            payload["type"] = value
            break

    for key in ["price", "salePrice", "listPrice", "msrp"]:
        value = _clean_text(next(iter(_iter_searchspring_field_values(item.get(key))), ""))
        if value:
            payload["price"] = value
            break

    for field_name in ["gtin14", "gtin13", "gtin12", "gtin8", "gtin", "upc", "ean", "barcode"]:
        value = _clean_text(next(iter(_iter_searchspring_field_values(item.get(field_name))), ""))
        if value and value.lower() not in {"none", "nan", "null"}:
            payload["barcode"] = value
            break

    application_values = []
    for key in ["application", "fitment", "vehicle_fitment"]:
        application_values.extend(_iter_searchspring_field_values(item.get(key)))
    if application_values:
        normalized_application = _format_searchspring_application(application_values)
        if normalized_application:
            payload["application"] = normalized_application

    media_values: list[str] = []
    for key in ["image", "imageUrl", "image_url", "product_image", "thumbnailImageUrl", "images", "media"]:
        media_values.extend(_iter_searchspring_field_values(item.get(key)))
    if media_values:
        normalized_media = _normalize_media_values(media_values, page_url=page_url)
        if normalized_media:
            payload["media_urls"] = " | ".join(normalized_media)

    return payload


def _extract_payload_from_browser_json_responses(
    response_bodies: list[tuple[str, str]],
    page_url: str,
    sku: str,
) -> dict[str, str]:
    best_payload: dict[str, str] = {}
    best_score = -1
    for response_url, body_text in response_bodies:
        parsed = _parse_json_with_fallbacks(body_text)
        if parsed is None:
            continue
        for item in _iter_nested_dicts(parsed):
            score = _score_browser_json_item(item, sku=sku, page_url=page_url)
            if score <= 0:
                continue
            candidate_payload = _build_browser_json_payload(item, page_url=response_url or page_url)
            if not candidate_payload:
                continue
            if score > best_score:
                best_score = score
                best_payload = candidate_payload
    return best_payload


def _fetch_html_with_real_chrome(
    url: str,
    timeout_ms: int = 35000,
    settle_ms: int = 4500,
) -> tuple[str, str, list[tuple[str, str]], str | None]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return url, "", [], f"Playwright unavailable: {exc}"

    chrome_path = _real_chrome_executable_path()
    if not chrome_path:
        return url, "", [], "Google Chrome executable not found"

    page_host = _normalize_host(urllib.parse.urlparse(url).netloc)
    last_error = "Browser detail fetch failed"

    with _BROWSER_DETAIL_SEMAPHORE:
        for attempt in range(2):
            _sleep_for_host_backoff(url)
            response_refs: list[object] = []
            try:
                with sync_playwright() as play:
                    browser = play.chromium.launch(
                        executable_path=chrome_path,
                        headless=True,
                    )
                    context = browser.new_context(user_agent=_REAL_BROWSER_USER_AGENT)
                    page = context.new_page()

                    def on_response(response: object) -> None:
                        try:
                            response_url = _clean_text(getattr(response, "url", ""))
                            response_host = _normalize_host(urllib.parse.urlparse(response_url).netloc)
                            if page_host and response_host and response_host != page_host:
                                return
                            status = int(getattr(response, "status", 0) or 0)
                            if status < 200 or status >= 400:
                                return
                            headers = getattr(response, "headers", {}) or {}
                            content_type = _clean_text(headers.get("content-type", "")).lower()
                            lower_url = response_url.lower()
                            if "application/json" not in content_type and not any(
                                token in lower_url for token in ["/api/", "graphql", "json.mvc", ".json"]
                            ):
                                return
                            response_refs.append(response)
                        except Exception:
                            return

                    page.on("response", on_response)
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    page.wait_for_timeout(settle_ms + (attempt * 1500))
                    final_url = _clean_text(page.url) or url
                    html = page.content()
                    title = _clean_text(page.title()).lower()
                    network_bodies: list[tuple[str, str]] = []
                    for response in response_refs[:20]:
                        try:
                            body_text = _clean_text(response.text())
                        except Exception:
                            continue
                        if not body_text or len(body_text) > 1_200_000:
                            continue
                        network_bodies.append((_clean_text(getattr(response, "url", "")), body_text))

                    if _looks_like_bot_challenge(html) or title in {"just a moment...", "verifying your connection..."}:
                        last_error = "Bot challenge page detected"
                        _register_host_fetch_result(url, last_error)
                        time.sleep(1.1 + (attempt * 0.9))
                        continue

                    _register_host_fetch_result(url, None)
                    return final_url, html, network_bodies, None
            except Exception as exc:
                last_error = str(exc)
                _register_host_fetch_result(url, last_error)
                if attempt == 0:
                    time.sleep(1.0)
                    continue
    return url, "", [], last_error


def _should_attempt_xtreme_browser_detail(
    product_url: str,
    payload: dict[str, str],
    requested_fields: set[str] | list[str] | tuple[str, ...] | None,
) -> bool:
    if not _is_xtreme_diesel_url(product_url):
        return False
    title = _clean_text(payload.get("title", "")).lower()
    if title in {"just a moment...", "verifying your connection..."}:
        return True

    requested = _normalize_requested_scrape_fields(requested_fields)
    if requested is None:
        fields_to_check = {"barcode"}
    else:
        fields_to_check = set(requested) & {"barcode", "weight", "application", "description_html", "price", "title", "vendor", "type"}
        if not fields_to_check:
            return False

    for field_name in fields_to_check:
        value = _clean_text(payload.get(field_name, ""))
        if not value or value.lower() in {"none", "nan", "null"}:
            return True
    return False


def _fetch_xtreme_detail_payload_via_browser(
    product_url: str,
    sku: str,
    scrape_images: bool,
) -> tuple[dict[str, str], str | None]:
    cache_key = _clean_text(product_url)
    with _BROWSER_DETAIL_CACHE_LOCK:
        cached = _BROWSER_DETAIL_CACHE.get(cache_key)
        if cached:
            return dict(cached), None

    final_url, html, network_bodies, error = _fetch_html_with_real_chrome(product_url)
    if error:
        return {}, error

    network_payload = _extract_payload_from_browser_json_responses(
        response_bodies=network_bodies,
        page_url=final_url,
        sku=sku,
    )
    dom_payload = _extract_page_payload(html, final_url, sku, scrape_images=scrape_images)
    merged = _merge_seed_payload(network_payload, dom_payload, page_url=final_url)
    provider = ""
    if network_payload and dom_payload:
        provider = "chrome_playwright_network_json_plus_dom"
    elif network_payload:
        provider = "chrome_playwright_network_json"
    elif dom_payload:
        provider = "chrome_playwright_dom"
    if provider:
        merged["detail_fetch_provider"] = provider
    if final_url and final_url != product_url:
        merged["product_url"] = final_url
        merged["source_url"] = final_url

    if merged:
        with _BROWSER_DETAIL_CACHE_LOCK:
            _BROWSER_DETAIL_CACHE[cache_key] = dict(merged)
    return merged, None


def _is_rate_limit_error(error_text: str) -> bool:
    text = _clean_text(error_text).lower()
    if not text:
        return False
    return ("http 429" in text) or ("too many requests" in text) or ("rate limit" in text)


def _extract_first(pattern: str, text: str, flags: int = 0) -> str:
    match = re.search(pattern, text, flags=flags)
    if not match:
        return ""
    if match.lastindex:
        return _clean_text(unescape(match.group(1)))
    return _clean_text(unescape(match.group(0)))


def _extract_meta_content(html: str, name: str) -> str:
    pattern = rf'<meta[^>]+(?:name|property)\s*=\s*["\']{re.escape(name)}["\'][^>]+content\s*=\s*["\']([^"\']+)["\']'
    return _extract_first(pattern, html, flags=re.IGNORECASE)


def _extract_canonical_page_url(html: str, page_url: str = "") -> str:
    canonical = _extract_first(
        r'<link[^>]+rel\s*=\s*["\']canonical["\'][^>]+href\s*=\s*["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if not canonical:
        canonical = _extract_first(
            r'<link[^>]+href\s*=\s*["\']([^"\']+)["\'][^>]+rel\s*=\s*["\']canonical["\']',
            html,
            flags=re.IGNORECASE,
        )
    if not canonical:
        canonical = _extract_meta_content(html, "og:url")
    canonical = _clean_text(canonical)
    if not canonical:
        return ""
    if canonical.startswith("//"):
        canonical = f"https:{canonical}"
    if page_url:
        canonical = urllib.parse.urljoin(page_url, canonical)
    return _clean_text(canonical)


def _extract_json_scripts(html: str) -> list[str]:
    scripts: list[str] = []
    for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, flags=re.IGNORECASE | re.DOTALL):
        body = _clean_text(match.group(1))
        if not body or len(body) < 2:
            continue
        scripts.append(body)
    return scripts


def _collect_product_nodes(obj: object, nodes: list[dict]) -> None:
    if isinstance(obj, dict):
        node_type = _clean_text(obj.get("@type", ""))
        if "product" in node_type.lower():
            nodes.append(obj)
        for value in obj.values():
            _collect_product_nodes(value, nodes)
        return
    if isinstance(obj, list):
        for item in obj:
            _collect_product_nodes(item, nodes)


def _from_json_ld(html: str, page_url: str, sku: str) -> dict[str, str]:
    output: dict[str, str] = {}
    media_values: list[str] = []
    scripts = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for raw in scripts:
        raw_text = _clean_text(raw)
        if not raw_text:
            continue
        try:
            parsed = json.loads(raw_text)
        except Exception:
            continue
        nodes: list[dict] = []
        _collect_product_nodes(parsed, nodes)
        for node in nodes:
            node_sku = normalize_sku(node.get("sku", "") or node.get("mpn", ""))
            if node_sku and node_sku != normalize_sku(sku):
                continue

            if not output.get("title"):
                output["title"] = _clean_text(node.get("name", ""))
            if not output.get("description_html"):
                output["description_html"] = _clean_text(node.get("description", ""))
            brand = node.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name", "")
            if not output.get("vendor"):
                output["vendor"] = _clean_text(brand)
            if not output.get("barcode"):
                for field_name in ["gtin14", "gtin13", "gtin12", "gtin8", "gtin", "upc", "ean", "barcode"]:
                    barcode = _clean_text(node.get(field_name, ""))
                    if barcode and barcode.lower() not in {"none", "nan", "null"}:
                        output["barcode"] = barcode
                        break
            offers = node.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict) and not output.get("price"):
                output["price"] = _clean_text(offers.get("price", ""))
            image = node.get("image")
            if isinstance(image, list):
                media_values.extend([_clean_text(item) for item in image if _clean_text(item)])
            else:
                image_text = _clean_text(image)
                if image_text:
                    media_values.append(image_text)

    if media_values:
        output["media_urls"] = " | ".join(_normalize_media_values(media_values, page_url))
    return output


def _find_context_near_sku(html: str, sku: str, span: int = 1000) -> str:
    pattern = re.compile(re.escape(sku), flags=re.IGNORECASE)
    match = pattern.search(html)
    if not match:
        return html[:span]
    start = max(0, match.start() - span)
    end = min(len(html), match.end() + span)
    return html[start:end]


def _normalize_media_candidate(value: str, page_url: str) -> str:
    text = _clean_text(unescape(value))
    if not text:
        return ""
    text = re.split(r'["\'<>\s]+', text, maxsplit=1)[0]
    text = text.rstrip("),;")
    text = text.replace("\\/", "/").replace("&amp;", "&")
    text = text.replace("\\u0026", "&").replace("\\u003D", "=")
    if text.startswith("//"):
        text = f"https:{text}"
    elif text.startswith("/"):
        text = urllib.parse.urljoin(page_url, text)
    elif text.lower().startswith("www."):
        text = f"https://{text}"
    if not re.match(r"^https?://", text, flags=re.IGNORECASE):
        return ""
    if not _is_probable_image_url(text):
        return ""
    return text


def _is_probable_image_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False

    path = _clean_text(parsed.path).lower()
    if not path:
        return False

    if re.search(r"\.(?:jpe?g|png|webp|gif|bmp|avif)$", path, flags=re.IGNORECASE):
        return True

    query = urllib.parse.parse_qs(parsed.query)
    format_value = "".join(query.get("format", [])).lower()
    if format_value in {"jpg", "jpeg", "png", "webp", "gif", "bmp", "avif"}:
        return True

    return False


def _strip_shopify_size_suffix(path: str) -> str:
    return re.sub(
        r"_(?:pico|icon|thumb|small|compact|medium|large|grande|master|original|[0-9]{2,4}x[0-9]{2,4})(?=\.[A-Za-z0-9]{2,6}$)",
        "",
        path,
        flags=re.IGNORECASE,
    )


def _upgrade_image_url(url: str) -> str:
    normalized = _normalize_media_candidate(url, page_url=url)
    if not normalized:
        return ""
    parsed = urllib.parse.urlparse(normalized)
    path = _strip_shopify_size_suffix(parsed.path)

    keep_pairs: list[tuple[str, str]] = []
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        lower = key.lower()
        if lower in {"w", "width"}:
            try:
                width = int(float(value))
            except Exception:
                width = 0
            if width and width < 1600:
                keep_pairs.append((key, "2048"))
                continue
            keep_pairs.append((key, value))
            continue
        if lower in {"h", "height", "crop", "fit", "dpr"}:
            continue
        keep_pairs.append((key, value))

    query = urllib.parse.urlencode(keep_pairs, doseq=True)
    upgraded = parsed._replace(path=path, query=query)
    return urllib.parse.urlunparse(upgraded)


def _image_quality_score(url: str) -> int:
    value = _clean_text(url).lower()
    if not value:
        return -999
    score = 0

    path = urllib.parse.urlparse(value).path
    if re.search(r"\.(?:jpg|jpeg|png|webp|gif|bmp)(?:$|\?)", path):
        score += 90
    elif path.endswith(".svg"):
        score -= 250

    dim_match = re.search(r"(?:_|-)(\d{2,4})x(\d{2,4})(?=\.[a-z0-9]{2,6}$)", path)
    if dim_match:
        try:
            width = int(dim_match.group(1))
            height = int(dim_match.group(2))
            score += min(width * height, 4_000_000) // 10_000
        except Exception:
            pass

    query_pairs = urllib.parse.parse_qs(urllib.parse.urlparse(value).query)
    width_text = (query_pairs.get("w") or query_pairs.get("width") or [""])[0]
    height_text = (query_pairs.get("h") or query_pairs.get("height") or [""])[0]
    try:
        width = int(float(width_text)) if width_text else 0
    except Exception:
        width = 0
    try:
        height = int(float(height_text)) if height_text else 0
    except Exception:
        height = 0
    if width:
        score += min(width, 5000) // 4
    if height:
        score += min(height, 5000) // 4

    if re.search(r"(?i)(?:^|[/._-])(thumbnail|thumb|small|tiny|icon|sprite|placeholder)(?:$|[/._-])", value):
        score -= 220
    if re.search(r"(?i)(?:^|[/._-])(zoom|master|original|full|large|xlarge|2048x2048)(?:$|[/._-])", value):
        score += 70
    if "cdn.shopify.com/" in value:
        score += 180
    if "sanity.io/" in value:
        score -= 90
    return score


def _image_canonical_key(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    path = _strip_shopify_size_suffix(parsed.path)
    # Normalize common ecommerce image transform tokens so width/quality variants
    # of the same underlying asset collapse to one canonical key.
    path = re.sub(r"(?i)(?:[_-](?:w|width)\d{2,5})", "", path)
    path = re.sub(r"(?i)(?:[_-](?:h|height)\d{2,5})", "", path)
    path = re.sub(r"(?i)(?:[_-]q\d{1,3})", "", path)
    path = re.sub(r"(?i)(?:[_-]t\d{6,15})", "", path)
    path = re.sub(r"(?i)(?:[_-]dpr\d(?:\.\d+)?)", "", path)
    path = re.sub(r"(?i)(?:[_-]\d{2,4}x\d{2,4})(?=\.[a-z0-9]{2,6}$)", "", path)
    path = re.sub(r"\.(?:jpe?g|png|webp|gif|bmp|avif)$", "", path, flags=re.IGNORECASE)
    asset_match = re.search(r"([0-9]{2,}-[0-9]{2,}_[a-z]{1,6}\d{0,3})", path.lower())
    if asset_match:
        path = f"/_asset/{asset_match.group(1)}"
    keep_pairs: list[tuple[str, str]] = []
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        lower = key.lower()
        if lower in {"w", "width", "h", "height", "crop", "fit", "dpr", "q", "quality", "auto", "format"}:
            continue
        keep_pairs.append((key, value))
    query = urllib.parse.urlencode(keep_pairs, doseq=True)
    canonical = parsed._replace(path=path, query=query, fragment="")
    return urllib.parse.urlunparse(canonical)


def _media_url_matches_target_sku(url: str, sku: str) -> bool:
    target = _compact_sku(sku).lower()
    if not target:
        return False
    compact_url = re.sub(r"[^a-z0-9]+", "", _clean_text(url).lower())
    if not compact_url:
        return False
    if target in compact_url:
        return True
    # Allow A/B suffix variants to match base-image filenames.
    if len(target) > 5 and target[-1].isalpha() and target[:-1] in compact_url:
        return True
    return False


def _normalize_media_values(values: list[str], page_url: str) -> list[str]:
    best_by_key: dict[str, tuple[int, str]] = {}
    first_index_by_key: dict[str, int] = {}

    for index, value in enumerate(values):
        normalized = _normalize_media_candidate(value, page_url=page_url)
        if not normalized:
            continue
        variants = [normalized]
        upgraded = _upgrade_image_url(normalized)
        if upgraded and upgraded != normalized:
            variants.append(upgraded)

        for variant in variants:
            key = _image_canonical_key(variant)
            quality = _image_quality_score(variant)
            if key not in best_by_key or quality > best_by_key[key][0]:
                best_by_key[key] = (quality, variant)
            if key not in first_index_by_key:
                first_index_by_key[key] = index

    ordered_keys = sorted(
        first_index_by_key.keys(),
        key=lambda key: (-best_by_key[key][0], first_index_by_key[key]),
    )
    return [best_by_key[key][1] for key in ordered_keys]


def _extract_srcset_urls(value: str) -> list[str]:
    parsed: list[tuple[int, str]] = []
    for part in value.split(","):
        cleaned = _clean_text(part)
        if not cleaned:
            continue
        tokens = cleaned.split()
        token = tokens[0]
        if token:
            weight = 0
            if len(tokens) > 1:
                descriptor = tokens[-1].strip().lower()
                if descriptor.endswith("w"):
                    try:
                        weight = int(float(descriptor[:-1]))
                    except Exception:
                        weight = 0
                elif descriptor.endswith("x"):
                    try:
                        weight = int(float(descriptor[:-1]) * 1000)
                    except Exception:
                        weight = 0
            parsed.append((weight, token))
    parsed.sort(key=lambda item: item[0], reverse=True)
    return [url for _, url in parsed]


def _decode_script_text(text: str) -> str:
    value = _clean_text(unescape(text))
    if not value:
        return ""
    value = value.replace('\\"', '"').replace("\\'", "'")
    value = value.replace("\\/", "/")
    value = value.replace("\\u002F", "/").replace("\\u003A", ":")
    value = value.replace("\\u0026", "&").replace("\\u003D", "=")
    value = value.replace("\\x2F", "/").replace("\\x3A", ":")
    return value


def _compact_sku(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", normalize_sku(value))


def _contains_compact_sku(text: str, sku: str) -> bool:
    compact_sku = _compact_sku(sku).lower()
    if not compact_sku:
        return False
    compact_text = re.sub(r"[^a-z0-9]", "", _clean_text(text).lower())
    if not compact_text:
        return False
    return compact_sku in compact_text


def _json_loads_safe(value: str) -> object | None:
    try:
        return json.loads(value)
    except Exception:
        return None


def _parse_json_with_fallbacks(raw_text: str) -> object | None:
    base = _clean_text(raw_text)
    if not base:
        return None

    attempts: list[str] = []
    for candidate in [
        base,
        _clean_text(unescape(base)),
        _decode_script_text(base),
        _decode_script_text(unescape(base)),
    ]:
        if candidate and candidate not in attempts:
            attempts.append(candidate)

    for candidate in attempts:
        parsed = _json_loads_safe(candidate)
        if parsed is not None:
            return parsed
    return None


def _iter_shopify_product_candidates_from_object(value: object) -> list[dict]:
    candidates: list[dict] = []
    if isinstance(value, dict):
        if isinstance(value.get("product"), dict):
            candidates.extend(_iter_shopify_product_candidates_from_object(value.get("product")))
        variants = value.get("variants")
        has_identity = bool(_clean_text(value.get("id", "")) or _clean_text(value.get("handle", "")) or _clean_text(value.get("title", "")))
        if isinstance(variants, list) and has_identity:
            candidates.append(value)
        for child in value.values():
            if isinstance(child, (dict, list)):
                candidates.extend(_iter_shopify_product_candidates_from_object(child))
    elif isinstance(value, list):
        for item in value:
            candidates.extend(_iter_shopify_product_candidates_from_object(item))
    return candidates


def _extract_embedded_shopify_products(html: str) -> list[dict]:
    products: list[dict] = []
    seen: set[str] = set()

    def add_product(product: dict) -> None:
        key = _clean_text(product.get("id", "")) or _clean_text(product.get("handle", "")).lower()
        if not key:
            key = json.dumps(product, sort_keys=True)[:1200]
        if key in seen:
            return
        seen.add(key)
        products.append(product)

    attr_patterns = [
        r'product-page-product\s*=\s*"([^"]+)"',
        r"product-page-product\s*=\s*'([^']+)'",
    ]
    for pattern in attr_patterns:
        for match in re.finditer(pattern, html, flags=re.IGNORECASE | re.DOTALL):
            raw = _clean_text(match.group(1))
            if not raw:
                continue
            parsed = _parse_json_with_fallbacks(raw)
            if parsed is None:
                continue
            for product in _iter_shopify_product_candidates_from_object(parsed):
                add_product(product)

    for match in re.finditer(
        r"<script[^>]+type=[\"']application/json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = _clean_text(match.group(1))
        if not raw:
            continue
        parsed = _parse_json_with_fallbacks(raw)
        if parsed is None:
            continue
        for product in _iter_shopify_product_candidates_from_object(parsed):
            add_product(product)

    return products


def _shopify_match_score_for_sku(product: dict, sku: str) -> int:
    target = _compact_sku(sku)
    if not target:
        return 1

    best_score = 0
    variants = product.get("variants")
    if isinstance(variants, list):
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            for key in ["sku", "barcode", "mpn"]:
                compact = _compact_sku(variant.get(key, ""))
                if not compact:
                    continue
                if compact == target:
                    best_score = max(best_score, 1000)
    if best_score:
        return best_score

    title = _clean_text(product.get("title", ""))
    if _contains_compact_sku(title, sku):
        return 250
    return 0


def _extract_shopify_media_from_product(product: dict) -> list[str]:
    media_values: list[str] = []
    seen: set[str] = set()

    def add(value: object) -> None:
        text = _clean_text(value)
        if not text or text in seen:
            return
        seen.add(text)
        media_values.append(text)

    images = product.get("images")
    if isinstance(images, list):
        for image in images:
            add(image)

    add(product.get("featured_image", ""))
    add(product.get("image", ""))

    media = product.get("media")
    if isinstance(media, list):
        for item in media:
            if not isinstance(item, dict):
                continue
            media_type = _clean_text(item.get("media_type", "")).lower()
            if media_type and "image" not in media_type:
                continue
            add(item.get("src", ""))
            preview_image = item.get("preview_image")
            if isinstance(preview_image, dict):
                add(preview_image.get("src", ""))

    return media_values


def _format_shopify_price(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        amount = float(value)
        if float(amount).is_integer():
            amount = amount / 100.0
        return f"{amount:.2f}"

    text = _clean_text(value)
    if not text:
        return ""
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return ""
    try:
        if re.fullmatch(r"-?\d+", cleaned):
            amount = float(int(cleaned)) / 100.0
        else:
            amount = float(cleaned)
    except Exception:
        return ""
    return f"{amount:.2f}"


def _select_embedded_shopify_product(html: str, page_url: str, sku: str) -> dict | None:
    candidates = _extract_embedded_shopify_products(html)
    if not candidates:
        return None

    page_path = urllib.parse.urlparse(page_url).path.lower()
    best: dict | None = None
    best_rank = -1
    for product in candidates:
        rank = _shopify_match_score_for_sku(product, sku) if _clean_text(sku) else 1
        if _clean_text(sku) and rank <= 0:
            continue

        handle = _clean_text(product.get("handle", "")).lower().strip("/")
        if handle and f"/products/{handle}" in page_path:
            rank += 120
        if _clean_text(product.get("description", "")) or _clean_text(product.get("content", "")):
            rank += 20
        if _extract_shopify_media_from_product(product):
            rank += 20
        if rank > best_rank:
            best = product
            best_rank = rank
    return best


def _from_shopify_embedded_product(html: str, page_url: str, sku: str) -> dict[str, str]:
    product = _select_embedded_shopify_product(html=html, page_url=page_url, sku=sku)
    if product is None:
        return {}

    payload: dict[str, str] = {}
    title = _clean_text(product.get("title", ""))
    if title:
        payload["title"] = title

    description = _clean_text(product.get("description", "") or product.get("content", ""))
    if description:
        payload["description_html"] = description

    vendor = _clean_text(product.get("vendor", ""))
    if vendor:
        payload["vendor"] = vendor

    product_type = _clean_text(product.get("type", ""))
    if product_type:
        payload["type"] = product_type

    price_value = (
        _format_shopify_price(product.get("price"))
        or _format_shopify_price(product.get("price_min"))
        or _format_shopify_price(product.get("compare_at_price"))
    )
    if price_value:
        payload["price"] = price_value

    variants = product.get("variants")
    if isinstance(variants, list):
        ranked_variants: list[tuple[int, dict]] = []
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            score = _shopify_match_score_for_sku({"variants": [variant]}, sku)
            ranked_variants.append((score, variant))
        ranked_variants.sort(key=lambda entry: entry[0], reverse=True)
        for _, variant in ranked_variants:
            barcode = _clean_text(variant.get("barcode", ""))
            if barcode:
                payload["barcode"] = barcode
                break

    media_values = _extract_shopify_media_from_product(product)
    if media_values:
        normalized_media = _normalize_media_values(media_values, page_url=page_url)
        if normalized_media:
            payload["media_urls"] = " | ".join(normalized_media)

    return payload


def _normalize_candidate_link(value: str, page_url: str) -> str:
    raw = _clean_text(unescape(value))
    if not raw:
        return ""
    raw = raw.replace("\\/", "/").strip("\"' ")
    raw = raw.rstrip("\\")
    if raw.startswith("#"):
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/"):
        raw = urllib.parse.urljoin(page_url, raw)
    elif not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        raw = urllib.parse.urljoin(page_url, raw)

    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    if parsed.path and re.search(
        r"\.(?:css|js|map|ico|svg|jpg|jpeg|png|gif|webp|bmp|woff|woff2|ttf|eot|pdf|zip)(?:$|\?)",
        parsed.path,
        flags=re.IGNORECASE,
    ):
        return ""
    cleaned = parsed._replace(fragment="")
    return urllib.parse.urlunparse(cleaned)


def _normalize_host(value: str) -> str:
    host = _clean_text(value).lower()
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


_KNOWN_SEARCHSPRING_SITE_IDS_BY_HOST: dict[str, tuple[str, ...]] = {
    "xtremediesel.com": ("k72wrs",),
}


def _searchspring_site_ids_for_vendor_url(page_url: str) -> list[str]:
    host = _normalize_host(urllib.parse.urlparse(_clean_text(page_url)).netloc)
    if not host:
        return []
    site_ids: list[str] = []
    seen: set[str] = set()
    for known_host, known_site_ids in _KNOWN_SEARCHSPRING_SITE_IDS_BY_HOST.items():
        normalized_known_host = _normalize_host(known_host)
        if not normalized_known_host:
            continue
        if (
            host != normalized_known_host
            and not host.endswith(f".{normalized_known_host}")
            and not normalized_known_host.endswith(f".{host}")
        ):
            continue
        for site_id in known_site_ids:
            cleaned_site_id = _clean_text(site_id).lower()
            if not cleaned_site_id or cleaned_site_id in seen:
                continue
            seen.add(cleaned_site_id)
            site_ids.append(cleaned_site_id)
    return site_ids


def _same_host_family(candidate_url: str, page_url: str) -> bool:
    candidate_host = _normalize_host(urllib.parse.urlparse(candidate_url).netloc)
    page_host = _normalize_host(urllib.parse.urlparse(page_url).netloc)
    if not candidate_host or not page_host:
        return False
    if candidate_host == page_host:
        return True
    return candidate_host.endswith(f".{page_host}") or page_host.endswith(f".{candidate_host}")


def _extract_anchor_link_candidates(html: str, page_url: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for match in re.finditer(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        href = _normalize_candidate_link(match.group(1), page_url=page_url)
        if not href:
            continue
        anchor_text = re.sub(r"<[^>]+>", " ", match.group(2) or "")
        anchor_text = _clean_text(unescape(anchor_text))
        candidates.append((href, anchor_text))
    return candidates


def _extract_script_link_candidates(html: str, page_url: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    path_parts = [part for part in urllib.parse.urlparse(page_url).path.split("/") if part]
    locale_prefix = ""
    if path_parts and re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", path_parts[0], flags=re.IGNORECASE):
        locale_prefix = f"/{path_parts[0].lower()}"

    def add(value: str) -> None:
        normalized = _normalize_candidate_link(value, page_url=page_url)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    for raw_script in _extract_json_scripts(html):
        script = _decode_script_text(raw_script)
        if not script:
            continue

        for handle_match in re.finditer(
            r'gid://shopify/Product/\d+","[^"]{1,700}?","([a-z0-9][a-z0-9-]{2,})","vendor"',
            script,
            flags=re.IGNORECASE,
        ):
            handle = _clean_text(handle_match.group(1)).lower()
            if not handle:
                continue
            add(f"/products/{handle}")
            if locale_prefix:
                add(f"{locale_prefix}/products/{handle}")

        for key_match in re.finditer(
            r'(?i)"(?P<key>url|href|path|productUrl|product_url|handle)"\s*:\s*"(?P<value>[^"]+)"',
            script,
        ):
            key = _clean_text(key_match.group("key")).lower()
            value = _clean_text(key_match.group("value"))
            if not value:
                continue
            if key == "handle" and "/" not in value:
                add(f"/products/{value}")
                add(f"/us-en/products/{value}")
                continue
            add(value)

        for url_match in re.finditer(r'(?i)(?:https?://|//)[^"\'\s<>]+', script):
            add(url_match.group(0))

        for path_match in re.finditer(r'(?i)"(/[^"\n\r]{4,200})"', script):
            add(path_match.group(1))

    return candidates


def _extract_shopify_state_product_candidates(html: str, page_url: str, sku: str) -> list[tuple[str, int]]:
    scored: dict[str, int] = {}
    path_parts = [part for part in urllib.parse.urlparse(page_url).path.split("/") if part]
    locale_prefix = ""
    if path_parts and re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", path_parts[0], flags=re.IGNORECASE):
        locale_prefix = f"/{path_parts[0].lower()}"

    def add(handle: str, score: int) -> None:
        if not handle:
            return
        handle_clean = _clean_text(handle).strip("/").lower()
        if not handle_clean:
            return
        for candidate in [f"/products/{handle_clean}", f"{locale_prefix}/products/{handle_clean}" if locale_prefix else ""]:
            if not candidate:
                continue
            href = _normalize_candidate_link(candidate, page_url=page_url)
            if not href:
                continue
            if href not in scored or score > scored[href]:
                scored[href] = score

    pattern = re.compile(
        r'"__typename","Product","gid://shopify/Product/\d+","([^"]{1,900})","([a-z0-9][a-z0-9-]{2,})","vendor"',
        flags=re.IGNORECASE,
    )
    for raw_script in _extract_json_scripts(html):
        script = _decode_script_text(raw_script)
        if not script or "gid://shopify/Product/" not in script:
            continue
        for match in pattern.finditer(script):
            product_title = _clean_text(match.group(1))
            product_handle = _clean_text(match.group(2)).lower()
            score = 85
            if _contains_compact_sku(product_title, sku):
                score += 180
            if _contains_compact_sku(product_handle, sku):
                score += 220
            add(product_handle, score)

    ordered = sorted(scored.items(), key=lambda item: (-item[1], len(item[0])))
    return ordered


def _score_product_candidate(url: str, sku: str, context_text: str = "") -> int:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()
    query = parsed.query.lower()
    full = url.lower()
    compact_url = re.sub(r"[^a-z0-9]", "", full)
    compact_sku = _compact_sku(sku).lower()
    score = 0

    if compact_sku and compact_sku in compact_url:
        score += 180

    if re.search(r"(?i)/(?:product|products|part|parts|item|items)/", path):
        score += 55
    elif re.search(r"(?i)/(?:product|products|part|parts)\b", path):
        score += 35
    if re.search(r"(?i)_p_\d+\.html$", path):
        score += 95
    if re.search(r"(?i)_c_\d+\.html$", path):
        score -= 55

    if any(token in path for token in ["/search", "/collections", "/blog", "/blogs", "/news", "/account", "/cart", "/checkout"]):
        score -= 220
    if "product-sections" in path:
        score -= 180

    if "sku=" in query or "part=" in query or "mpn=" in query:
        score += 20
    if "q=" in query:
        score -= 120

    if path.rstrip("/") in {"/product", "/products", "/part", "/parts", "/item", "/items"}:
        score -= 70

    context = _clean_text(context_text).lower()
    if context:
        compact_context = re.sub(r"[^a-z0-9]", "", context)
        if compact_sku and compact_sku in compact_context:
            score += 35
        if any(token in context for token in ["view product", "learn more", "shop now", "details"]):
            score += 10

    if parsed.path.endswith("/") and len(parsed.path.strip("/")) <= 3:
        score -= 20
    return score


def _extract_product_page_candidates(html: str, page_url: str, sku: str) -> list[tuple[str, int]]:
    scored: dict[str, int] = {}

    for href, score in _extract_shopify_state_product_candidates(html=html, page_url=page_url, sku=sku):
        if href not in scored or score > scored[href]:
            scored[href] = score

    for href, anchor_text in _extract_anchor_link_candidates(html, page_url=page_url):
        if not _same_host_family(href, page_url=page_url):
            continue
        score = _score_product_candidate(href, sku=sku, context_text=anchor_text)
        if score <= 30:
            continue
        if href not in scored or score > scored[href]:
            scored[href] = score

    for href in _extract_script_link_candidates(html, page_url=page_url):
        if not _same_host_family(href, page_url=page_url):
            continue
        score = _score_product_candidate(href, sku=sku, context_text="")
        if score <= 30:
            continue
        if href not in scored or score > scored[href]:
            scored[href] = score

    ordered = sorted(scored.items(), key=lambda item: (-item[1], len(item[0])))
    return ordered


def _extract_searchspring_site_ids(html: str) -> list[str]:
    text = _decode_script_text(_clean_text(html))
    if not text:
        return []
    site_ids: list[str] = []
    seen: set[str] = set()
    patterns = [
        r"https?://([a-z0-9]{4,12})\.a\.searchspring\.io/api/search/search\.json",
        r"https?://([a-z0-9]{4,12})\.a\.searchspring\.io/api/(?:meta|search|suggest)/",
        r"[?&](?:siteId|pubId)=([a-z0-9]{4,12})\b",
        r'"(?:siteId|pubId)"\s*:\s*"([a-z0-9]{4,12})"',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            site_id = _clean_text(match.group(1)).lower()
            if not site_id or site_id in seen:
                continue
            seen.add(site_id)
            site_ids.append(site_id)
    return site_ids


def _iter_searchspring_field_values(value: object) -> list[str]:
    values: list[str] = []
    if isinstance(value, list):
        for item in value:
            values.extend(_iter_searchspring_field_values(item))
        return values
    if isinstance(value, tuple):
        for item in value:
            values.extend(_iter_searchspring_field_values(item))
        return values
    if isinstance(value, set):
        for item in value:
            values.extend(_iter_searchspring_field_values(item))
        return values
    text = _clean_text(value)
    if text:
        values.append(text)
    return values


def _collect_searchspring_item_codes(item: dict) -> list[str]:
    values: list[str] = []
    for key in ["sku", "ss_sku", "product_code", "part_number", "partnumber", "mpn"]:
        values.extend(_iter_searchspring_field_values(item.get(key)))

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = _compact_sku(value)
        if not compact or compact in seen:
            continue
        seen.add(compact)
        deduped.append(value)
    return deduped


def _format_searchspring_application(value: object) -> str:
    entries: list[str] = []
    seen: set[str] = set()
    for raw in _iter_searchspring_field_values(value):
        text = _clean_text(unescape(raw))
        if not text:
            continue
        text = text.replace(">", " > ")
        text = re.sub(r"\s+", " ", text).strip()
        if not text or " > " not in text or text.lower().endswith(" > all"):
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        entries.append(text)
    return " | ".join(entries[:12])


def _build_searchspring_seed_payload(item: dict, page_url: str) -> dict[str, str]:
    payload: dict[str, str] = {}

    title = _clean_text(unescape(item.get("name", "") or item.get("ss_product_type_name", "")))
    if title:
        payload["title"] = title

    description = _clean_text(unescape(item.get("short_desc", "")))
    if description:
        payload["description_html"] = description

    vendor = _clean_text(unescape(item.get("brand", "")))
    if not vendor:
        manufacturer_values = _iter_searchspring_field_values(item.get("manufacturer_name"))
        if manufacturer_values:
            vendor = _clean_text(unescape(manufacturer_values[0]))
    if vendor:
        payload["vendor"] = vendor

    product_types = _iter_searchspring_field_values(item.get("product_type"))
    if product_types:
        product_type = _clean_text(unescape(product_types[0]))
        if product_type:
            payload["type"] = product_type

    price = _clean_text(item.get("price", "")) or _clean_text(item.get("dealer_level_1", ""))
    if price:
        payload["price"] = price

    application = _format_searchspring_application(item.get("raw_parts_finder_data"))
    if not application:
        application = _format_searchspring_application(item.get("parts_finder_data"))
    if application:
        payload["application"] = application

    media_values: list[str] = []
    for key in ["thumbnailImageUrl", "imageUrl", "product_image"]:
        media_values.extend(_iter_searchspring_field_values(item.get(key)))
    if media_values:
        normalized = _normalize_media_values(media_values, page_url=page_url)
        if normalized:
            payload["media_urls"] = " | ".join(normalized)

    return payload


def _score_searchspring_item(item: dict, sku: str) -> int:
    target = _compact_sku(sku)
    if not target:
        return 0

    score = 240
    exact_code = False
    partial_code = False
    for value in _collect_searchspring_item_codes(item):
        compact = _compact_sku(value)
        if not compact:
            continue
        if compact == target:
            exact_code = True
            break
        if compact.endswith(target) or target.endswith(compact) or target in compact:
            partial_code = True

    if exact_code:
        score += 820
    elif partial_code:
        score += 320

    title = _clean_text(item.get("name", "") or item.get("ss_product_type_name", ""))
    if _contains_compact_sku(title, sku):
        score += 220

    link = _clean_text(item.get("product_url", "") or item.get("url", "") or item.get("link", ""))
    if _contains_compact_sku(link, sku):
        score += 200

    description = _clean_text(item.get("short_desc", ""))
    if _contains_compact_sku(description, sku):
        score += 80

    return score


def _fetch_searchspring_items(
    site_id: str,
    sku: str,
    max_results: int,
) -> tuple[list[dict], str | None, str]:
    query_modes: list[tuple[str, dict[str, str]]] = [
        ("sku", {"bgfilter.sku": sku}),
        ("ss_sku", {"bgfilter.ss_sku": sku}),
        ("mpn", {"bgfilter.mpn": sku}),
        ("q", {"q": sku}),
    ]
    errors: list[str] = []
    for query_mode, extra_params in query_modes:
        params: dict[str, str | int] = {
            "siteId": site_id,
            "resultsFormat": "native",
            "resultsPerPage": int(max_results),
        }
        params.update(extra_params)
        query_url = (
            f"https://{urllib.parse.quote(site_id)}.a.searchspring.io/api/search/search.json?"
            f"{urllib.parse.urlencode(params, doseq=True)}"
        )
        body, error = _fetch_html(query_url, timeout=30)
        if error:
            errors.append(f"{query_url} ({error})")
            continue
        if not body:
            continue
        try:
            payload = json.loads(body)
        except Exception as exc:
            errors.append(f"{query_url} (invalid json: {exc})")
            continue
        items = payload.get("results")
        if not isinstance(items, list):
            continue
        typed_items = [item for item in items if isinstance(item, dict)]
        if typed_items:
            return typed_items, None, query_mode
    if errors:
        return [], " | ".join(errors[:3]), "q"
    return [], None, "q"


def _searchspring_candidates_from_site_ids(
    site_ids: list[str],
    page_url: str,
    sku: str,
) -> tuple[list[tuple[str, int, dict[str, str]]], list[str]]:
    if not site_ids:
        return [], []

    errors: list[str] = []
    scored: dict[str, tuple[int, dict[str, str]]] = {}
    for site_id in site_ids:
        items, item_error, query_mode = _fetch_searchspring_items(
            site_id=site_id,
            sku=sku,
            max_results=40,
        )
        if item_error:
            errors.append(item_error)
            continue
        for item in items:
            link = _clean_text(item.get("product_url", "") or item.get("url", "") or item.get("link", ""))
            candidate_url = _normalize_candidate_link(link, page_url=page_url)
            if not candidate_url:
                continue
            score = _score_searchspring_item(item, sku)
            if query_mode != "q":
                score = max(score, 1120)
            seed_payload = _build_searchspring_seed_payload(item, page_url=candidate_url)
            seed_payload["search_provider"] = "searchspring" if query_mode == "q" else f"searchspring_{query_mode}"
            existing = scored.get(candidate_url)
            if existing is None or score > existing[0]:
                scored[candidate_url] = (score, seed_payload)

    if not scored:
        return [], errors

    ordered = sorted(
        [(url, score, payload) for url, (score, payload) in scored.items()],
        key=lambda entry: (-entry[1], len(entry[0])),
    )
    return ordered, errors


def _searchspring_candidates_from_search_page(
    search_html: str,
    page_url: str,
    sku: str,
) -> tuple[list[tuple[str, int, dict[str, str]]], list[str]]:
    site_ids = _extract_searchspring_site_ids(search_html)
    return _searchspring_candidates_from_site_ids(site_ids=site_ids, page_url=page_url, sku=sku)


def _extract_searchanise_api_keys(html: str) -> list[str]:
    text = _clean_text(html)
    if not text:
        return []
    normalized = text.replace("\\/", "/")
    keys: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(
        r"searchanise[^\"'\s>]+/widgets/shopify/init\.js\?a=([A-Za-z0-9]+)",
        normalized,
        flags=re.IGNORECASE,
    ):
        key = _clean_text(match.group(1))
        if not key or key in seen:
            continue
        seen.add(key)
        keys.append(key)
    return keys


def _collect_searchanise_variant_skus(item: dict) -> list[str]:
    values: list[str] = []
    direct = item.get("variant_skus")
    if isinstance(direct, str):
        values.extend(_split_multi_value(direct.replace("[:ATTR:]", "|")))
    elif isinstance(direct, list):
        values.extend([_clean_text(value) for value in direct if _clean_text(value)])

    variants = item.get("shopify_variants")
    if isinstance(variants, list):
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            sku_value = _clean_text(variant.get("sku", ""))
            if sku_value:
                values.append(sku_value)

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = _compact_sku(value)
        if not compact or compact in seen:
            continue
        seen.add(compact)
        deduped.append(value)
    return deduped


def _collect_searchanise_item_codes(item: dict) -> list[str]:
    values: list[str] = []
    product_code = _clean_text(item.get("product_code", ""))
    if product_code:
        values.append(product_code)
    values.extend(_collect_searchanise_variant_skus(item))
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = _compact_sku(value)
        if not compact or compact in seen:
            continue
        seen.add(compact)
        deduped.append(value)
    return deduped


def _levenshtein_distance_limited(a: str, b: str, max_distance: int = 2) -> int:
    if a == b:
        return 0
    if not a or not b:
        return max(len(a), len(b))
    if abs(len(a) - len(b)) > max_distance:
        return max_distance + 1

    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i]
        row_min = current[0]
        for j, cb in enumerate(b, start=1):
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + (0 if ca == cb else 1)
            value = min(insert_cost, delete_cost, replace_cost)
            current.append(value)
            if value < row_min:
                row_min = value
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def _numeric_delta_or_large(a: str, b: str) -> int:
    if a.isdigit() and b.isdigit() and len(a) == len(b):
        try:
            return abs(int(a) - int(b))
        except Exception:
            return 999999999
    return 999999999


def _build_searchanise_seed_payload(item: dict, page_url: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    title = _clean_text(item.get("title", ""))
    if title:
        payload["title"] = title
    description = _clean_text(item.get("description", ""))
    if description:
        payload["description_html"] = description
    price = _clean_text(item.get("price", ""))
    if price:
        payload["price"] = price
    vendor = _clean_text(item.get("vendor", ""))
    if vendor:
        payload["vendor"] = vendor

    media_values: list[str] = []
    images = item.get("shopify_images")
    if isinstance(images, list):
        media_values.extend([_clean_text(value) for value in images if _clean_text(value)])
    image_link = _clean_text(item.get("image_link", ""))
    if image_link:
        media_values.append(image_link)
    if media_values:
        normalized = _normalize_media_values(media_values, page_url=page_url)
        if normalized:
            payload["media_urls"] = " | ".join(normalized)

    variants = item.get("shopify_variants")
    if isinstance(variants, list):
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            barcode = _clean_text(variant.get("barcode", ""))
            if barcode:
                payload["barcode"] = barcode
                break

    return payload


def _score_searchanise_item(item: dict, sku: str) -> int:
    target = _compact_sku(sku)
    if not target:
        return 0

    score = 240

    product_code = _clean_text(item.get("product_code", ""))
    compact_product_code = _compact_sku(product_code)
    if compact_product_code == target:
        score += 720
    elif target and target in compact_product_code:
        score += 260

    variant_exact = False
    variant_partial = False
    for value in _collect_searchanise_variant_skus(item):
        compact_value = _compact_sku(value)
        if compact_value == target:
            variant_exact = True
            break
        if target and target in compact_value:
            variant_partial = True
    if variant_exact:
        score += 760
    elif variant_partial:
        score += 300

    title = _clean_text(item.get("title", ""))
    if _contains_compact_sku(title, sku):
        score += 220

    link = _clean_text(item.get("link", ""))
    if _contains_compact_sku(link, sku):
        score += 200

    return score


def _fetch_searchanise_items(
    api_key: str,
    sku: str,
    mode: str,
    max_results: int,
) -> tuple[list[dict], str | None]:
    if mode not in {"q", "query"}:
        return [], "Invalid Searchanise mode"
    query_url = (
        "https://www.searchanise.com/getresults"
        f"?api_key={urllib.parse.quote(api_key)}"
        f"&{mode}={urllib.parse.quote(sku)}"
        f"&output=json&maxResults={int(max_results)}"
    )
    body, error = _fetch_html(query_url, timeout=30)
    if error:
        return [], f"{query_url} ({error})"
    if not body:
        return [], None
    try:
        payload = json.loads(body)
    except Exception as exc:
        return [], f"{query_url} (invalid json: {exc})"
    items = payload.get("items")
    if not isinstance(items, list):
        return [], None
    return [item for item in items if isinstance(item, dict)], None


def _searchanise_candidates_from_search_page(
    search_html: str,
    page_url: str,
    sku: str,
) -> tuple[list[tuple[str, int, dict[str, str]]], list[str]]:
    keys = _extract_searchanise_api_keys(search_html)
    if not keys:
        return [], []

    errors: list[str] = []
    target = _compact_sku(sku)
    for api_key in keys:
        exact_items, exact_error = _fetch_searchanise_items(
            api_key=api_key,
            sku=sku,
            mode="q",
            max_results=40,
        )
        if exact_error:
            errors.append(exact_error)

        candidates: list[tuple[str, int, dict[str, str]]] = []
        for item in exact_items:
            link = _clean_text(item.get("link", ""))
            candidate_url = _normalize_candidate_link(link, page_url=page_url)
            if not candidate_url:
                continue
            score = _score_searchanise_item(item, sku)
            seed_payload = _build_searchanise_seed_payload(item, page_url=candidate_url)
            seed_payload["search_provider"] = "searchanise"
            candidates.append((candidate_url, score, seed_payload))
        if candidates:
            candidates.sort(key=lambda entry: (-entry[1], len(entry[0])))
            return candidates, errors

        # Fallback for fuzzy SKU matches when exact q search returns no items.
        fuzzy_items, fuzzy_error = _fetch_searchanise_items(
            api_key=api_key,
            sku=sku,
            mode="query",
            max_results=250,
        )
        if fuzzy_error:
            errors.append(fuzzy_error)
            continue
        if not fuzzy_items:
            continue

        ranked_fuzzy: list[tuple[int, int, int, dict]] = []
        for item in fuzzy_items:
            codes = _collect_searchanise_item_codes(item)
            if not codes:
                continue
            best_distance = 99
            best_delta = 999999999
            for code in codes:
                compact_code = _compact_sku(code)
                if not compact_code or not target:
                    continue
                distance = _levenshtein_distance_limited(target, compact_code, max_distance=2)
                if distance > 1:
                    continue
                delta = _numeric_delta_or_large(target, compact_code)
                if distance < best_distance or (distance == best_distance and delta < best_delta):
                    best_distance = distance
                    best_delta = delta
            if best_distance > 1:
                continue
            base_score = _score_searchanise_item(item, sku)
            fuzzy_score = base_score - (best_distance * 30) - min(best_delta, 999) // 5
            ranked_fuzzy.append((best_distance, best_delta, -fuzzy_score, item))

        if not ranked_fuzzy:
            continue
        ranked_fuzzy.sort(key=lambda entry: (entry[0], entry[1], entry[2]))
        best_distance = ranked_fuzzy[0][0]
        best_delta = ranked_fuzzy[0][1]
        filtered = [entry for entry in ranked_fuzzy if entry[0] == best_distance and entry[1] == best_delta]

        for _, _, _, item in filtered[:6]:
            link = _clean_text(item.get("link", ""))
            candidate_url = _normalize_candidate_link(link, page_url=page_url)
            if not candidate_url:
                continue
            score = max(260, _score_searchanise_item(item, sku) - 25)
            seed_payload = _build_searchanise_seed_payload(item, page_url=candidate_url)
            seed_payload["search_provider"] = "searchanise_fuzzy"
            candidates.append((candidate_url, score, seed_payload))
        if candidates:
            candidates.sort(key=lambda entry: (-entry[1], len(entry[0])))
            return candidates, errors

    return [], errors


def _strip_html_tags(value: object) -> str:
    text = _clean_text(unescape(value))
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_text(text)


def _extract_convermax_origins(html: str) -> list[str]:
    text = _clean_text(html).replace("\\/", "/")
    if not text:
        return []
    origins: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"https?://[a-z0-9.-]+\.myconvermax\.com", text, flags=re.IGNORECASE):
        origin = _clean_text(match.group(0)).rstrip("/")
        if not origin:
            continue
        lower = origin.lower()
        if lower in seen:
            continue
        seen.add(lower)
        origins.append(origin)
    return origins


def _fetch_convermax_items(origin: str, sku: str, max_results: int = 40) -> tuple[list[dict], str | None]:
    query_url = (
        f"{origin.rstrip('/')}/search.json"
        f"?query={urllib.parse.quote(sku)}"
        f"&pageSize={int(max_results)}"
        "&pageNumber=0"
    )
    body, error = _fetch_html(query_url, timeout=30)
    if error:
        return [], f"{query_url} ({error})"
    if not body:
        return [], None
    try:
        payload = json.loads(body)
    except Exception as exc:
        return [], f"{query_url} (invalid json: {exc})"
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], None
    if not isinstance(payload, dict):
        return [], None
    items = payload.get("Items")
    if not isinstance(items, list):
        return [], None
    return [item for item in items if isinstance(item, dict)], None


def _collect_convermax_item_codes(item: dict) -> list[str]:
    values: list[str] = []
    for key in ["sku", "product_code", "mpn", "id"]:
        value = _strip_html_tags(item.get(key, ""))
        if value:
            values.extend(_split_multi_value(value))
    variant_ids = item.get("variant_ids")
    if isinstance(variant_ids, list):
        for value in variant_ids:
            text = _strip_html_tags(value)
            if text:
                values.append(text)

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = _compact_sku(value)
        if not compact or compact in seen:
            continue
        seen.add(compact)
        deduped.append(value)
    return deduped


def _score_convermax_item(item: dict, sku: str) -> int:
    target = _compact_sku(sku)
    if not target:
        return 0
    score = 260

    code_exact = False
    code_partial = False
    for value in _collect_convermax_item_codes(item):
        compact = _compact_sku(value)
        if compact == target:
            code_exact = True
            break
        if target in compact:
            code_partial = True
    if code_exact:
        score += 760
    elif code_partial:
        score += 280

    title = _strip_html_tags(item.get("title", ""))
    if _contains_compact_sku(title, sku):
        score += 220

    link = _clean_text(item.get("url", "") or item.get("link", ""))
    if _contains_compact_sku(link, sku):
        score += 210

    handle = _clean_text(item.get("handle", ""))
    if _contains_compact_sku(handle, sku):
        score += 170

    return score


def _build_convermax_seed_payload(item: dict, page_url: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    title = _strip_html_tags(item.get("title", ""))
    if title:
        payload["title"] = title

    description = _strip_html_tags(item.get("description", ""))
    if description:
        payload["description_html"] = description

    price = _clean_text(item.get("price", ""))
    if price:
        payload["price"] = price

    vendor = _strip_html_tags(item.get("vendor", ""))
    if vendor:
        payload["vendor"] = vendor

    media_values: list[str] = []
    for key in ["image", "image2", "image_link"]:
        value = _clean_text(item.get(key, ""))
        if value:
            media_values.append(value)
    images = item.get("images")
    if isinstance(images, list):
        media_values.extend([_clean_text(value) for value in images if _clean_text(value)])
    if media_values:
        normalized = _normalize_media_values(media_values, page_url=page_url)
        if normalized:
            payload["media_urls"] = " | ".join(normalized)

    return payload


def _convermax_candidates_from_search_page(
    search_html: str,
    page_url: str,
    sku: str,
) -> tuple[list[tuple[str, int, dict[str, str]]], list[str]]:
    origins = _extract_convermax_origins(search_html)
    if not origins:
        return [], []

    errors: list[str] = []
    scored: dict[str, tuple[int, dict[str, str]]] = {}
    for origin in origins:
        items, item_error = _fetch_convermax_items(origin=origin, sku=sku, max_results=60)
        if item_error:
            errors.append(item_error)
            continue
        for item in items:
            link = _clean_text(item.get("url", "") or item.get("link", ""))
            candidate_url = _normalize_candidate_link(link, page_url=page_url) if link else ""
            if not candidate_url and link:
                candidate_url = _normalize_candidate_link(link, page_url=origin)
            if not candidate_url:
                handle = _clean_text(item.get("handle", ""))
                if handle:
                    candidate_url = _normalize_candidate_link(f"/products/{handle}", page_url=page_url)
            if not candidate_url:
                continue
            score = _score_convermax_item(item, sku)
            seed_payload = _build_convermax_seed_payload(item, page_url=candidate_url)
            seed_payload["search_provider"] = "convermax"
            existing = scored.get(candidate_url)
            if existing is None or score > existing[0]:
                scored[candidate_url] = (score, seed_payload)

    if not scored:
        return [], errors

    ordered = sorted(
        [(url, score, payload) for url, (score, payload) in scored.items()],
        key=lambda entry: (-entry[1], len(entry[0])),
    )
    return ordered, errors


def _extract_sunhammer_api_keys(html: str) -> list[str]:
    text = _clean_text(html).replace("\\/", "/")
    if not text:
        return []
    keys: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"""(?i)\bAPI_KEY\s*:\s*["']([0-9a-f-]{20,})["']""", text):
        key = _clean_text(match.group(1))
        if not key:
            continue
        lower = key.lower()
        if lower in seen:
            continue
        seen.add(lower)
        keys.append(key)
    return keys


def _collect_sunhammer_item_codes(item: dict) -> list[str]:
    values: list[str] = []
    for key in ["dealerid", "stockid", "sku", "mpn", "part_number"]:
        value = _clean_text(item.get(key, ""))
        if value:
            values.append(value)

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = _compact_sku(value)
        if not compact or compact in seen:
            continue
        seen.add(compact)
        deduped.append(value)
    return deduped


def _score_sunhammer_item(item: dict, sku: str) -> int:
    target = _compact_sku(sku)
    if not target:
        return 0

    score = 250
    exact_code = False
    suffix_code = False
    partial_code = False
    for value in _collect_sunhammer_item_codes(item):
        compact = _compact_sku(value)
        if not compact:
            continue
        if compact == target:
            exact_code = True
            break
        if compact.endswith(target) or target.endswith(compact):
            suffix_code = True
        elif target in compact:
            partial_code = True

    if exact_code:
        score += 780
    elif suffix_code:
        score += 720
    elif partial_code:
        score += 290

    title = _clean_text(item.get("title", ""))
    if _contains_compact_sku(title, sku):
        score += 160

    link = _clean_text(item.get("url", ""))
    if _contains_compact_sku(link, sku):
        score += 210

    return score


def _build_sunhammer_seed_payload(item: dict, page_url: str) -> dict[str, str]:
    payload: dict[str, str] = {}

    title = _clean_text(item.get("title", ""))
    if title:
        payload["title"] = title

    vendor = _clean_text(item.get("brand_name", ""))
    if vendor:
        payload["vendor"] = vendor

    price = _clean_text(item.get("price", ""))
    if price:
        payload["price"] = price

    image_url = _clean_text(item.get("image_url", ""))
    if image_url:
        normalized = _normalize_media_values([image_url], page_url=page_url)
        if normalized:
            payload["media_urls"] = " | ".join(normalized)

    return payload


def _fetch_sunhammer_items(
    api_key: str,
    sku: str,
    page_url: str,
    max_results: int = 60,
) -> tuple[list[dict], str | None]:
    parsed = urllib.parse.urlparse(page_url)
    origin = ""
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
    referer = f"{origin}/" if origin else page_url

    items: list[dict] = []
    page = 1
    while len(items) < max_results:
        page_size = max(1, min(20, max_results - len(items)))
        query_url = (
            "https://api.sunhammer.io/products"
            f"?limit={int(page_size)}"
            f"&page={int(page)}"
            f"&q={urllib.parse.quote(sku)}"
            "&fitment="
        )
        headers = {
            "User-Agent": _REQUEST_USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": _REQUEST_ACCEPT_LANGUAGE,
            "sunhammer-api-key": api_key,
        }
        if origin:
            headers["Origin"] = origin
        if referer:
            headers["Referer"] = referer
        request = urllib.request.Request(
            url=query_url,
            method="GET",
            headers=headers,
        )
        opener = _get_session_opener_for_host(query_url)
        try:
            if opener is not None:
                with opener.open(request, timeout=30) as response:
                    body = response.read()
            else:
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = response.read()
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                detail = ""
            return [], f"{query_url} (HTTP {exc.code}: {detail[:240] or 'request failed'})"
        except Exception as exc:
            return [], f"{query_url} ({exc})"

        try:
            payload = json.loads(body.decode("utf-8", errors="ignore"))
        except Exception as exc:
            return [], f"{query_url} (invalid json: {exc})"

        page_items = payload.get("list")
        if not isinstance(page_items, list):
            return items, None

        typed_items = [item for item in page_items if isinstance(item, dict)]
        if typed_items:
            items.extend(typed_items)

        total_value = payload.get("total")
        try:
            total = int(total_value)
        except Exception:
            total = 0

        if not typed_items or len(typed_items) < page_size:
            break
        if total > 0 and len(items) >= total:
            break
        page += 1

    return items[:max_results], None


def _sunhammer_candidates_from_search_page(
    search_html: str,
    page_url: str,
    sku: str,
) -> tuple[list[tuple[str, int, dict[str, str]]], list[str]]:
    api_keys = _extract_sunhammer_api_keys(search_html)
    if not api_keys:
        return [], []

    errors: list[str] = []
    scored: dict[str, tuple[int, dict[str, str]]] = {}
    for api_key in api_keys:
        items, item_error = _fetch_sunhammer_items(
            api_key=api_key,
            sku=sku,
            page_url=page_url,
            max_results=60,
        )
        if item_error:
            errors.append(item_error)
            continue
        for item in items:
            link = _clean_text(item.get("url", ""))
            candidate_url = _normalize_candidate_link(link, page_url=page_url)
            if not candidate_url:
                item_id = _clean_text(item.get("id", ""))
                if item_id:
                    candidate_url = _normalize_candidate_link(f"/i-{item_id}", page_url=page_url)
            if not candidate_url:
                continue
            score = _score_sunhammer_item(item, sku)
            seed_payload = _build_sunhammer_seed_payload(item, page_url=candidate_url)
            seed_payload["search_provider"] = "sunhammer"
            existing = scored.get(candidate_url)
            if existing is None or score > existing[0]:
                scored[candidate_url] = (score, seed_payload)

    if not scored:
        return [], errors

    ordered = sorted(
        [(url, score, payload) for url, (score, payload) in scored.items()],
        key=lambda entry: (-entry[1], len(entry[0])),
    )
    return ordered, errors


def _merge_seed_payload(
    parsed_payload: dict[str, str],
    seed_payload: dict[str, str],
    page_url: str,
) -> dict[str, str]:
    merged = dict(parsed_payload or {})
    seed = seed_payload or {}

    if seed:
        for key in [
            "title",
            "description_html",
            "type",
            "price",
            "cost",
            "barcode",
            "weight",
            "application",
            "vendor",
            "core_charge_product_code",
        ]:
            if _clean_text(merged.get(key, "")):
                continue
            value = _clean_text(seed.get(key, ""))
            if value:
                merged[key] = value

        provider = _clean_text(seed.get("search_provider", ""))
        parsed_media = _split_multi_value(_clean_text(merged.get("media_urls", "")))
        seed_media = _split_multi_value(_clean_text(seed.get("media_urls", "")))
        if provider.startswith("searchanise") and seed_media:
            # Searchanise gives a curated product image list; trust it over noisy full-page media extraction.
            combined = _normalize_media_values(seed_media, page_url=page_url)
            if combined:
                merged["media_urls"] = " | ".join(combined)
        elif parsed_media or seed_media:
            combined = _normalize_media_values(parsed_media + seed_media, page_url=page_url)
            if combined:
                merged["media_urls"] = " | ".join(combined)

        if provider and not _clean_text(merged.get("search_provider", "")):
            merged["search_provider"] = provider

    return merged


_SCRAPE_METADATA_FIELDS = {
    "source_url",
    "search_url",
    "product_url",
    "search_provider",
    "detail_fetch_provider",
    "search_term",
    "extract_error",
    "product_link_error",
    "search_provider_error",
    "detail_fetch_error",
    "image_download_error",
    "media_folder",
    "media_local_files",
}


def _normalize_requested_scrape_fields(requested_fields: set[str] | list[str] | tuple[str, ...] | None) -> set[str] | None:
    if requested_fields is None:
        return None
    normalized: set[str] = set()
    for field_name in requested_fields:
        value = _clean_text(field_name)
        if value:
            normalized.add(value)
    return normalized


def _filter_requested_scrape_payload(
    payload: dict[str, str],
    requested_fields: set[str] | list[str] | tuple[str, ...] | None,
) -> dict[str, str]:
    requested = _normalize_requested_scrape_fields(requested_fields)
    if requested is None:
        return dict(payload or {})

    keep = set(_SCRAPE_METADATA_FIELDS)
    keep.update(requested)
    output: dict[str, str] = {}
    for key, value in (payload or {}).items():
        if key not in keep:
            continue
        cleaned = _clean_text(value)
        if cleaned:
            output[key] = cleaned
    return output


def _extract_page_payload(html: str, page_url: str, sku: str, scrape_images: bool) -> dict[str, str]:
    merged: dict[str, str] = {}
    from_shopify_embed = _from_shopify_embedded_product(html, page_url, sku)
    from_jsonld = _from_json_ld(html, page_url, sku)
    from_heuristic = _heuristic_extract(html, page_url, sku, scrape_images=scrape_images)

    # Prefer product-scoped embedded media first. Heuristic media is broad and can
    # include recommendation/upsell images on some vendor templates.
    embed_media = _split_multi_value(_clean_text(from_shopify_embed.get("media_urls", "")))
    json_media = _split_multi_value(_clean_text(from_jsonld.get("media_urls", "")))
    heuristic_media = _split_multi_value(_clean_text(from_heuristic.get("media_urls", "")))

    combined_media_candidates: list[str] = []
    if embed_media:
        combined_media_candidates = embed_media + json_media
    elif json_media:
        combined_media_candidates = json_media
    else:
        combined_media_candidates = heuristic_media

    if combined_media_candidates:
        combined_media = _normalize_media_values(combined_media_candidates, page_url=page_url)
        if combined_media:
            merged["media_urls"] = " | ".join(combined_media)

    for key in [
        "title",
        "description_html",
        "type",
        "price",
        "cost",
        "barcode",
        "weight",
        "application",
        "vendor",
        "core_charge_product_code",
    ]:
        if key == "application" and from_shopify_embed:
            # Product-page embedded payload is authoritative; avoid noisy full-page fitment lines.
            value = _clean_text(from_shopify_embed.get(key, "")) or _clean_text(from_jsonld.get(key, ""))
        else:
            value = (
                _clean_text(from_shopify_embed.get(key, ""))
                or _clean_text(from_jsonld.get(key, ""))
                or _clean_text(from_heuristic.get(key, ""))
            )
        if value:
            merged[key] = value
    return merged


def _score_image_candidate(url: str) -> int:
    value = _clean_text(url).lower()
    if not value:
        return -999
    score = 0
    if re.search(r"\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?|$)", value):
        score += 25
    if re.search(r"(?i)(?:^|[/._-])(product|products|gallery|slideshow|carousel|zoom|main|media)(?:$|[/._-])", value):
        score += 20
    if re.search(r"(?i)(cdn|cloudfront|shopify|bigcommerce|woocommerce)", value):
        score += 8
    if re.search(r"(?i)(?:^|[/._-])(logo|icon|sprite|favicon|payment|badge|loader|placeholder)(?:$|[/._-])", value):
        score -= 50
    if re.search(r"(?i)(thumbnail|thumb|small)", value):
        score -= 35
    if value.endswith(".svg") or ".svg?" in value:
        score -= 220
    return score


def _extract_shopify_product_image_candidates(
    html: str,
    page_url: str = "",
    sku: str = "",
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        url = _clean_text(value)
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    products = _extract_embedded_shopify_products(html)
    if not products:
        return candidates

    ranked_products: list[tuple[int, dict]] = []
    for product in products:
        score = _shopify_match_score_for_sku(product, sku) if _clean_text(sku) else 1
        if _clean_text(sku) and score <= 0:
            continue
        handle = _clean_text(product.get("handle", "")).lower().strip("/")
        if handle and page_url and f"/products/{handle}" in urllib.parse.urlparse(page_url).path.lower():
            score += 120
        media_count = len(_extract_shopify_media_from_product(product))
        score += min(40, media_count * 4)
        ranked_products.append((score, product))

    ranked_products.sort(key=lambda entry: entry[0], reverse=True)
    if ranked_products:
        _, best_product = ranked_products[0]
        for value in _extract_shopify_media_from_product(best_product):
            add(value)

    return candidates


def _extract_script_gallery_candidates(html: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        url = _clean_text(value)
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    for raw_script in _extract_json_scripts(html):
        script = _decode_script_text(raw_script)
        if not script:
            continue
        if "image" not in script.lower() and "gallery" not in script.lower() and "media" not in script.lower():
            continue

        key_patterns = [
            r'(?i)"(?:image|images|gallery|media|src|url|original|full|zoom|large|main|featured_image)"\s*:\s*"([^"]+)"',
            r"(?i)'(?:image|images|gallery|media|src|url|original|full|zoom|large|main|featured_image)'\s*:\s*'([^']+)'",
        ]
        for pattern in key_patterns:
            for match in re.finditer(pattern, script):
                add(match.group(1))

        for match in re.finditer(r'(?i)(?:https?://|//)[^"\'\s<>]+', script):
            add(match.group(0))

    return candidates


def _extract_gallery_scope_blocks(html: str) -> list[str]:
    blocks: list[str] = []
    seen: set[str] = set()
    patterns = [
        r'(?is)<section[^>]+class=["\'][^"\']*gallery[^"\']*["\'][^>]*>.*?</section>',
        r'(?is)<div[^>]+class=["\'][^"\']*product-feature-image-wrapper[^"\']*["\'][^>]*>.*?</div>',
        r'(?is)<(?:div|section|ul|ol)[^>]+(?:id|class)=["\'][^"\']*(?:product[^"\']*(?:media|gallery|image|thumb|carousel|slider)|(?:media|gallery|image|thumb|carousel|slider)[^"\']*product|product__media|product-gallery|media-gallery)[^"\']*["\'][^>]*>.*?</(?:div|section|ul|ol)>',
        r'(?is)<(?:div|section|ul|ol)[^>]+(?:id|class)=["\'][^"\']*(?:product[^"\']*(?:thumbnail|thumbnails)|(?:thumbnail|thumbnails)[^"\']*product|gallery__thumb|gallery-thumbnail)[^"\']*["\'][^>]*>.*?</(?:div|section|ul|ol)>',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html):
            block = _clean_text(match.group(0))
            if not block:
                continue
            key = str(hash(block))
            if key in seen:
                continue
            seen.add(key)
            blocks.append(block)
    return blocks


def _collect_gallery_image_candidates(html: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        url = _clean_text(value)
        if not url or url in seen:
            return
        seen.add(url)
        candidates.append(url)

    scope_blocks = _extract_gallery_scope_blocks(html)
    source_blocks = scope_blocks if scope_blocks else [html]

    # Image and slideshow/gallery attributes used by many ecommerce themes.
    image_attr_patterns = [
        r'<img[^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+data-src=["\']([^"\']+)["\']',
        r'<img[^>]+data-lazy=["\']([^"\']+)["\']',
        r'<img[^>]+data-zoom-image=["\']([^"\']+)["\']',
        r'<img[^>]+data-large-image=["\']([^"\']+)["\']',
        r'<img[^>]+data-image=["\']([^"\']+)["\']',
        r'<img[^>]+data-full=["\']([^"\']+)["\']',
        r'<img[^>]+data-original=["\']([^"\']+)["\']',
        r'<img[^>]+srcset=["\']([^"\']+)["\']',
        r'<img[^>]+data-srcset=["\']([^"\']+)["\']',
        r'<source[^>]+srcset=["\']([^"\']+)["\']',
        r'(?i)data-(?:image|zoom-image|large-image|full|original|src|lazy|media)=["\']([^"\']+)["\']',
        r'(?i)style=["\'][^"\']*background-image\s*:\s*url\(([^)]+)\)',
    ]
    for source in source_blocks:
        for pattern in image_attr_patterns:
            for match in re.finditer(pattern, source, flags=re.IGNORECASE):
                raw = _clean_text(match.group(1))
                if not raw:
                    continue
                if "srcset" in pattern:
                    for url in _extract_srcset_urls(raw):
                        add(url)
                    continue
                if "background-image" in pattern:
                    cleaned = raw.strip("\"' ")
                    add(cleaned)
                    continue
                add(raw)

    # Some galleries store full-size image URLs in anchor href attributes.
    for source in source_blocks:
        for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\']', source, flags=re.IGNORECASE):
            href = _clean_text(match.group(1))
            if not href:
                continue
            if re.search(r"\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?|$)", href, flags=re.IGNORECASE):
                add(href)

    if not candidates and not scope_blocks:
        # Last-resort fallback when no gallery containers were found.
        for value in _extract_script_gallery_candidates(html):
            add(value)
        for match in re.finditer(
            r'https?://[^"\'\s<>]+?\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?[^"\'\s<>]*)?',
            html,
            flags=re.IGNORECASE,
        ):
            add(match.group(0))

    ranked = sorted(
        enumerate(candidates),
        key=lambda item: (-_score_image_candidate(item[1]), item[0]),
    )
    return [url for _, url in ranked]


def _split_multi_value(raw: str) -> list[str]:
    text = _clean_text(raw)
    if not text:
        return []
    parts = re.split(r"[|,;\n]+", text)
    return [item.strip() for item in parts if item and item.strip()]


def _infer_vendor_from_title(title_text: str) -> str:
    title = _clean_text(title_text)
    if not title:
        return ""
    parts = [_clean_text(part) for part in title.split("|") if _clean_text(part)]
    if not parts:
        return ""
    # Many vendor pages format as:
    # "<Product Title> | #<SKU> | <Vendor Name>"
    candidate = parts[-1]
    if re.fullmatch(r"#?\s*[A-Z0-9][A-Z0-9._/-]{2,}", candidate, flags=re.IGNORECASE) and len(parts) >= 2:
        candidate = parts[-2]
    lowered = candidate.lower()
    if lowered in {"search", "search results", "products", "product"}:
        return ""
    return candidate


def _safe_folder_name(value: str) -> str:
    text = normalize_sku(value)
    text = re.sub(r"[^A-Z0-9._-]+", "_", text)
    text = text.strip("._-")
    return text or "SKU"


def _safe_file_token(value: str, fallback: str) -> str:
    token = re.sub(r"[^A-Z0-9]+", "", normalize_sku(value))
    if not token:
        return fallback
    return token[:32]


def _fetch_binary(url: str, timeout: int = 45) -> tuple[bytes, str, str | None]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={
            "User-Agent": _REQUEST_USER_AGENT,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": _REQUEST_ACCEPT_LANGUAGE,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            content_type = str(response.headers.get("Content-Type", "")).lower()
    except urllib.error.HTTPError as exc:
        return b"", "", f"HTTP {exc.code}"
    except Exception as exc:
        return b"", "", str(exc)
    return body, content_type, None


def _extension_for_download(url: str, content_type: str) -> str:
    path = urllib.parse.urlparse(url).path
    suffix = Path(path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
        return suffix
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    if "gif" in content_type:
        return ".gif"
    return ".jpg"


def _is_probable_image_content_type(content_type: str, url: str) -> bool:
    text = _clean_text(content_type).lower()
    if not text:
        return _is_probable_image_url(url)
    base = text.split(";", 1)[0].strip()
    if base.startswith("image/"):
        return True
    if base in {"application/octet-stream", "binary/octet-stream"} and _is_probable_image_url(url):
        return True
    return False


def _normalize_image_to_square_jpeg(
    body: bytes,
    target_size: int = 1500,
) -> tuple[bytes | None, str | None]:
    try:
        from PIL import Image, ImageOps
    except Exception as exc:
        return None, f"Pillow unavailable: {exc}"

    try:
        with Image.open(BytesIO(body)) as source:
            source = ImageOps.exif_transpose(source)
            if source.mode in {"RGBA", "LA"} or (
                source.mode == "P" and source.info.get("transparency") is not None
            ):
                alpha = source.convert("RGBA")
                base = Image.new("RGB", alpha.size, (255, 255, 255))
                base.paste(alpha, mask=alpha.split()[-1])
                image = base
            else:
                image = source.convert("RGB")

            width, height = image.size
            if width <= 0 or height <= 0:
                return None, "invalid image dimensions"

            scale = float(target_size) / float(max(width, height))
            resized_width = max(1, int(round(width * scale)))
            resized_height = max(1, int(round(height * scale)))

            if hasattr(Image, "Resampling"):
                resample = Image.Resampling.LANCZOS
            else:
                resample = Image.LANCZOS
            if resized_width != width or resized_height != height:
                image = image.resize((resized_width, resized_height), resample=resample)

            canvas = Image.new("RGB", (target_size, target_size), (255, 255, 255))
            offset_x = (target_size - resized_width) // 2
            offset_y = (target_size - resized_height) // 2
            canvas.paste(image, (offset_x, offset_y))

            output = BytesIO()
            canvas.save(output, format="JPEG", quality=92, optimize=True)
            return output.getvalue(), None
    except Exception as exc:
        return None, str(exc)


def _download_images_for_sku(
    sku: str,
    media_urls: list[str],
    image_output_root: Path,
    vendor_hint: str = "",
    max_images: int = 30,
) -> tuple[list[str], str, str | None]:
    output_files: list[str] = []
    seen_hashes: set[str] = set()
    folder = image_output_root / _safe_folder_name(sku)
    folder.mkdir(parents=True, exist_ok=True)
    first_error: str | None = None
    # Clear stale files from previous runs so each scrape reflects current media exactly.
    for existing in folder.iterdir():
        if not existing.is_file():
            continue
        try:
            existing.unlink()
        except Exception as exc:
            if first_error is None:
                first_error = str(exc)
    for index, url in enumerate(media_urls[:max_images], start=1):
        body, content_type, error = _fetch_binary(url)
        if error or not body:
            if first_error is None:
                first_error = error or "empty response body"
            continue
        if not _is_probable_image_content_type(content_type, url):
            if first_error is None:
                first_error = f"non-image content-type: {content_type or 'unknown'}"
            continue
        digest = hashlib.sha1(body).hexdigest()
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        normalized_body, normalize_error = _normalize_image_to_square_jpeg(body, target_size=1500)
        if normalized_body is None:
            if first_error is None and normalize_error:
                first_error = f"image normalization failed: {normalize_error}"
            normalized_body = body
            extension = _extension_for_download(url, content_type)
        else:
            extension = ".jpg"

        vendor_token = _safe_file_token(vendor_hint, "IMG")
        sku_token = _safe_file_token(sku, "SKU")
        file_index = len(output_files) + 1
        base_name = f"{vendor_token}_{sku_token}_{file_index}"
        file_path = folder / f"{base_name}{extension}"
        while file_path.exists():
            file_index += 1
            base_name = f"{vendor_token}_{sku_token}_{file_index}"
            file_path = folder / f"{base_name}{extension}"
        try:
            file_path.write_bytes(normalized_body)
        except Exception as exc:
            if first_error is None:
                first_error = str(exc)
            continue
        output_files.append(str(file_path))
    return output_files, str(folder), first_error


def _html_fragment_to_lines(fragment: str) -> list[str]:
    value = _clean_text(fragment)
    if not value:
        return []
    value = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"<style[\s\S]*?</style>", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"<noscript[\s\S]*?</noscript>", " ", value, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "\n", unescape(value))
    lines: list[str] = []
    for raw in text.splitlines():
        line = _clean_text(raw)
        if not line:
            continue
        if line.lower() in {"product details", "warranty", "reviews", "specifications"}:
            continue
        lines.append(line)
    return lines


def _extract_tab_pane_sections(html: str) -> list[tuple[str, str, str]]:
    sections: list[tuple[str, str, str]] = []
    seen_ids: set[str] = set()

    def _extract_pane_html(pane_id: str) -> str:
        pane_patterns = [
            rf'(?is)<div[^>]+id=["\']{re.escape(pane_id)}["\'][^>]*class=["\'][^"\']*tab-pane[^"\']*["\'][^>]*>(.*?)</div>\s*(?=<div[^>]+class=["\'][^"\']*tab-pane[^"\']*["\']|</section>|</article>|</main>)',
            rf'(?is)<div[^>]+id=["\']{re.escape(pane_id)}["\'][^>]*role=["\']tabpanel["\'][^>]*>(.*?)</div>\s*(?=<div[^>]+role=["\']tabpanel["\']|</section>|</article>|</main>)',
            rf'(?is)<div[^>]+id=["\']{re.escape(pane_id)}["\'][^>]*>(.*?)</div>\s*(?=<div[^>]+id=["\']|</section>|</article>|</main>)',
        ]
        for pane_pattern in pane_patterns:
            pane_match = re.search(pane_pattern, html)
            if pane_match:
                return _clean_text(pane_match.group(1))
        return ""

    tab_matches = list(
        re.finditer(
            r'(?is)<a[^>]+(?:data-target|href)=["\']#([^"\']+)["\'][^>]*>(.*?)</a>',
            html,
        )
    )
    tab_matches.extend(
        re.finditer(
            r'(?is)<button[^>]+(?:data-target|href)=["\']#([^"\']+)["\'][^>]*>(.*?)</button>',
            html,
        )
    )

    for match in tab_matches:
        pane_id = _clean_text(match.group(1))
        if not pane_id or pane_id in seen_ids:
            continue
        seen_ids.add(pane_id)
        label = _clean_text(re.sub(r"<[^>]+>", " ", unescape(match.group(2) or "")))
        if not label:
            continue
        pane_html = _extract_pane_html(pane_id)
        if not pane_html:
            continue
        sections.append((pane_id, label, pane_html))
    return sections


def _extract_description_from_tabs(html: str) -> str:
    sections = _extract_tab_pane_sections(html)
    if not sections:
        return ""

    preferred_labels = [
        r"product\s*details?",
        r"\bdescription\b",
        r"\boverview\b",
        r"about\s+this\s+product",
        r"\bfeatures?\b",
    ]
    reject_labels = [r"\breviews?\b", r"\bwarranty\b", r"\bspecifications?\b"]

    for _, label, pane_html in sections:
        label_low = label.lower()
        if any(re.search(pattern, label_low, flags=re.IGNORECASE) for pattern in reject_labels):
            continue
        if not any(re.search(pattern, label_low, flags=re.IGNORECASE) for pattern in preferred_labels):
            continue
        lines = _html_fragment_to_lines(pane_html)
        if not lines:
            continue
        # Keep concise but complete enough for product detail content.
        description = "\n".join(lines[:18]).strip()
        if description:
            return description
    return ""


def _extract_structured_fitment_lines(html: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(line: str) -> None:
        text = _clean_text(line)
        if not text:
            return
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"\s*\.\.\.\s*see application guide.*$", "", text, flags=re.IGNORECASE)
        if not text:
            return
        key = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
        if not key or key in seen:
            return
        seen.add(key)
        candidates.append(text)

    # Common ecommerce fitment block on Westin and similar templates.
    for match in re.finditer(
        r'(?is)<div[^>]+class=["\'][^"\']*product-list-container[^"\']*["\'][^>]*>(.*?)</div>\s*</section>',
        html,
    ):
        block = _clean_text(match.group(1))
        if not block:
            continue
        for li_match in re.finditer(r"(?is)<li[^>]*>(.*?)</li>", block):
            line = _clean_text(re.sub(r"<[^>]+>", " ", unescape(li_match.group(1) or "")))
            if not line:
                continue
            if re.search(r"\b(19|20)\d{2}\b", line) and re.search(
                r"(?i)(ford|ram|dodge|gmc|chevy|chevrolet|gm|jeep|nissan|toyota)",
                line,
            ):
                add(line)

    # Generic fitment-style tabs for vendors that keep applications in tabs.
    for _, label, pane_html in _extract_tab_pane_sections(html):
        if not re.search(r"(?i)(fit|what\s+it\s+fits|application|compatib|specification)", label):
            continue
        for line in _html_fragment_to_lines(pane_html):
            if re.search(r"\b(19|20)\d{2}\b", line) and re.search(
                r"(?i)(ford|ram|dodge|gmc|chevy|chevrolet|gm|jeep|nissan|toyota)",
                line,
            ):
                add(line)

    return candidates[:40]


def _extract_heuristic_barcode(html: str, context: str) -> str:
    barcode = _extract_first(r'(?i)(?:upc|gtin|barcode|ean)\D{0,20}([0-9]{8,14})', context)
    if barcode and barcode.lower() not in {"none", "nan", "null"}:
        return barcode

    for pattern in [
        r'(?i)"(?:gtin(?:8|12|13|14)?|upc|ean|barcode)"\s*:\s*"([0-9]{8,14})"',
        r"(?i)'(?:gtin(?:8|12|13|14)?|upc|ean|barcode)'\s*:\s*'([0-9]{8,14})'",
        r'(?is)<(?:tr|li|div|dl)[^>]*>.*?(?:GTIN|UPC|Barcode|EAN)\s*#?:?\s*</(?:th|dt|label|span|div)>.*?(?:<[^>]+>)*([0-9]{8,14})',
        r'(?is)(?:GTIN|UPC|Barcode|EAN)\s*#?:?\s*(?:</[^>]+>\s*)*(?:<[^>]+>\s*)*([0-9]{8,14})',
    ]:
        value = _clean_text(unescape(_extract_first(pattern, html, flags=re.IGNORECASE | re.DOTALL)))
        if value and value.lower() not in {"none", "nan", "null"}:
            return value

    for pattern in [
        r'(?is)<li[^>]+wsm-prod-upccode[^>]*>.*?<label>\s*UPC\s*#?:\s*</label>\s*(?:<span[^>]*>)?([^<]+)',
        r'(?is)<label[^>]*>\s*(?:UPC|GTIN|Barcode|EAN)\s*#?:?\s*</label>\s*(?:<span[^>]*>)?([^<]+)',
        r'(?i)(?:upc|gtin|barcode|ean)\s*#?:?\s*([A-Z0-9][A-Z0-9._/-]{3,})',
    ]:
        value = _clean_text(unescape(_extract_first(pattern, html, flags=re.IGNORECASE | re.DOTALL)))
        if value and value.lower() not in {"none", "nan", "null"}:
            return value
    return ""


def _heuristic_extract(html: str, page_url: str, sku: str, scrape_images: bool) -> dict[str, str]:
    context = _find_context_near_sku(html, sku)
    output: dict[str, str] = {}

    output["title"] = _extract_meta_content(html, "og:title") or _extract_first(r"<title>([^<]+)</title>", html, flags=re.IGNORECASE)
    output["description_html"] = _extract_meta_content(html, "description") or _extract_meta_content(html, "og:description")
    output["price"] = _extract_first(r'(?i)(?:\"price\"|price|msrp|retail)\D{0,20}([0-9]{1,6}(?:\.[0-9]{1,2})?)', context)
    output["core_charge_product_code"] = _extract_first(
        r"(?i)(?:core(?:\s*charge)?|corecharge)\D{0,28}(\$?\s*[0-9]{1,5}(?:\.[0-9]{1,2})?)",
        context,
    )
    output["weight"] = _extract_first(r'(?i)(?:weight|wt)\D{0,20}([0-9]{1,3}(?:\.[0-9]{1,3})?)', context)
    output["barcode"] = _extract_heuristic_barcode(html, context)
    output["vendor"] = _extract_meta_content(html, "og:site_name") or _infer_vendor_from_title(output.get("title", ""))
    if not _clean_text(output.get("description_html", "")):
        tab_description = _extract_description_from_tabs(html)
        if tab_description:
            output["description_html"] = tab_description

    if scrape_images:
        shopify_media_values = _extract_shopify_product_image_candidates(html, page_url=page_url, sku=sku)
        if shopify_media_values:
            normalized_media = _normalize_media_values(shopify_media_values, page_url)
        else:
            media_values = _collect_gallery_image_candidates(html)
            normalized_media = _normalize_media_values(media_values, page_url)
            sku_scoped_media = [value for value in normalized_media if _media_url_matches_target_sku(value, sku)]
            if sku_scoped_media:
                normalized_media = sku_scoped_media
        output["media_urls"] = " | ".join(normalized_media[:30])

    application_lines: list[str] = []
    structured_fitment_lines = _extract_structured_fitment_lines(html)
    if structured_fitment_lines:
        application_lines.extend(structured_fitment_lines)
    # Scope fitment heuristics near the matched SKU to avoid unrelated recommendation tiles.
    fitment_context = _find_context_near_sku(html, sku, span=12000)
    fitment_source = fitment_context if _clean_text(fitment_context) else html
    # Remove script/style blocks before fitment heuristics so JSON/JS blobs do not pollute application text.
    fitment_source = re.sub(r"<script[\s\S]*?</script>", " ", fitment_source, flags=re.IGNORECASE)
    fitment_source = re.sub(r"<style[\s\S]*?</style>", " ", fitment_source, flags=re.IGNORECASE)
    fitment_text = re.sub(r"<[^>]+>", "\n", unescape(fitment_source))
    for line in fitment_text.splitlines():
        text = _clean_text(line)
        if len(text) < 8 or len(text) > 220:
            continue
        if re.search(r"[{}\[\]\"]", text):
            continue
        if "sociallist" in text.lower():
            continue
        if re.search(r"\b(19|20)\d{2}\b", text) and re.search(
            r"(?i)(ford|chevy|gmc|ram|dodge|duramax|cummins|powerstroke|diesel|f[-\s]?250|f[-\s]?350)",
            text,
        ):
            application_lines.append(text)
        if len(application_lines) >= 24:
            break
    title_text = _clean_text(output.get("title", ""))
    if title_text and re.search(r"\b(19|20)\d{2}\b", title_text) and re.search(
        r"(?i)(ford|chevy|chevrolet|gmc|gm|ram|dodge|duramax|cummins|powerstroke|diesel|jeep|nissan)",
        title_text,
    ):
        application_lines.append(title_text)
    if application_lines:
        deduped: list[str] = []
        seen: set[str] = set()
        for item in application_lines:
            key = re.sub(r"[^a-z0-9]+", " ", item.lower()).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        if deduped:
            output["application"] = " | ".join(deduped)

    return output


def _should_probe_search_candidates(
    search_html: str,
    target_url: str,
    sku: str,
    payload: dict[str, str],
    direct_product_url: str,
) -> bool:
    if _clean_text(direct_product_url):
        return False

    if _extract_searchspring_site_ids(search_html):
        return True

    if _extract_sunhammer_api_keys(search_html):
        return True

    target_path = urllib.parse.urlparse(target_url).path.lower()
    title = _clean_text(payload.get("title", ""))
    title_lower = title.lower()
    if "/search" not in target_path and title and "search results" not in title_lower:
        return False

    evidence_text = " ".join(
        [
            title,
            _clean_text(payload.get("description_html", "")),
            _clean_text(payload.get("application", "")),
            _clean_text(payload.get("barcode", "")),
            _find_context_near_sku(search_html, sku, span=2800),
        ]
    )
    has_sku_evidence = _contains_compact_sku(evidence_text, sku)
    has_product_signals = sum(
        [
            1 if title and "search results" not in title_lower else 0,
            1 if _clean_text(payload.get("price", "")) else 0,
            1 if _clean_text(payload.get("description_html", "")) else 0,
            1 if _clean_text(payload.get("application", "")) else 0,
            1 if _clean_text(payload.get("media_urls", "")) else 0,
        ]
    )
    if has_sku_evidence and has_product_signals >= 2:
        return False
    return True


def _scrape_single_sku(
    sku: str,
    base_url: str,
    resolver_profile: VendorResolverProfile | None,
    retry_count: int,
    delay_seconds: float,
    scrape_images: bool,
    image_output_root: Path | None,
    search_term: str = "",
    requested_fields: set[str] | list[str] | tuple[str, ...] | None = None,
) -> tuple[str, dict[str, str], str | None]:
    query_value = normalize_sku(search_term) or sku
    target_url = _normalize_url(base_url, query_value)
    if not target_url:
        return sku, {}, "Missing vendor search URL"

    if _is_alliant_parts_search_url(base_url) or _is_alliant_parts_search_url(target_url):
        return _scrape_single_sku_via_alliant_graph(
            sku=sku,
            base_url=base_url,
            scrape_images=scrape_images,
            search_term=query_value,
        )

    if resolver_profile is not None and _clean_text(resolver_profile.interaction_strategy).lower() == "json_api_id_detail":
        return _scrape_single_sku_via_json_api_id_detail(
            sku=sku,
            profile=resolver_profile,
            scrape_images=scrape_images,
            search_term=query_value,
        )

    direct_searchspring_site_ids = _searchspring_site_ids_for_vendor_url(base_url)
    if direct_searchspring_site_ids:
        searchspring_candidates, searchspring_errors = _searchspring_candidates_from_site_ids(
            site_ids=direct_searchspring_site_ids,
            page_url=target_url,
            sku=query_value,
        )
        if searchspring_candidates:
            product_fetch_errors: list[str] = []
            best_candidate_url = ""
            best_candidate_html = ""
            best_candidate_payload: dict[str, str] = {}
            best_candidate_rank = -1
            for candidate_url, candidate_score, seed_payload in searchspring_candidates[:10]:
                product_html, product_error = _fetch_html(candidate_url)
                if product_error:
                    if seed_payload and candidate_score >= 260:
                        candidate_payload = _merge_seed_payload({}, seed_payload, page_url=candidate_url)
                        payload_match_text = " ".join(
                            [
                                candidate_url,
                                _clean_text(candidate_payload.get("title", "")),
                                _clean_text(candidate_payload.get("description_html", "")),
                                _clean_text(candidate_payload.get("application", "")),
                                _clean_text(candidate_payload.get("barcode", "")),
                            ]
                        )
                        matches_sku = _contains_compact_sku(payload_match_text, query_value)
                        payload_score = 0
                        if _clean_text(candidate_payload.get("title", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("description_html", "")):
                            payload_score += 1
                        if _clean_text(candidate_payload.get("application", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("media_urls", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("price", "")):
                            payload_score += 1
                        if matches_sku:
                            payload_score += 4
                        rank = (payload_score * 20) + candidate_score
                        if rank > best_candidate_rank:
                            best_candidate_rank = rank
                            best_candidate_url = candidate_url
                            best_candidate_html = ""
                            best_candidate_payload = candidate_payload
                    else:
                        product_fetch_errors.append(f"{candidate_url} ({product_error})")
                    continue

                candidate_payload = _extract_page_payload(product_html, candidate_url, query_value, scrape_images=False)
                candidate_payload = _merge_seed_payload(candidate_payload, seed_payload, page_url=candidate_url)
                payload_match_text = " ".join(
                    [
                        candidate_url,
                        _clean_text(candidate_payload.get("title", "")),
                        _clean_text(candidate_payload.get("description_html", "")),
                        _clean_text(candidate_payload.get("application", "")),
                        _clean_text(candidate_payload.get("barcode", "")),
                    ]
                )
                raw_html_match = _contains_compact_sku(_find_context_near_sku(product_html, query_value, span=2400), query_value)
                matches_sku = _contains_compact_sku(payload_match_text, query_value) or raw_html_match
                payload_score = 0
                if _clean_text(candidate_payload.get("title", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("description_html", "")):
                    payload_score += 1
                if _clean_text(candidate_payload.get("application", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("media_urls", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("price", "")):
                    payload_score += 1
                if matches_sku:
                    payload_score += 4
                rank = (payload_score * 20) + candidate_score
                if rank > best_candidate_rank:
                    best_candidate_rank = rank
                    best_candidate_url = candidate_url
                    best_candidate_html = product_html
                    best_candidate_payload = candidate_payload
                if matches_sku and payload_score >= 4:
                    break

            if best_candidate_url:
                merged: dict[str, str] = {
                    "source_url": best_candidate_url,
                    "search_url": target_url,
                    "product_url": best_candidate_url,
                }
                if query_value and query_value != sku:
                    merged["search_term"] = query_value
                provider = _clean_text(best_candidate_payload.get("search_provider", ""))
                if provider:
                    merged["search_provider"] = provider

                try:
                    resolved_payload = {}
                    if best_candidate_html:
                        resolved_payload = _extract_page_payload(
                            best_candidate_html,
                            best_candidate_url,
                            query_value,
                            scrape_images=scrape_images,
                        )
                    resolved_payload = _merge_seed_payload(
                        resolved_payload,
                        best_candidate_payload,
                        page_url=best_candidate_url,
                    )
                    for key in [
                        "title",
                        "description_html",
                        "media_urls",
                        "type",
                        "price",
                        "cost",
                        "barcode",
                        "weight",
                        "application",
                        "vendor",
                        "core_charge_product_code",
                    ]:
                        value = _clean_text(resolved_payload.get(key, ""))
                        if value:
                            merged[key] = value
                except Exception as exc:
                    merged["extract_error"] = str(exc)

                if _should_attempt_xtreme_browser_detail(
                    product_url=best_candidate_url,
                    payload=merged,
                    requested_fields=requested_fields,
                ):
                    browser_payload, browser_error = _fetch_xtreme_detail_payload_via_browser(
                        product_url=best_candidate_url,
                        sku=query_value,
                        scrape_images=scrape_images,
                    )
                    if browser_payload:
                        browser_url = _clean_text(browser_payload.get("product_url", "")) or best_candidate_url
                        for key in [
                            "title",
                            "description_html",
                            "media_urls",
                            "type",
                            "price",
                            "cost",
                            "barcode",
                            "weight",
                            "application",
                            "vendor",
                            "core_charge_product_code",
                            "detail_fetch_provider",
                        ]:
                            value = _clean_text(browser_payload.get(key, ""))
                            if value:
                                merged[key] = value
                        if browser_url and browser_url != best_candidate_url:
                            merged["product_url"] = browser_url
                            merged["source_url"] = browser_url
                    elif browser_error:
                        merged["detail_fetch_error"] = browser_error

                if product_fetch_errors and not best_candidate_html:
                    merged["product_link_error"] = " | ".join(product_fetch_errors[:3])
                if searchspring_errors and not best_candidate_html:
                    merged["search_provider_error"] = " | ".join(searchspring_errors[:3])

                if scrape_images and image_output_root is not None:
                    media_values = _split_multi_value(merged.get("media_urls", ""))
                    if media_values:
                        local_files, media_folder, image_error = _download_images_for_sku(
                            sku=sku,
                            media_urls=media_values,
                            image_output_root=image_output_root,
                            vendor_hint=_clean_text(merged.get("vendor", "")),
                        )
                        if media_folder:
                            merged["media_folder"] = media_folder
                        if local_files:
                            merged["media_local_files"] = " | ".join(local_files)
                        if image_error and not local_files:
                            merged["image_download_error"] = image_error
                return sku, merged, None

    last_error: str | None = None
    remaining_attempts = max(retry_count + 1, 1)
    rate_limit_bonus_remaining = 1
    rate_limit_hits = 0
    while remaining_attempts > 0:
        remaining_attempts -= 1
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        search_html, error = _fetch_html(target_url)
        if error:
            last_error = f"{target_url} ({error})"
            if _is_rate_limit_error(error) and rate_limit_bonus_remaining > 0:
                rate_limit_hits += 1
                rate_limit_bonus_remaining -= 1
                remaining_attempts += 1
                backoff_base = max(float(delay_seconds or 0.0), 0.45)
                backoff = min(2.5, backoff_base * (1.5 + (rate_limit_hits * 0.5)))
                jitter = ((sum(ord(ch) for ch in sku) + (rate_limit_hits * 13)) % 21) / 100.0
                time.sleep(backoff + jitter)
            continue

        resolved_url = target_url
        resolved_html = search_html
        search_payload = _extract_page_payload(search_html, target_url, query_value, scrape_images=scrape_images)
        direct_product_url = ""
        canonical_from_search = _extract_canonical_page_url(search_html, page_url=target_url)
        if canonical_from_search:
            canonical_path = urllib.parse.urlparse(canonical_from_search).path.lower()
            if canonical_path and canonical_path != "/" and "/search" not in canonical_path:
                direct_evidence_text = " ".join(
                    [
                        _clean_text(search_payload.get("title", "")),
                        _clean_text(search_payload.get("description_html", "")),
                        _clean_text(canonical_from_search),
                        _find_context_near_sku(search_html, query_value, span=2000),
                    ]
                )
                if _contains_compact_sku(direct_evidence_text, query_value) or bool(_clean_text(search_payload.get("title", ""))):
                    direct_product_url = canonical_from_search
                    resolved_url = direct_product_url

        searchspring_candidates: list[tuple[str, int, dict[str, str]]] = []
        searchanise_candidates: list[tuple[str, int, dict[str, str]]] = []
        convermax_candidates: list[tuple[str, int, dict[str, str]]] = []
        sunhammer_candidates: list[tuple[str, int, dict[str, str]]] = []
        searchspring_errors: list[str] = []
        searchanise_errors: list[str] = []
        convermax_errors: list[str] = []
        sunhammer_errors: list[str] = []
        candidate_seed_payloads: dict[str, dict[str, str]] = {}
        candidates: list[tuple[str, int]] = []
        probe_candidates = _should_probe_search_candidates(
            search_html=search_html,
            target_url=target_url,
            sku=query_value,
            payload=search_payload,
            direct_product_url=direct_product_url,
        )
        if resolver_profile is not None:
            low_hint = _clean_text(resolver_profile.resolver_hint).lower()
            interaction_strategy = _clean_text(resolver_profile.interaction_strategy).lower()
            if "results_page_clickthrough" in low_hint or interaction_strategy == "results_page_clickthrough":
                probe_candidates = True
        if probe_candidates and not direct_product_url:
            searchspring_candidates, searchspring_errors = _searchspring_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=query_value,
            )
            searchanise_candidates, searchanise_errors = _searchanise_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=query_value,
            )
            convermax_candidates, convermax_errors = _convermax_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=query_value,
            )
            sunhammer_candidates, sunhammer_errors = _sunhammer_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=query_value,
            )
            candidates = _extract_product_page_candidates(search_html, page_url=target_url, sku=query_value)
            if resolver_profile is not None:
                profile_candidates = _extract_profile_result_candidates(
                    html=search_html,
                    page_url=target_url,
                    sku=query_value,
                    profile=resolver_profile,
                )
                if profile_candidates:
                    merged_scores: dict[str, int] = {href: score for href, score in candidates}
                    for candidate_url, candidate_score in profile_candidates:
                        existing_score = merged_scores.get(candidate_url, -9999)
                        if candidate_score > existing_score:
                            merged_scores[candidate_url] = candidate_score
                    candidates = sorted(merged_scores.items(), key=lambda item: (-item[1], len(item[0])))
            provider_candidates = (
                list(searchspring_candidates[:20])
                + list(searchanise_candidates[:20])
                + list(convermax_candidates[:20])
                + list(sunhammer_candidates[:20])
            )
            if provider_candidates:
                merged_scores: dict[str, int] = {href: score for href, score in candidates}
                for candidate_url, candidate_score, seed_payload in provider_candidates:
                    existing_score = merged_scores.get(candidate_url, -9999)
                    if candidate_score > existing_score:
                        merged_scores[candidate_url] = candidate_score
                    if seed_payload:
                        candidate_seed_payloads[candidate_url] = seed_payload
                candidates = sorted(merged_scores.items(), key=lambda item: (-item[1], len(item[0])))

        product_fetch_errors: list[str] = []
        best_candidate_url = ""
        best_candidate_html = ""
        best_candidate_payload: dict[str, str] = {}
        best_candidate_rank = -1
        if probe_candidates and not direct_product_url:
            for candidate_url, candidate_score in candidates[:10]:
                if candidate_url == target_url:
                    continue
                seed_payload = candidate_seed_payloads.get(candidate_url, {})
                product_html, product_error = _fetch_html(candidate_url)
                if product_error:
                    if seed_payload and candidate_score >= 260:
                        candidate_payload = _merge_seed_payload({}, seed_payload, page_url=candidate_url)
                        payload_match_text = " ".join(
                            [
                                candidate_url,
                                _clean_text(candidate_payload.get("title", "")),
                                _clean_text(candidate_payload.get("description_html", "")),
                                _clean_text(candidate_payload.get("application", "")),
                                _clean_text(candidate_payload.get("barcode", "")),
                            ]
                        )
                        matches_sku = _contains_compact_sku(payload_match_text, query_value)
                        payload_score = 0
                        if _clean_text(candidate_payload.get("title", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("description_html", "")):
                            payload_score += 1
                        if _clean_text(candidate_payload.get("application", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("media_urls", "")):
                            payload_score += 2
                        if _clean_text(candidate_payload.get("price", "")):
                            payload_score += 1
                        if matches_sku:
                            payload_score += 4
                        rank = (payload_score * 20) + candidate_score
                        if rank > best_candidate_rank:
                            best_candidate_rank = rank
                            best_candidate_url = candidate_url
                            best_candidate_html = ""
                            best_candidate_payload = candidate_payload
                    else:
                        product_fetch_errors.append(f"{candidate_url} ({product_error})")
                    continue
                candidate_payload = _extract_page_payload(product_html, candidate_url, query_value, scrape_images=False)
                candidate_payload = _merge_seed_payload(candidate_payload, seed_payload, page_url=candidate_url)
                payload_match_text = " ".join(
                    [
                        candidate_url,
                        _clean_text(candidate_payload.get("title", "")),
                        _clean_text(candidate_payload.get("description_html", "")),
                        _clean_text(candidate_payload.get("application", "")),
                        _clean_text(candidate_payload.get("barcode", "")),
                    ]
                )
                raw_html_match = _contains_compact_sku(_find_context_near_sku(product_html, query_value, span=2400), query_value)
                matches_sku = _contains_compact_sku(payload_match_text, query_value) or raw_html_match
                payload_score = 0
                if _clean_text(candidate_payload.get("title", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("description_html", "")):
                    payload_score += 1
                if _clean_text(candidate_payload.get("application", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("media_urls", "")):
                    payload_score += 2
                if _clean_text(candidate_payload.get("price", "")):
                    payload_score += 1
                candidate_title = _clean_text(candidate_payload.get("title", "")).lower()
                if "search results" in candidate_title:
                    payload_score -= 4
                if matches_sku:
                    payload_score += 4

                # Only accept candidate pages with SKU evidence, unless URL score is very strong.
                if not matches_sku and candidate_score < 160:
                    continue

                rank = (payload_score * 20) + candidate_score
                if rank > best_candidate_rank:
                    best_candidate_rank = rank
                    best_candidate_url = candidate_url
                    best_candidate_html = product_html
                    best_candidate_payload = candidate_payload

                if matches_sku and payload_score >= 4:
                    break

        if best_candidate_url:
            resolved_url = best_candidate_url
            resolved_html = best_candidate_html
        resolved_seed_payload = best_candidate_payload if best_candidate_url else {}
        if not resolved_seed_payload and resolved_url != target_url:
            resolved_seed_payload = candidate_seed_payloads.get(resolved_url, {})

        merged: dict[str, str] = {"source_url": resolved_url}
        if resolved_url != target_url:
            merged["search_url"] = target_url
            merged["product_url"] = resolved_url
        if query_value and query_value != sku:
            merged["search_term"] = query_value
        if resolved_seed_payload and resolved_url != target_url:
            provider = _clean_text(resolved_seed_payload.get("search_provider", ""))
            merged["search_provider"] = provider or "search_seed"

        try:
            if resolved_html:
                resolved_payload = _extract_page_payload(resolved_html, resolved_url, query_value, scrape_images=scrape_images)
            else:
                resolved_payload = {}
            resolved_payload = _merge_seed_payload(resolved_payload, resolved_seed_payload, page_url=resolved_url)
            for key in [
                "title",
                "description_html",
                "media_urls",
                "type",
                "price",
                "cost",
                "barcode",
                "weight",
                "application",
                "vendor",
                "core_charge_product_code",
            ]:
                value = _clean_text(resolved_payload.get(key, "")) or _clean_text(search_payload.get(key, ""))
                if value:
                    merged[key] = value
        except Exception as exc:
            # Keep successful page fetches as scrape hits even if parsing raises.
            merged["extract_error"] = str(exc)

        if product_fetch_errors and resolved_url == target_url:
            merged["product_link_error"] = " | ".join(product_fetch_errors[:3])
        provider_errors = (
            list(searchspring_errors or [])
            + list(searchanise_errors or [])
            + list(convermax_errors or [])
            + list(sunhammer_errors or [])
        )
        if provider_errors and resolved_url == target_url:
            merged["search_provider_error"] = " | ".join(provider_errors[:3])

        canonical_product_url = ""
        canonical_product_path = ""
        if resolved_html:
            canonical_url = _extract_canonical_page_url(resolved_html, page_url=resolved_url)
            if canonical_url:
                canonical_path = urllib.parse.urlparse(canonical_url).path.lower()
                if canonical_path and canonical_path != "/" and "/search" not in canonical_path:
                    evidence_text = " ".join(
                        [
                            _clean_text(merged.get("title", "")),
                            _clean_text(merged.get("description_html", "")),
                            _clean_text(merged.get("application", "")),
                            _clean_text(canonical_url),
                            _find_context_near_sku(resolved_html, query_value, span=2000),
                        ]
                    )
                    if _contains_compact_sku(evidence_text, query_value) or bool(_clean_text(merged.get("title", ""))):
                        canonical_product_url = canonical_url
                        canonical_product_path = canonical_path

        if canonical_product_url and resolved_url == target_url:
            canonical_html, canonical_error = _fetch_html(canonical_product_url)
            if not canonical_error and canonical_html:
                canonical_payload = _extract_page_payload(
                    canonical_html,
                    canonical_product_url,
                    query_value,
                    scrape_images=scrape_images,
                )
                for key in [
                    "title",
                    "description_html",
                    "media_urls",
                    "type",
                    "price",
                    "cost",
                    "barcode",
                    "weight",
                    "application",
                    "vendor",
                    "core_charge_product_code",
                ]:
                    canonical_value = _clean_text(canonical_payload.get(key, ""))
                    if canonical_value:
                        merged[key] = canonical_value
                resolved_html = canonical_html
            merged["search_url"] = target_url
            merged["product_url"] = canonical_product_url
            merged["source_url"] = canonical_product_url
            resolved_url = canonical_product_url

        resolved_path = urllib.parse.urlparse(resolved_url).path.lower()
        title_lower = _clean_text(merged.get("title", "")).lower()
        if (
            resolved_url == target_url
            and ("/search" in resolved_path or "search results" in title_lower)
            and not canonical_product_path
        ):
            detail = _clean_text(merged.get("product_link_error", ""))
            if detail:
                last_error = f"No SKU-specific product result found from search page. {detail}"
            else:
                last_error = "No SKU-specific product result found from search page."
            continue

        if scrape_images and image_output_root is not None:
            media_values = _split_multi_value(merged.get("media_urls", ""))
            if media_values:
                local_files, media_folder, image_error = _download_images_for_sku(
                    sku=sku,
                    media_urls=media_values,
                    image_output_root=image_output_root,
                    vendor_hint=_clean_text(merged.get("vendor", "")),
                )
                if media_folder:
                    merged["media_folder"] = media_folder
                if local_files:
                    merged["media_local_files"] = " | ".join(local_files)
                if image_error and not local_files:
                    merged["image_download_error"] = image_error
        return sku, merged, None
    return sku, {}, last_error or "Failed to fetch search page"


def scrape_vendor_records(
    vendor_search_url: str,
    skus: list[str],
    workers: int = 3,
    retry_count: int = 2,
    delay_seconds: float = 0.35,
    scrape_images: bool = True,
    image_output_root: str | Path | None = None,
    search_terms_by_sku: dict[str, str] | None = None,
    requested_fields: set[str] | list[str] | tuple[str, ...] | None = None,
) -> tuple[dict[str, dict[str, str]], dict[str, str], list[str]]:
    sku_values = [normalize_sku(sku) for sku in skus if normalize_sku(sku)]
    if not vendor_search_url or not sku_values:
        return {}, {}, []
    search_term_lookup = {
        normalize_sku(key): normalize_sku(value) or normalize_sku(key)
        for key, value in (search_terms_by_sku or {}).items()
        if normalize_sku(key)
    }

    vendor_search_url, resolver_profile = resolve_canonical_search_url(vendor_search_url)

    unresolved_vendor = _match_unresolved_vendor(vendor_search_url)
    if unresolved_vendor is not None:
        error_text = _format_unresolved_vendor_error(unresolved_vendor)
        return (
            {},
            {sku: error_text for sku in sku_values},
            [f"Vendor marked unresolved; skipped scrape for {_clean_text(unresolved_vendor.get('display_name') or unresolved_vendor.get('vendor'))}."],
        )

    def _is_suffix_variant_sku(value: str) -> bool:
        text = normalize_sku(value)
        return bool(re.search(r"\d[A-Z]$", text))

    # Process base SKUs before suffixed variants (e.g., ABC123 before ABC123A)
    # to maximize useful hits when vendors enforce aggressive rate limits.
    ordered_skus = sorted(sku_values, key=lambda item: (1 if _is_suffix_variant_sku(item) else 0, item))

    results: dict[str, dict[str, str]] = {}
    sku_errors: dict[str, str] = {}
    general_errors: list[str] = []
    max_workers = max(1, workers)
    effective_delay = float(delay_seconds or 0.0)
    normalized_requested_fields = _normalize_requested_scrape_fields(requested_fields)
    effective_scrape_images = bool(
        scrape_images and (normalized_requested_fields is None or "media_urls" in normalized_requested_fields)
    )

    if resolver_profile is not None:
        if resolver_profile.blocking_risk.lower() == "high":
            max_workers = 1
            effective_delay = max(effective_delay, 0.95)
        elif resolver_profile.blocking_risk.lower() == "medium":
            max_workers = min(max_workers, 2)
            effective_delay = max(effective_delay, 0.55)
        elif resolver_profile.browser_required.lower() == "yes":
            max_workers = min(max_workers, 2)
            effective_delay = max(effective_delay, 0.35)

    # Preflight the vendor endpoint once; if immediately throttled/challenged,
    # switch to safer pacing for this batch instead of burning all SKUs.
    preflight_seed = search_term_lookup.get(ordered_skus[0], ordered_skus[0]) if ordered_skus else ""
    direct_searchspring_site_ids = _searchspring_site_ids_for_vendor_url(vendor_search_url)
    preflight_url = ""
    if preflight_seed and not direct_searchspring_site_ids:
        preflight_url = _normalize_url(vendor_search_url, preflight_seed)
    if preflight_url:
        preflight_error = ""
        for attempt in range(2):
            _, preflight_error = _fetch_html(preflight_url)
            if not preflight_error:
                break
            if not _is_rate_limit_error(preflight_error) and "bot challenge" not in preflight_error.lower():
                break
            time.sleep(0.85 + (attempt * 0.65))
        if preflight_error and (_is_rate_limit_error(preflight_error) or "bot challenge" in preflight_error.lower()):
            max_workers = 1
            effective_delay = max(effective_delay, 0.95)
            general_errors.append("Vendor throttling detected; using safe scrape mode for this run.")

    output_root: Path | None = None
    if image_output_root is not None:
        try:
            output_root = Path(image_output_root).resolve()
            output_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            output_root = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _scrape_single_sku,
                sku=sku,
                base_url=vendor_search_url,
                resolver_profile=resolver_profile,
                retry_count=retry_count,
                delay_seconds=effective_delay,
                scrape_images=effective_scrape_images,
                image_output_root=output_root,
                search_term=search_term_lookup.get(sku, sku),
                requested_fields=normalized_requested_fields,
            )
            for sku in ordered_skus
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                sku, payload, error = future.result()
            except Exception as exc:
                general_errors.append(str(exc))
                continue
            if payload:
                results[sku] = _filter_requested_scrape_payload(payload, normalized_requested_fields)
            if error:
                sku_errors[sku] = error

    rate_limited_skus = [
        sku for sku, error_text in list(sku_errors.items()) if _is_rate_limit_error(error_text)
    ]
    if rate_limited_skus:
        heavy_rate_limit = len(rate_limited_skus) >= 5
        retry_delay = max(effective_delay, 0.95 if heavy_rate_limit else 0.65)
        # Keep fallback bounded and fast: do one clean retry pass per SKU.
        retry_count_adaptive = 0
        if heavy_rate_limit:
            # Give aggressive anti-bot systems a brief cool-down window before
            # retrying sequentially.
            time.sleep(1.35)
        fallback_start = time.monotonic()
        max_fallback_seconds = min(45.0, max(15.0, float(len(rate_limited_skus)) * 2.5))
        recovered = 0
        still_rate_limited = 0
        for sku in rate_limited_skus:
            if (time.monotonic() - fallback_start) >= max_fallback_seconds:
                general_errors.append(
                    f"Stopped rate-limit recovery early after {max_fallback_seconds:.0f}s to keep runtime bounded."
                )
                break
            _, payload, error = _scrape_single_sku(
                sku=sku,
                base_url=vendor_search_url,
                resolver_profile=resolver_profile,
                retry_count=retry_count_adaptive,
                delay_seconds=retry_delay,
                scrape_images=effective_scrape_images,
                image_output_root=output_root,
                search_term=search_term_lookup.get(sku, sku),
                requested_fields=normalized_requested_fields,
            )
            if payload:
                results[sku] = _filter_requested_scrape_payload(payload, normalized_requested_fields)
                sku_errors.pop(sku, None)
                recovered += 1
                continue
            if error:
                sku_errors[sku] = error
                if _is_rate_limit_error(error):
                    still_rate_limited += 1
        if recovered > 0:
            general_errors.append(f"Recovered {recovered} SKU(s) after temporary rate limits.")
        if still_rate_limited > 0:
            general_errors.append(
                f"{still_rate_limited} SKU(s) remain rate-limited. Try workers=1 and delay>=1.0 for this vendor."
            )

    return results, sku_errors, general_errors
