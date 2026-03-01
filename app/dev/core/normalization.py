from __future__ import annotations

import re
from pathlib import Path

from product_prospector.core.pricing_rules import (
    calculate_cost_from_price,
    choose_best_discount,
    find_vendor_discount_file,
    load_vendor_discounts,
)
from product_prospector.core.core_charge_codes import normalize_core_charge_product_code
from product_prospector.core.processing import normalize_sku
from product_prospector.core.product_model import Product
from product_prospector.core.vendor_profiles import resolve_vendor_profile
from product_prospector.core.vendor_normalization import (
    normalize_vendor_name as normalize_vendor_from_rules,
    resolve_vendor_title_name,
)


_HTML_STRIP_RE = re.compile(r"<[^>]+>")


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _normalize_description_html(text: str) -> str:
    value = _clean_text(text)
    if not value:
        return ""
    # Remove potentially unsafe script/style blocks before preserving simple HTML text.
    value = re.sub(r"<script[\s\S]*?</script>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"<style[\s\S]*?</style>", "", value, flags=re.IGNORECASE)
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    return value.strip()


def _normalize_vendor_name(vendor: str, required_root: Path | None = None) -> str:
    value = _clean_text(vendor)
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    normalized = normalize_vendor_from_rules(value.strip(), required_root=required_root)
    return _clean_text(normalized) or value.strip()


def _normalize_prefix_token(value: str) -> str:
    token = _clean_text(value).upper()
    token = re.sub(r"[^A-Z0-9]+", "", token)
    return token


def _apply_vendor_sku_prefix(raw_sku: str, sku_prefix: str) -> str:
    sku = normalize_sku(raw_sku)
    prefix = _normalize_prefix_token(sku_prefix)
    if not sku or not prefix:
        return sku
    if sku.startswith(f"{prefix}-") or sku == prefix or sku.startswith(prefix):
        return sku
    if "-" in sku:
        # SKU already appears prefixed; avoid overriding user-provided patterns.
        return sku
    return f"{prefix}-{sku}"


def _sanitize_text_for_parse(value: str) -> str:
    text = _clean_text(_HTML_STRIP_RE.sub(" ", value))
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _short_year_token(year_text: str) -> str:
    text = _clean_text(year_text)
    if not text:
        return ""
    half = ".5" if ".5" in text else ""
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if len(digits) >= 4:
        value = digits[-2:]
    elif len(digits) == 2:
        value = digits
    else:
        return ""
    return f"{value}{half}"


def _expand_year_token_to_full(year_text: str) -> str:
    text = _clean_text(year_text)
    if not text:
        return ""
    half = ".5" if ".5" in text else ""
    year_part = text.split(".", 1)[0] if "." in text else text
    digits = re.sub(r"[^0-9]", "", year_part)
    if not digits:
        return ""
    if len(digits) >= 4:
        return f"{digits[:4]}{half}"
    if len(digits) == 2:
        yy = int(digits)
        prefix = "19" if yy >= 90 else "20"
        return f"{prefix}{digits}{half}"
    return ""


def _extract_year_ranges(text: str) -> list[str]:
    source = _sanitize_text_for_parse(text)
    if not source:
        return []
    pattern = re.compile(
        r"\b(\d{2,4}(?:\.5)?)\s*[-/]\s*(\d{2,4}(?:\.5)?)\b",
        flags=re.IGNORECASE,
    )
    output: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(source):
        start = _short_year_token(match.group(1))
        end = _short_year_token(match.group(2))
        if not start or not end:
            continue
        token = f"{start}-{end}"
        if token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def _expand_short_year_ranges(text: str) -> str:
    source = _sanitize_text_for_parse(text)
    if not source:
        return ""
    pattern = re.compile(r"\b(\d{2,4}(?:\.5)?)\s*-\s*(\d{2,4}(?:\.5)?)\b", flags=re.IGNORECASE)

    def repl(match: re.Match) -> str:
        start = _expand_year_token_to_full(match.group(1))
        end = _expand_year_token_to_full(match.group(2))
        if not start or not end:
            return match.group(0)
        return f"{start}-{end}"

    return pattern.sub(repl, source)


def _derive_application_from_title(title_text: str) -> str:
    source = _sanitize_text_for_parse(title_text)
    if not source:
        return ""
    pattern = re.compile(r"\b\d{2,4}(?:\.5)?\s*-\s*\d{2,4}(?:\.5)?\b", flags=re.IGNORECASE)
    match = pattern.search(source)
    if not match:
        return ""
    fitment = source[match.start() :].strip(" -")
    fitment = _expand_short_year_ranges(fitment)
    fitment = re.sub(r"\s+", " ", fitment).strip()
    return fitment


def _detect_make_label(text: str) -> str:
    low = _sanitize_text_for_parse(text).lower()
    if re.search(r"\b(chevy|chevrolet|gmc|gm)\b", low):
        return "GM"
    if re.search(r"\b(ram|dodge)\b", low):
        return "Ram"
    if re.search(r"\b(ford)\b", low):
        return "Ford"
    return ""


def _detect_engine_liter(text: str) -> str:
    source = _sanitize_text_for_parse(text)
    match = re.search(r"\b(\d\.\d)\s*l\b", source, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}L"


def _detect_drive_family(text: str, make_label: str) -> str:
    low = _sanitize_text_for_parse(text).lower()
    if make_label == "Ford":
        if "super duty" in low or "superduty" in low:
            return "Super Duty"
        return "Powerstroke"
    if make_label == "GM":
        if re.search(r"\b(2500hd\s*/\s*3500|2500hd|3500)\b", low) and "duramax" not in low:
            return "2500HD / 3500"
        return "Duramax"
    if make_label == "Ram":
        if re.search(r"\b(2500\s*/\s*3500|2500|3500)\b", low) and "cummins" not in low:
            return "2500 / 3500"
        return "Cummins"
    return ""


def _is_universal_fitment(text: str) -> bool:
    low = _sanitize_text_for_parse(text).lower()
    return any(
        token in low
        for token in [
            "universal",
            "all makes",
            "all models",
            "fits all",
            "multi fit",
            "multi-fit",
        ]
    )


def _build_fitment_suffix(application: str, title: str, description: str) -> str:
    combined = " ".join([application, title, description]).strip()
    if not combined or _is_universal_fitment(combined):
        return ""

    years = _extract_year_ranges(application) or _extract_year_ranges(title) or _extract_year_ranges(description)
    make_label = _detect_make_label(combined)
    liter = _detect_engine_liter(combined)
    drive = _detect_drive_family(combined, make_label)

    parts: list[str] = []
    if years:
        parts.append(years[0])
    if make_label:
        parts.append(make_label)
    if liter:
        parts.append(liter)
    if drive:
        parts.append(drive)

    return " ".join([item for item in parts if item]).strip()


def _strip_vendor_tokens(text: str, vendor_tokens: list[str]) -> str:
    output = f" {text} "
    for token in vendor_tokens:
        key = _sanitize_text_for_parse(token)
        if not key:
            continue
        pattern = re.compile(re.escape(key), flags=re.IGNORECASE)
        output = pattern.sub(" ", output)
    return _sanitize_text_for_parse(output)


def _build_concise_description(
    title: str,
    application: str,
    description: str,
    vendor_tokens: list[str],
) -> str:
    source = _sanitize_text_for_parse(title)
    if not source:
        source = _sanitize_text_for_parse(description)
    if not source:
        return ""

    source = _strip_vendor_tokens(source, vendor_tokens)
    source = re.sub(r"\b\d{2,4}(?:\.5)?\s*[-/]\s*\d{2,4}(?:\.5)?\b", " ", source)
    source = re.sub(r"\b(?:ford|gm|gmc|chevy|chevrolet|ram|dodge)\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(r"\b\d\.\d\s*l\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(r"\b(?:powerstroke|duramax|cummins|super\s*duty)\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(r"\b(?:lly|lbz|lmm|lb7|l5p|lml|6r140|68rfe|48re|47re)(?:/[a-z0-9]+)*\b", " ", source, flags=re.IGNORECASE)
    source = re.sub(
        r"\b(?:no\s+valve\s+seat\s+machining|no\s+seats?\s+or\s+valves?|w/?o\s+valves?)\b",
        " ",
        source,
        flags=re.IGNORECASE,
    )
    source = re.sub(r"[|]+", " ", source)
    source = source.replace(" - ", " ")
    source = source.replace("-", " ")
    source = re.sub(r"\s+", " ", source).strip(" ,;/")

    if not source:
        return ""

    words = source.split(" ")
    if len(words) > 9:
        words = words[:9]
    concise = " ".join(words)
    concise = re.sub(r"\s+", " ", concise).strip()
    return concise


def _normalize_title(
    vendor: str,
    title: str,
    application: str,
    description: str,
    required_root: Path | None = None,
) -> str:
    canonical_vendor = _normalize_vendor_name(vendor, required_root=required_root)
    title_vendor = resolve_vendor_title_name(canonical_vendor or vendor, required_root=required_root)
    brand_label = title_vendor or canonical_vendor or _clean_text(vendor)

    vendor_tokens = [vendor, canonical_vendor, title_vendor]
    concise = _build_concise_description(
        title=title,
        application=application,
        description=description,
        vendor_tokens=[token for token in vendor_tokens if _clean_text(token)],
    )
    fitment = _build_fitment_suffix(application=application, title=title, description=description)

    parts: list[str] = []
    if brand_label:
        parts.append(brand_label)
    if concise:
        parts.append(concise)
    if fitment:
        parts.append(fitment)

    if not parts:
        return _sanitize_text_for_parse(title)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _normalize_media_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in values:
        url = _clean_text(raw)
        if not url:
            continue
        if url.lower().startswith("www."):
            url = f"https://{url}"
        if not re.match(r"^https?://", url, flags=re.IGNORECASE):
            continue
        if url in seen:
            continue
        seen.add(url)
        normalized.append(url)
    return normalized


def _normalize_weight(weight: str) -> str:
    value = _to_float(weight)
    if value is None:
        return "2"
    value = max(2.0, min(value, 149.0))
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _format_currency(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _normalize_currency_text(value: str) -> str:
    return _format_currency(_to_float(value))


def _resolve_effective_price(product: Product) -> tuple[str, str]:
    map_value = _to_float(product.map_price)
    base_value = _to_float(product.price)
    msrp_value = _to_float(product.msrp_price)
    jobber_value = _to_float(product.jobber_price)

    if map_value is not None:
        return _format_currency(map_value), "map_price"
    if base_value is not None:
        return _format_currency(base_value), "price"
    if msrp_value is not None:
        return _format_currency(msrp_value), "msrp_price"
    if jobber_value is not None:
        return _format_currency(jobber_value), "jobber_price"
    return "", "missing"


def _infer_cost_if_missing(product: Product, discounts_root: Path) -> tuple[str, str]:
    price_value = _to_float(product.price)
    if price_value is None:
        return "", "missing_price"
    if _to_float(product.cost) is not None:
        return _format_currency(_to_float(product.cost)), "already_present"

    discount_file = find_vendor_discount_file(discounts_root)
    if discount_file is None:
        return "", "discount_file_missing"

    discounts_df = load_vendor_discounts(discount_file)
    profile = resolve_vendor_profile(product.vendor, required_root=discounts_root)
    vendor_for_discount = (
        (profile.discount_vendor_key if profile is not None else "")
        or normalize_vendor_from_rules(product.vendor, required_root=discounts_root)
        or product.vendor
    )
    discount_percent, _, status = choose_best_discount(
        discounts_df=discounts_df,
        vendor_name=vendor_for_discount,
        product_title=product.title,
        product_type=product.type,
    )
    if discount_percent is None:
        return "", status
    inferred = calculate_cost_from_price(price=price_value, discount_percent=discount_percent)
    return _format_currency(inferred), "discount_applied"


def normalize_product(
    product: Product,
    required_root: Path,
    mode: str = "new",
    update_fields: set[str] | None = None,
) -> Product:
    selected = set(update_fields or [])

    def should(field_name: str) -> bool:
        if mode != "update":
            return True
        return field_name in selected

    normalized_vendor = _normalize_vendor_name(product.vendor, required_root=required_root)
    profile = resolve_vendor_profile(normalized_vendor or product.vendor, required_root=required_root)
    profile_vendor = _clean_text(profile.shopify_vendor_value) if profile is not None else ""
    if not profile_vendor:
        profile_vendor = _clean_text(profile.canonical_vendor) if profile is not None else ""
    profile_brand = _clean_text(profile.brand_name) if profile is not None else ""
    profile_sku_prefix = _clean_text(profile.sku_prefix) if profile is not None else ""

    if should("vendor"):
        product.vendor = profile_vendor or normalized_vendor
    if should("application"):
        product.application = _clean_text(product.application)
    if should("description_html"):
        product.description_html = _normalize_description_html(product.description_html)
    if should("title"):
        product.title = _normalize_title(
            vendor=product.vendor,
            title=product.title,
            application=product.application,
            description=product.description_html,
            required_root=required_root,
        )
    if should("application") and mode != "update":
        derived_application = _derive_application_from_title(product.title)
        if derived_application:
            product.application = derived_application
    if should("media_urls"):
        product.media_urls = _normalize_media_urls(product.media_urls)
    if should("price"):
        product.map_price = _normalize_currency_text(product.map_price)
        product.msrp_price = _normalize_currency_text(product.msrp_price)
        product.jobber_price = _normalize_currency_text(product.jobber_price)
        resolved_price, price_source = _resolve_effective_price(product)
        product.price = resolved_price
        product.field_sources["price_rule"] = price_source
    if should("weight"):
        product.weight = _normalize_weight(product.weight)
    if mode != "update" or "core_charge_product_code" in selected:
        product.core_charge_product_code = normalize_core_charge_product_code(
            product.core_charge_product_code,
            required_root=required_root,
        )
    product.inventory = 3000000

    if mode != "update":
        product.sku = _apply_vendor_sku_prefix(product.sku, profile_sku_prefix)
        if not _clean_text(product.brand) and profile_brand:
            product.brand = profile_brand

    if should("cost"):
        cost_value = _to_float(product.cost)
        dealer_cost_value = _to_float(product.dealer_cost)
        if dealer_cost_value is not None:
            product.dealer_cost = _format_currency(dealer_cost_value)

        known_costs = [value for value in [cost_value, dealer_cost_value] if value is not None]
        if known_costs:
            lowest_cost = min(known_costs)
            product.cost = _format_currency(lowest_cost)
            if dealer_cost_value is not None and (cost_value is None or dealer_cost_value <= cost_value):
                product.field_sources["cost_inference"] = "dealer_lowest"
            else:
                product.field_sources["cost_inference"] = "already_present"
        else:
            inferred_cost, reason = _infer_cost_if_missing(product, required_root)
            if inferred_cost:
                product.cost = inferred_cost
            product.field_sources["cost_inference"] = reason

    product.finalize_defaults()
    return product
