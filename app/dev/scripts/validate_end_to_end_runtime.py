from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

from validate_runtime_by_family import _build_sample_sets, _clean, _load_csv, _select_rows_by_family

from product_prospector.core.blog_tagging import is_valid_product_tag, load_tag_catalog, suggest_tags_for_product
from product_prospector.core.normalization import normalize_product
from product_prospector.core.processing import normalize_sku
from product_prospector.core.scraper_engine import scrape_vendor_records
from product_prospector.core.session_state import AppSession, MODE_NEW
from product_prospector.core.shopify_collections import resolve_collection_assignments
from product_prospector.core.type_mapping_engine import TypeCategoryMapper
from product_prospector.core.workflow_build import build_products_from_session


OWNER_TAG_KEYS = {"josh", "andrew", "alondra", "mike k", "michael v"}

SUMMARY_FIELDS = [
    "vendor",
    "display_name",
    "search_family",
    "interaction_strategy",
    "runtime_preference",
    "blocking_risk",
    "verification_level",
    "search_url_template",
    "tested_skus",
    "tested_sku_count",
    "built_products",
    "payload_success_count",
    "scrape_success_count",
    "unresolved_count",
    "failed_count",
    "core_complete_count",
    "title_count",
    "description_count",
    "media_count",
    "vendor_count",
    "type_count",
    "google_type_count",
    "category_count",
    "subtype_count",
    "removed_count",
    "excluded_count",
    "end_to_end_status",
    "dominant_error",
    "general_warnings",
    "runtime_seconds",
]

DETAIL_FIELDS = [
    "vendor",
    "display_name",
    "search_family",
    "interaction_strategy",
    "runtime_preference",
    "blocking_risk",
    "verification_level",
    "sku",
    "scrape_status",
    "scrape_fields_found",
    "scrape_error",
    "search_provider",
    "product_url",
    "title",
    "vendor_value",
    "type",
    "google_product_type",
    "category_code",
    "product_subtype",
    "description_found",
    "media_count",
    "collections",
    "tags",
    "core_complete",
    "remove_marked",
    "remove_reason",
    "excluded",
    "exclusion_reason",
]


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _collection_titles_text_from_targets(targets: list[dict]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for target in targets:
        title = str((target or {}).get("collection_title", "") or "").strip()
        key = re.sub(r"\s+", " ", title).strip().lower()
        if not title or not key or key in seen:
            continue
        seen.add(key)
        ordered.append(title)
    return ", ".join(ordered)


def _apply_tag_suggestions(product, required_root: Path, tag_catalog: list[str]) -> None:
    base_tags: list[str] = []
    seen_tags: set[str] = set()
    for raw_tag in list(getattr(product, "tags", []) or []):
        tag_value = str(raw_tag or "").strip()
        tag_key = tag_value.lower()
        if not tag_value or not is_valid_product_tag(tag_value) or tag_key in seen_tags or tag_key in OWNER_TAG_KEYS:
            continue
        seen_tags.add(tag_key)
        base_tags.append(tag_value)

    suggested_tags = suggest_tags_for_product(
        title=str(getattr(product, "title", "") or ""),
        description_html=str(getattr(product, "description_html", "") or ""),
        application=str(getattr(product, "application", "") or ""),
        vendor=str(getattr(product, "vendor", "") or ""),
        product_type=str(getattr(product, "type", "") or ""),
        tags_list=tag_catalog,
        max_tags=2,
    )
    for tag_value in suggested_tags:
        tag_key = str(tag_value or "").strip().lower()
        if not tag_value or not is_valid_product_tag(tag_value) or tag_key in seen_tags or tag_key in OWNER_TAG_KEYS:
            continue
        seen_tags.add(tag_key)
        base_tags.append(tag_value)
    product.tags = base_tags


def _apply_scrape_diagnostics(products, target_skus: list[str], should_scrape: bool, scrape_records, scrape_sku_errors) -> None:
    records = {normalize_sku(sku): dict(payload or {}) for sku, payload in (scrape_records or {}).items()}
    errors = {normalize_sku(sku): str(error or "").strip() for sku, error in (scrape_sku_errors or {}).items()}
    product_by_sku = {normalize_sku(product.sku): product for product in products if normalize_sku(product.sku)}
    found_fields_order = [
        "title",
        "description_html",
        "media_urls",
        "price",
        "map_price",
        "msrp_price",
        "jobber_price",
        "cost",
        "dealer_cost",
        "core_charge_product_code",
        "barcode",
        "weight",
        "application",
        "vendor",
        "media_local_files",
    ]

    for sku in [normalize_sku(value) for value in target_skus if normalize_sku(value)]:
        product = product_by_sku.get(sku)
        if product is None:
            continue
        if not should_scrape:
            product.scrape_status = "not run"
            product.scrape_fields_found = ""
            product.scrape_error = ""
            product.scrape_mismatch_error = ""
            continue

        payload = records.get(sku, {})
        if payload:
            found = [field for field in found_fields_order if str(payload.get(field, "")).strip()]
            provider_value = str(payload.get("search_provider", "")).strip().lower()
            if provider_value.endswith("_fuzzy"):
                found.append("fuzzy_match")
            product.scrape_status = "success check"
            product.scrape_fields_found = ", ".join(found)
            parse_error = str(payload.get("extract_error", "")).strip()
            image_error = str(payload.get("image_download_error", "")).strip()
            fuzzy_warning = ""
            if provider_value.endswith("_fuzzy"):
                fuzzy_warning = "Fuzzy SKU match from search provider. Verify product selection."
            product.scrape_error = image_error or parse_error or fuzzy_warning
            product.scrape_mismatch_error = ""
            continue

        error_text = errors.get(sku, "") or "No scrape data found"
        product.scrape_status = "unresolved" if error_text.lower().startswith("unresolved vendor search route:") else "fail X"
        product.scrape_fields_found = ""
        product.scrape_error = error_text
        product.scrape_mismatch_error = ""


def _build_session(
    sample_skus: list[str],
    search_url_template: str,
    chrome_workers: int,
    retry_count: int,
    delay_seconds: float,
    scrape_images: bool,
) -> AppSession:
    session = AppSession()
    session.mode = MODE_NEW
    session.target_skus = list(sample_skus)
    session.missing_fields = ["title", "description_html", "media_urls", "vendor", "type", "application", "product_url"]
    session.source_mapping.sku = "sku"
    session.scrape_settings.vendor_search_url = search_url_template
    session.scrape_settings.chrome_workers = max(1, int(chrome_workers))
    session.scrape_settings.retry_count = max(0, int(retry_count))
    session.scrape_settings.delay_seconds = max(0.0, float(delay_seconds))
    session.scrape_settings.scrape_images = bool(scrape_images)
    session.scrape_settings.force_scrape = True
    session.inventory_default = 5_000_000
    return session


def _run_end_to_end_vendor(
    row: dict[str, str],
    sample_skus: list[str],
    required_root: Path,
    runtime_output_root: Path,
    chrome_workers: int,
    retry_count: int,
    delay_seconds: float,
    scrape_images: bool,
    type_mapper: TypeCategoryMapper,
    tag_catalog: list[str],
) -> tuple[dict[str, str], list[dict[str, str]], dict[str, object]]:
    vendor = _clean(row.get("vendor"))
    display_name = _clean(row.get("display_name"))
    family = _clean(row.get("search_family"))
    interaction = _clean(row.get("interaction_strategy"))
    runtime_pref = _clean(row.get("runtime_preference"))
    blocking_risk = _clean(row.get("blocking_risk"))
    verification_level = _clean(row.get("verification_level"))
    template = _clean(row.get("search_url_template"))
    started = time.monotonic()

    if not template or not sample_skus:
        summary = {
            "vendor": vendor,
            "display_name": display_name,
            "search_family": family,
            "interaction_strategy": interaction,
            "runtime_preference": runtime_pref,
            "blocking_risk": blocking_risk,
            "verification_level": verification_level,
            "search_url_template": template,
            "tested_skus": " | ".join(sample_skus),
            "tested_sku_count": str(len(sample_skus)),
            "built_products": "0",
            "payload_success_count": "0",
            "scrape_success_count": "0",
            "unresolved_count": "0",
            "failed_count": str(len(sample_skus)),
            "core_complete_count": "0",
            "title_count": "0",
            "description_count": "0",
            "media_count": "0",
            "vendor_count": "0",
            "type_count": "0",
            "google_type_count": "0",
            "category_count": "0",
            "subtype_count": "0",
            "removed_count": "0",
            "excluded_count": "0",
            "end_to_end_status": "missing_input",
            "dominant_error": "Missing search template or sample SKUs",
            "general_warnings": "",
            "runtime_seconds": f"{time.monotonic() - started:.2f}",
        }
        return summary, [], {"summary": summary, "details": []}

    session = _build_session(
        sample_skus=sample_skus,
        search_url_template=template,
        chrome_workers=chrome_workers,
        retry_count=retry_count,
        delay_seconds=delay_seconds,
        scrape_images=scrape_images,
    )
    vendor_image_root = runtime_output_root / "images" / re.sub(r"[^A-Za-z0-9._-]+", "_", vendor or display_name or "vendor")

    scrape_records, scrape_sku_errors, scrape_general_errors = scrape_vendor_records(
        vendor_search_url=session.scrape_settings.vendor_search_url,
        skus=session.target_skus,
        workers=session.scrape_settings.chrome_workers,
        retry_count=session.scrape_settings.retry_count,
        delay_seconds=session.scrape_settings.delay_seconds,
        scrape_images=session.scrape_settings.scrape_images,
        image_output_root=vendor_image_root,
    )

    products, _build_stats = build_products_from_session(
        session=session,
        existing_shopify_index={},
        scraped_records=scrape_records,
        required_root=required_root,
    )

    normalized_products = []
    for product in products:
        normalized = normalize_product(
            product=product,
            required_root=required_root,
            mode=session.mode,
            update_fields=set(),
            default_inventory=int(session.inventory_default or 5_000_000),
        )
        normalized = type_mapper.apply(product=normalized, allow_category_overwrite=True)
        if not str(getattr(normalized, "collections", "") or "").strip():
            collection_targets, _collection_warnings = resolve_collection_assignments(
                product_type=str(getattr(normalized, "type", "") or ""),
                application_text=str(getattr(normalized, "application", "") or ""),
                required_root=required_root,
                title_text=str(getattr(normalized, "title", "") or ""),
                description_text=str(getattr(normalized, "description_html", "") or ""),
            )
            auto_collections = _collection_titles_text_from_targets(collection_targets)
            if auto_collections:
                normalized.collections = auto_collections
        normalized.finalize_defaults()
        _apply_tag_suggestions(normalized, required_root=required_root, tag_catalog=tag_catalog)
        normalized_products.append(normalized)

    _apply_scrape_diagnostics(
        products=normalized_products,
        target_skus=session.target_skus,
        should_scrape=True,
        scrape_records=scrape_records,
        scrape_sku_errors=scrape_sku_errors,
    )

    detail_rows: list[dict[str, str]] = []
    product_by_sku = {normalize_sku(product.sku): product for product in normalized_products if normalize_sku(product.sku)}
    record_by_sku = {normalize_sku(sku): dict(payload or {}) for sku, payload in (scrape_records or {}).items()}
    error_by_sku = {normalize_sku(sku): str(error or "").strip() for sku, error in (scrape_sku_errors or {}).items()}

    counts = Counter()
    dominant_errors = Counter()
    for raw_sku in sample_skus:
        sku = normalize_sku(raw_sku)
        product = product_by_sku.get(sku)
        payload = record_by_sku.get(sku, {})
        scrape_error = ""
        if product is not None:
            scrape_error = _clean(getattr(product, "scrape_error", ""))
        if not scrape_error:
            scrape_error = error_by_sku.get(sku, "")

        if payload:
            counts["payload_success_count"] += 1
        if product is not None:
            counts["built_products"] += 1
            if _clean(getattr(product, "scrape_status", "")) == "success check":
                counts["scrape_success_count"] += 1
            elif _clean(getattr(product, "scrape_status", "")) == "unresolved":
                counts["unresolved_count"] += 1
            else:
                counts["failed_count"] += 1
            if _clean(getattr(product, "title", "")):
                counts["title_count"] += 1
            if _clean(getattr(product, "description_html", "")):
                counts["description_count"] += 1
            if list(getattr(product, "media_urls", []) or []):
                counts["media_count"] += 1
            if _clean(getattr(product, "vendor", "")):
                counts["vendor_count"] += 1
            if _clean(getattr(product, "type", "")):
                counts["type_count"] += 1
            if _clean(getattr(product, "google_product_type", "")):
                counts["google_type_count"] += 1
            if _clean(getattr(product, "category_code", "")):
                counts["category_count"] += 1
            if _clean(getattr(product, "product_subtype", "")):
                counts["subtype_count"] += 1
            if bool(getattr(product, "remove_marked", False)):
                counts["removed_count"] += 1
            if bool(getattr(product, "excluded", False)):
                counts["excluded_count"] += 1

        core_complete = False
        if product is not None:
            core_complete = bool(
                _clean(getattr(product, "product_url", ""))
                and _clean(getattr(product, "title", ""))
                and _clean(getattr(product, "vendor", ""))
                and _clean(getattr(product, "type", ""))
            )
        if core_complete:
            counts["core_complete_count"] += 1

        detail_rows.append(
            {
                "vendor": vendor,
                "display_name": display_name,
                "search_family": family,
                "interaction_strategy": interaction,
                "runtime_preference": runtime_pref,
                "blocking_risk": blocking_risk,
                "verification_level": verification_level,
                "sku": sku,
                "scrape_status": _clean(getattr(product, "scrape_status", "")) if product is not None else "missing_product",
                "scrape_fields_found": _clean(getattr(product, "scrape_fields_found", "")) if product is not None else "",
                "scrape_error": scrape_error,
                "search_provider": _clean(payload.get("search_provider", "")),
                "product_url": _clean(getattr(product, "product_url", "")) if product is not None else _clean(payload.get("product_url", "")),
                "title": _clean(getattr(product, "title", "")) if product is not None else _clean(payload.get("title", "")),
                "vendor_value": _clean(getattr(product, "vendor", "")) if product is not None else "",
                "type": _clean(getattr(product, "type", "")) if product is not None else "",
                "google_product_type": _clean(getattr(product, "google_product_type", "")) if product is not None else "",
                "category_code": _clean(getattr(product, "category_code", "")) if product is not None else "",
                "product_subtype": _clean(getattr(product, "product_subtype", "")) if product is not None else "",
                "description_found": "yes" if product is not None and _clean(getattr(product, "description_html", "")) else "",
                "media_count": str(len(list(getattr(product, "media_urls", []) or []))) if product is not None else "0",
                "collections": _clean(getattr(product, "collections", "")) if product is not None else "",
                "tags": " | ".join([str(tag).strip() for tag in list(getattr(product, "tags", []) or []) if str(tag).strip()]) if product is not None else "",
                "core_complete": "yes" if core_complete else "",
                "remove_marked": "yes" if product is not None and bool(getattr(product, "remove_marked", False)) else "",
                "remove_reason": _clean(getattr(product, "remove_reason", "")) if product is not None else "",
                "excluded": "yes" if product is not None and bool(getattr(product, "excluded", False)) else "",
                "exclusion_reason": _clean(getattr(product, "exclusion_reason", "")) if product is not None else "",
            }
        )
        if scrape_error:
            dominant_errors[scrape_error] += 1

    tested_count = len(sample_skus)
    if counts["scrape_success_count"] == tested_count and counts["core_complete_count"] == tested_count and tested_count > 0:
        end_to_end_status = "validated"
    elif counts["scrape_success_count"] > 0 or counts["core_complete_count"] > 0:
        end_to_end_status = "partial"
    elif counts["unresolved_count"] == tested_count and tested_count > 0:
        end_to_end_status = "unresolved"
    else:
        end_to_end_status = "failed"

    summary = {
        "vendor": vendor,
        "display_name": display_name,
        "search_family": family,
        "interaction_strategy": interaction,
        "runtime_preference": runtime_pref,
        "blocking_risk": blocking_risk,
        "verification_level": verification_level,
        "search_url_template": template,
        "tested_skus": " | ".join(sample_skus),
        "tested_sku_count": str(tested_count),
        "built_products": str(counts["built_products"]),
        "payload_success_count": str(counts["payload_success_count"]),
        "scrape_success_count": str(counts["scrape_success_count"]),
        "unresolved_count": str(counts["unresolved_count"]),
        "failed_count": str(counts["failed_count"]),
        "core_complete_count": str(counts["core_complete_count"]),
        "title_count": str(counts["title_count"]),
        "description_count": str(counts["description_count"]),
        "media_count": str(counts["media_count"]),
        "vendor_count": str(counts["vendor_count"]),
        "type_count": str(counts["type_count"]),
        "google_type_count": str(counts["google_type_count"]),
        "category_count": str(counts["category_count"]),
        "subtype_count": str(counts["subtype_count"]),
        "removed_count": str(counts["removed_count"]),
        "excluded_count": str(counts["excluded_count"]),
        "end_to_end_status": end_to_end_status,
        "dominant_error": dominant_errors.most_common(1)[0][0] if dominant_errors else "",
        "general_warnings": " | ".join([_clean(value) for value in scrape_general_errors if _clean(value)]),
        "runtime_seconds": f"{time.monotonic() - started:.2f}",
    }
    return summary, detail_rows, {"summary": summary, "details": detail_rows}


def _print_progress(index: int, total: int, summary: dict[str, str]) -> None:
    print(
        f"[{index}/{total}] {summary['vendor']}: "
        f"{summary['end_to_end_status']} "
        f"scrape={summary['scrape_success_count']}/{summary['tested_sku_count']} "
        f"core={summary['core_complete_count']}/{summary['tested_sku_count']}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an end-to-end runtime validation through scrape, build, normalize, and type mapping."
    )
    parser.add_argument("--profiles-input", default="app/required/mappings/discovery/VendorResolverProfiles.csv")
    parser.add_argument("--vendor-profiles", default="app/required/mappings/VendorProfiles.csv")
    parser.add_argument("--hints-input", default="app/required/mappings/VendorSkuPrefixHints.csv")
    parser.add_argument("--shopify-cache", default="app/config/shopify_sku_cache.csv")
    parser.add_argument("--summary-output", default="app/required/mappings/discovery/VendorEndToEndValidationSummary.csv")
    parser.add_argument("--detail-output", default="app/required/mappings/discovery/VendorEndToEndValidationDetails.csv")
    parser.add_argument("--json-output", default="app/required/mappings/discovery/VendorEndToEndValidation.json")
    parser.add_argument("--runtime-output-root", default="app/runtime_output/end_to_end_validation")
    parser.add_argument("--vendors-per-family", type=int, default=2)
    parser.add_argument("--max-samples", type=int, default=3)
    parser.add_argument("--vendor-workers", type=int, default=2)
    parser.add_argument("--chrome-workers", type=int, default=6)
    parser.add_argument("--retry-count", type=int, default=2)
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument("--family-filter", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--skip-image-downloads", action="store_true")
    args = parser.parse_args()

    profiles_input = Path(args.profiles_input).expanduser().resolve()
    vendor_profiles_path = Path(args.vendor_profiles).expanduser().resolve()
    hints_path = Path(args.hints_input).expanduser().resolve()
    shopify_cache_path = Path(args.shopify_cache).expanduser().resolve()
    summary_output = Path(args.summary_output).expanduser().resolve()
    detail_output = Path(args.detail_output).expanduser().resolve()
    json_output = Path(args.json_output).expanduser().resolve()
    runtime_output_root = Path(args.runtime_output_root).expanduser().resolve()
    required_root = DEV_ROOT.parent / "required"

    rows = _load_csv(profiles_input)
    selected_rows = _select_rows_by_family(
        rows=rows,
        vendors_per_family=max(1, int(args.vendors_per_family)),
        family_filter=args.family_filter,
    )
    if args.limit and args.limit > 0:
        selected_rows = selected_rows[: int(args.limit)]

    sample_sets = _build_sample_sets(
        rows=selected_rows,
        vendor_profiles_path=vendor_profiles_path,
        hints_path=hints_path,
        shopify_cache_path=shopify_cache_path,
        max_samples=max(1, int(args.max_samples)),
    )
    type_mapper = TypeCategoryMapper.from_required_root(required_root)
    tag_catalog = load_tag_catalog(required_root=required_root)
    total = len(selected_rows)

    summary_rows: list[dict[str, str]] = []
    detail_rows: list[dict[str, str]] = []
    json_rows: list[dict[str, object]] = []

    with ThreadPoolExecutor(max_workers=max(1, int(args.vendor_workers))) as executor:
        futures = {
            executor.submit(
                _run_end_to_end_vendor,
                row,
                sample_sets.get(_clean(row.get("vendor")), []),
                required_root,
                runtime_output_root,
                max(1, int(args.chrome_workers)),
                max(0, int(args.retry_count)),
                float(args.delay_seconds),
                not bool(args.skip_image_downloads),
                type_mapper,
                tag_catalog,
            ): row
            for row in selected_rows
        }
        completed = 0
        for future in as_completed(futures):
            summary, vendor_detail_rows, payload = future.result()
            summary_rows.append(summary)
            detail_rows.extend(vendor_detail_rows)
            json_rows.append(payload)
            completed += 1
            _print_progress(completed, total, summary)

    summary_rows.sort(key=lambda item: (_clean(item.get("search_family")), _clean(item.get("vendor")).lower()))
    detail_rows.sort(key=lambda item: (_clean(item.get("search_family")), _clean(item.get("vendor")).lower(), _clean(item.get("sku"))))
    json_rows.sort(key=lambda item: (_clean(item.get("summary", {}).get("search_family")), _clean(item.get("summary", {}).get("vendor")).lower()))

    _write_csv(summary_output, summary_rows, SUMMARY_FIELDS)
    _write_csv(detail_output, detail_rows, DETAIL_FIELDS)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    with json_output.open("w", encoding="utf-8") as handle:
        json.dump(json_rows, handle, indent=2, ensure_ascii=True)

    totals = Counter(_clean(row.get("end_to_end_status")) for row in summary_rows)
    print(
        f"Wrote {len(summary_rows)} vendor summaries and {len(detail_rows)} SKU details. "
        f"validated={totals.get('validated', 0)} "
        f"partial={totals.get('partial', 0)} "
        f"failed={totals.get('failed', 0)} "
        f"unresolved={totals.get('unresolved', 0)}"
    )


if __name__ == "__main__":
    main()
