from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import urllib.parse

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

import discover_vendor_search_urls as dv
from product_prospector.core.processing import normalize_sku


CSV_FIELDS = [
    "vendor",
    "display_name",
    "official_website_url",
    "search_url_template",
    "tested_skus",
    "tested_sku_count",
    "success_count",
    "success_ratio",
    "validation_status",
    "success_skus",
    "failed_skus",
    "first_product_url",
    "dominant_error",
    "warning_count",
    "first_warning",
    "runtime_seconds",
]


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_hint_samples(path: Path, alias_map: dict[str, str]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_vendor = _clean(row.get("shopify_vendor") or row.get("\ufeffshopify_vendor"))
            if not source_vendor:
                continue
            canonical_vendor = alias_map.get(dv._norm_key(source_vendor), source_vendor)
            values = output.setdefault(canonical_vendor, [])
            seen = {normalize_sku(value) for value in values if normalize_sku(value)}
            for field_name in ["sample_sku_1", "sample_sku_2", "sample_sku_3"]:
                sku = _clean(row.get(field_name))
                normalized = normalize_sku(sku)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                values.append(sku)
    return output


def _build_sample_sets(
    resolved_rows: list[dict[str, str]],
    vendor_profiles_path: Path,
    hints_path: Path,
    shopify_cache_path: Path,
    max_samples: int,
) -> dict[str, list[str]]:
    profiles = dv._load_vendor_profiles(vendor_profiles_path)
    alias_map = dv._build_alias_map(profiles)
    cache_samples = dv._load_vendor_sample_skus(shopify_cache_path, alias_map)
    hint_samples = _load_hint_samples(hints_path, alias_map)

    output: dict[str, list[str]] = {}
    for row in resolved_rows:
        vendor = _clean(row.get("vendor"))
        display_name = _clean(row.get("display_name"))
        preferred = [_clean(row.get("sample_sku"))]
        candidates = (
            preferred
            + hint_samples.get(vendor, [])
            + hint_samples.get(display_name, [])
            + cache_samples.get(vendor, [])
            + cache_samples.get(display_name, [])
        )
        chosen: list[str] = []
        seen: set[str] = set()
        for sku in candidates:
            normalized = normalize_sku(sku)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            chosen.append(sku)
            if len(chosen) >= max_samples:
                break
        output[vendor] = chosen
    return output


def _summarize_result_kind(payload: dict[str, str]) -> str:
    product_url = _clean(payload.get("product_url"))
    source_url = _clean(payload.get("source_url"))
    search_url = _clean(payload.get("search_url"))
    if product_url:
        return "product_page"
    if source_url and search_url and source_url != search_url:
        return "resolved_nonproduct"
    if search_url:
        return "search_results"
    return "payload_only"


def _render_template(template: str, sku: str) -> str:
    encoded = urllib.parse.quote(_clean(sku), safe="")
    return _clean(template).replace("{sku}", encoded)


def _validate_vendor(
    row: dict[str, str],
    sample_skus: list[str],
    request_timeout: int,
) -> tuple[dict[str, str], dict[str, object]]:
    vendor = _clean(row.get("vendor"))
    display_name = _clean(row.get("display_name"))
    website = _clean(row.get("official_website_url"))
    template = _clean(row.get("search_url_template"))
    started = time.monotonic()

    if not template:
        runtime_seconds = time.monotonic() - started
        summary = {
            "vendor": vendor,
            "display_name": display_name,
            "official_website_url": website,
            "search_url_template": template,
            "tested_skus": "",
            "tested_sku_count": "0",
            "success_count": "0",
            "success_ratio": "0.00",
            "validation_status": "missing_template",
            "success_skus": "",
            "failed_skus": "",
            "first_product_url": "",
            "dominant_error": "Missing search_url_template",
            "warning_count": "0",
            "first_warning": "",
            "runtime_seconds": f"{runtime_seconds:.2f}",
        }
        detail = {"summary": summary, "sku_results": []}
        return summary, detail

    if not sample_skus:
        runtime_seconds = time.monotonic() - started
        summary = {
            "vendor": vendor,
            "display_name": display_name,
            "official_website_url": website,
            "search_url_template": template,
            "tested_skus": "",
            "tested_sku_count": "0",
            "success_count": "0",
            "success_ratio": "0.00",
            "validation_status": "no_samples",
            "success_skus": "",
            "failed_skus": "",
            "first_product_url": "",
            "dominant_error": "No sample SKUs available",
            "warning_count": "0",
            "first_warning": "",
            "runtime_seconds": f"{runtime_seconds:.2f}",
        }
        detail = {"summary": summary, "sku_results": []}
        return summary, detail

    success_skus: list[str] = []
    failed_skus: list[str] = []
    dominant_error = ""
    first_product_url = ""
    general_errors: list[str] = []
    sku_results: list[dict[str, str]] = []

    for sku in sample_skus:
        probe_url = _render_template(template, sku)
        html, status, final_url, error = dv._fetch_url(probe_url, timeout=max(5, int(request_timeout)))
        result_kind, _, success, blocked, notes = dv._classify_probe(
            probe_url=probe_url,
            final_url=final_url,
            html=html,
            http_status=int(status or 0),
            error=error,
            sku=sku,
        )
        normalized = normalize_sku(sku)
        product_url = _clean(final_url or probe_url)
        error_text = _clean(notes or error)
        if success and not first_product_url:
            first_product_url = product_url
        if success:
            success_skus.append(sku)
        else:
            failed_skus.append(sku)
            if error_text and not dominant_error:
                dominant_error = error_text
            if blocked and error_text:
                general_errors.append(error_text)
        sku_results.append(
            {
                "sku": sku,
                "normalized_sku": normalized,
                "status": "success" if success else "fail",
                "result_kind": result_kind,
                "product_url": product_url,
                "fields_found": "",
                "error": error_text,
            }
        )

    tested_count = len(sample_skus)
    success_count = len(success_skus)
    success_ratio = (success_count / tested_count) if tested_count else 0.0
    if success_count == tested_count and tested_count >= 3:
        validation_status = "validated"
    elif success_count > 0:
        validation_status = "partial"
    else:
        validation_status = "failed"

    runtime_seconds = time.monotonic() - started
    summary = {
        "vendor": vendor,
        "display_name": display_name,
        "official_website_url": website,
        "search_url_template": template,
        "tested_skus": " | ".join(sample_skus),
        "tested_sku_count": str(tested_count),
        "success_count": str(success_count),
        "success_ratio": f"{success_ratio:.2f}",
        "validation_status": validation_status,
        "success_skus": " | ".join(success_skus),
        "failed_skus": " | ".join(failed_skus),
        "first_product_url": first_product_url,
        "dominant_error": dominant_error,
        "warning_count": str(len(general_errors)),
        "first_warning": _clean(general_errors[0]) if general_errors else "",
        "runtime_seconds": f"{runtime_seconds:.2f}",
    }
    detail = {
        "summary": summary,
        "general_errors": general_errors,
        "sku_results": sku_results,
    }
    return summary, detail


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate resolved vendor search templates against 3-4 live SKUs.")
    parser.add_argument("--resolved-input", default="app/required/mappings/discovery/VendorDiscoveryResolvedWorklist.csv")
    parser.add_argument("--vendor-profiles", default="app/required/mappings/VendorProfiles.csv")
    parser.add_argument("--hints-input", default="app/required/mappings/VendorSkuPrefixHints.csv")
    parser.add_argument("--shopify-cache", default="app/config/shopify_sku_cache.csv")
    parser.add_argument("--csv-output", default="app/required/mappings/discovery/VendorResolverValidation.csv")
    parser.add_argument("--json-output", default="app/required/mappings/discovery/VendorResolverValidation.json")
    parser.add_argument("--vendor-workers", type=int, default=6)
    parser.add_argument("--max-samples", type=int, default=4)
    parser.add_argument("--request-timeout", type=int, default=20)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--vendor-filter", default="")
    args = parser.parse_args()

    resolved_path = Path(args.resolved_input).expanduser().resolve()
    vendor_profiles_path = Path(args.vendor_profiles).expanduser().resolve()
    hints_path = Path(args.hints_input).expanduser().resolve()
    shopify_cache_path = Path(args.shopify_cache).expanduser().resolve()
    csv_output_path = Path(args.csv_output).expanduser().resolve()
    json_output_path = Path(args.json_output).expanduser().resolve()

    resolved_rows = _load_csv(resolved_path)
    sample_sets = _build_sample_sets(
        resolved_rows=resolved_rows,
        vendor_profiles_path=vendor_profiles_path,
        hints_path=hints_path,
        shopify_cache_path=shopify_cache_path,
        max_samples=max(1, int(args.max_samples)),
    )

    vendor_filter = _clean(args.vendor_filter).lower()
    if vendor_filter:
        resolved_rows = [
            row
            for row in resolved_rows
            if vendor_filter in _clean(row.get("vendor")).lower() or vendor_filter in _clean(row.get("display_name")).lower()
        ]
    if args.limit and args.limit > 0:
        resolved_rows = resolved_rows[: int(args.limit)]

    summaries: list[dict[str, str]] = []
    details: list[dict[str, object]] = []
    total = len(resolved_rows)

    with ThreadPoolExecutor(max_workers=max(1, int(args.vendor_workers))) as executor:
        futures = {
            executor.submit(
                _validate_vendor,
                row,
                sample_sets.get(_clean(row.get("vendor")), []),
                max(5, int(args.request_timeout)),
            ): row
            for row in resolved_rows
        }
        completed = 0
        for future in as_completed(futures):
            row = futures[future]
            vendor = _clean(row.get("vendor"))
            summary, detail = future.result()
            summaries.append(summary)
            details.append(detail)
            completed += 1
            print(
                f"[{completed}/{total}] {vendor}: {summary['validation_status']} "
                f"({summary['success_count']}/{summary['tested_sku_count']})"
            )

    summaries.sort(key=lambda item: (_clean(item.get("validation_status")), _clean(item.get("vendor")).lower()))
    details.sort(key=lambda item: _clean(item.get("summary", {}).get("vendor")).lower())

    _write_csv(csv_output_path, summaries)
    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    with json_output_path.open("w", encoding="utf-8") as handle:
        json.dump(details, handle, indent=2, ensure_ascii=True)

    validated = sum(1 for row in summaries if row.get("validation_status") == "validated")
    partial = sum(1 for row in summaries if row.get("validation_status") == "partial")
    failed = sum(1 for row in summaries if row.get("validation_status") == "failed")
    print(
        f"Wrote {len(summaries)} validation rows to {csv_output_path}. "
        f"validated={validated} partial={partial} failed={failed}"
    )


if __name__ == "__main__":
    main()
