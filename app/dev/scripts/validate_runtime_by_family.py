from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

import discover_vendor_search_urls as dv
from product_prospector.core.processing import normalize_sku
from product_prospector.core.scraper_engine import scrape_vendor_records


DETAIL_FIELDS = [
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
    "success_count",
    "success_ratio",
    "validation_status",
    "success_skus",
    "failed_skus",
    "first_product_url",
    "first_title",
    "dominant_error",
    "warning_count",
    "first_warning",
    "runtime_seconds",
]

SUMMARY_FIELDS = [
    "search_family",
    "vendors_tested",
    "validated_count",
    "partial_count",
    "failed_count",
    "no_samples_count",
    "success_any_count",
    "total_tested_skus",
    "total_success_skus",
    "family_success_ratio",
    "dominant_error",
]


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def _load_active_search_samples(path: Path, alias_map: dict[str, str]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    if not path.exists():
        return output
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_vendor = _clean(row.get("vendor") or row.get("display_name"))
            if not source_vendor:
                continue
            canonical_vendor = alias_map.get(dv._norm_key(source_vendor), source_vendor)
            values = output.setdefault(canonical_vendor, [])
            seen = {normalize_sku(value) for value in values if normalize_sku(value)}
            for field_name in [
                "active_search_sku_1",
                "active_search_sku_2",
                "candidate_search_sku_1",
                "candidate_search_sku_2",
            ]:
                sku = _clean(row.get(field_name))
                normalized = normalize_sku(sku)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                values.append(sku)
    return output


def _build_sample_sets(
    rows: list[dict[str, str]],
    vendor_profiles_path: Path,
    hints_path: Path,
    shopify_cache_path: Path,
    max_samples: int,
) -> dict[str, list[str]]:
    profiles = dv._load_vendor_profiles(vendor_profiles_path)
    alias_map = dv._build_alias_map(profiles)
    cache_samples = dv._load_vendor_sample_skus(shopify_cache_path, alias_map)
    hint_samples = _load_hint_samples(hints_path, alias_map)
    active_search_samples = _load_active_search_samples(
        DEV_ROOT.parent / "required" / "mappings" / "discovery" / "VendorActiveSearchSamples.csv",
        alias_map,
    )

    output: dict[str, list[str]] = {}
    for row in rows:
        vendor = _clean(row.get("vendor"))
        display_name = _clean(row.get("display_name"))
        preferred = [_clean(row.get("sample_sku"))]
        candidates = (
            active_search_samples.get(vendor, [])
            + active_search_samples.get(display_name, [])
            + preferred
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


def _vendor_sort_key(row: dict[str, str]) -> tuple[int, int, str]:
    verification = _clean(row.get("verification_level")).lower()
    blocking_risk = _clean(row.get("blocking_risk")).lower()
    verification_score = {"verified": 0, "strong": 1, "probed": 2, "detected": 3, "review": 4}.get(verification, 5)
    risk_score = {"low": 0, "medium": 1, "high": 2}.get(blocking_risk, 3)
    return (verification_score, risk_score, _clean(row.get("vendor")).lower())


def _select_rows_by_family(
    rows: list[dict[str, str]],
    vendors_per_family: int,
    family_filter: str,
) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    family_filter = _clean(family_filter).lower()
    for row in rows:
        family = _clean(row.get("search_family"))
        if not family:
            continue
        if family_filter and family_filter not in family.lower():
            continue
        grouped[family].append(row)

    selected: list[dict[str, str]] = []
    for family in sorted(grouped):
        family_rows = sorted(grouped[family], key=_vendor_sort_key)
        selected.extend(family_rows[: max(1, vendors_per_family)])
    return selected


def _validate_vendor_runtime(
    row: dict[str, str],
    sample_skus: list[str],
    request_workers: int,
    retry_count: int,
    delay_seconds: float,
) -> tuple[dict[str, str], dict[str, object]]:
    vendor = _clean(row.get("vendor"))
    display_name = _clean(row.get("display_name"))
    family = _clean(row.get("search_family"))
    interaction = _clean(row.get("interaction_strategy"))
    runtime_pref = _clean(row.get("runtime_preference"))
    blocking_risk = _clean(row.get("blocking_risk"))
    verification_level = _clean(row.get("verification_level"))
    template = _clean(row.get("search_url_template"))
    started = time.monotonic()

    if not template:
        runtime_seconds = time.monotonic() - started
        detail = {
            "vendor": vendor,
            "display_name": display_name,
            "search_family": family,
            "interaction_strategy": interaction,
            "runtime_preference": runtime_pref,
            "blocking_risk": blocking_risk,
            "verification_level": verification_level,
            "search_url_template": template,
            "tested_skus": "",
            "tested_sku_count": "0",
            "success_count": "0",
            "success_ratio": "0.00",
            "validation_status": "missing_template",
            "success_skus": "",
            "failed_skus": "",
            "first_product_url": "",
            "first_title": "",
            "dominant_error": "Missing search_url_template",
            "warning_count": "0",
            "first_warning": "",
            "runtime_seconds": f"{runtime_seconds:.2f}",
        }
        return detail, {"detail": detail, "sku_results": []}

    if not sample_skus:
        runtime_seconds = time.monotonic() - started
        detail = {
            "vendor": vendor,
            "display_name": display_name,
            "search_family": family,
            "interaction_strategy": interaction,
            "runtime_preference": runtime_pref,
            "blocking_risk": blocking_risk,
            "verification_level": verification_level,
            "search_url_template": template,
            "tested_skus": "",
            "tested_sku_count": "0",
            "success_count": "0",
            "success_ratio": "0.00",
            "validation_status": "no_samples",
            "success_skus": "",
            "failed_skus": "",
            "first_product_url": "",
            "first_title": "",
            "dominant_error": "No sample SKUs available",
            "warning_count": "0",
            "first_warning": "",
            "runtime_seconds": f"{runtime_seconds:.2f}",
        }
        return detail, {"detail": detail, "sku_results": []}

    records, errors, warnings = scrape_vendor_records(
        vendor_search_url=template,
        skus=sample_skus,
        workers=max(1, request_workers),
        retry_count=max(0, retry_count),
        delay_seconds=max(0.0, delay_seconds),
        scrape_images=False,
    )

    success_skus: list[str] = []
    failed_skus: list[str] = []
    first_product_url = ""
    first_title = ""
    dominant_error = ""
    sku_results: list[dict[str, str]] = []

    for sku in sample_skus:
        normalized = normalize_sku(sku)
        payload = dict(records.get(normalized, {}) or {})
        error_text = _clean(errors.get(normalized))
        if payload:
            success_skus.append(sku)
            if not first_product_url:
                first_product_url = _clean(payload.get("product_url") or payload.get("source_url") or payload.get("search_url"))
            if not first_title:
                first_title = _clean(payload.get("title"))
        else:
            failed_skus.append(sku)
            if error_text and not dominant_error:
                dominant_error = error_text
        sku_results.append(
            {
                "sku": sku,
                "status": "success" if payload else "fail",
                "product_url": _clean(payload.get("product_url") or payload.get("source_url") or payload.get("search_url")),
                "title": _clean(payload.get("title")),
                "error": error_text,
                "fields_found": ", ".join(sorted(key for key, value in payload.items() if _clean(value))),
            }
        )

    tested_count = len(sample_skus)
    success_count = len(success_skus)
    success_ratio = (success_count / tested_count) if tested_count else 0.0
    if success_count == tested_count and tested_count >= 2:
        validation_status = "validated"
    elif success_count > 0:
        validation_status = "partial"
    else:
        validation_status = "failed"

    runtime_seconds = time.monotonic() - started
    detail = {
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
        "success_count": str(success_count),
        "success_ratio": f"{success_ratio:.2f}",
        "validation_status": validation_status,
        "success_skus": " | ".join(success_skus),
        "failed_skus": " | ".join(failed_skus),
        "first_product_url": first_product_url,
        "first_title": first_title,
        "dominant_error": dominant_error,
        "warning_count": str(len(warnings)),
        "first_warning": _clean(warnings[0]) if warnings else "",
        "runtime_seconds": f"{runtime_seconds:.2f}",
    }
    return detail, {"detail": detail, "warnings": warnings, "sku_results": sku_results}


def _family_summary(detail_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in detail_rows:
        grouped[_clean(row.get("search_family"))].append(row)

    summary_rows: list[dict[str, str]] = []
    for family in sorted(grouped):
        rows = grouped[family]
        status_counts = Counter(_clean(row.get("validation_status")) for row in rows)
        total_skus = sum(int(_clean(row.get("tested_sku_count")) or 0) for row in rows)
        total_success = sum(int(_clean(row.get("success_count")) or 0) for row in rows)
        dominant_errors = Counter(_clean(row.get("dominant_error")) for row in rows if _clean(row.get("dominant_error")))
        summary_rows.append(
            {
                "search_family": family,
                "vendors_tested": str(len(rows)),
                "validated_count": str(status_counts.get("validated", 0)),
                "partial_count": str(status_counts.get("partial", 0)),
                "failed_count": str(status_counts.get("failed", 0)),
                "no_samples_count": str(status_counts.get("no_samples", 0)),
                "success_any_count": str(sum(1 for row in rows if int(_clean(row.get("success_count")) or 0) > 0)),
                "total_tested_skus": str(total_skus),
                "total_success_skus": str(total_success),
                "family_success_ratio": f"{(total_success / total_skus) if total_skus else 0.0:.2f}",
                "dominant_error": dominant_errors.most_common(1)[0][0] if dominant_errors else "",
            }
        )
    return summary_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full runtime scraper against a few vendors from each search family.")
    parser.add_argument("--profiles-input", default="app/required/mappings/discovery/VendorResolverProfiles.csv")
    parser.add_argument("--vendor-profiles", default="app/required/mappings/VendorProfiles.csv")
    parser.add_argument("--hints-input", default="app/required/mappings/VendorSkuPrefixHints.csv")
    parser.add_argument("--shopify-cache", default="app/config/shopify_sku_cache.csv")
    parser.add_argument("--detail-output", default="app/required/mappings/discovery/VendorRuntimeFamilyValidation.csv")
    parser.add_argument("--summary-output", default="app/required/mappings/discovery/VendorRuntimeFamilySummary.csv")
    parser.add_argument("--json-output", default="app/required/mappings/discovery/VendorRuntimeFamilyValidation.json")
    parser.add_argument("--vendors-per-family", type=int, default=3)
    parser.add_argument("--max-samples", type=int, default=3)
    parser.add_argument("--vendor-workers", type=int, default=5)
    parser.add_argument("--request-workers", type=int, default=2)
    parser.add_argument("--retry-count", type=int, default=1)
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument("--family-filter", default="")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    profiles_input = Path(args.profiles_input).expanduser().resolve()
    vendor_profiles_path = Path(args.vendor_profiles).expanduser().resolve()
    hints_path = Path(args.hints_input).expanduser().resolve()
    shopify_cache_path = Path(args.shopify_cache).expanduser().resolve()
    detail_output = Path(args.detail_output).expanduser().resolve()
    summary_output = Path(args.summary_output).expanduser().resolve()
    json_output = Path(args.json_output).expanduser().resolve()

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

    detail_rows: list[dict[str, str]] = []
    detail_json: list[dict[str, object]] = []
    total = len(selected_rows)

    with ThreadPoolExecutor(max_workers=max(1, int(args.vendor_workers))) as executor:
        futures = {
            executor.submit(
                _validate_vendor_runtime,
                row,
                sample_sets.get(_clean(row.get("vendor")), []),
                max(1, int(args.request_workers)),
                max(0, int(args.retry_count)),
                float(args.delay_seconds),
            ): row
            for row in selected_rows
        }
        completed = 0
        for future in as_completed(futures):
            row = futures[future]
            detail, payload = future.result()
            detail_rows.append(detail)
            detail_json.append(payload)
            completed += 1
            print(
                f"[{completed}/{total}] {_clean(row.get('vendor'))}: "
                f"{detail['validation_status']} ({detail['success_count']}/{detail['tested_sku_count']})"
            )

    detail_rows.sort(key=lambda item: (_clean(item.get("search_family")), _clean(item.get("vendor")).lower()))
    detail_json.sort(key=lambda item: (_clean(item.get("detail", {}).get("search_family")), _clean(item.get("detail", {}).get("vendor")).lower()))
    summary_rows = _family_summary(detail_rows)

    _write_csv(detail_output, detail_rows, DETAIL_FIELDS)
    _write_csv(summary_output, summary_rows, SUMMARY_FIELDS)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    with json_output.open("w", encoding="utf-8") as handle:
        json.dump(detail_json, handle, indent=2, ensure_ascii=True)

    totals = Counter(_clean(row.get("validation_status")) for row in detail_rows)
    print(
        f"Wrote {len(detail_rows)} runtime validation rows. "
        f"validated={totals.get('validated', 0)} "
        f"partial={totals.get('partial', 0)} "
        f"failed={totals.get('failed', 0)} "
        f"no_samples={totals.get('no_samples', 0)}"
    )


if __name__ == "__main__":
    main()
