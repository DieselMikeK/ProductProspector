from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _norm_key(value: object) -> str:
    return "".join(ch for ch in _clean(value).lower() if ch.isalnum())


def _interaction_map(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.exists():
        return {}
    rows = _load_csv(path)
    output: dict[str, dict[str, str]] = {}
    for row in rows:
        key = _norm_key(row.get("vendor"))
        if key:
            output[key] = row
    return output


def _runtime_preference(transport: str, browser_required: str, platform_family: str, blocking_hint: str) -> str:
    transport = transport.upper()
    browser_required = browser_required.lower()
    platform_family = platform_family.lower()
    blocking_hint = blocking_hint.lower()
    if browser_required == "yes":
        return "browser_first"
    if transport == "POST+GET":
        return "browser_or_session_post"
    if blocking_hint in {"http_403", "http_429", "bot_challenge"}:
        return "http_first_with_browser_fallback"
    if platform_family in {
        "shopify",
        "magento",
        "wordpress_like",
        "bigcommerce_like",
        "generic_get_search",
        "aspnet_like",
        "partslogic",
        "custom_api",
    }:
        return "http_first"
    return "http_first_with_browser_fallback"


def _http_search_allowed(transport: str, browser_required: str) -> str:
    transport = transport.upper()
    browser_required = browser_required.lower()
    if browser_required == "yes":
        return "conditional"
    if transport in {"GET", "POST+GET"}:
        return "yes"
    return "conditional"


def _blocking_risk(blocking_hint: str) -> str:
    hint = blocking_hint.lower()
    if hint in {"bot_challenge", "http_429"}:
        return "high"
    if hint in {"http_403", "http_405"}:
        return "medium"
    if hint:
        return "low"
    return "low"


def _verification_level(confidence_bucket: str, template_status: str) -> str:
    confidence_bucket = confidence_bucket.lower()
    template_status = template_status.lower()
    if template_status == "confirmed":
        return "verified"
    if confidence_bucket == "high":
        return "strong"
    if template_status == "probed":
        return "probed"
    if template_status == "detected":
        return "detected"
    return "review"


def _search_entry_mode(transport: str, search_family: str) -> str:
    transport = transport.upper()
    search_family = search_family.lower()
    if transport == "POST+GET":
        return "post_then_results_page"
    if search_family == "json_product_search":
        return "json_api_search"
    if search_family == "partslogic_search_redirect":
        return "overlay_redirect_search"
    if search_family in {"wordpress_s_query", "generic_q_search", "generic_query_search", "generic_keyword_search"}:
        return "direct_query_template"
    if search_family in {
        "magento_catalogsearch",
        "bigcommerce_search_html_q",
        "bigcommerce_search_php_query",
        "asp_search_ss",
        "searchasp_keyword",
    }:
        return "catalog_or_storefront_search"
    return "custom_route_template"


def _product_fetch_mode(browser_required: str, blocking_hint: str) -> str:
    browser_required = browser_required.lower()
    blocking_hint = blocking_hint.lower()
    if browser_required == "yes" or blocking_hint in {"bot_challenge", "http_429"}:
        return "http_first_browser_fallback"
    return "http_first"


def build_profiles(
    resolved_rows: list[dict[str, str]],
    interaction_by_vendor: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, object]]:
    interaction_by_vendor = interaction_by_vendor or {}
    profiles: list[dict[str, object]] = []
    for row in resolved_rows:
        transport = _clean(row.get("transport"))
        browser_required = _clean(row.get("browser_required_hint"))
        platform_family = _clean(row.get("platform_family"))
        blocking_hint = _clean(row.get("blocking_hint"))
        confidence_bucket = _clean(row.get("confidence_bucket"))
        template_status = _clean(row.get("search_template_status"))
        search_family = _clean(row.get("search_family"))
        interaction = interaction_by_vendor.get(_norm_key(row.get("vendor")), {})
        profile = {
            "vendor": _clean(row.get("vendor")),
            "display_name": _clean(row.get("display_name")),
            "official_website_status": _clean(row.get("official_website_status")),
            "official_website_url": _clean(row.get("official_website_url")),
            "sample_sku": _clean(row.get("sample_sku")),
            "search_url_template": _clean(row.get("search_url_template")),
            "search_result_mode": _clean(row.get("search_result_mode")),
            "search_transport": transport,
            "search_parameter_name": _clean(row.get("parameter_name")),
            "platform_family": platform_family,
            "search_family": search_family,
            "search_entry_mode": _search_entry_mode(transport=transport, search_family=search_family),
            "runtime_preference": _runtime_preference(
                transport=transport,
                browser_required=browser_required,
                platform_family=platform_family,
                blocking_hint=blocking_hint,
            ),
            "http_search_allowed": _http_search_allowed(transport=transport, browser_required=browser_required),
            "browser_required": browser_required or "no",
            "blocking_risk": _blocking_risk(blocking_hint),
            "verification_level": _verification_level(
                confidence_bucket=confidence_bucket,
                template_status=template_status,
            ),
            "template_status": template_status,
            "confidence_bucket": confidence_bucket,
            "resolver_hint": _clean(row.get("resolver_hint")),
            "product_fetch_mode": _product_fetch_mode(
                browser_required=browser_required,
                blocking_hint=blocking_hint,
            ),
            "interaction_strategy": _clean(interaction.get("interaction_strategy")),
            "direct_url_sufficient": _clean(interaction.get("direct_url_sufficient")),
            "search_entry_url": _clean(interaction.get("search_entry_url")),
            "search_container_selector": _clean(interaction.get("search_container_selector")),
            "search_input_selector": _clean(interaction.get("search_input_selector")),
            "search_submit_selector": _clean(interaction.get("search_submit_selector")),
            "result_container_selector": _clean(interaction.get("result_container_selector")),
            "result_link_selector": _clean(interaction.get("result_link_selector")),
            "result_match_mode": _clean(interaction.get("result_match_mode")),
            "api_request_url_template": _clean(interaction.get("api_request_url_template")),
            "api_response_collection": _clean(interaction.get("api_response_collection")),
            "api_result_id_field": _clean(interaction.get("api_result_id_field")),
            "api_result_sku_field": _clean(interaction.get("api_result_sku_field")),
            "product_url_template": _clean(interaction.get("product_url_template")),
            "product_extraction_priority": "jsonld>embedded_state>dom",
            "media_strategy": "gallery_scope_only",
            "notes": " | ".join(
                item
                for item in (
                    _clean(row.get("review_notes")),
                    _clean(interaction.get("notes")),
                )
                if item
            ),
        }
        profiles.append(profile)
    profiles.sort(
        key=lambda item: (
            item["runtime_preference"],
            item["platform_family"],
            item["vendor"].lower(),
        )
    )
    return profiles


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build app-facing resolver profiles from the resolved vendor discovery worklist.")
    parser.add_argument("--resolved", default="app/required/mappings/discovery/VendorDiscoveryResolvedWorklist.csv")
    parser.add_argument("--interactions", default="app/required/mappings/discovery/VendorSearchInteractionProfiles.csv")
    parser.add_argument("--csv-output", default="app/required/mappings/discovery/VendorResolverProfiles.csv")
    parser.add_argument("--json-output", default="app/required/mappings/discovery/VendorResolverProfiles.json")
    args = parser.parse_args()

    resolved_rows = _load_csv(Path(args.resolved).expanduser().resolve())
    interaction_by_vendor = _interaction_map(Path(args.interactions).expanduser().resolve())
    profiles = build_profiles(resolved_rows, interaction_by_vendor=interaction_by_vendor)
    write_csv(Path(args.csv_output).expanduser().resolve(), profiles)
    write_json(Path(args.json_output).expanduser().resolve(), profiles)
    print(f"Wrote {len(profiles)} resolver profiles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
