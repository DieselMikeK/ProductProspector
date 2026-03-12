from __future__ import annotations

import argparse
import csv
import re
import urllib.parse
from pathlib import Path


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _parse_query_param(template: str) -> str:
    if not template or "{sku}" not in template:
        return ""
    parsed = urllib.parse.urlparse(template)
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    for key, value in pairs:
        if "{sku}" in value:
            return key
    return ""


def _infer_platform_family(template: str, resolver_hint: str, review_notes: str, website_url: str) -> str:
    low_template = template.lower()
    low_hint = resolver_hint.lower()
    low_notes = review_notes.lower()
    low_site = website_url.lower()

    if "partslogic" in low_hint or "partslogic" in low_notes:
        return "partslogic"
    if "/products/search?search=" in low_template:
        return "custom_api"
    if "search.asp?keyword=" in low_template:
        return "3dcart_like"
    if "catalogsearch/result" in low_template:
        return "magento"
    if "options[prefix]=last" in low_template or "options%5bprefix%5d=last" in low_template:
        return "shopify"
    if "/search.html" in low_template or "/search.php" in low_template or "search_query=" in low_template:
        return "bigcommerce_like"
    if low_template.endswith("/search/{sku}") or "?s={sku}" in low_template:
        return "wordpress_like"
    if "searchresults.asp" in low_template or "search.aspx" in low_template or "/searchresults" in low_template:
        return "aspnet_like"
    if "/api/search/" in low_template or "autocomplete" in low_template:
        return "custom_api"
    if "searchspring" in low_hint or "searchspring" in low_notes:
        return "searchspring"
    if "search?q=" in low_template or "search?query=" in low_template or "search?keyword=" in low_template:
        return "generic_get_search"
    if "wp-content" in low_notes or "woocommerce" in low_notes or "wordpress" in low_hint:
        return "wordpress_like"
    if "shopify" in low_site:
        return "shopify"
    return "custom"


def _infer_search_family(template: str, resolver_hint: str, review_notes: str) -> str:
    low_template = template.lower()
    low_hint = resolver_hint.lower()
    low_notes = review_notes.lower()

    if not template and "post /api/ue/addsitevisitorsearchresults" in low_notes:
        return "post_search_results"
    if not template:
        return "unresolved"
    if "partslogic" in low_hint or "partslogic" in low_notes:
        return "partslogic_search_redirect"
    if "/products/search?search=" in low_template:
        return "json_product_search"
    if "search.asp?keyword=" in low_template:
        return "searchasp_keyword"
    if "?s={sku}" in low_template:
        return "wordpress_s_query"
    if low_template.endswith("/search/{sku}"):
        return "wordpress_search_path"
    if "catalogsearch/result" in low_template:
        return "magento_catalogsearch"
    if "/search.html" in low_template and "q={sku}" in low_template:
        return "bigcommerce_search_html_q"
    if "/search.php" in low_template and "search_query={sku}" in low_template:
        return "bigcommerce_search_php_query"
    if "searchresults.asp" in low_template and "q={sku}" in low_template:
        return "asp_searchresults_q"
    if "search.aspx" in low_template and "ss={sku}" in low_template:
        return "asp_search_ss"
    if "/search/partnosearch" in low_template:
        return "part_number_search"
    if "substring={sku}" in low_template:
        return "substring_search"
    if "search_api_fulltext={sku}" in low_template:
        return "drupal_search_api"
    if "search_prod={sku}" in low_template:
        return "product_search_search_prod"
    if "search?type=product" in low_template:
        return "shopify_product_search"
    if "search?q={sku}" in low_template:
        return "generic_q_search"
    if "search?query={sku}" in low_template:
        return "generic_query_search"
    if "search?keyword={sku}" in low_template or "search?keywords={sku}" in low_template:
        return "generic_keyword_search"
    if low_template.endswith("{sku}"):
        return "path_or_query_search"
    if "post /api/ue/addsitevisitorsearchresults" in low_notes:
        return "post_search_results"
    return "custom_search"


def _infer_transport(template: str, review_notes: str) -> str:
    low_template = template.lower()
    low_notes = review_notes.lower()
    if not template and "post /api/ue/addsitevisitorsearchresults" in low_notes:
        return "POST+GET"
    if low_template.startswith("post "):
        return "POST"
    return "GET" if template else ""


def _infer_browser_flags(resolver_hint: str, review_notes: str) -> tuple[str, str]:
    low_hint = resolver_hint.lower()
    low_notes = review_notes.lower()
    browser_validated = "yes" if any(
        token in low_hint or token in low_notes
        for token in (
            "playwright",
            "browser-validated",
            "human verification",
            "bot challenge",
            "no visible search input",
            "partslogic",
        )
    ) else "no"
    browser_required = "yes" if any(
        token in low_notes
        for token in ("human verification", "bot challenge", "405", "429", "no visible search input")
    ) else "no"
    return browser_validated, browser_required


def _infer_blocking_hint(review_notes: str) -> str:
    low_notes = review_notes.lower()
    if "429" in low_notes or "too many requests" in low_notes:
        return "http_429"
    if "403" in low_notes:
        return "http_403"
    if "405" in low_notes:
        return "http_405"
    if "500" in low_notes:
        return "http_500"
    if "ssl:" in low_notes:
        return "ssl"
    if "human verification" in low_notes or "bot challenge" in low_notes:
        return "bot_challenge"
    if "no visible search input found" in low_notes:
        return "hidden_or_missing_search_ui"
    if "search returned no results" in low_notes:
        return "sample_sku_no_results"
    if "unknown page type" in low_notes:
        return "unknown_page_type"
    return ""


def _confidence_bucket(template_status: str, review_notes: str) -> str:
    low_status = template_status.lower()
    low_notes = review_notes.lower()
    if low_status == "confirmed":
        return "high"
    if low_status in {"detected", "probed"}:
        return "medium"
    if low_status in {"probable", "custom"}:
        return "review"
    if any(token in low_notes for token in ("bot challenge", "403", "405", "429")):
        return "blocked"
    return "low"


def build_profiles(master_path: Path, output_path: Path) -> int:
    with master_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    output_fields = [
        "vendor",
        "display_name",
        "official_website_status",
        "official_website_url",
        "sample_sku",
        "search_url_template",
        "search_template_status",
        "search_result_mode",
        "platform_family",
        "search_family",
        "transport",
        "parameter_name",
        "browser_validated",
        "browser_required_hint",
        "blocking_hint",
        "confidence_bucket",
        "resolver_hint",
        "review_notes",
    ]

    out_rows: list[dict[str, str]] = []
    for row in rows:
        if _clean(row.get("official_website_status")) == "placeholder":
            continue
        template = _clean(row.get("search_url_template"))
        review_notes = _clean(row.get("review_notes"))
        resolver_hint = _clean(row.get("resolver_hint"))
        platform_family = _infer_platform_family(
            template=template,
            resolver_hint=resolver_hint,
            review_notes=review_notes,
            website_url=_clean(row.get("official_website_url")),
        )
        search_family = _infer_search_family(template=template, resolver_hint=resolver_hint, review_notes=review_notes)
        browser_validated, browser_required = _infer_browser_flags(resolver_hint=resolver_hint, review_notes=review_notes)
        out_rows.append(
            {
                "vendor": _clean(row.get("vendor")),
                "display_name": _clean(row.get("display_name")),
                "official_website_status": _clean(row.get("official_website_status")),
                "official_website_url": _clean(row.get("official_website_url")),
                "sample_sku": _clean(row.get("sample_sku")),
                "search_url_template": template,
                "search_template_status": _clean(row.get("search_template_status")),
                "search_result_mode": _clean(row.get("search_result_mode")),
                "platform_family": platform_family,
                "search_family": search_family,
                "transport": _infer_transport(template=template, review_notes=review_notes),
                "parameter_name": _parse_query_param(template),
                "browser_validated": browser_validated,
                "browser_required_hint": browser_required,
                "blocking_hint": _infer_blocking_hint(review_notes=review_notes),
                "confidence_bucket": _confidence_bucket(
                    template_status=_clean(row.get("search_template_status")),
                    review_notes=review_notes,
                ),
                "resolver_hint": resolver_hint,
                "review_notes": review_notes,
            }
        )

    out_rows.sort(key=lambda item: (item["platform_family"], item["search_family"], item["vendor"]))
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(out_rows)
    return len(out_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a derived vendor search structure profile table from VendorDiscoveryMaster.csv")
    parser.add_argument("--master", default="app/required/mappings/discovery/VendorDiscoveryMaster.csv")
    parser.add_argument("--output", default="app/required/mappings/discovery/VendorSearchStructureProfiles.csv")
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    count = build_profiles(master_path=master_path, output_path=output_path)
    print(f"Wrote {count} vendor structure profiles to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
