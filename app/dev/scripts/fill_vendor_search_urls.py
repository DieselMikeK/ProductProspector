from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import discover_vendor_search_urls as dv


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

SEARCH_FIELD_NAMES = {"q", "s", "query", "keyword", "search", "term"}
PLAYWRIGHT_POPUP_SELECTORS = [
    "button:has-text('Accept')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
    "button:has-text('Agree')",
    "button:has-text('Got it')",
    "button:has-text('OK')",
    "button:has-text('Okay')",
    "button:has-text('Close')",
    "button:has-text('No Thanks')",
    "button:has-text('Not now')",
    "button[aria-label*='close' i]",
    "button[aria-label*='dismiss' i]",
    ".klaviyo-close-form",
    "[data-testid*='close' i]",
]
PLAYWRIGHT_SEARCH_TOGGLE_SELECTORS = [
    "a[href*='/search']",
    "button[aria-label*='search' i]",
    "summary[aria-label*='search' i]",
    "[class*='search-toggle' i]",
    "[class*='header-search' i]",
    "[class*='search-icon' i]",
]
PLAYWRIGHT_SEARCH_INPUT_SELECTORS = [
    "input[type='search']:not([readonly]):not([tabindex='-1'])",
    "input[name='q']:not([readonly]):not([tabindex='-1'])",
    "input[name='s']:not([readonly]):not([tabindex='-1'])",
    "input[name='query']:not([readonly]):not([tabindex='-1'])",
    "input[name='keyword']:not([readonly]):not([tabindex='-1'])",
    "input[placeholder*='Search' i]:not([readonly]):not([tabindex='-1'])",
    "input[aria-label*='Search' i]:not([readonly]):not([tabindex='-1'])",
    "input[type='search']",
    "input[name='q']",
    "input[name='s']",
    "input[name='query']",
    "input[name='keyword']",
    "input[placeholder*='Search' i]",
    "input[aria-label*='Search' i]",
]


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _root_url(url: str) -> str:
    parsed = urllib.parse.urlparse(_clean(url))
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    return f"{scheme}://{netloc}/" if netloc else ""


def _fetch(url: str, timeout: int = 25) -> tuple[str, str]:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="ignore"), ""
    except Exception as exc:
        return "", str(exc)


def _extract_json_ld_templates(html: str) -> list[str]:
    results: list[str] = []
    for match in re.finditer(r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html):
        body = _clean(match.group(1))
        if not body:
            continue
        try:
            payload = json.loads(body)
        except Exception:
            continue

        def walk(value: object) -> None:
            if isinstance(value, dict):
                if _clean(value.get("@type", "")).lower() == "searchaction":
                    target = value.get("target")
                    if isinstance(target, dict):
                        template = _clean(target.get("urlTemplate", ""))
                        if template:
                            results.append(template)
                    else:
                        template = _clean(target)
                        if template:
                            results.append(template)
                    template = _clean(value.get("urlTemplate", ""))
                    if template:
                        results.append(template)
                for item in value.values():
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)
    return results


def _parse_attrs(fragment: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for key, value in re.findall(r'([a-zA-Z0-9_:\-]+)\s*=\s*["\']([^"\']*)["\']', fragment):
        attrs[key.lower()] = value
    return attrs


def _build_form_template(base_url: str, form_attrs: dict[str, str], form_html: str) -> str:
    method = _clean(form_attrs.get("method", "get")).lower()
    if method and method != "get":
        return ""
    action = _clean(form_attrs.get("action", "")) or base_url
    action_url = urllib.parse.urljoin(base_url, action)
    parsed = urllib.parse.urlparse(action_url)
    params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)

    search_param_name = ""
    for input_match in re.finditer(r'(?is)<input\b([^>]*)>', form_html):
        attrs = _parse_attrs(input_match.group(1))
        input_name = _clean(attrs.get("name", ""))
        if not input_name:
            continue
        input_type = _clean(attrs.get("type", "text")).lower()
        if input_name.lower() in SEARCH_FIELD_NAMES or input_type == "search":
            if not search_param_name:
                search_param_name = input_name
            continue
        if input_type in {"hidden", "text"}:
            params.append((input_name, _clean(attrs.get("value", ""))))
    if not search_param_name:
        return ""
    params.append((search_param_name, "{sku}"))
    query = urllib.parse.urlencode(params, doseq=True)
    rebuilt = parsed._replace(query=query, fragment="")
    return urllib.parse.urlunparse(rebuilt)


def _extract_form_templates(base_url: str, html: str) -> list[str]:
    results: list[str] = []
    for match in re.finditer(r'(?is)<form\b([^>]*)>(.*?)</form>', html):
        form_attrs = _parse_attrs(match.group(1))
        form_html = match.group(2)
        context = f"{match.group(1)} {form_html[:800]}".lower()
        if "search" not in context and 'role="search"' not in context and "name=\"q\"" not in context and "name=\"s\"" not in context:
            continue
        template = _build_form_template(base_url, form_attrs, form_html)
        if template:
            results.append(template)
    return results


def _playwright_rendered_html(url: str, timeout_ms: int = 25000) -> tuple[str, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return "", f"Playwright unavailable: {exc}"

    try:
        with sync_playwright() as play:
            browser = play.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1200)
            _dismiss_common_popups(page)
            _open_search_ui(page)
            _dismiss_common_popups(page)
            page.wait_for_timeout(700)
            html = page.content()
            browser.close()
            return html, ""
    except PlaywrightTimeoutError as exc:
        return "", f"Playwright timeout: {exc}"
    except Exception as exc:
        return "", str(exc)


def _dismiss_common_popups(page) -> None:
    for selector in PLAYWRIGHT_POPUP_SELECTORS:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible(timeout=500):
                locator.click(timeout=800, force=True)
                page.wait_for_timeout(250)
        except Exception:
            continue


def _open_search_ui(page) -> None:
    for selector in PLAYWRIGHT_SEARCH_TOGGLE_SELECTORS:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible(timeout=500):
                locator.click(timeout=800, force=True)
                page.wait_for_timeout(500)
                return
        except Exception:
            continue


def _playwright_find_search_input(page):
    for selector in PLAYWRIGHT_SEARCH_INPUT_SELECTORS:
        try:
            locator = page.locator(selector)
            count = locator.count()
            for index in range(count):
                candidate = locator.nth(index)
                if candidate.is_visible(timeout=700):
                    return candidate
        except Exception:
            continue
    return None


def _template_from_captured_url(url: str, sku: str) -> str:
    text = _clean(url)
    token = _clean(sku)
    if not text or not token:
        return ""
    variants = {
        token,
        urllib.parse.quote(token, safe=""),
        urllib.parse.quote_plus(token),
        token.lower(),
        token.upper(),
        urllib.parse.quote(token.lower(), safe=""),
        urllib.parse.quote(token.upper(), safe=""),
        urllib.parse.quote_plus(token.lower()),
        urllib.parse.quote_plus(token.upper()),
    }
    template = text
    replaced = False
    for variant in sorted(variants, key=len, reverse=True):
        if not variant:
            continue
        if variant in template:
            template = template.replace(variant, "{sku}")
            replaced = True
    template = template.replace("{sku}*", "{sku}")
    template = template.replace("*{sku}", "{sku}")
    return template if replaced and "{sku}" in template else ""


def _playwright_submit_search_template(url: str, sku: str, timeout_ms: int = 25000) -> tuple[str, str, str, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return "", "", "", f"Playwright unavailable: {exc}"

    try:
        with sync_playwright() as play:
            browser = play.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1200)
            _dismiss_common_popups(page)

            search_input = _playwright_find_search_input(page)
            if search_input is None:
                _open_search_ui(page)
                _dismiss_common_popups(page)
                search_input = _playwright_find_search_input(page)
            if search_input is None:
                browser.close()
                return "", "", "", "No visible search input found"

            start_url = page.url
            try:
                search_input.scroll_into_view_if_needed(timeout=1200)
            except Exception:
                pass
            try:
                search_input.evaluate(
                    "(el) => { el.removeAttribute('readonly'); el.removeAttribute('disabled'); }"
                )
            except Exception:
                pass
            try:
                search_input.focus(timeout=1200)
            except Exception:
                try:
                    search_input.click(timeout=1200, force=True)
                except Exception:
                    pass
            try:
                search_input.fill(sku, timeout=2500)
            except Exception:
                try:
                    search_input.evaluate(
                        """(el, value) => {
                            el.value = value;
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        sku,
                    )
                except Exception:
                    browser.close()
                    return "", "", "", "Unable to populate search input"
            page.wait_for_timeout(250)

            navigated = False
            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=9000):
                    search_input.press("Enter")
                navigated = True
            except Exception:
                try:
                    form = search_input.locator("xpath=ancestor::form[1]")
                    submit = form.locator("button, input[type='submit']").first
                    if submit.count():
                        with page.expect_navigation(wait_until="domcontentloaded", timeout=9000):
                            submit.click(timeout=1500, force=True)
                        navigated = True
                except Exception:
                    pass

            if not navigated:
                page.wait_for_timeout(2500)

            final_url = page.url
            html = page.content()
            title = _clean(page.title()).lower()
            template = _template_from_captured_url(final_url, sku)
            low_final_path = urllib.parse.urlparse(final_url).path.lower()
            looks_product_path = (
                "/products/" in low_final_path or "/product/" in low_final_path
            ) and "/products/search" not in low_final_path and "/product/search" not in low_final_path
            looks_search_path = (
                "/search" in low_final_path
                or any(token in final_url.lower() for token in ("?q=", "&q=", "?s=", "&s=", "?query=", "&query="))
            )
            page_has_sku = _clean(sku).lower() in html.lower() or _clean(sku).lower() in final_url.lower()
            obvious_challenge = any(signal in title for signal in ("just a moment", "verifying", "attention required"))
            if template and not obvious_challenge and final_url != start_url:
                if looks_product_path:
                    browser.close()
                    return template, "confirmed", "product_page", "Resolved to product page"
                if looks_search_path or page_has_sku:
                    browser.close()
                    return template, "confirmed", "search_results", "Resolved via browser-submitted search"

            kind, _, success, _, notes = dv._classify_probe(start_url, final_url, html, 200, "", sku)
            browser.close()
            if not template:
                return "", "", "", notes or "Search submit did not produce a usable URL template"
            if kind not in {"product_page", "search_results"}:
                if not success:
                    return "", "", "", notes or f"Search submit landed on {kind}"
            mode = "product_page" if kind == "product_page" else "search_results"
            return template, "confirmed", mode, notes
    except PlaywrightTimeoutError as exc:
        return "", "", "", f"Playwright timeout: {exc}"
    except Exception as exc:
        return "", "", "", str(exc)


def _normalize_template(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""
    text = text.replace("{search_term_string}", "{sku}")
    text = text.replace("{search_term}", "{sku}")
    return text


def _choose_best_template(templates: list[str]) -> tuple[str, str, str]:
    if not templates:
        return "", "", ""
    unique: list[str] = []
    seen: set[str] = set()
    for raw in templates:
        template = _normalize_template(raw)
        if not template or "{sku}" not in template:
            continue
        if template in seen:
            continue
        seen.add(template)
        unique.append(template)
    if not unique:
        return "", "", ""

    def score(value: str) -> tuple[int, int]:
        low = value.lower()
        base = 0
        if "/search" in low:
            base += 4
        if "/catalogsearch/" in low:
            base += 3
        if "?q={sku}" in low or "&q={sku}" in low:
            base += 3
        if "?s={sku}" in low or "&s={sku}" in low:
            base += 2
        if "cx=" in low or "cof=" in low:
            base += 1
        if "{" in low:
            base += 1
        return base, -len(value)

    best = sorted(unique, key=score, reverse=True)[0]
    mode = "custom_search" if "cx=" in best.lower() or "cof=" in best.lower() else "search_results"
    status = "detected"
    return best, status, mode


def _probe_common_templates(vendor: str, site_url: str, sku: str, use_playwright: bool) -> tuple[str, str, str]:
    if not _clean(site_url) or not _clean(sku):
        return "", "", ""
    probes = dv._probe_search_templates(
        vendor=vendor,
        site_url=site_url,
        sku=sku,
        use_playwright=use_playwright,
        delay_seconds=0.0,
    )
    best = dv._best_probe(probes)
    if best is None:
        return "", "", ""
    if best.result_kind not in {"product_page", "search_results"}:
        return "", "", ""
    root = dv._root_url(site_url)
    template = root + best.template if root else ""
    if not template:
        return "", "", ""
    mode = "product_page" if best.result_kind == "product_page" else "search_results"
    return template, "probed", mode


def _resolve_search_template_for_target(
    vendor: str,
    base_url: str,
    sample_sku: str,
    use_playwright: bool,
    probe_use_playwright: bool,
) -> dict[str, str]:
    notes: list[str] = []
    templates: list[str] = []
    used_rendered_html = False
    resolver_hint = ""

    html, error = _fetch(base_url)
    if html:
        templates.extend(_extract_json_ld_templates(html))
        templates.extend(_extract_form_templates(base_url, html))
    elif error:
        notes.append(f"search-template fetch error: {error}")

    if not templates and use_playwright:
        rendered_html, rendered_error = _playwright_rendered_html(base_url)
        if rendered_html:
            templates.extend(_extract_json_ld_templates(rendered_html))
            templates.extend(_extract_form_templates(base_url, rendered_html))
            used_rendered_html = True
        elif rendered_error:
            notes.append(f"playwright fallback error: {rendered_error}")

    best, status, mode = _choose_best_template(templates)
    if best:
        resolver_hint = "jsonld_searchaction_or_html_form" + ("_with_playwright" if used_rendered_html else "")

    if not best and use_playwright:
        submitted, submitted_status, submitted_mode, submitted_error = _playwright_submit_search_template(
            base_url,
            sample_sku,
        )
        if submitted:
            best, status, mode = submitted, submitted_status, submitted_mode
            resolver_hint = "playwright_search_submit"
        elif submitted_error:
            notes.append(submitted_error)

    if not best:
        probed, probed_status, probed_mode = _probe_common_templates(
            vendor=vendor,
            site_url=base_url,
            sku=sample_sku,
            use_playwright=probe_use_playwright,
        )
        if probed:
            best, status, mode = probed, probed_status, probed_mode
            resolver_hint = "common_template_probe" + ("_with_playwright" if probe_use_playwright else "")

    if best:
        return {
            "search_url_template": best,
            "search_template_status": status,
            "search_result_mode": mode,
            "resolver_hint": resolver_hint,
            "review_notes": "",
            "log_message": f"-> {best}",
        }

    note = next((item for item in notes if _clean(item)), "search-template detection found no usable template")
    return {
        "search_url_template": "",
        "search_template_status": "unresolved",
        "search_result_mode": "",
        "resolver_hint": "",
        "review_notes": note,
        "log_message": f"unresolved: {note}",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fill search_url_template from vendor websites already stored in the master CSV.")
    parser.add_argument("--master", default="app/required/mappings/discovery/VendorDiscoveryMaster.csv")
    parser.add_argument("--vendor-filter", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--use-playwright", action="store_true", help="Use Playwright fallback for popup/JS-driven search UIs.")
    parser.add_argument("--probe-use-playwright", action="store_true", help="Also use Playwright inside common URL probing. Slower, use only for stubborn vendors.")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel vendor workers to run.")
    parser.add_argument(
        "--website-statuses",
        default="confirmed",
        help="Comma-separated official_website_status values to include, for example 'confirmed' or 'confirmed,probable'.",
    )
    args = parser.parse_args()

    master_path = Path(args.master).expanduser().resolve()
    with master_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0].keys()) if rows else []
    if not rows:
        print(f"ERROR: no rows in {master_path}")
        return 1

    needle = _clean(args.vendor_filter).lower()
    allowed_statuses = {
        _clean(part).lower()
        for part in args.website_statuses.split(",")
        if _clean(part)
    } or {"confirmed"}
    targets: list[tuple[int, dict[str, str]]] = []
    for row_index, row in enumerate(rows):
        vendor = _clean(row.get("vendor", ""))
        if not vendor:
            continue
        if needle and needle not in vendor.lower() and needle not in _clean(row.get("display_name", "")).lower():
            continue
        if _clean(row.get("search_url_template", "")):
            continue
        if _clean(row.get("official_website_status", "")).lower() not in allowed_statuses:
            continue
        if not _clean(row.get("official_website_url", "")):
            continue
        targets.append((row_index, row))
    if args.limit > 0:
        targets = targets[: args.limit]

    print(f"Vendors queued: {len(targets)}")
    print(f"Workers: {max(1, args.workers)}")
    print(f"Website statuses: {', '.join(sorted(allowed_statuses))}")
    updated = 0
    if max(1, args.workers) == 1:
        for index, (row_index, row) in enumerate(targets, start=1):
            vendor = _clean(row.get("vendor", ""))
            base_url = _root_url(_clean(row.get("official_website_url", "")))
            print(f"[{index}/{len(targets)}] {vendor}")
            result = _resolve_search_template_for_target(
                vendor=vendor,
                base_url=base_url,
                sample_sku=_clean(row.get("sample_sku", "")),
                use_playwright=bool(args.use_playwright),
                probe_use_playwright=bool(args.probe_use_playwright),
            )
            row["search_url_template"] = result["search_url_template"]
            row["search_template_status"] = result["search_template_status"]
            row["search_result_mode"] = result["search_result_mode"]
            row["resolver_hint"] = result["resolver_hint"]
            row["review_notes"] = result["review_notes"]
            if result["search_url_template"]:
                updated += 1
            print(f"  {result['log_message']}")
            with master_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
    else:
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {}
            for row_index, row in targets:
                vendor = _clean(row.get("vendor", ""))
                base_url = _root_url(_clean(row.get("official_website_url", "")))
                future = executor.submit(
                    _resolve_search_template_for_target,
                    vendor,
                    base_url,
                    _clean(row.get("sample_sku", "")),
                    bool(args.use_playwright),
                    bool(args.probe_use_playwright),
                )
                future_map[future] = (row_index, vendor)

            completed = 0
            for future in as_completed(future_map):
                row_index, vendor = future_map[future]
                completed += 1
                row = rows[row_index]
                print(f"[{completed}/{len(targets)}] {vendor}")
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "search_url_template": "",
                        "search_template_status": "unresolved",
                        "search_result_mode": "",
                        "resolver_hint": "",
                        "review_notes": f"worker error: {exc}",
                        "log_message": f"unresolved: worker error: {exc}",
                    }
                row["search_url_template"] = result["search_url_template"]
                row["search_template_status"] = result["search_template_status"]
                row["search_result_mode"] = result["search_result_mode"]
                row["resolver_hint"] = result["resolver_hint"]
                row["review_notes"] = result["review_notes"]
                if result["search_url_template"]:
                    updated += 1
                print(f"  {result['log_message']}")
                with master_path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

    print(f"Updated search templates: {updated}")
    print(f"Master file: {master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
