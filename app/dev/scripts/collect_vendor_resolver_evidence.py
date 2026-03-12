from __future__ import annotations

import argparse
import csv
import json
import re
import shlex
import sys
import urllib.parse
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))

import discover_vendor_search_urls as dv
import fill_vendor_search_urls as fv


SEARCH_HINT_TOKENS = ("search", "query", "autocomplete", "catalogsearch", "find", "lookup")
STATIC_EXTENSIONS = {
    ".js",
    ".css",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".map",
    ".webp",
    ".mp4",
    ".webm",
}

CSV_FIELDS = [
    "vendor",
    "display_name",
    "official_website_url",
    "search_url_template",
    "sample_sku_primary",
    "validation_sample_skus",
    "validation_success_count",
    "validation_total_count",
    "validation_summary",
    "search_recipe_kind",
    "search_submit_status",
    "dom_search_candidates_found",
    "dom_search_input_selector",
    "dom_search_input_name",
    "dom_search_input_type",
    "dom_search_input_visible",
    "dom_form_selector",
    "dom_form_action",
    "dom_form_method",
    "dom_submit_selector",
    "primary_request_method",
    "primary_request_resource_type",
    "primary_request_url_template",
    "primary_request_post_data_template",
    "primary_request_status",
    "primary_request_header_names",
    "primary_request_cookie_header",
    "primary_request_curl",
    "primary_xhr_method",
    "primary_xhr_url_template",
    "primary_xhr_post_data_template",
    "primary_xhr_status",
    "primary_xhr_header_names",
    "primary_xhr_cookie_header",
    "primary_xhr_curl",
    "all_relevant_xhr_templates",
    "final_url_template",
    "final_result_kind",
    "api_observed",
    "hidden_form_observed",
    "browser_required_observed",
    "notes",
]


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _norm_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean(value).lower())


def _json_compact(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_sample_map(hints_path: Path, vendor_profiles_path: Path) -> dict[str, list[str]]:
    profiles = dv._load_vendor_profiles(vendor_profiles_path)
    alias_map = dv._build_alias_map(profiles)
    canonical_aliases: dict[str, set[str]] = {}
    for profile in profiles:
        names = canonical_aliases.setdefault(profile.canonical_vendor, set())
        for value in [profile.canonical_vendor, profile.display_name, *profile.aliases]:
            text = _clean(value)
            if text:
                names.add(text)

    output: dict[str, list[str]] = {}
    with hints_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_vendor = _clean(row.get("shopify_vendor"))
            if not source_vendor:
                continue
            samples = []
            for field in ("sample_sku_1", "sample_sku_2", "sample_sku_3"):
                sku = _clean(row.get(field))
                if sku and sku not in samples:
                    samples.append(sku)
            if not samples:
                continue
            canonical = alias_map.get(_norm_key(source_vendor), source_vendor)
            all_names = {source_vendor, canonical, *canonical_aliases.get(canonical, set())}
            for name in all_names:
                key = _norm_key(name)
                if not key:
                    continue
                existing = output.setdefault(key, [])
                for sku in samples:
                    if sku not in existing:
                        existing.append(sku)
    return output


def _sample_skus_for_row(row: dict[str, str], sample_map: dict[str, list[str]], validation_limit: int) -> list[str]:
    out: list[str] = []
    for candidate in (_clean(row.get("vendor")), _clean(row.get("display_name"))):
        key = _norm_key(candidate)
        if not key:
            continue
        for sku in sample_map.get(key, []):
            if sku and sku not in out:
                out.append(sku)
    fallback = _clean(row.get("sample_sku"))
    if fallback and fallback not in out:
        out.insert(0, fallback)
    if validation_limit > 0:
        out = out[:validation_limit]
    return out


def _templateize_text(text: str, sku: str) -> str:
    source = _clean(text)
    token = _clean(sku)
    if not source or not token:
        return source
    variants = {
        token,
        token.lower(),
        token.upper(),
        urllib.parse.quote(token, safe=""),
        urllib.parse.quote(token.lower(), safe=""),
        urllib.parse.quote(token.upper(), safe=""),
        urllib.parse.quote_plus(token),
        urllib.parse.quote_plus(token.lower()),
        urllib.parse.quote_plus(token.upper()),
    }
    replaced = False
    for variant in sorted((item for item in variants if item), key=len, reverse=True):
        if variant in source:
            source = source.replace(variant, "{sku}")
            replaced = True
    return source if replaced else _clean(text)


def _render_template(template: str, sku: str) -> str:
    encoded = urllib.parse.quote(_clean(sku), safe="")
    return _clean(template).replace("{sku}", encoded)


def _shorten(text: object, limit: int = 600) -> str:
    value = _clean(text)
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _is_search_like(url: str, body: str, sku: str) -> bool:
    low_url = _clean(url).lower()
    low_body = _clean(body).lower()
    low_sku = _clean(sku).lower()
    if low_sku and (low_sku in low_url or low_sku in low_body):
        return True
    if any(token in low_url for token in SEARCH_HINT_TOKENS):
        return True
    if any(token in low_body for token in SEARCH_HINT_TOKENS):
        return True
    return False


def _request_is_static(url: str) -> bool:
    parsed = urllib.parse.urlparse(_clean(url))
    path = parsed.path.lower()
    return any(path.endswith(ext) for ext in STATIC_EXTENSIONS)


def _looks_like_analytics_request(url: str, body: str) -> bool:
    text = f"{_clean(url).lower()} {_clean(body).lower()}"
    tokens = (
        "affirm",
        "clarity",
        "hubapi",
        "judge.me",
        "/api/collect",
        "/api/event/collect",
        "analytics",
        "pixel",
        "telemetry",
        "chrono",
    )
    return any(token in text for token in tokens)


def _header_map(request) -> dict[str, str]:
    try:
        headers = request.all_headers()
    except Exception:
        try:
            headers = request.headers
        except Exception:
            headers = {}
    return {str(key): str(value) for key, value in dict(headers).items()}


def _request_post_data(request) -> str:
    try:
        return _clean(request.post_data)
    except Exception:
        try:
            buffer = request.post_data_buffer
        except Exception:
            buffer = None
        if not buffer:
            return ""
        return f"<{len(buffer)} bytes binary>"


def _to_curl(method: str, url: str, headers: dict[str, str], body: str) -> str:
    parts = ["curl", "-X", method.upper(), shlex.quote(url)]
    for key, value in headers.items():
        parts.extend(["-H", shlex.quote(f"{key}: {value}")])
    if body:
        parts.extend(["--data-raw", shlex.quote(body)])
    return " ".join(parts)


def _is_search_xhr_candidate(item: dict[str, object], search_template: str, sku: str, root_domain: str) -> bool:
    url = _clean(item.get("url"))
    body = _clean(item.get("post_data"))
    resource_type = _clean(item.get("resource_type"))
    domain = urllib.parse.urlparse(url).netloc.lower()
    if resource_type not in {"xhr", "fetch"}:
        return False
    if domain != root_domain:
        return False
    if _looks_like_analytics_request(url, body):
        return False
    low_url = url.lower()
    low_template = _clean(item.get("url_template")).lower()
    if _clean(search_template).lower() == low_template:
        return True
    if any(token in low_url for token in ("search", "suggest", "autocomplete", "catalogsearch", "predictive")):
        return True
    if _clean(sku).lower() and _clean(sku).lower() in body.lower():
        return True
    return False


def _dom_candidates(page) -> list[dict[str, object]]:
    return page.evaluate(
        """() => {
            const SEARCH_NAMES = new Set(['q', 's', 'query', 'keyword', 'search', 'term', 'search-field']);
            const nodes = Array.from(document.querySelectorAll('input'));
            function escapeCss(value) {
              try { return CSS.escape(value); } catch (err) { return value; }
            }
            function selectorFor(el) {
              if (!el) return '';
              const tag = (el.tagName || 'input').toLowerCase();
              if (el.id) return `${tag}#${escapeCss(el.id)}`;
              if (el.name) return `${tag}[name="${String(el.name).replace(/"/g, '\\"')}"]`;
              if (el.type) return `${tag}[type="${String(el.type).replace(/"/g, '\\"')}"]`;
              return tag;
            }
            function isVisible(el) {
              if (!el) return false;
              const style = window.getComputedStyle(el);
              const rect = el.getBoundingClientRect();
              if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
              return rect.width > 0 && rect.height > 0;
            }
            function submitSelector(form) {
              if (!form) return '';
              const submit = form.querySelector('button[type="submit"], input[type="submit"], button:not([type]), [role="button"]');
              return selectorFor(submit);
            }
            return nodes.map((el) => {
              const form = el.form || el.closest('form');
              const type = (el.getAttribute('type') || 'text').toLowerCase();
              const name = el.getAttribute('name') || '';
              const placeholder = el.getAttribute('placeholder') || '';
              const ariaLabel = el.getAttribute('aria-label') || '';
              const id = el.getAttribute('id') || '';
              const searchish = type === 'search'
                || SEARCH_NAMES.has(name.toLowerCase())
                || /search/i.test(placeholder)
                || /search/i.test(ariaLabel)
                || /search/i.test(id);
              if (!searchish) return null;
              return {
                selector: selectorFor(el),
                name,
                type,
                id,
                placeholder,
                aria_label: ariaLabel,
                visible: isVisible(el),
                form_selector: selectorFor(form),
                form_action: form ? (form.getAttribute('action') || '') : '',
                form_method: form ? ((form.getAttribute('method') || 'get').toLowerCase()) : '',
                submit_selector: submitSelector(form),
              };
            }).filter(Boolean);
        }"""
    )


def _chosen_locator_meta(locator) -> dict[str, object]:
    return locator.evaluate(
        """(el) => {
            function escapeCss(value) {
              try { return CSS.escape(value); } catch (err) { return value; }
            }
            function selectorFor(node) {
              if (!node) return '';
              const tag = (node.tagName || 'input').toLowerCase();
              if (node.id) return `${tag}#${escapeCss(node.id)}`;
              if (node.name) return `${tag}[name="${String(node.name).replace(/"/g, '\\"')}"]`;
              if (node.type) return `${tag}[type="${String(node.type).replace(/"/g, '\\"')}"]`;
              return tag;
            }
            function isVisible(node) {
              const style = window.getComputedStyle(node);
              const rect = node.getBoundingClientRect();
              if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
              return rect.width > 0 && rect.height > 0;
            }
            const form = el.form || el.closest('form');
            const submit = form ? form.querySelector('button[type="submit"], input[type="submit"], button:not([type]), [role="button"]') : null;
            return {
              selector: selectorFor(el),
              name: el.getAttribute('name') || '',
              type: (el.getAttribute('type') || 'text').toLowerCase(),
              visible: isVisible(el),
              form_selector: selectorFor(form),
              form_action: form ? (form.getAttribute('action') || '') : '',
              form_method: form ? ((form.getAttribute('method') || 'get').toLowerCase()) : '',
              submit_selector: selectorFor(submit),
            };
        }"""
    )


def _capture_search_submit(
    page,
    context,
    root_url: str,
    search_template: str,
    sku: str,
) -> dict[str, object]:
    captured: list[dict[str, object]] = []
    index_by_request_id: dict[int, int] = {}
    root_domain = urllib.parse.urlparse(root_url).netloc.lower()

    def on_request(request) -> None:
        headers = _header_map(request)
        record = {
            "url": _clean(request.url),
            "method": _clean(request.method).upper(),
            "resource_type": _clean(request.resource_type),
            "headers": headers,
            "post_data": _request_post_data(request),
            "status": 0,
        }
        index_by_request_id[id(request)] = len(captured)
        captured.append(record)

    def on_response(response) -> None:
        idx = index_by_request_id.get(id(response.request))
        if idx is None:
            return
        try:
            captured[idx]["status"] = int(response.status)
        except Exception:
            captured[idx]["status"] = 0

    page.on("request", on_request)
    page.on("response", on_response)
    navigation_status = 0

    try:
        page.goto(root_url, wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(1200)
        fv._dismiss_common_popups(page)
        dom_candidates = _dom_candidates(page)
        search_input = fv._playwright_find_search_input(page)
        if search_input is None:
            fv._open_search_ui(page)
            page.wait_for_timeout(600)
            fv._dismiss_common_popups(page)
            dom_candidates = _dom_candidates(page)
            search_input = fv._playwright_find_search_input(page)

        chosen_meta: dict[str, object] = {}
        if search_input is not None:
            chosen_meta = _chosen_locator_meta(search_input)
            try:
                search_input.scroll_into_view_if_needed(timeout=1200)
            except Exception:
                pass
            try:
                search_input.evaluate("(el) => { el.removeAttribute('readonly'); el.removeAttribute('disabled'); }")
            except Exception:
                pass

        start_url = _clean(page.url)
        submit_error = ""
        if search_input is None:
            submit_error = "No visible search input found"
        else:
            try:
                search_input.click(timeout=1500, force=True)
            except Exception:
                pass
            try:
                search_input.fill(sku, timeout=2500)
            except Exception:
                search_input.evaluate(
                    """(el, value) => {
                        el.value = value;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    sku,
                )
            page.wait_for_timeout(250)
            captured.clear()
            index_by_request_id.clear()
            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=9000) as nav_info:
                    search_input.press("Enter")
                response = nav_info.value
                navigation_status = int(response.status) if response is not None else 0
            except Exception:
                try:
                    form = search_input.locator("xpath=ancestor::form[1]")
                    submit = form.locator("button, input[type='submit']").first
                    if submit.count():
                        with page.expect_navigation(wait_until="domcontentloaded", timeout=9000) as nav_info:
                            submit.click(timeout=1500, force=True)
                        response = nav_info.value
                        navigation_status = int(response.status) if response is not None else 0
                    else:
                        page.wait_for_timeout(2500)
                except Exception as exc:
                    submit_error = _clean(exc)
            page.wait_for_timeout(2200)

        final_url = _clean(page.url)
        html = page.content()
        kind, _, success, blocked, notes = dv._classify_probe(
            start_url,
            final_url,
            html,
            navigation_status or 200,
            submit_error,
            sku,
        )
        context_cookies = context.cookies()

        relevant_requests: list[dict[str, object]] = []
        for item in captured:
            url = _clean(item.get("url"))
            body = _clean(item.get("post_data"))
            domain = urllib.parse.urlparse(url).netloc.lower()
            if not url or _request_is_static(url):
                continue
            if item.get("resource_type") not in {"document", "xhr", "fetch"} and not _is_search_like(url, body, sku):
                continue
            if domain and domain != root_domain and not _is_search_like(url, body, sku):
                continue
            relevant = dict(item)
            relevant["url_template"] = _templateize_text(url, sku)
            relevant["post_data_template"] = _templateize_text(body, sku)
            relevant_requests.append(relevant)

        synthetic_request = {}
        primary_url = _render_template(search_template, sku) if search_template else final_url
        method = _clean(chosen_meta.get("form_method")).upper() or "GET"
        if primary_url:
            synthetic_request = {
                "url": primary_url,
                "method": method,
                "resource_type": "document",
                "headers": {},
                "post_data": "",
                "status": navigation_status,
                "url_template": _clean(search_template) or _templateize_text(primary_url, sku),
                "post_data_template": "",
                "synthetic": True,
            }
            input_name = _clean(chosen_meta.get("name"))
            if method == "POST" and input_name:
                synthetic_request["post_data"] = f"{urllib.parse.quote_plus(input_name)}={urllib.parse.quote_plus(sku)}"
                synthetic_request["post_data_template"] = f"{input_name}={{sku}}"
            relevant_requests.append(synthetic_request)

        def score_request(item: dict[str, object]) -> tuple[int, int]:
            url = _clean(item.get("url"))
            body = _clean(item.get("post_data"))
            resource_type = _clean(item.get("resource_type"))
            status = int(item.get("status") or 0)
            domain = urllib.parse.urlparse(url).netloc.lower()
            low_url = url.lower()
            score = 0
            if resource_type in {"xhr", "fetch"}:
                score += 80
            elif resource_type == "document":
                score += 40
            if _is_search_like(url, body, sku):
                score += 120
            if item.get("synthetic"):
                score += 200
            if domain == root_domain:
                score += 100
            else:
                score -= 120
            if _clean(item.get("url_template")) == _clean(search_template):
                score += 180
            if _clean(item.get("url_template")) == _clean(final_url_template := _templateize_text(final_url, sku)):
                score += 120
            if any(token in low_url for token in ("analytics", "collect", "pixel", "affirm", "clarity", "hubapi")):
                score -= 160
            if "/api/" in url.lower() or "autocomplete" in url.lower():
                score += 50
            if _clean(item.get("method")).upper() == "POST":
                score += 20
            if 200 <= status < 400:
                score += 10
            return score, -len(url)

        relevant_requests.sort(key=score_request, reverse=True)
        primary_request = relevant_requests[0] if relevant_requests else {}
        primary_xhr = next(
            (
                item
                for item in relevant_requests
                if _is_search_xhr_candidate(item, search_template=search_template, sku=sku, root_domain=root_domain)
            ),
            {},
        )

        api_observed = "yes" if primary_xhr else "no"
        hidden_form_observed = "yes" if dom_candidates and not any(bool(item.get("visible")) for item in dom_candidates) else "no"
        browser_required_observed = "yes" if search_input is None or blocked else "no"

        if primary_xhr:
            recipe_kind = "xhr_api"
            if _clean(chosen_meta.get("form_method")).lower() == "post":
                recipe_kind = "form_post_with_xhr"
        elif _clean(chosen_meta.get("form_method")).lower() == "post":
            recipe_kind = "form_post_then_results_page"
        elif hidden_form_observed == "yes":
            recipe_kind = "hidden_form_get"
        elif chosen_meta:
            recipe_kind = "form_get"
        elif search_template:
            recipe_kind = "direct_template_get"
        else:
            recipe_kind = "unknown"

        final_url_template = _templateize_text(final_url, sku)
        return {
            "search_submit_status": "ok" if success else "review",
            "dom_candidates": dom_candidates,
            "chosen_dom": chosen_meta,
            "captured_requests": relevant_requests[:12],
            "primary_request": primary_request,
            "primary_xhr": primary_xhr,
            "context_cookies": context_cookies,
            "final_url_template": final_url_template,
            "final_result_kind": kind,
            "notes": _clean(notes or submit_error),
            "search_recipe_kind": recipe_kind,
            "api_observed": api_observed,
            "hidden_form_observed": hidden_form_observed,
            "browser_required_observed": browser_required_observed,
        }
    finally:
        try:
            page.remove_listener("request", on_request)
            page.remove_listener("response", on_response)
        except Exception:
            pass


def _validate_template(page, search_template: str, skus: list[str]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for sku in skus:
        url = _render_template(search_template, sku)
        html, status, final_url, error = dv._fetch_url(url)
        kind, _, success, blocked, notes = dv._classify_probe(url, final_url, html, status or 0, error, sku)
        results.append(
            {
                "sku": sku,
                "probe_url": url,
                "final_url": final_url,
                "status": status,
                "result_kind": kind,
                "success": success,
                "blocked": blocked,
                "notes": _clean(notes or error),
            }
        )
    return results


def _build_summary(validation_results: list[dict[str, object]]) -> tuple[int, int, str]:
    total = len(validation_results)
    success_count = sum(1 for item in validation_results if bool(item.get("success")))
    if not validation_results:
        return 0, 0, ""
    parts = []
    for item in validation_results:
        parts.append(
            f"{item['sku']}:{item['result_kind']}"
            + ("" if item.get("success") else f"({item.get('notes') or 'fail'})")
        )
    return success_count, total, " | ".join(parts)


def _headers_summary(request_item: dict[str, object]) -> str:
    headers = request_item.get("headers") or {}
    if not isinstance(headers, dict):
        return ""
    return "|".join(sorted(headers.keys()))


def _cookie_header(request_item: dict[str, object]) -> str:
    headers = request_item.get("headers") or {}
    if not isinstance(headers, dict):
        return ""
    for key, value in headers.items():
        if str(key).lower() == "cookie":
            return _clean(value)
    return ""


def _request_with_curl(request_item: dict[str, object]) -> dict[str, object]:
    item = dict(request_item or {})
    headers = item.get("headers") if isinstance(item.get("headers"), dict) else {}
    item["curl"] = _to_curl(
        method=_clean(item.get("method") or "GET"),
        url=_clean(item.get("url") or item.get("url_template")),
        headers={str(key): str(value) for key, value in headers.items()},
        body=_clean(item.get("post_data")),
    ) if item else ""
    return item


def _evidence_row(
    row: dict[str, str],
    sample_skus: list[str],
    submit_data: dict[str, object],
    validation_results: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    primary_request = _request_with_curl(submit_data.get("primary_request") or {})
    primary_xhr = _request_with_curl(submit_data.get("primary_xhr") or {})
    success_count, total_count, validation_summary = _build_summary(validation_results)
    chosen_dom = submit_data.get("chosen_dom") or {}
    dom_candidates = submit_data.get("dom_candidates") or []
    relevant_requests = submit_data.get("captured_requests") or []
    xhr_templates = []
    for item in relevant_requests:
        if item.get("resource_type") in {"xhr", "fetch"}:
            template = _clean(item.get("url_template") or item.get("url"))
            if template and template not in xhr_templates:
                xhr_templates.append(template)

    csv_row = {
        "vendor": _clean(row.get("vendor")),
        "display_name": _clean(row.get("display_name")),
        "official_website_url": _clean(row.get("official_website_url")),
        "search_url_template": _clean(row.get("search_url_template")),
        "sample_sku_primary": sample_skus[0] if sample_skus else _clean(row.get("sample_sku")),
        "validation_sample_skus": "|".join(sample_skus),
        "validation_success_count": success_count,
        "validation_total_count": total_count,
        "validation_summary": validation_summary,
        "search_recipe_kind": _clean(submit_data.get("search_recipe_kind")),
        "search_submit_status": _clean(submit_data.get("search_submit_status")),
        "dom_search_candidates_found": len(dom_candidates),
        "dom_search_input_selector": _clean(chosen_dom.get("selector")),
        "dom_search_input_name": _clean(chosen_dom.get("name")),
        "dom_search_input_type": _clean(chosen_dom.get("type")),
        "dom_search_input_visible": _clean(chosen_dom.get("visible")),
        "dom_form_selector": _clean(chosen_dom.get("form_selector")),
        "dom_form_action": _clean(chosen_dom.get("form_action")),
        "dom_form_method": _clean(chosen_dom.get("form_method")),
        "dom_submit_selector": _clean(chosen_dom.get("submit_selector")),
        "primary_request_method": _clean(primary_request.get("method")),
        "primary_request_resource_type": _clean(primary_request.get("resource_type")),
        "primary_request_url_template": _clean(primary_request.get("url_template") or primary_request.get("url")),
        "primary_request_post_data_template": _clean(primary_request.get("post_data_template")),
        "primary_request_status": _clean(primary_request.get("status")),
        "primary_request_header_names": _headers_summary(primary_request),
        "primary_request_cookie_header": _shorten(_cookie_header(primary_request), 240),
        "primary_request_curl": _shorten(primary_request.get("curl"), 700),
        "primary_xhr_method": _clean(primary_xhr.get("method")),
        "primary_xhr_url_template": _clean(primary_xhr.get("url_template") or primary_xhr.get("url")),
        "primary_xhr_post_data_template": _clean(primary_xhr.get("post_data_template")),
        "primary_xhr_status": _clean(primary_xhr.get("status")),
        "primary_xhr_header_names": _headers_summary(primary_xhr),
        "primary_xhr_cookie_header": _shorten(_cookie_header(primary_xhr), 240),
        "primary_xhr_curl": _shorten(primary_xhr.get("curl"), 700),
        "all_relevant_xhr_templates": " | ".join(xhr_templates),
        "final_url_template": _clean(submit_data.get("final_url_template")),
        "final_result_kind": _clean(submit_data.get("final_result_kind")),
        "api_observed": _clean(submit_data.get("api_observed")),
        "hidden_form_observed": _clean(submit_data.get("hidden_form_observed")),
        "browser_required_observed": _clean(submit_data.get("browser_required_observed")),
        "notes": _clean(submit_data.get("notes")),
    }

    json_row = {
        **csv_row,
        "platform_family": _clean(row.get("platform_family")),
        "search_family": _clean(row.get("search_family")),
        "transport": _clean(row.get("transport")),
        "parameter_name": _clean(row.get("parameter_name")),
        "confidence_bucket": _clean(row.get("confidence_bucket")),
        "resolver_hint": _clean(row.get("resolver_hint")),
        "existing_review_notes": _clean(row.get("review_notes")),
        "dom_candidates": dom_candidates,
        "chosen_dom": chosen_dom,
        "relevant_requests": relevant_requests,
        "primary_request_full": primary_request,
        "primary_xhr_full": primary_xhr,
        "context_cookies": submit_data.get("context_cookies") or [],
        "validation_results": validation_results,
        "machine_recipe": {
            "entry_url": _clean(row.get("official_website_url")),
            "search_url_template": _clean(row.get("search_url_template")),
            "recipe_kind": _clean(submit_data.get("search_recipe_kind")),
            "dom": {
                "input_selector": _clean(chosen_dom.get("selector")),
                "input_name": _clean(chosen_dom.get("name")),
                "form_selector": _clean(chosen_dom.get("form_selector")),
                "form_action": _clean(chosen_dom.get("form_action")),
                "form_method": _clean(chosen_dom.get("form_method")),
                "submit_selector": _clean(chosen_dom.get("submit_selector")),
            },
            "request": {
                "method": _clean(primary_request.get("method")),
                "resource_type": _clean(primary_request.get("resource_type")),
                "url_template": _clean(primary_request.get("url_template") or primary_request.get("url")),
                "post_data_template": _clean(primary_request.get("post_data_template")),
            },
            "xhr": {
                "method": _clean(primary_xhr.get("method")),
                "url_template": _clean(primary_xhr.get("url_template") or primary_xhr.get("url")),
                "post_data_template": _clean(primary_xhr.get("post_data_template")),
            },
            "validation_sample_skus": sample_skus,
            "validation_success_count": success_count,
            "validation_total_count": total_count,
            "final_result_kind": _clean(submit_data.get("final_result_kind")),
            "api_observed": _clean(submit_data.get("api_observed")),
            "hidden_form_observed": _clean(submit_data.get("hidden_form_observed")),
            "browser_required_observed": _clean(submit_data.get("browser_required_observed")),
        },
    }
    return csv_row, json_row


def _error_evidence_row(row: dict[str, str], sample_skus: list[str], error: str) -> tuple[dict[str, object], dict[str, object]]:
    csv_row = {
        "vendor": _clean(row.get("vendor")),
        "display_name": _clean(row.get("display_name")),
        "official_website_url": _clean(row.get("official_website_url")),
        "search_url_template": _clean(row.get("search_url_template")),
        "sample_sku_primary": sample_skus[0] if sample_skus else _clean(row.get("sample_sku")),
        "validation_sample_skus": "|".join(sample_skus),
        "validation_success_count": 0,
        "validation_total_count": 0,
        "validation_summary": "",
        "search_recipe_kind": "error",
        "search_submit_status": "error",
        "dom_search_candidates_found": 0,
        "dom_search_input_selector": "",
        "dom_search_input_name": "",
        "dom_search_input_type": "",
        "dom_search_input_visible": "",
        "dom_form_selector": "",
        "dom_form_action": "",
        "dom_form_method": "",
        "dom_submit_selector": "",
        "primary_request_method": "",
        "primary_request_resource_type": "",
        "primary_request_url_template": _clean(row.get("search_url_template")),
        "primary_request_post_data_template": "",
        "primary_request_status": "",
        "primary_request_header_names": "",
        "primary_request_cookie_header": "",
        "primary_request_curl": "",
        "primary_xhr_method": "",
        "primary_xhr_url_template": "",
        "primary_xhr_post_data_template": "",
        "primary_xhr_status": "",
        "primary_xhr_header_names": "",
        "primary_xhr_cookie_header": "",
        "primary_xhr_curl": "",
        "all_relevant_xhr_templates": "",
        "final_url_template": "",
        "final_result_kind": "error",
        "api_observed": "no",
        "hidden_form_observed": "no",
        "browser_required_observed": "unknown",
        "notes": _clean(error),
    }
    json_row = {
        **csv_row,
        "platform_family": _clean(row.get("platform_family")),
        "search_family": _clean(row.get("search_family")),
        "transport": _clean(row.get("transport")),
        "parameter_name": _clean(row.get("parameter_name")),
        "confidence_bucket": _clean(row.get("confidence_bucket")),
        "resolver_hint": _clean(row.get("resolver_hint")),
        "existing_review_notes": _clean(row.get("review_notes")),
        "dom_candidates": [],
        "chosen_dom": {},
        "relevant_requests": [],
        "primary_request_full": {},
        "primary_xhr_full": {},
        "context_cookies": [],
        "validation_results": [],
        "machine_recipe": {
            "entry_url": _clean(row.get("official_website_url")),
            "search_url_template": _clean(row.get("search_url_template")),
            "recipe_kind": "error",
            "error": _clean(error),
        },
    }
    return csv_row, json_row


def _collect_vendor_evidence(
    row: dict[str, str],
    sample_skus: list[str],
) -> dict[str, object]:
    from playwright.sync_api import sync_playwright

    root_url = dv._root_url(_clean(row.get("official_website_url")))
    search_template = _clean(row.get("search_url_template"))
    if not root_url or not search_template:
        return {
            "error": "missing root_url or search template",
            "row": row,
            "sample_skus": sample_skus,
        }

    with sync_playwright() as play:
        browser = play.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=dv.USER_AGENT,
            ignore_https_errors=True,
        )
        page = context.new_page()
        try:
            primary_sku = sample_skus[0] if sample_skus else _clean(row.get("sample_sku"))
            submit_data = _capture_search_submit(
                page=page,
                context=context,
                root_url=root_url,
                search_template=search_template,
                sku=primary_sku,
            )
            validation_results = _validate_template(page=page, search_template=search_template, skus=sample_skus)
            csv_row, json_row = _evidence_row(
                row=row,
                sample_skus=sample_skus,
                submit_data=submit_data,
                validation_results=validation_results,
            )
            return {"csv_row": csv_row, "json_row": json_row}
        finally:
            try:
                page.close()
            except Exception:
                pass
            context.close()
            browser.close()


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect request-level resolver evidence for vendors that already have search_url_template."
    )
    parser.add_argument("--resolved", default="app/required/mappings/discovery/VendorDiscoveryResolvedWorklist.csv")
    parser.add_argument("--vendor-profiles", default="app/required/mappings/VendorProfiles.csv")
    parser.add_argument("--sample-hints", default="app/required/mappings/VendorSkuPrefixHints.csv")
    parser.add_argument("--csv-output", default="app/required/mappings/discovery/VendorResolverEvidence.csv")
    parser.add_argument("--json-output", default="app/required/mappings/discovery/VendorResolverEvidence.json")
    parser.add_argument("--vendor-filter", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--validation-limit", type=int, default=3)
    args = parser.parse_args()

    resolved_path = Path(args.resolved).expanduser().resolve()
    vendor_profiles_path = Path(args.vendor_profiles).expanduser().resolve()
    sample_hints_path = Path(args.sample_hints).expanduser().resolve()
    csv_output_path = Path(args.csv_output).expanduser().resolve()
    json_output_path = Path(args.json_output).expanduser().resolve()

    rows = _load_csv(resolved_path)
    sample_map = _load_sample_map(sample_hints_path, vendor_profiles_path)

    needle = _clean(args.vendor_filter).lower()
    targets: list[tuple[dict[str, str], list[str]]] = []
    for row in rows:
        vendor = _clean(row.get("vendor"))
        display_name = _clean(row.get("display_name"))
        if needle and needle not in vendor.lower() and needle not in display_name.lower():
            continue
        sample_skus = _sample_skus_for_row(row, sample_map, max(1, args.validation_limit))
        if not sample_skus:
            sample_skus = [_clean(row.get("sample_sku"))] if _clean(row.get("sample_sku")) else []
        targets.append((row, sample_skus))
    if args.limit > 0:
        targets = targets[: args.limit]

    print(f"Vendors queued: {len(targets)}")
    print(f"Workers: {max(1, args.workers)}")
    csv_rows: list[dict[str, object]] = []
    json_rows: list[dict[str, object]] = []

    if max(1, args.workers) == 1:
        for index, (row, sample_skus) in enumerate(targets, start=1):
            vendor = _clean(row.get("vendor"))
            print(f"[{index}/{len(targets)}] {vendor}")
            try:
                result = _collect_vendor_evidence(row, sample_skus)
            except Exception as exc:
                result = {}
                csv_row, json_row = _error_evidence_row(row, sample_skus, _clean(exc))
                csv_rows.append(csv_row)
                json_rows.append(json_row)
                _write_csv(csv_output_path, csv_rows)
                _write_json(json_output_path, json_rows)
                print(f"  error: {exc}")
                continue
            if result.get("error"):
                csv_row, json_row = _error_evidence_row(row, sample_skus, _clean(result["error"]))
                csv_rows.append(csv_row)
                json_rows.append(json_row)
                _write_csv(csv_output_path, csv_rows)
                _write_json(json_output_path, json_rows)
                print(f"  error: {result['error']}")
                continue
            csv_rows.append(result["csv_row"])
            json_rows.append(result["json_row"])
            _write_csv(csv_output_path, csv_rows)
            _write_json(json_output_path, json_rows)
            print(
                "  "
                + _clean(result["csv_row"].get("search_recipe_kind"))
                + " | "
                + _clean(result["csv_row"].get("validation_summary"))
            )
    else:
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {
                executor.submit(_collect_vendor_evidence, row, sample_skus): (row, sample_skus)
                for row, sample_skus in targets
            }
            completed = 0
            for future in as_completed(future_map):
                completed += 1
                row, sample_skus = future_map[future]
                vendor = _clean(row.get("vendor"))
                try:
                    result = future.result()
                except Exception as exc:
                    csv_row, json_row = _error_evidence_row(row, sample_skus, _clean(exc))
                    csv_rows.append(csv_row)
                    json_rows.append(json_row)
                    csv_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                    json_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                    _write_csv(csv_output_path, csv_rows)
                    _write_json(json_output_path, json_rows)
                    print(f"[{completed}/{len(targets)}] {vendor} -> error: {exc}")
                    continue
                if result.get("error"):
                    csv_row, json_row = _error_evidence_row(row, sample_skus, _clean(result["error"]))
                    csv_rows.append(csv_row)
                    json_rows.append(json_row)
                    csv_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                    json_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                    _write_csv(csv_output_path, csv_rows)
                    _write_json(json_output_path, json_rows)
                    print(f"[{completed}/{len(targets)}] {vendor} -> error: {result['error']}")
                    continue
                csv_rows.append(result["csv_row"])
                json_rows.append(result["json_row"])
                csv_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                json_rows.sort(key=lambda item: _clean(item.get("vendor")).lower())
                _write_csv(csv_output_path, csv_rows)
                _write_json(json_output_path, json_rows)
                print(
                    f"[{completed}/{len(targets)}] {vendor} -> "
                    f"{_clean(result['csv_row'].get('search_recipe_kind'))} | "
                    f"{_clean(result['csv_row'].get('validation_summary'))}"
                )

    print(f"Wrote {len(csv_rows)} evidence rows to {csv_output_path}")
    print(f"Wrote {len(json_rows)} evidence rows to {json_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
