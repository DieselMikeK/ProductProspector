from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

DEV_ROOT = Path(__file__).resolve().parents[1]
if str(DEV_ROOT) not in sys.path:
    sys.path.insert(0, str(DEV_ROOT))


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

SOCIAL_OR_NOISE_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "linkedin.com",
    "wikipedia.org",
    "pinterest.com",
    "reddit.com",
    "duckduckgo.com",
    "bing.com",
    "google.com",
    "yahoo.com",
}

MARKETPLACE_OR_RESELLER_DOMAINS = {
    "amazon.com",
    "ebay.com",
    "walmart.com",
    "shopify.com",
    "aliexpress.com",
    "temu.com",
    "summitracing.com",
    "dieselpowerproducts.com",
    "thoroughbreddiesel.com",
    "oreillyauto.com",
    "autozone.com",
    "advanceautoparts.com",
    "jegs.com",
    "4wheelparts.com",
    "xtremediesel.com",
    "puredieselpower.com",
    "alligatorperformance.com",
    "realtruck.com",
}

SUSPICIOUS_SUBDOMAIN_PREFIXES = {
    "blog",
    "forum",
    "forums",
    "community",
    "news",
    "support",
    "help",
}

RESELLER_HINT_PHRASES = {
    "authorized dealer",
    "shop now",
    "buy now",
    "find the best",
    "free shipping",
    "in stock",
    "discover premium",
}

OFFICIAL_HINT_PHRASES = {
    "official site",
    "official website",
    "manufacturer",
    "factory",
    "our story",
    "about us",
}

SEARCH_TEMPLATES = [
    "/search?q={sku}",
    "/search/?q={sku}",
    "/search?q={sku}&options[prefix]=last",
    "/search?q={sku}&options%5Bprefix%5D=last",
    "/search?query={sku}",
    "/search?keyword={sku}",
    "/search?keywords={sku}",
    "/search?s={sku}",
    "/?s={sku}",
    "/search?type=product&q={sku}",
    "/search?type=product&q={sku}&options[prefix]=last",
    "/search?type=product&options[prefix]=last&q={sku}",
    "/search?type=product&options[prefix]=last&options[unavailable_products]=last&q={sku}",
    "/search?type=product&options%5Bprefix%5D=last&options%5Bunavailable_products%5D=last&q={sku}",
    "/search?type=product%2Cpage%2Carticle%2Ccollection&options[prefix]=last&q={sku}",
    "/search?type=product%2Cpage%2Carticle%2Ccollection&options%5Bprefix%5D=last&q={sku}",
    "/search?type=product%2Carticle%2Cpage%2Ccollection&options[prefix]=last&q={sku}",
    "/search/{sku}",
    "/pages/search?query={sku}",
    "/pages/search?find={sku}",
    "/pages/search-results-page?q={sku}",
    "/catalogsearch/result/?q={sku}",
    "/catalogsearch/result?q={sku}",
    "/products/search?q={sku}",
    "/search.php?search_query={sku}",
    "/search.html?q={sku}",
    "/search.html?Search={sku}",
    "/search-diesel.html?orientation=vertical&q={sku}",
    "/search.asp?keyword={sku}",
    "/search.aspx?find={sku}",
    "/?target=search&mode=search&substring={sku}",
    "/?target=search&mode=search&substring={sku}&including=all",
    "/Products/Search?search={sku}",
    "/catalogue?typeofsearch=1&search_query_adv={sku}",
    "/catalog?search_api_fulltext={sku}",
    "/gsearch.aspx?type=oesearch&origin=oesearch&q={sku}",
    "/isearch3?searchterm={sku}",
    "/Search/PartNoSearch?q={sku}",
]

AUTO_ACCEPT_RESULT_KINDS = {"product_page", "search_results"}

STOP_WORDS = {
    "and",
    "inc",
    "co",
    "company",
    "products",
    "performance",
    "diesel",
    "offroad",
    "off-road",
    "the",
    "llc",
    "usa",
    "official",
}


@dataclass
class VendorProfile:
    canonical_vendor: str
    display_name: str
    aliases: list[str] = field(default_factory=list)


@dataclass
class WebsiteCandidate:
    vendor: str
    query: str
    candidate_url: str
    domain: str
    score: int
    reasons: str
    title: str = ""
    snippet: str = ""


@dataclass
class ProbeResult:
    vendor: str
    site_url: str
    template: str
    probe_url: str
    final_url: str
    http_status: int
    result_kind: str
    sku_present: bool
    success: bool
    blocked: bool
    used_playwright: bool
    error: str = ""
    notes: str = ""


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_key(value: object) -> str:
    text = _clean_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _compact_space(value: str) -> str:
    text = _clean_text(value)
    return re.sub(r"\s+", " ", text).strip()


def _split_aliases(value: object) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    parts = re.split(r"[|\n;]+", text)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        item = _compact_space(part)
        if not item:
            continue
        key = _norm_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _vendor_tokens(value: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9]+", " ", _clean_text(value).lower())
    tokens: list[str] = []
    for token in cleaned.split():
        if len(token) < 3:
            continue
        if token in STOP_WORDS:
            continue
        tokens.append(token)
    return tokens


def _domain_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    domain = _clean_text(parsed.netloc).lower()
    if ":" in domain:
        domain = domain.split(":", 1)[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _root_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    domain = _domain_from_url(url)
    if not domain:
        return ""
    return f"{scheme}://{domain}"


def _looks_like_bot_challenge(html: str) -> bool:
    low = _clean_text(html).lower()
    if not low:
        return False
    signals = [
        "<title>just a moment",
        "<title>verifying your connection",
        "__cf_chl",
        "cf-browser-verification",
        "challenge-platform",
        "captcha",
    ]
    return any(signal in low for signal in signals)


def _has_rate_limit_error(status_code: int, error_text: str) -> bool:
    low = _clean_text(error_text).lower()
    if status_code == 429:
        return True
    return ("http 429" in low) or ("too many requests" in low) or ("rate limit" in low)


def _fetch_url(url: str, timeout: int = 20) -> tuple[str, int, str, str]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            status = int(response.getcode() or 0)
            final_url = _clean_text(response.geturl() or url)
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            error_body = ""
        return error_body, int(exc.code or 0), _clean_text(exc.geturl() or url), f"HTTP {exc.code}"
    except Exception as exc:
        return "", 0, _clean_text(url), str(exc)

    text = body.decode("utf-8", errors="ignore")
    return text, status, final_url, ""


def _playwright_fetch(url: str, timeout_ms: int = 25000) -> tuple[str, int, str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return "", 0, url, f"Playwright unavailable: {exc}"

    try:
        with sync_playwright() as play:
            browser = play.chromium.launch(headless=True)
            page = browser.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1200)
            html = page.content()
            final_url = _clean_text(page.url or url)
            status = int(response.status) if response is not None else 0
            browser.close()
            return html, status, final_url, ""
    except Exception as exc:
        return "", 0, url, str(exc)


def _extract_ddg_results(html: str) -> list[tuple[str, str, str]]:
    if not html:
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    anchor_pattern = re.compile(
        r'(?is)<a[^>]+class=["\'][^"\']*result__a[^"\']*["\'][^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>'
    )
    snippet_pattern = re.compile(r'(?is)<a[^>]+class=["\'][^"\']*result__snippet[^"\']*["\'][^>]*>(.*?)</a>')
    snippets = [re.sub(r"<[^>]+>", " ", match.group(1)) for match in snippet_pattern.finditer(html)]
    snippet_index = 0

    for match in anchor_pattern.finditer(html):
        href_raw = _clean_text(match.group(1))
        title = _compact_space(re.sub(r"<[^>]+>", " ", match.group(2)))
        if not href_raw:
            continue
        href = urllib.parse.unquote(href_raw)
        if "duckduckgo.com/l/?" in href:
            parsed = urllib.parse.urlparse(href)
            params = urllib.parse.parse_qs(parsed.query)
            uddg = _clean_text((params.get("uddg") or [""])[0])
            if uddg:
                href = urllib.parse.unquote(uddg)
        if href.startswith("//"):
            href = f"https:{href}"
        if href.startswith("/"):
            href = urllib.parse.urljoin("https://duckduckgo.com", href)
            parsed = urllib.parse.urlparse(href)
            params = urllib.parse.parse_qs(parsed.query)
            uddg = _clean_text((params.get("uddg") or [""])[0])
            if uddg:
                href = urllib.parse.unquote(uddg)
        if not href.lower().startswith("http"):
            continue
        domain = _domain_from_url(href)
        if not domain:
            continue
        if href in seen:
            continue
        seen.add(href)
        snippet = ""
        if snippet_index < len(snippets):
            snippet = _compact_space(snippets[snippet_index])
            snippet_index += 1
        out.append((href, title, snippet))
    return out


def _extract_proxy_markdown_links(text: str) -> list[tuple[str, str, str]]:
    if not text:
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r"\[([^\]]*)\]\(([^)]+)\)", text):
        title = _compact_space(match.group(1))
        href = _clean_text(match.group(2))
        if not href or href.startswith("data:"):
            continue
        if title.startswith("!"):
            continue
        if href.startswith("http://duckduckgo.com/l/?") or href.startswith("https://duckduckgo.com/l/?"):
            parsed = urllib.parse.urlparse(href)
            params = urllib.parse.parse_qs(parsed.query)
            uddg = _clean_text((params.get("uddg") or [""])[0])
            if uddg:
                href = urllib.parse.unquote(uddg)
        if href.startswith("//"):
            href = f"https:{href}"
        if not href.lower().startswith("http"):
            continue
        domain = _domain_from_url(href)
        if not domain:
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append((href, title, ""))
    return out


def _extract_bing_results(html: str) -> list[tuple[str, str, str]]:
    if not html:
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    block_pattern = re.compile(r'(?is)<li[^>]+class=["\'][^"\']*b_algo[^"\']*["\'][^>]*>(.*?)</li>')
    for block_match in block_pattern.finditer(html):
        block = block_match.group(1)
        anchor_match = re.search(r'(?is)<h2[^>]*>\s*<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', block)
        if anchor_match is None:
            continue
        href = _clean_text(anchor_match.group(1))
        if not href or not href.lower().startswith("http"):
            continue
        domain = _domain_from_url(href)
        if not domain:
            continue
        if href in seen:
            continue
        seen.add(href)
        title = _compact_space(re.sub(r"<[^>]+>", " ", anchor_match.group(2)))
        snippet_match = re.search(r'(?is)<p>(.*?)</p>', block)
        snippet = _compact_space(re.sub(r"<[^>]+>", " ", snippet_match.group(1))) if snippet_match else ""
        out.append((href, title, snippet))
    return out


def _looks_like_ddg_challenge_page(html: str) -> bool:
    low = _clean_text(html).lower()
    if not low:
        return False
    challenge_signals = [
        "unusual traffic",
        "automated requests",
        "human verification",
        "sorry, this page isn't available right now",
    ]
    return any(signal in low for signal in challenge_signals)


def _score_website_candidate(
    url: str,
    vendor_names: list[str],
    query: str,
    title: str,
    snippet: str,
) -> tuple[int, list[str]]:
    domain = _domain_from_url(url)
    if not domain:
        return -999, ["invalid domain"]
    score = 0
    reasons: list[str] = []
    domain_key = _norm_key(domain)
    query_key = _norm_key(query)
    normalized_vendor_names: list[str] = []
    for name in vendor_names:
        text = _clean_text(name)
        if text:
            normalized_vendor_names.append(text)
    full_vendor_keys = [_norm_key(name) for name in normalized_vendor_names if _norm_key(name)]
    tokens: list[str] = []
    seen_tokens: set[str] = set()
    for name in normalized_vendor_names:
        for token in _vendor_tokens(name):
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            tokens.append(token)

    low_title = _clean_text(title).lower()
    low_snippet = _clean_text(snippet).lower()
    context_text = f"{low_title} {low_snippet}".strip()

    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in SOCIAL_OR_NOISE_DOMAINS):
        score -= 450
        reasons.append("social/noise domain")
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in MARKETPLACE_OR_RESELLER_DOMAINS):
        score -= 280
        reasons.append("marketplace/reseller domain")

    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lower()
    query_text = parsed.query.lower()
    host_parts = domain.split(".")
    if host_parts and host_parts[0] in SUSPICIOUS_SUBDOMAIN_PREFIXES:
        score -= 80
        reasons.append("suspicious subdomain")
    if "ad_domain=" in query_text or path.startswith("/y.js"):
        score -= 420
        reasons.append("ad redirect link")

    if domain.endswith(".com"):
        score += 12
        reasons.append(".com tld")

    full_match_hits = 0
    for vendor_key in full_vendor_keys:
        if len(vendor_key) < 5:
            continue
        if vendor_key in domain_key:
            full_match_hits += 1
    if full_match_hits:
        score += min(260, full_match_hits * 145)
        reasons.append(f"{full_match_hits} full vendor match(es) in domain")

    token_hits = 0
    for token in tokens:
        if token in domain_key:
            token_hits += 1
    if token_hits:
        score += min(210, token_hits * 60)
        reasons.append(f"{token_hits} vendor token hit(s)")

    context_matches = 0
    for vendor_key in full_vendor_keys:
        if len(vendor_key) < 5:
            continue
        if vendor_key in _norm_key(context_text):
            context_matches += 1
    if context_matches:
        score += min(120, context_matches * 40)
        reasons.append("vendor key present in result text")

    if any(phrase in context_text for phrase in OFFICIAL_HINT_PHRASES):
        score += 70
        reasons.append("official/manufacturer phrase")
    if any(phrase in context_text for phrase in RESELLER_HINT_PHRASES):
        score -= 90
        reasons.append("reseller phrase")

    if query_key and query_key in _norm_key(url):
        score += 15
        reasons.append("query context in url")

    if path in {"", "/"}:
        score += 26
        reasons.append("root path")
    elif path and path not in {"", "/"}:
        if "/products/" in path or "/product/" in path:
            score -= 42
            reasons.append("deep product path candidate")
        elif "/collections/" in path or "/pages/" in path or "/category/" in path:
            score -= 24
            reasons.append("non-home path candidate")
        elif "/search" in path:
            score -= 18
            reasons.append("search-page candidate")
        else:
            score -= 8
            reasons.append("deep path candidate")

    if parsed.query and "utm_" in parsed.query.lower():
        score -= 12
        reasons.append("tracking query params")

    return score, reasons


def _load_vendor_profiles(path: Path) -> list[VendorProfile]:
    profiles: list[VendorProfile] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            canonical = _clean_text(row.get("canonical_vendor", ""))
            shopify_vendor = _clean_text(row.get("shopify_vendor_value", ""))
            brand_name = _clean_text(row.get("brand_name", ""))
            aliases = _split_aliases(row.get("aliases", ""))
            base_name = canonical or shopify_vendor or brand_name
            if not base_name:
                continue
            merged_aliases: list[str] = []
            seen: set[str] = set()
            for value in [canonical, shopify_vendor, brand_name, *aliases]:
                name = _compact_space(value)
                if not name:
                    continue
                key = _norm_key(name)
                if not key or key in seen:
                    continue
                seen.add(key)
                merged_aliases.append(name)
            profiles.append(VendorProfile(canonical_vendor=base_name, display_name=brand_name or shopify_vendor or base_name, aliases=merged_aliases))
    profiles.sort(key=lambda item: item.canonical_vendor.lower())
    return profiles


def _load_seeded_vendor_sites(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return {}
        fieldnames = {str(name) for name in reader.fieldnames}
        if "vendor" not in fieldnames or "official_website_url" not in fieldnames:
            return {}
        output: dict[str, str] = {}
        for row in reader:
            vendor = _clean_text(row.get("vendor", ""))
            website = _clean_text(row.get("official_website_url", ""))
            if not vendor or not website:
                continue
            output[vendor] = website
        return output


def _build_alias_map(profiles: Iterable[VendorProfile]) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for profile in profiles:
        for alias in profile.aliases:
            key = _norm_key(alias)
            if not key:
                continue
            alias_map.setdefault(key, profile.canonical_vendor)
    return alias_map


def _load_vendor_sample_skus(sku_csv_path: Path, alias_map: dict[str, str]) -> dict[str, list[str]]:
    vendor_candidates = ["vendor", "shopify_vendor", "canonical_vendor", "brand_name"]
    sku_candidates = ["sku", "variant_sku"]

    with sku_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return {}
        fields = [str(field) for field in reader.fieldnames]
        vendor_col = next((field for field in vendor_candidates if field in fields), "")
        sku_col = next((field for field in sku_candidates if field in fields), "")
        if not vendor_col or not sku_col:
            return {}

        output: dict[str, list[str]] = {}
        seen_by_vendor: dict[str, set[str]] = {}
        for row in reader:
            raw_vendor = _clean_text(row.get(vendor_col, ""))
            raw_sku = _clean_text(row.get(sku_col, ""))
            if not raw_vendor or not raw_sku:
                continue
            if _norm_key(raw_sku).startswith("corecharge"):
                continue
            if len(_norm_key(raw_sku)) < 4:
                continue
            vendor_key = _norm_key(raw_vendor)
            canonical_vendor = alias_map.get(vendor_key, raw_vendor)
            normalized_sku = _compact_space(raw_sku)
            output.setdefault(canonical_vendor, [])
            seen_by_vendor.setdefault(canonical_vendor, set())
            dedupe_key = _norm_key(normalized_sku)
            if not dedupe_key or dedupe_key in seen_by_vendor[canonical_vendor]:
                continue
            seen_by_vendor[canonical_vendor].add(dedupe_key)
            output[canonical_vendor].append(normalized_sku)
    return output


def _choose_sample_sku(values: list[str]) -> str:
    if not values:
        return ""

    def score(value: str) -> tuple[int, int]:
        text = _clean_text(value)
        has_alpha = bool(re.search(r"[A-Za-z]", text))
        has_digit = bool(re.search(r"[0-9]", text))
        quality = 0
        if has_alpha and has_digit:
            quality += 4
        elif has_digit:
            quality += 2
        if 5 <= len(text) <= 24:
            quality += 2
        if "-" in text:
            quality += 1
        if "/" in text or " " in text:
            quality -= 1
        return quality, -len(text)

    ordered = sorted(values, key=score, reverse=True)
    return ordered[0] if ordered else ""


def _run_ddg_query(query: str, timeout: int = 25) -> tuple[list[tuple[str, str, str]], str]:
    url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query, "kl": "us-en"})
    html, status, _, error = _fetch_url(url, timeout=timeout)
    if error:
        proxy_results, proxy_error = _run_ddg_proxy_query(query, timeout=timeout)
        if proxy_results:
            return proxy_results, ""
        return [], error if not proxy_error else f"{error}; proxy={proxy_error}"
    if status >= 400:
        proxy_results, proxy_error = _run_ddg_proxy_query(query, timeout=timeout)
        if proxy_results:
            return proxy_results, ""
        return [], f"HTTP {status}" if not proxy_error else f"HTTP {status}; proxy={proxy_error}"
    results = _extract_ddg_results(html)
    if results:
        return results, ""
    if _looks_like_ddg_challenge_page(html):
        proxy_results, proxy_error = _run_ddg_proxy_query(query, timeout=timeout)
        if proxy_results:
            return proxy_results, ""
        return [], "DuckDuckGo challenge page" if not proxy_error else f"DuckDuckGo challenge page; proxy={proxy_error}"
    proxy_results, proxy_error = _run_ddg_proxy_query(query, timeout=timeout)
    if proxy_results:
        return proxy_results, ""
    return [], proxy_error


def _run_ddg_proxy_query(query: str, timeout: int = 25) -> tuple[list[tuple[str, str, str]], str]:
    upstream = "http://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query, "kl": "us-en"})
    url = "https://r.jina.ai/" + upstream
    text, status, _, error = _fetch_url(url, timeout=timeout)
    if error:
        return [], error
    if status >= 400:
        return [], f"HTTP {status}"
    results = _extract_proxy_markdown_links(text)
    return results, "" if results else "no results"


def _run_bing_query(query: str, timeout: int = 25) -> tuple[list[tuple[str, str, str]], str]:
    url = "https://www.bing.com/search?" + urllib.parse.urlencode({"q": query, "setlang": "en-US"})
    html, status, _, error = _fetch_url(url, timeout=timeout)
    if error:
        return [], error
    if status >= 400:
        return [], f"HTTP {status}"
    return _extract_bing_results(html), ""


def _run_google_cse_query(
    query: str,
    api_key: str,
    cx: str,
    timeout: int = 25,
) -> tuple[list[tuple[str, str, str]], str]:
    api_key_text = _clean_text(api_key)
    cx_text = _clean_text(cx)
    if not api_key_text or not cx_text:
        return [], "Google CSE credentials missing"
    params = {
        "key": api_key_text,
        "cx": cx_text,
        "q": query,
        "num": "10",
    }
    url = "https://www.googleapis.com/customsearch/v1?" + urllib.parse.urlencode(params)
    body, status, _, error = _fetch_url(url, timeout=timeout)
    if error:
        return [], error
    if status >= 400:
        return [], f"HTTP {status}"
    try:
        payload = json.loads(body)
    except Exception as exc:
        return [], f"Invalid Google JSON response: {exc}"

    items = payload.get("items")
    if not isinstance(items, list):
        return [], ""
    out: list[tuple[str, str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        link = _clean_text(item.get("link", ""))
        if not link or not link.lower().startswith("http"):
            continue
        title = _compact_space(item.get("title", ""))
        snippet = _compact_space(item.get("snippet", ""))
        out.append((link, title, snippet))
    return out, ""


def _discover_vendor_website_candidates(
    vendor: VendorProfile,
    max_results_per_query: int,
    delay_seconds: float,
    search_provider: str,
    google_api_key: str,
    google_cx: str,
) -> tuple[list[WebsiteCandidate], list[str]]:
    queries = [
        f"{vendor.display_name} diesel official website",
        f"{vendor.display_name} official website",
    ]
    all_candidates: list[WebsiteCandidate] = []
    errors: list[str] = []
    best_by_domain: dict[str, WebsiteCandidate] = {}

    for query in queries:
        if delay_seconds > 0:
            time.sleep(delay_seconds)

        results: list[tuple[str, str, str]] = []
        query_error = ""
        provider_used = search_provider

        if search_provider == "google":
            results, query_error = _run_google_cse_query(query=query, api_key=google_api_key, cx=google_cx)
        elif search_provider == "ddg":
            results, query_error = _run_ddg_query(query)
        elif search_provider == "bing":
            results, query_error = _run_bing_query(query)
        else:  # auto
            if _clean_text(google_api_key) and _clean_text(google_cx):
                provider_used = "google"
                results, query_error = _run_google_cse_query(query=query, api_key=google_api_key, cx=google_cx)
                if not results or query_error:
                    ddg_results, ddg_error = _run_ddg_query(query)
                    if ddg_results:
                        provider_used = "ddg_fallback"
                        results = ddg_results
                        query_error = ""
                    else:
                        query_error_parts: list[str] = []
                        if query_error:
                            query_error_parts.append(f"google={query_error}")
                        if ddg_error:
                            query_error_parts.append(f"ddg={ddg_error}")
                        if not query_error_parts:
                            query_error_parts.append("google/ddg returned no results")
                        query_error = "; ".join(query_error_parts)
            else:
                provider_used = "ddg"
                results, query_error = _run_ddg_query(query)

        if query_error:
            errors.append(f"{query} [{provider_used}]: {query_error}")
            continue
        if not results:
            errors.append(f"{query} [{provider_used}]: no results")
            continue

        for url, title, snippet in results[:max_results_per_query]:
            domain = _domain_from_url(url)
            if not domain:
                continue
            score, reasons = _score_website_candidate(
                url=url,
                vendor_names=[vendor.display_name, vendor.canonical_vendor, *vendor.aliases],
                query=query,
                title=title,
                snippet=snippet,
            )
            candidate = WebsiteCandidate(
                vendor=vendor.canonical_vendor,
                query=query,
                candidate_url=url,
                domain=domain,
                score=score,
                reasons="; ".join(reasons),
                title=_compact_space(title),
                snippet=_compact_space(snippet),
            )
            existing = best_by_domain.get(domain)
            if existing is None or candidate.score > existing.score:
                best_by_domain[domain] = candidate

    all_candidates = sorted(best_by_domain.values(), key=lambda item: (-item.score, len(item.candidate_url)))
    return all_candidates, errors


def _classify_probe(
    probe_url: str,
    final_url: str,
    html: str,
    http_status: int,
    error: str,
    sku: str,
) -> tuple[str, bool, bool, bool, str]:
    low_error = _clean_text(error).lower()
    blocked = http_status in {403, 429} or _has_rate_limit_error(http_status, error)

    sku_key = _norm_key(sku)
    sku_present = bool(sku_key and sku_key in _norm_key(html))
    low_html = _clean_text(html).lower()
    low_final_path = urllib.parse.urlparse(final_url).path.lower()
    low_probe_path = urllib.parse.urlparse(probe_url).path.lower()

    if blocked:
        return "blocked", sku_present, False, True, error or "Bot challenge or rate limit"
    if http_status >= 400:
        return "fetch_error", sku_present, False, False, f"HTTP {http_status}"
    if _looks_like_bot_challenge(html):
        return "blocked", sku_present, False, True, error or "Bot challenge page"
    if error and not html:
        return "fetch_error", sku_present, False, False, error

    stripped_html = _clean_text(html)
    if stripped_html.startswith("{") or stripped_html.startswith("["):
        try:
            payload = json.loads(stripped_html)
        except Exception:
            payload = None
        if payload is not None:
            payload_text = _norm_key(json.dumps(payload, ensure_ascii=True))
            json_sku_present = bool(sku_key and sku_key in payload_text)

            def _extract_result_count(value: object) -> int:
                if isinstance(value, dict):
                    for key in ("totalResults", "total_results", "count", "numResults", "total"):
                        if key in value:
                            try:
                                return int(value.get(key) or 0)
                            except Exception:
                                pass
                    for key in ("products", "items", "results", "hits"):
                        child = value.get(key)
                        if isinstance(child, list):
                            return len(child)
                    return 0
                if isinstance(value, list):
                    return len(value)
                return 0

            result_count = _extract_result_count(payload)
            if result_count <= 0 and not json_sku_present:
                return "search_no_results", json_sku_present, False, False, "JSON search API returned no results"
            if result_count > 0 or json_sku_present:
                return "search_results", json_sku_present, True, False, "JSON search API response"

    canonical = ""
    canonical_match = re.search(
        r'(?is)<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
        html,
    )
    if canonical_match:
        canonical = _clean_text(canonical_match.group(1))
        if canonical:
            canonical = urllib.parse.urljoin(final_url, canonical)
    canonical_path = urllib.parse.urlparse(canonical).path.lower() if canonical else ""

    product_links = len(re.findall(r'(?i)href=["\'][^"\']*/products?/[^"\']+["\']', html))
    has_search_indicators = (
        ("/search" in low_final_path)
        or ("/search" in low_probe_path)
        or ("search results" in low_html)
        or ("result for" in low_html)
        or ("results for" in low_html)
    )
    has_no_results = bool(
        re.search(
            r"(?i)(no results|0 results|did not match any products|try a different search term)",
            low_html,
        )
    )
    def _is_product_path(path_text: str) -> bool:
        if not path_text:
            return False
        if "/products/search" in path_text or "/product/search" in path_text:
            return False
        if "/products/" in path_text or "/product/" in path_text:
            return True
        return False

    looks_product_path = (
        _is_product_path(low_final_path)
        or (canonical_path and "/search" not in canonical_path and _is_product_path(canonical_path))
    )

    if has_no_results:
        return "search_no_results", sku_present, False, False, "Search returned no results"
    if looks_product_path:
        notes = "Resolved to product page"
        success = True
        return "product_page", sku_present, success, False, notes
    if has_search_indicators or product_links >= 2:
        success = bool(sku_present)
        notes = "Search results page"
        if not sku_present:
            notes = "Search results page but SKU not visible in HTML (possible JS-rendered results)"
        return "search_results", sku_present, success, False, notes

    success = bool(sku_present and (http_status == 200 or http_status == 0))
    notes = "Unknown page type"
    return "unknown", sku_present, success, False, notes


def _probe_search_templates(
    vendor: str,
    site_url: str,
    sku: str,
    use_playwright: bool,
    delay_seconds: float,
) -> list[ProbeResult]:
    if not site_url or not sku:
        return []
    root = _root_url(site_url)
    if not root:
        return []

    results: list[ProbeResult] = []
    for template in SEARCH_TEMPLATES:
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        probe_url = root + template.format(sku=urllib.parse.quote(sku))
        html, status, final_url, error = _fetch_url(probe_url, timeout=25)
        used_playwright = False

        if (not html or status in {403, 429} or _looks_like_bot_challenge(html)) and use_playwright:
            p_html, p_status, p_final_url, p_error = _playwright_fetch(probe_url)
            if p_html:
                html = p_html
                status = p_status
                final_url = p_final_url
                error = p_error
                used_playwright = True

        kind, sku_present, success, blocked, notes = _classify_probe(
            probe_url=probe_url,
            final_url=final_url,
            html=html,
            http_status=status,
            error=error,
            sku=sku,
        )
        results.append(
            ProbeResult(
                vendor=vendor,
                site_url=site_url,
                template=template,
                probe_url=probe_url,
                final_url=final_url,
                http_status=status,
                result_kind=kind,
                sku_present=sku_present,
                success=success,
                blocked=blocked,
                used_playwright=used_playwright,
                error=error,
                notes=notes,
            )
        )
    return results


def _probe_rank(probe: ProbeResult) -> int:
    if probe.blocked:
        return -100
    if probe.result_kind == "product_page":
        return 130 if probe.sku_present else 110
    if probe.result_kind == "search_results":
        return 95 if probe.sku_present else 60
    if probe.result_kind == "search_no_results":
        return 18
    if probe.result_kind == "unknown":
        return 35 if probe.sku_present else 12
    return 0


def _best_probe(probes: list[ProbeResult]) -> ProbeResult | None:
    if not probes:
        return None
    ordered = sorted(probes, key=lambda item: (_probe_rank(item), -len(item.template)), reverse=True)
    return ordered[0]


def _review_status_for_vendor(
    chosen_site_url: str,
    chosen_site_score: int,
    sample_sku: str,
    best: ProbeResult | None,
    site_errors: list[str],
    site_score_threshold: int,
    auto_accept_site_score_threshold: int,
    reject_site_score_threshold: int,
) -> tuple[str, list[str]]:
    notes: list[str] = []

    if not chosen_site_url:
        if site_errors:
            return "manual_review", ["No website candidate found", "Search query errors encountered"]
        return "reject", ["No website candidate found"]

    if chosen_site_score < site_score_threshold:
        notes.append(f"Low site confidence score ({chosen_site_score})")

    if not sample_sku:
        return "manual_review", [*notes, "No vendor SKU available for probe"]

    if best is None:
        if site_errors:
            return "manual_review", [*notes, "No search template probes executed", "Search query errors encountered"]
        if chosen_site_score <= reject_site_score_threshold:
            return "reject", [*notes, "No search template probes executed"]
        return "manual_review", [*notes, "No search template probes executed"]

    if site_errors:
        notes.append("Search query errors encountered")

    if best.blocked:
        return "manual_review", [*notes, f"Best probe blocked ({best.result_kind})"]

    if (
        best.success
        and best.result_kind in AUTO_ACCEPT_RESULT_KINDS
        and chosen_site_score >= auto_accept_site_score_threshold
    ):
        return "auto_accept", [*notes, best.notes]

    if not best.success:
        failure_note = f"Best probe is not successful ({best.result_kind})"
        if chosen_site_score <= reject_site_score_threshold:
            return "reject", [*notes, failure_note, best.notes]
        return "manual_review", [*notes, failure_note, best.notes]

    return "manual_review", [*notes, best.notes]


def _resolve_default_required_root() -> Path:
    here = Path(__file__).resolve()
    dev_root = here.parents[1]
    runtime_app = dev_root.parent
    required_root = runtime_app / "required"
    required_root.mkdir(parents=True, exist_ok=True)
    return required_root


def _resolve_default_sku_source(required_root: Path) -> Path | None:
    repo_root = DEV_ROOT.parent.parent
    candidates = [
        DEV_ROOT / "config" / "shopify_sku_cache.csv",
        repo_root / "ProductProspector.app.backup-20260305-012344/Contents/Resources/app/config/shopify_sku_cache.csv",
        required_root / "mappings" / "shopify_sku_cache.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _clean_text(value) if not isinstance(value, bool) else ("1" if value else "0") for key, value in row.items()})


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Discover vendor websites and search URL templates, then probe each template "
            "with a vendor SKU and classify result type/success."
        )
    )
    parser.add_argument(
        "--vendor-profiles",
        default="",
        help="Path to VendorProfiles.csv (default app/required/mappings/VendorProfiles.csv).",
    )
    parser.add_argument(
        "--sku-source",
        default="",
        help=(
            "Path to CSV containing vendor + sku columns. "
            "Expected fields include vendor/shopify_vendor and sku. "
            "Default tries app/config/shopify_sku_cache.csv then backup cache."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory for CSV reports (default app/required/mappings/discovery).",
    )
    parser.add_argument(
        "--seed-master",
        default="",
        help="Optional path to VendorDiscoveryMaster.csv; seeded official_website_url values skip search-engine discovery.",
    )
    parser.add_argument("--max-vendors", type=int, default=0, help="Limit vendors for smoke tests (0 = all).")
    parser.add_argument("--vendor-filter", default="", help="Only process vendors containing this text.")
    parser.add_argument("--max-results-per-query", type=int, default=8, help="Search results per query to score.")
    parser.add_argument("--query-delay", type=float, default=0.35, help="Delay seconds between external requests.")
    parser.add_argument("--site-score-threshold", type=int, default=55, help="Min score to trust site candidate.")
    parser.add_argument(
        "--auto-accept-site-score-threshold",
        type=int,
        default=120,
        help="Min site score for an auto-accepted vendor website/template.",
    )
    parser.add_argument(
        "--reject-site-score-threshold",
        type=int,
        default=20,
        help="Reject low-confidence site candidates below this score when probes also fail.",
    )
    parser.add_argument(
        "--search-provider",
        choices=["auto", "google", "ddg", "bing"],
        default="auto",
        help="Search provider for website discovery (default auto: Google CSE if configured, else DDG).",
    )
    parser.add_argument(
        "--google-api-key",
        default="",
        help="Google CSE API key. If omitted, uses GOOGLE_CSE_API_KEY env var.",
    )
    parser.add_argument(
        "--google-cx",
        default="",
        help="Google Programmable Search Engine cx id. If omitted, uses GOOGLE_CSE_CX env var.",
    )
    parser.add_argument("--use-playwright", action="store_true", help="Use Playwright fallback on blocked/JS pages.")
    parser.add_argument("--skip-probes", action="store_true", help="Only discover vendor websites; skip search-template probing.")
    parser.add_argument(
        "--write-master-websites",
        action="store_true",
        help="Write discovered best website candidates back into VendorDiscoveryMaster.csv when missing.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-vendor progress.")
    args = parser.parse_args()

    required_root = _resolve_default_required_root()
    vendor_profiles_path = (
        Path(args.vendor_profiles).expanduser().resolve()
        if args.vendor_profiles
        else (required_root / "mappings" / "VendorProfiles.csv")
    )
    if not vendor_profiles_path.exists():
        print(f"ERROR: Vendor profiles file not found: {vendor_profiles_path}")
        return 1

    google_api_key = _clean_text(args.google_api_key) or _clean_text(os.environ.get("GOOGLE_CSE_API_KEY", ""))
    google_cx = _clean_text(args.google_cx) or _clean_text(os.environ.get("GOOGLE_CSE_CX", ""))
    if args.search_provider == "google" and (not google_api_key or not google_cx):
        print("ERROR: --search-provider google requires --google-api-key and --google-cx (or env vars).")
        return 1
    if args.search_provider == "auto":
        if google_api_key and google_cx:
            print("Website discovery provider: Google CSE (auto)")
        else:
            print("Website discovery provider: DuckDuckGo (auto fallback, no Google CSE credentials found)")
    else:
        print(f"Website discovery provider: {args.search_provider}")

    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (required_root / "mappings" / "discovery")
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    seed_master_path = (
        Path(args.seed_master).expanduser().resolve()
        if args.seed_master
        else (output_dir / "VendorDiscoveryMaster.csv")
    )

    sku_source_path: Path | None = None
    if args.sku_source:
        sku_source_path = Path(args.sku_source).expanduser().resolve()
        if not sku_source_path.exists():
            print(f"ERROR: sku source not found: {sku_source_path}")
            return 1
    else:
        sku_source_path = _resolve_default_sku_source(required_root)

    profiles = _load_vendor_profiles(vendor_profiles_path)
    alias_map = _build_alias_map(profiles)
    seeded_sites = _load_seeded_vendor_sites(seed_master_path)
    if seeded_sites:
        print(f"Loaded seeded vendor sites: {seed_master_path} ({len(seeded_sites)})")

    sample_sku_map: dict[str, list[str]] = {}
    if sku_source_path is not None:
        sample_sku_map = _load_vendor_sample_skus(sku_source_path, alias_map=alias_map)
        print(f"Loaded SKU source: {sku_source_path}")
    else:
        print("WARNING: No vendor SKU source found. Search template probes will be skipped unless you pass --sku-source.")

    candidates_rows: list[dict[str, object]] = []
    probe_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    auto_accept_rows: list[dict[str, object]] = []
    manual_review_rows: list[dict[str, object]] = []
    reject_rows: list[dict[str, object]] = []

    vendors = profiles
    if args.vendor_filter:
        needle = _clean_text(args.vendor_filter).lower()
        vendors = [item for item in vendors if needle in item.canonical_vendor.lower() or needle in item.display_name.lower()]
    if args.max_vendors > 0:
        vendors = vendors[: args.max_vendors]

    total = len(vendors)
    print(f"Processing vendors: {total}")
    for index, vendor in enumerate(vendors, start=1):
        if args.verbose:
            print(f"[{index}/{total}] {vendor.canonical_vendor}")

        seeded_site_url = _clean_text(seeded_sites.get(vendor.canonical_vendor, ""))
        chosen_site_source = "seeded_master" if seeded_site_url else "search_discovery"
        if seeded_site_url:
            site_candidates = []
            site_errors = []
            chosen_site = None
            chosen_site_url = seeded_site_url
            chosen_site_score = max(args.auto_accept_site_score_threshold, args.site_score_threshold)
        else:
            site_candidates, site_errors = _discover_vendor_website_candidates(
                vendor=vendor,
                max_results_per_query=max(1, args.max_results_per_query),
                delay_seconds=max(0.0, args.query_delay),
                search_provider=args.search_provider,
                google_api_key=google_api_key,
                google_cx=google_cx,
            )
            chosen_site = site_candidates[0] if site_candidates else None
            chosen_site_url = chosen_site.candidate_url if chosen_site else ""
            chosen_site_score = int(chosen_site.score) if chosen_site else -999

        sample_skus = sample_sku_map.get(vendor.canonical_vendor, [])
        sample_sku = _choose_sample_sku(sample_skus)

        probes: list[ProbeResult] = []
        if chosen_site_url and sample_sku and not args.skip_probes:
            probes = _probe_search_templates(
                vendor=vendor.canonical_vendor,
                site_url=chosen_site_url,
                sku=sample_sku,
                use_playwright=bool(args.use_playwright),
                delay_seconds=max(0.0, args.query_delay),
            )
        best = _best_probe(probes)

        for candidate in site_candidates:
            candidates_rows.append(
                {
                    "vendor": vendor.canonical_vendor,
                    "display_name": vendor.display_name,
                    "query": candidate.query,
                    "candidate_url": candidate.candidate_url,
                    "domain": candidate.domain,
                    "score": candidate.score,
                    "reasons": candidate.reasons,
                    "title": candidate.title,
                    "snippet": candidate.snippet,
                }
            )

        for probe in probes:
            probe_rows.append(
                {
                    "vendor": probe.vendor,
                    "site_url": probe.site_url,
                    "template": probe.template,
                    "probe_url": probe.probe_url,
                    "final_url": probe.final_url,
                    "http_status": probe.http_status,
                    "result_kind": probe.result_kind,
                    "sku_present": probe.sku_present,
                    "success": probe.success,
                    "blocked": probe.blocked,
                    "used_playwright": probe.used_playwright,
                    "error": probe.error,
                    "notes": probe.notes,
                }
            )

        if best is None:
            best_template = ""
            best_kind = ""
            best_success = False
            best_blocked = False
            best_probe_url = ""
            best_final_url = ""
            best_http = 0
            best_notes = ""
        else:
            best_template = best.template
            best_kind = best.result_kind
            best_success = bool(best.success)
            best_blocked = bool(best.blocked)
            best_probe_url = best.probe_url
            best_final_url = best.final_url
            best_http = best.http_status
            best_notes = best.notes
        review_status, review_notes = _review_status_for_vendor(
            chosen_site_url=chosen_site_url,
            chosen_site_score=chosen_site_score,
            sample_sku=sample_sku,
            best=best,
            site_errors=site_errors,
            site_score_threshold=args.site_score_threshold,
            auto_accept_site_score_threshold=args.auto_accept_site_score_threshold,
            reject_site_score_threshold=args.reject_site_score_threshold,
        )
        audit_row = {
            "vendor": vendor.canonical_vendor,
            "display_name": vendor.display_name,
            "sample_sku": sample_sku,
            "chosen_site_url": chosen_site_url,
            "chosen_site_source": chosen_site_source,
            "chosen_site_score": chosen_site_score if chosen_site_url else "",
            "best_template": best_template,
            "best_probe_url": best_probe_url,
            "best_final_url": best_final_url,
            "best_http_status": best_http if best else "",
            "best_result_kind": best_kind,
            "best_success": best_success,
            "best_blocked": best_blocked,
            "review_status": review_status,
            "needs_manual_review": review_status == "manual_review",
            "notes": " | ".join(note for note in review_notes if note),
            "site_query_errors": " | ".join(site_errors[:3]),
        }
        audit_rows.append(audit_row)
        if review_status == "auto_accept":
            auto_accept_rows.append(audit_row)
        elif review_status == "reject":
            reject_rows.append(audit_row)
        else:
            manual_review_rows.append(audit_row)

    candidates_path = output_dir / "vendor_website_candidates.csv"
    probes_path = output_dir / "vendor_search_template_probes.csv"
    audit_path = output_dir / "vendor_search_template_audit.csv"
    auto_accept_path = output_dir / "vendor_discovery_auto_accept.csv"
    manual_review_path = output_dir / "vendor_discovery_manual_review.csv"
    reject_path = output_dir / "vendor_discovery_reject.csv"

    _write_csv(
        candidates_path,
        [
            "vendor",
            "display_name",
            "query",
            "candidate_url",
            "domain",
            "score",
            "reasons",
            "title",
            "snippet",
        ],
        candidates_rows,
    )
    _write_csv(
        probes_path,
        [
            "vendor",
            "site_url",
            "template",
            "probe_url",
            "final_url",
            "http_status",
            "result_kind",
            "sku_present",
            "success",
            "blocked",
            "used_playwright",
            "error",
            "notes",
        ],
        probe_rows,
    )
    _write_csv(
        audit_path,
        [
            "vendor",
            "display_name",
            "sample_sku",
            "chosen_site_url",
            "chosen_site_source",
            "chosen_site_score",
            "best_template",
            "best_probe_url",
            "best_final_url",
            "best_http_status",
            "best_result_kind",
            "best_success",
            "best_blocked",
            "review_status",
            "needs_manual_review",
            "notes",
            "site_query_errors",
        ],
        audit_rows,
    )
    audit_fieldnames = [
        "vendor",
        "display_name",
        "sample_sku",
        "chosen_site_url",
        "chosen_site_source",
        "chosen_site_score",
        "best_template",
        "best_probe_url",
        "best_final_url",
        "best_http_status",
        "best_result_kind",
        "best_success",
        "best_blocked",
        "review_status",
        "needs_manual_review",
        "notes",
        "site_query_errors",
    ]
    _write_csv(auto_accept_path, audit_fieldnames, auto_accept_rows)
    _write_csv(manual_review_path, audit_fieldnames, manual_review_rows)
    _write_csv(reject_path, audit_fieldnames, reject_rows)

    if args.write_master_websites and seed_master_path.exists():
        with seed_master_path.open("r", encoding="utf-8", newline="") as handle:
            master_rows = list(csv.DictReader(handle))
            master_fields = list(master_rows[0].keys()) if master_rows else []
        audit_by_vendor = {row["vendor"]: row for row in audit_rows if _clean_text(row.get("chosen_site_url", ""))}
        updated = 0
        for row in master_rows:
            vendor = _clean_text(row.get("vendor", ""))
            if not vendor:
                continue
            if _clean_text(row.get("official_website_url", "")):
                continue
            audit = audit_by_vendor.get(vendor)
            if audit is None:
                continue
            chosen_url = _clean_text(audit.get("chosen_site_url", ""))
            chosen_score_text = _clean_text(audit.get("chosen_site_score", ""))
            try:
                chosen_score = int(chosen_score_text) if chosen_score_text else 0
            except Exception:
                chosen_score = 0
            confidence = "low"
            if chosen_score >= args.auto_accept_site_score_threshold:
                confidence = "high"
            elif chosen_score >= args.site_score_threshold:
                confidence = "medium"
            row["official_website_url"] = chosen_url
            row["official_website_status"] = "probable"
            row["official_website_source"] = f"search_discovery:{args.search_provider}"
            row["official_website_confidence"] = confidence
            updated += 1
        if master_rows and master_fields:
            with seed_master_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=master_fields)
                writer.writeheader()
                writer.writerows(master_rows)
            print(f"Updated master websites:  {seed_master_path} (+{updated})")

    print(f"Saved website candidates: {candidates_path}")
    print(f"Saved template probes:   {probes_path}")
    print(f"Saved vendor audit:      {audit_path}")
    print(f"Saved auto-accept list:  {auto_accept_path} ({len(auto_accept_rows)})")
    print(f"Saved manual-review:     {manual_review_path} ({len(manual_review_rows)})")
    print(f"Saved reject list:       {reject_path} ({len(reject_rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
