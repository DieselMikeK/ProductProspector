from __future__ import annotations

import concurrent.futures
import http.cookiejar
import hashlib
import gzip
import json
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


_REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
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


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def _looks_like_bot_challenge(html: str) -> bool:
    text = _clean_text(html).lower()
    if not text:
        return False
    signals = [
        "<title>just a moment",
        "<title>verifying your connection",
        "cf-browser-verification",
        "__cf_chl",
        "challenge-platform",
        "/cdn-cgi/challenge-platform",
        "id=\"challenge-running\"",
        "id=\"challenge-error-text\"",
    ]
    return any(signal in text for signal in signals)


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

    parsed = urllib.parse.urlparse(raw)
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
    output["barcode"] = _extract_first(r'(?i)(?:upc|gtin|barcode|ean)\D{0,20}([0-9]{8,14})', context)
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
    retry_count: int,
    delay_seconds: float,
    scrape_images: bool,
    image_output_root: Path | None,
) -> tuple[str, dict[str, str], str | None]:
    target_url = _normalize_url(base_url, sku)
    if not target_url:
        return sku, {}, "Missing vendor search URL"

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
        search_payload = _extract_page_payload(search_html, target_url, sku, scrape_images=scrape_images)
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
                        _find_context_near_sku(search_html, sku, span=2000),
                    ]
                )
                if _contains_compact_sku(direct_evidence_text, sku) or bool(_clean_text(search_payload.get("title", ""))):
                    direct_product_url = canonical_from_search
                    resolved_url = direct_product_url

        searchanise_candidates: list[tuple[str, int, dict[str, str]]] = []
        convermax_candidates: list[tuple[str, int, dict[str, str]]] = []
        searchanise_errors: list[str] = []
        convermax_errors: list[str] = []
        candidate_seed_payloads: dict[str, dict[str, str]] = {}
        candidates: list[tuple[str, int]] = []
        probe_candidates = _should_probe_search_candidates(
            search_html=search_html,
            target_url=target_url,
            sku=sku,
            payload=search_payload,
            direct_product_url=direct_product_url,
        )
        if probe_candidates and not direct_product_url:
            searchanise_candidates, searchanise_errors = _searchanise_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=sku,
            )
            convermax_candidates, convermax_errors = _convermax_candidates_from_search_page(
                search_html=search_html,
                page_url=target_url,
                sku=sku,
            )
            candidates = _extract_product_page_candidates(search_html, page_url=target_url, sku=sku)
            provider_candidates = list(searchanise_candidates[:20]) + list(convermax_candidates[:20])
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
                        matches_sku = _contains_compact_sku(payload_match_text, sku)
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
                candidate_payload = _extract_page_payload(product_html, candidate_url, sku, scrape_images=False)
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
                matches_sku = _contains_compact_sku(payload_match_text, sku)
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
        if resolved_seed_payload and resolved_url != target_url:
            provider = _clean_text(resolved_seed_payload.get("search_provider", ""))
            merged["search_provider"] = provider or "search_seed"

        try:
            if resolved_html:
                resolved_payload = _extract_page_payload(resolved_html, resolved_url, sku, scrape_images=scrape_images)
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
        provider_errors = list(searchanise_errors or []) + list(convermax_errors or [])
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
                            _find_context_near_sku(resolved_html, sku, span=2000),
                        ]
                    )
                    if _contains_compact_sku(evidence_text, sku) or bool(_clean_text(merged.get("title", ""))):
                        canonical_product_url = canonical_url
                        canonical_product_path = canonical_path

        if canonical_product_url and resolved_url == target_url:
            canonical_html, canonical_error = _fetch_html(canonical_product_url)
            if not canonical_error and canonical_html:
                canonical_payload = _extract_page_payload(
                    canonical_html,
                    canonical_product_url,
                    sku,
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
) -> tuple[dict[str, dict[str, str]], dict[str, str], list[str]]:
    sku_values = [normalize_sku(sku) for sku in skus if normalize_sku(sku)]
    if not vendor_search_url or not sku_values:
        return {}, {}, []

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
    if len(sku_values) >= 8 and max_workers > 2:
        max_workers = 2
    effective_delay = float(delay_seconds or 0.0)

    # Preflight the vendor endpoint once; if immediately throttled/challenged,
    # switch to safer pacing for this batch instead of burning all SKUs.
    preflight_url = _normalize_url(vendor_search_url, ordered_skus[0]) if ordered_skus else ""
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
                retry_count=retry_count,
                delay_seconds=effective_delay,
                scrape_images=scrape_images,
                image_output_root=output_root,
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
                results[sku] = payload
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
                retry_count=retry_count_adaptive,
                delay_seconds=retry_delay,
                scrape_images=scrape_images,
                image_output_root=output_root,
            )
            if payload:
                results[sku] = payload
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
