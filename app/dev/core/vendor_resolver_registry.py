from __future__ import annotations

import json
import re
import threading
import urllib.parse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VendorResolverProfile:
    vendor: str = ""
    display_name: str = ""
    official_website_status: str = ""
    official_website_url: str = ""
    sample_sku: str = ""
    search_url_template: str = ""
    search_result_mode: str = ""
    search_transport: str = ""
    search_parameter_name: str = ""
    platform_family: str = ""
    search_family: str = ""
    search_entry_mode: str = ""
    runtime_preference: str = ""
    http_search_allowed: str = ""
    browser_required: str = ""
    blocking_risk: str = ""
    verification_level: str = ""
    template_status: str = ""
    confidence_bucket: str = ""
    resolver_hint: str = ""
    product_fetch_mode: str = ""
    interaction_strategy: str = ""
    direct_url_sufficient: str = ""
    search_entry_url: str = ""
    search_container_selector: str = ""
    search_input_selector: str = ""
    search_submit_selector: str = ""
    result_container_selector: str = ""
    result_link_selector: str = ""
    result_match_mode: str = ""
    api_request_url_template: str = ""
    api_response_collection: str = ""
    api_result_id_field: str = ""
    api_result_sku_field: str = ""
    product_url_template: str = ""
    product_extraction_priority: str = ""
    media_strategy: str = ""
    notes: str = ""


_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_CACHE_KEY = ""
_PROFILE_CACHE_ROWS: list[VendorResolverProfile] = []


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _mapping_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[2] / "required" / "mappings" / "discovery" / filename


def _normalize_host(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    candidate = text
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    try:
        host = _clean_text(urllib.parse.urlparse(candidate).netloc).lower()
    except Exception:
        host = text.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _same_host_family(left: object, right: object) -> bool:
    left_host = _normalize_host(left)
    right_host = _normalize_host(right)
    if not left_host or not right_host:
        return False
    return (
        left_host == right_host
        or left_host.endswith(f".{right_host}")
        or right_host.endswith(f".{left_host}")
    )


def _url_shape_key(value: object) -> tuple[str, str, tuple[str, ...]]:
    text = _clean_text(value)
    if not text:
        return "", "", ()
    candidate = text
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    parsed = urllib.parse.urlparse(candidate)
    path = _clean_text(parsed.path).rstrip("/").lower()
    query_keys = tuple(sorted(set(key.lower() for key, _ in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))))
    return _normalize_host(candidate), path, query_keys


def _is_root_like_path(path: str) -> bool:
    return path in {"", "/"}


def _profile_rank(profile: VendorResolverProfile) -> int:
    score = 0
    template_status = profile.template_status.lower()
    verification = profile.verification_level.lower()
    confidence = profile.confidence_bucket.lower()
    if template_status == "confirmed":
        score += 50
    elif template_status == "probed":
        score += 35
    elif template_status == "detected":
        score += 20
    if verification == "verified":
        score += 30
    elif verification == "strong":
        score += 20
    if confidence == "high":
        score += 15
    elif confidence == "medium":
        score += 8
    return score


def _profile_match_score(profile: VendorResolverProfile, search_url: str) -> int:
    if not _clean_text(search_url):
        return -1
    candidate_text = _clean_text(search_url)
    low_candidate = candidate_text.lower()
    score = _profile_rank(profile)

    template = _clean_text(profile.search_url_template)
    if template and low_candidate == template.lower():
        score += 500

    candidate_host, candidate_path, candidate_query_keys = _url_shape_key(candidate_text)
    template_host, template_path, template_query_keys = _url_shape_key(template)
    official_host, official_path, _ = _url_shape_key(profile.official_website_url)
    entry_host, entry_path, _ = _url_shape_key(profile.search_entry_url)
    matched_host = False

    template_same_host = candidate_host == template_host if template_host and candidate_host else False
    template_path_aligned = bool(template_path and candidate_path == template_path)
    template_query_aligned = bool(template_query_keys and candidate_query_keys == template_query_keys)
    if (
        template_host
        and candidate_host
        and _same_host_family(candidate_host, template_host)
        and (template_same_host or template_path_aligned or template_query_aligned)
    ):
        matched_host = True
        score += 220
        if template_path_aligned:
            score += 70
        if template_query_aligned:
            score += 40

    official_path_aligned = bool(official_path and candidate_path == official_path) or _is_root_like_path(candidate_path)
    if (
        official_host
        and candidate_host
        and _same_host_family(candidate_host, official_host)
        and official_path_aligned
    ):
        matched_host = True
        score += 140
        if official_path and candidate_path == official_path:
            score += 15
        if _is_root_like_path(candidate_path):
            score += 25

    entry_path_aligned = bool(entry_path and candidate_path == entry_path) or _is_root_like_path(candidate_path)
    if (
        entry_host
        and candidate_host
        and _same_host_family(candidate_host, entry_host)
        and entry_path_aligned
    ):
        matched_host = True
        score += 120
        if entry_path and candidate_path == entry_path:
            score += 20

    if not matched_host:
        return -1
    return score


def _profile_from_dict(row: dict[str, object]) -> VendorResolverProfile:
    return VendorResolverProfile(**{field: _clean_text(row.get(field, "")) for field in VendorResolverProfile.__dataclass_fields__})


def load_resolver_profiles() -> list[VendorResolverProfile]:
    global _PROFILE_CACHE_KEY, _PROFILE_CACHE_ROWS

    path = _mapping_path("VendorResolverProfiles.json")
    try:
        stat = path.stat()
        cache_key = f"{path}:{stat.st_mtime_ns}:{stat.st_size}"
    except Exception:
        return []

    with _PROFILE_CACHE_LOCK:
        if cache_key == _PROFILE_CACHE_KEY:
            return list(_PROFILE_CACHE_ROWS)

        rows: list[VendorResolverProfile] = []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        rows.append(_profile_from_dict(item))
        except Exception:
            rows = []

        _PROFILE_CACHE_KEY = cache_key
        _PROFILE_CACHE_ROWS = rows
        return list(rows)


def find_resolver_profile(search_url: str) -> VendorResolverProfile | None:
    best_profile: VendorResolverProfile | None = None
    best_score = -1
    for profile in load_resolver_profiles():
        score = _profile_match_score(profile, search_url)
        if score > best_score:
            best_score = score
            best_profile = profile
    if best_score < 80:
        return None
    return best_profile


def resolve_canonical_search_url(search_url: str) -> tuple[str, VendorResolverProfile | None]:
    profile = find_resolver_profile(search_url)
    if profile is None:
        return _clean_text(search_url), None
    canonical = _clean_text(profile.search_url_template) or _clean_text(search_url)
    return canonical, profile
