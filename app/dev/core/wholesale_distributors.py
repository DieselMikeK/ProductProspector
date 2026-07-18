from __future__ import annotations

import csv
import ctypes
import json
import os
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from product_prospector.core.vendor_profiles import resolve_vendor_profile


@dataclass(frozen=True)
class WholesaleDistributor:
    key: str
    label: str
    search_url: str
    display_search_url: str = ""

    @property
    def processing_search_url(self) -> str:
        """Return the endpoint template that should be shown in the Processing UI."""
        return self.display_search_url or self.search_url


WHOLESALE_DISTRIBUTORS: tuple[WholesaleDistributor, ...] = (
    WholesaleDistributor(
        key="keystone",
        label="Keystone Automotive",
        search_url="https://wwwsc.ekeystone.com/search?issl=1&SearchTerm={sku}",
    ),
    WholesaleDistributor(
        key="meyer",
        label="Meyer Distributing",
        search_url="https://online.meyerdistributing.com/parts/search;search={sku};search_within={sku}",
    ),
    WholesaleDistributor(
        key="premier_apg",
        label="Premier Performance / APG Wholesale",
        search_url="https://apgwholesale.com/pages/search-results-page?q={sku}",
    ),
    WholesaleDistributor(
        key="turn14",
        label="Turn 14 Distribution",
        search_url="https://turn14.com/search/index.php?vmmPart={sku}",
    ),
    WholesaleDistributor(
        key="xdp",
        label="Xtreme Diesel Power",
        # ProductProspector already has XDP-specific bot-sensitive handling for
        # this confirmed search route.
        search_url="https://www.xtremediesel.com/xtreme-diesel-performance-xdp-search?q={sku}",
    ),
)

WHOLESALE_DISTRIBUTOR_BY_KEY = {item.key: item for item in WHOLESALE_DISTRIBUTORS}
WD_VENDOR_AVAILABILITY_FILENAME = "WDVendorAvailability.csv"

_BRAND_SUFFIX_TOKENS = {
    "accessories",
    "aftermarket",
    "automotive",
    "industries",
    "manufacturing",
    "performance",
    "power",
    "products",
    "suspension",
    "systems",
    "usa",
}
_BRAND_DESCRIPTOR_TOKENS = _BRAND_SUFFIX_TOKENS | {
    "brake",
    "brakes",
    "clutch",
    "clutches",
    "co",
    "company",
    "diesel",
    "engineering",
    "exhaust",
    "fabrication",
    "fab",
    "filter",
    "filters",
    "inc",
    "incorporated",
    "jack",
    "jacks",
    "led",
    "light",
    "lighting",
    "lights",
    "llc",
    "ltd",
    "offroad",
    "radiator",
    "radiators",
    "shock",
    "shocks",
    "shifter",
    "shifters",
    "starter",
    "starters",
    "truck",
}
_availability_cache: dict[tuple[str, int], dict[str, tuple[str, ...]]] = {}


def selected_distributors(keys: Iterable[str]) -> list[WholesaleDistributor]:
    selected = {str(key or "").strip() for key in keys}
    return [item for item in WHOLESALE_DISTRIBUTORS if item.key in selected]


def processing_distributors(keys: Iterable[str]) -> list[WholesaleDistributor]:
    """Return selected endpoints in Processing UI order, with Turn 14 last."""
    selected = selected_distributors(keys)
    return [item for item in selected if item.key != "turn14"] + [
        item for item in selected if item.key == "turn14"
    ]


def _brand_match_keys(value: object) -> set[str]:
    text = str(value or "").strip().lower().lstrip("*").strip()
    if not text:
        return set()
    tokens = re.findall(r"[a-z0-9]+", text)
    if not tokens:
        return set()
    keys = {"".join(tokens)}
    shortened = list(tokens)
    while len(shortened) > 1 and shortened[-1] in _BRAND_SUFFIX_TOKENS:
        shortened.pop()
        keys.add("".join(shortened))
    return {key for key in keys if key}


def _brand_core_key(value: object) -> str:
    """Return a conservative brand identity with catalog descriptors removed."""
    text = str(value or "").strip().lower().lstrip("*").strip()
    tokens = re.findall(r"[a-z0-9]+", text)
    while len(tokens) > 1 and tokens[-1] in _BRAND_DESCRIPTOR_TOKENS:
        tokens.pop()
    return "".join(tokens)


def load_wd_vendor_availability(required_root: Path) -> dict[str, tuple[str, ...]]:
    mapping_path = Path(required_root) / "mappings" / WD_VENDOR_AVAILABILITY_FILENAME
    if not mapping_path.exists():
        return {}
    try:
        cache_key = (str(mapping_path.resolve()), int(mapping_path.stat().st_mtime_ns))
    except Exception:
        cache_key = (str(mapping_path), 0)
    cached = _availability_cache.get(cache_key)
    if cached is not None:
        return dict(cached)

    by_label = {item.label: item.key for item in WHOLESALE_DISTRIBUTORS}
    values: dict[str, list[str]] = {item.key: [] for item in WHOLESALE_DISTRIBUTORS}
    try:
        with mapping_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                for label, distributor_key in by_label.items():
                    brand = str(row.get(label, "") or "").strip()
                    if brand and brand not in values[distributor_key]:
                        values[distributor_key].append(brand)
    except Exception:
        return {}

    loaded = {key: tuple(items) for key, items in values.items()}
    _availability_cache.clear()
    _availability_cache[cache_key] = loaded
    return dict(loaded)


def vendor_context_names(vendor_name: str, required_root: Path) -> list[str]:
    values = [str(vendor_name or "").strip()]
    profile = resolve_vendor_profile(vendor_name, required_root=required_root)
    if profile is not None:
        values.extend(
            [
                str(profile.canonical_vendor or "").strip(),
                str(profile.shopify_vendor_value or "").strip(),
                str(profile.brand_name or "").strip(),
            ]
        )
        values.extend(
            part.strip()
            for part in re.split(r"[|,;\n]+", str(profile.aliases or ""))
            if part.strip()
        )
    return list(dict.fromkeys(value for value in values if value))


def distributor_supports_vendor(
    distributor_key: str,
    vendor_name: str,
    required_root: Path,
) -> bool | None:
    """Return True/False from the bundled matrix, or None if matrix data is unavailable."""
    availability = load_wd_vendor_availability(required_root)
    site_brands = availability.get(str(distributor_key or "").strip())
    if site_brands is None:
        return None
    context_names = vendor_context_names(vendor_name, required_root)
    vendor_keys = {key for candidate in context_names for key in _brand_match_keys(candidate)}
    if not vendor_keys:
        return None
    site_keys = {key for brand in site_brands for key in _brand_match_keys(brand)}
    if vendor_keys.intersection(site_keys):
        return True

    # WD catalogs commonly append a different business descriptor than our
    # Shopify-facing vendor name ("MBRP" vs "MBRP Exhaust", for example).
    # Compare only exact descriptor-stripped identities; do not use fuzzy text
    # similarity because similarly named automotive brands are common.
    vendor_cores = {_brand_core_key(value) for value in context_names}
    vendor_cores.discard("")
    site_cores = {_brand_core_key(value) for value in site_brands}
    site_cores.discard("")
    if vendor_cores.intersection(site_cores):
        return True

    # A profile SKU prefix is also a reliable identity only when it equals the
    # complete WD brand label. This maps MBRP -> MBRP Exhaust and K&N -> KN
    # without allowing short prefixes such as TS to match "TS Performance".
    profile = resolve_vendor_profile(vendor_name, required_root=required_root)
    sku_prefix = re.sub(r"[^a-z0-9]+", "", str(getattr(profile, "sku_prefix", "") or "").lower())
    if len(sku_prefix) >= 2:
        raw_site_keys = {
            re.sub(r"[^a-z0-9]+", "", str(brand or "").lower())
            for brand in site_brands
        }
        if sku_prefix in raw_site_keys:
            return True
    return False


def wd_session_root() -> Path:
    """Return a private, non-repository directory for WD browser sessions."""
    local_app_data = str(os.environ.get("LOCALAPPDATA", "") or "").strip()
    if local_app_data:
        return Path(local_app_data) / "ProductProspector" / "wd_sessions"
    return Path.home() / "AppData" / "Local" / "ProductProspector" / "wd_sessions"


def wd_error_report_path() -> Path:
    """Return the private persistent log used for full WD scrape diagnostics."""
    return wd_session_root().parent / "logs" / "wd_scrape_errors.log"


def compact_distributor_error(error_text: str) -> str:
    """Turn a technical scraper exception into a stable one-line UI status."""
    text = str(error_text or "").strip()
    lowered = text.lower()
    if not text:
        return "Not found"
    if (
        "playwright firefox unavailable" in lowered
        or ("browsertype.launch" in lowered and "firefox" in lowered)
        or ("executable doesn't exist" in lowered and "firefox" in lowered)
    ):
        return "Playwright Firefox unavailable"
    if "bot challenge" in lowered or "access denied" in lowered or "error 15" in lowered:
        return "Bot challenge detected"
    if "timed out" in lowered or "timeout" in lowered:
        return "Request timed out"
    if (
        "session expired" in lowered
        or "session is no longer valid" in lowered
        or "saved chrome session" in lowered
        or "authentication required" in lowered
        or "login required" in lowered
    ):
        return "Authentication required"
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "Not found")
    if len(first_line) > 120:
        return first_line[:117].rstrip() + "..."
    return first_line


def append_distributor_error_report(
    distributor_key: str,
    sku: str,
    error_text: str,
    report_path: Path | None = None,
) -> Path | None:
    """Append a full WD error without exposing it in the review grid."""
    text = str(error_text or "").strip()
    if not text:
        return None
    target = Path(report_path) if report_path is not None else wd_error_report_path()
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "distributor": str(distributor_key or "").strip(),
        "sku": str(sku or "").strip(),
        "summary": compact_distributor_error(text),
        "details": text,
    }
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return target
    except Exception:
        return None


def _windows_credential_target(distributor_key: str) -> str:
    return f"ProductProspector/WD/{str(distributor_key or '').strip().lower()}"


def load_distributor_credentials(distributor_key: str) -> tuple[str, str, str | None]:
    """Read a WD login from Windows Credential Manager without exposing it in source."""
    key = str(distributor_key or "").strip().lower()
    if not key:
        return "", "", "Distributor credential key is blank."
    if sys.platform != "win32":
        return "", "", "Windows Credential Manager is unavailable on this platform."

    from ctypes import wintypes

    class CREDENTIALW(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.DWORD),
            ("Type", wintypes.DWORD),
            ("TargetName", wintypes.LPWSTR),
            ("Comment", wintypes.LPWSTR),
            ("LastWritten", wintypes.FILETIME),
            ("CredentialBlobSize", wintypes.DWORD),
            ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
            ("Persist", wintypes.DWORD),
            ("AttributeCount", wintypes.DWORD),
            ("Attributes", ctypes.c_void_p),
            ("TargetAlias", wintypes.LPWSTR),
            ("UserName", wintypes.LPWSTR),
        ]

    credential_pointer = ctypes.POINTER(CREDENTIALW)()
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    advapi32.CredReadW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(CREDENTIALW)),
    ]
    advapi32.CredReadW.restype = wintypes.BOOL
    advapi32.CredFree.argtypes = [ctypes.c_void_p]
    advapi32.CredFree.restype = None
    target = _windows_credential_target(key)
    if not advapi32.CredReadW(target, 1, 0, ctypes.byref(credential_pointer)):
        return "", "", f"No saved Windows credential was found for {key}."
    try:
        credential = credential_pointer.contents
        username = str(credential.UserName or "").strip()
        blob = ctypes.string_at(credential.CredentialBlob, credential.CredentialBlobSize)
        password = blob.decode("utf-16-le") if blob else ""
        if not username or not password:
            return "", "", f"The saved Windows credential for {key} is incomplete."
        return username, password, None
    finally:
        advapi32.CredFree(credential_pointer)


def store_distributor_credentials(
    distributor_key: str,
    username: str,
    password: str,
) -> str | None:
    """Store a WD login in Windows Credential Manager for backend authentication."""
    key = str(distributor_key or "").strip().lower()
    user = str(username or "").strip()
    secret = str(password or "")
    if not key or not user or not secret:
        return "Distributor key, username, and password are required."
    if sys.platform != "win32":
        return "Windows Credential Manager is unavailable on this platform."

    from ctypes import wintypes

    class CREDENTIALW(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.DWORD),
            ("Type", wintypes.DWORD),
            ("TargetName", wintypes.LPWSTR),
            ("Comment", wintypes.LPWSTR),
            ("LastWritten", wintypes.FILETIME),
            ("CredentialBlobSize", wintypes.DWORD),
            ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
            ("Persist", wintypes.DWORD),
            ("AttributeCount", wintypes.DWORD),
            ("Attributes", ctypes.c_void_p),
            ("TargetAlias", wintypes.LPWSTR),
            ("UserName", wintypes.LPWSTR),
        ]

    blob_bytes = secret.encode("utf-16-le")
    blob_buffer = (ctypes.c_ubyte * len(blob_bytes)).from_buffer_copy(blob_bytes)
    target = _windows_credential_target(key)
    credential = CREDENTIALW()
    credential.Type = 1
    credential.TargetName = target
    credential.CredentialBlobSize = len(blob_bytes)
    credential.CredentialBlob = ctypes.cast(blob_buffer, ctypes.POINTER(ctypes.c_ubyte))
    credential.Persist = 2
    credential.UserName = user
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    advapi32.CredWriteW.argtypes = [ctypes.POINTER(CREDENTIALW), wintypes.DWORD]
    advapi32.CredWriteW.restype = wintypes.BOOL
    if not advapi32.CredWriteW(ctypes.byref(credential), 0):
        return f"Could not save the Windows credential for {key} (error {ctypes.get_last_error()})."
    return None


def _load_live_firefox_cookies(domain_suffix: str) -> list[dict[str, object]]:
    """Read current Firefox cookies without modifying or locking its profile."""
    app_data = str(os.environ.get("APPDATA", "") or "").strip()
    if not app_data:
        return []
    profiles_root = Path(app_data) / "Mozilla" / "Firefox" / "Profiles"
    if not profiles_root.exists():
        return []
    cookie_files = sorted(
        profiles_root.glob("*/cookies.sqlite"),
        key=lambda path: path.stat().st_mtime_ns if path.exists() else 0,
        reverse=True,
    )
    suffix = str(domain_suffix or "").strip().lower().lstrip(".")
    if not suffix:
        return []

    for cookie_path in cookie_files:
        try:
            connection = sqlite3.connect(
                f"file:{cookie_path.as_posix()}?mode=ro",
                uri=True,
                timeout=2,
            )
            try:
                rows = connection.execute(
                    "SELECT name, value, host, path, expiry, isSecure, isHttpOnly, sameSite "
                    "FROM moz_cookies WHERE lower(host) LIKE ?",
                    (f"%{suffix}",),
                ).fetchall()
            finally:
                connection.close()
        except Exception:
            continue
        if not rows:
            continue

        same_site_values = {0: "None", 1: "Lax", 2: "Strict"}
        output: list[dict[str, object]] = []
        for name, value, host, path, expiry, secure, http_only, same_site in rows:
            cookie: dict[str, object] = {
                "name": str(name or ""),
                "value": str(value or ""),
                "domain": str(host or ""),
                "path": str(path or "/"),
                "expirationDate": float(expiry or 0),
                "secure": bool(secure),
                "httpOnly": bool(http_only),
            }
            if same_site in same_site_values:
                cookie["sameSite"] = same_site_values[same_site]
            if cookie["name"]:
                output.append(cookie)
        return output
    return []


def _merge_cookie_sets(
    saved: list[dict[str, object]],
    current: list[dict[str, object]],
) -> list[dict[str, object]]:
    merged: dict[tuple[str, str, str], dict[str, object]] = {}
    for item in [*saved, *current]:
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        key = (
            name,
            str(item.get("domain", "") or "").strip().lower(),
            str(item.get("path", "/") or "/").strip(),
        )
        merged[key] = dict(item)
    return list(merged.values())


def _add_cookie_domain_alias(
    cookies: list[dict[str, object]],
    source_host: str,
    target_domain: str,
) -> list[dict[str, object]]:
    """Clone host-scoped Chrome exports for an equivalent canonical site host."""
    source = str(source_host or "").strip().lower().lstrip(".")
    target = str(target_domain or "").strip().lower()
    if not source or not target:
        return list(cookies)
    aliases: list[dict[str, object]] = []
    for item in cookies:
        domain = str(item.get("domain", "") or "").strip().lower().lstrip(".")
        if domain != source:
            continue
        clone = dict(item)
        clone["domain"] = target
        aliases.append(clone)
    return _merge_cookie_sets(list(cookies), aliases)


def load_distributor_cookies(distributor_key: str) -> tuple[list[dict[str, object]], str | None]:
    """Load an EditThisCookie-compatible session without storing secrets in source.

    A future authenticated session can be placed at
    ``%LOCALAPPDATA%/ProductProspector/wd_sessions/<key>.json`` as either a cookie
    list or ``{"cookies": [...]}``. Missing sessions are allowed so public search
    routes can still be exercised.
    """
    key = str(distributor_key or "").strip()
    if key not in WHOLESALE_DISTRIBUTOR_BY_KEY:
        return [], f"Unknown WD session key: {key or '(blank)'}"

    session_path = wd_session_root() / f"{key}.json"
    if not session_path.exists():
        return [], None
    try:
        raw = json.loads(session_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], f"Could not read {session_path.name}: {exc}"

    values = raw.get("cookies") if isinstance(raw, dict) else raw
    if not isinstance(values, list):
        return [], f"{session_path.name} must contain a cookie list or a cookies array."
    cookies = [dict(item) for item in values if isinstance(item, dict)]
    if not cookies and values:
        return [], f"{session_path.name} does not contain valid cookie objects."
    if key == "keystone":
        # The saved export holds ASP.NET authentication while Firefox receives
        # rotating Imperva trust cookies during normal browsing. Merge the
        # current read-only Firefox values so users do not have to re-export the
        # session whenever Imperva rotates one of those cookies.
        cookies = _merge_cookie_sets(cookies, _load_live_firefox_cookies("ekeystone.com"))
    elif key == "turn14":
        # EditThisCookie may export the authenticated PHP session against
        # www.turn14.com even though confirmed search URLs use turn14.com.
        # Chrome will not send a www-only cookie to the apex host, so retain the
        # original and add an equivalent apex-domain alias for browser injection.
        cookies = _add_cookie_domain_alias(cookies, "www.turn14.com", ".turn14.com")
    return cookies, None


def save_distributor_cookies(
    distributor_key: str,
    cookies: Iterable[dict[str, object]],
) -> str | None:
    """Replace one private WD session with a user-supplied cookie export.

    The prior file is retained as ``<key>.previous.json`` so a bad or incomplete
    export does not destroy the last session irreversibly.
    """
    key = str(distributor_key or "").strip()
    if key not in WHOLESALE_DISTRIBUTOR_BY_KEY:
        return f"Unknown WD session key: {key or '(blank)'}"

    values = [dict(item) for item in cookies if isinstance(item, dict) and str(item.get("name", "") or "").strip()]
    if not values:
        return "The cookie export does not contain any valid cookie objects."

    session_root = wd_session_root()
    session_path = session_root / f"{key}.json"
    previous_path = session_root / f"{key}.previous.json"
    pending_path = session_root / f"{key}.pending.json"
    try:
        session_root.mkdir(parents=True, exist_ok=True)
        pending_path.write_text(
            json.dumps({"cookies": values}, indent=2),
            encoding="utf-8",
        )
        if session_path.exists():
            shutil.copy2(session_path, previous_path)
        os.replace(pending_path, session_path)
    except Exception as exc:
        try:
            pending_path.unlink(missing_ok=True)
        except Exception:
            pass
        return f"Could not save the {WHOLESALE_DISTRIBUTOR_BY_KEY[key].label} session: {exc}"
    return None


def flatten_distributor_results(
    results: dict[str, dict[str, object]],
    requested_fields: Iterable[str],
) -> dict[str, str]:
    """Create stable, human-readable CSV/preview columns for one SKU."""
    fields = [str(field or "").strip() for field in requested_fields if str(field or "").strip()]
    flattened: dict[str, str] = {}
    for distributor in WHOLESALE_DISTRIBUTORS:
        payload = results.get(distributor.key)
        if not isinstance(payload, dict):
            continue
        for field_name in fields:
            value = payload.get(field_name, "")
            if isinstance(value, (list, tuple, set)):
                text = " | ".join(str(item).strip() for item in value if str(item).strip())
            else:
                text = str(value or "").strip()
            flattened[f"{distributor.label} - {field_name}"] = text
    return flattened
