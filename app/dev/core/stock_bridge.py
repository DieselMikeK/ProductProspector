from __future__ import annotations

import ctypes
from ctypes import wintypes
import json
import os
import urllib.error
import urllib.request


DEFAULT_STOCK_BRIDGE_BASE_URL = "https://stockbridgedpp.vercel.app"
STOCK_BRIDGE_CREDENTIAL_TARGET = "ProductProspector/StockBridge"
_CRED_TYPE_GENERIC = 1
_CRED_PERSIST_LOCAL_MACHINE = 2


class StockBridgeError(RuntimeError):
    pass


def assess_price_review(shopify_price: float | None, current_cost: float | None, new_cost: float | None) -> dict[str, object]:
    margin_percent = None
    cost_change_percent = None
    warnings: list[str] = []
    if shopify_price is None or shopify_price <= 0:
        warnings.append("Warning: Shopify selling price unavailable")
    elif new_cost is not None:
        margin_percent = ((shopify_price - new_cost) / shopify_price) * 100.0
        if margin_percent <= 0:
            warnings.append("Warning: zero/negative margin")
        elif margin_percent < 20:
            warnings.append("Warning: margin below 20%")
    if current_cost is not None and current_cost > 0 and new_cost is not None:
        cost_change_percent = ((new_cost - current_cost) / current_cost) * 100.0
        if abs(cost_change_percent) > 20:
            warnings.append("Warning: cost changed more than 20%")
    return {
        "marginPercent": margin_percent,
        "costChangePercent": cost_change_percent,
        "warnings": warnings,
    }


class _Credential(ctypes.Structure):
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
        ("Attributes", wintypes.LPVOID),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def _credential_api():
    if os.name != "nt":
        return None
    try:
        api = ctypes.WinDLL("Advapi32.dll")
        api.CredReadW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(ctypes.POINTER(_Credential))]
        api.CredReadW.restype = wintypes.BOOL
        api.CredWriteW.argtypes = [ctypes.POINTER(_Credential), wintypes.DWORD]
        api.CredWriteW.restype = wintypes.BOOL
        api.CredFree.argtypes = [wintypes.LPVOID]
        return api
    except Exception:
        return None


def load_stock_bridge_api_key() -> str:
    environment_key = str(os.environ.get("PRODUCT_PROSPECTOR_API_KEY", "") or "").strip()
    if environment_key:
        return environment_key

    api = _credential_api()
    if api is None:
        return ""
    pointer = ctypes.POINTER(_Credential)()
    if not api.CredReadW(STOCK_BRIDGE_CREDENTIAL_TARGET, _CRED_TYPE_GENERIC, 0, ctypes.byref(pointer)):
        return ""
    try:
        credential = pointer.contents
        raw = ctypes.string_at(credential.CredentialBlob, credential.CredentialBlobSize)
        return raw.decode("utf-16-le").strip()
    except Exception:
        return ""
    finally:
        api.CredFree(pointer)


def save_stock_bridge_api_key(api_key: str) -> bool:
    value = str(api_key or "").strip()
    api = _credential_api()
    if not value or api is None:
        return False

    encoded = value.encode("utf-16-le")
    blob = (ctypes.c_ubyte * len(encoded)).from_buffer_copy(encoded)
    credential = _Credential()
    credential.Type = _CRED_TYPE_GENERIC
    credential.TargetName = STOCK_BRIDGE_CREDENTIAL_TARGET
    credential.CredentialBlobSize = len(encoded)
    credential.CredentialBlob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte))
    credential.Persist = _CRED_PERSIST_LOCAL_MACHINE
    credential.UserName = "ProductProspector"
    return bool(api.CredWriteW(ctypes.byref(credential), 0))


def _post_json(path: str, payload: dict[str, object], api_key: str, *, base_url: str, timeout_seconds: int) -> dict[str, object]:
    key = str(api_key or "").strip()
    if not key:
        raise StockBridgeError("StockBridge API key is missing.")
    url = f"{str(base_url or DEFAULT_STOCK_BRIDGE_BASE_URL).rstrip('/')}/{path.lstrip('/')}"
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "User-Agent": "ProductProspector/StockBridge",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            message = str(json.loads(detail).get("message", "") or "").strip()
        except Exception:
            message = ""
        raise StockBridgeError(message or f"StockBridge returned HTTP {exc.code}.") from exc
    except Exception as exc:
        raise StockBridgeError(f"Could not connect to StockBridge: {exc}") from exc

    try:
        parsed = json.loads(body or "{}")
    except Exception as exc:
        raise StockBridgeError("StockBridge returned an invalid response.") from exc
    if not isinstance(parsed, dict):
        raise StockBridgeError("StockBridge returned an invalid response.")
    return parsed


def preview_wd_prices(sku: str, prices: list[dict[str, object]], api_key: str, *, base_url: str = DEFAULT_STOCK_BRIDGE_BASE_URL, timeout_seconds: int = 30) -> dict[str, object]:
    return _post_json(
        "/integrations/product-prospector/wd-prices/preview",
        {"sku": str(sku or "").strip(), "prices": prices},
        api_key,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )


def stage_wd_prices(sku: str, prices: list[dict[str, object]], api_key: str, *, base_url: str = DEFAULT_STOCK_BRIDGE_BASE_URL, timeout_seconds: int = 30) -> dict[str, object]:
    return _post_json(
        "/integrations/product-prospector/wd-prices",
        {"sku": str(sku or "").strip(), "prices": prices},
        api_key,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
