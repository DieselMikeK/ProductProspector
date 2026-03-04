from __future__ import annotations

import json
import shutil
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path


def _frozen_bundle_app_dir(exe_dir: Path) -> Path | None:
    candidates: list[Path] = []
    meipass = getattr(sys, "_MEIPASS", "")
    if meipass:
        candidates.append(Path(meipass) / "app")
    if sys.platform == "darwin":
        candidates.append(exe_dir.parent / "Resources" / "app")
    candidates.append(exe_dir / "app")

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if sys.platform == "darwin":
            app_dir = Path.home() / "Library" / "Application Support" / "ProductProspector"
            app_dir.mkdir(parents=True, exist_ok=True)
            (app_dir / "config").mkdir(parents=True, exist_ok=True)

            bundle_app_dir = _frozen_bundle_app_dir(exe_dir)
            if bundle_app_dir is not None:
                template_src = bundle_app_dir / "config" / "shopify.json.template"
                template_dst = app_dir / "config" / "shopify.json.template"
                if template_src.exists() and not template_dst.exists():
                    try:
                        shutil.copy2(template_src, template_dst)
                    except Exception:
                        pass
            return app_dir

        app_dir = exe_dir / "app"
        app_dir.mkdir(parents=True, exist_ok=True)
        return app_dir

    project_root = Path(__file__).resolve().parents[2]
    app_dir = project_root / "app"
    if app_dir.exists():
        return app_dir
    return project_root


APP_BASE_DIR = _app_base_dir()
APP_SETTINGS_PATH = APP_BASE_DIR / "product_prospector.settings.json"
SHOPIFY_CONFIG_PATH = APP_BASE_DIR / "config" / "shopify.json"
SHOPIFY_TOKEN_PATH = APP_BASE_DIR / "config" / "shopify_token.json"


@dataclass
class AppSettings:
    run_mode: str = "Update Existing"
    year_policy: str = "merge"
    carry_down_sku: bool = True
    propose_title_year_update: bool = True
    only_rows_with_year_changes: bool = True
    inventory_owner: str = "Mike K"


@dataclass
class ShopifyConfig:
    shop_domain: str
    storefront_domain: str
    client_id: str
    client_secret: str
    admin_api_access_token: str
    auth_mode: str
    scopes: list[str]
    redirect_uri: str
    callback_bind_host: str
    callback_bind_port: int
    auth_timeout_seconds: int
    api_version: str = "2025-10"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_app_settings() -> AppSettings:
    if not APP_SETTINGS_PATH.exists():
        return AppSettings()
    try:
        raw = _load_json(APP_SETTINGS_PATH)
    except Exception:
        return AppSettings()

    return AppSettings(
        run_mode=str(raw.get("run_mode", "Update Existing")),
        year_policy=str(raw.get("year_policy", "merge")),
        carry_down_sku=bool(raw.get("carry_down_sku", True)),
        propose_title_year_update=bool(raw.get("propose_title_year_update", True)),
        only_rows_with_year_changes=bool(raw.get("only_rows_with_year_changes", True)),
        inventory_owner=str(raw.get("inventory_owner", "Mike K")),
    )


def save_app_settings(settings: AppSettings) -> None:
    APP_SETTINGS_PATH.write_text(
        json.dumps(
            {
                "run_mode": settings.run_mode,
                "year_policy": settings.year_policy,
                "carry_down_sku": settings.carry_down_sku,
                "propose_title_year_update": settings.propose_title_year_update,
                "only_rows_with_year_changes": settings.only_rows_with_year_changes,
                "inventory_owner": settings.inventory_owner,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def load_shopify_config() -> ShopifyConfig | None:
    if not SHOPIFY_CONFIG_PATH.exists():
        return None

    raw = _load_json(SHOPIFY_CONFIG_PATH)
    shop_domain = str(raw.get("shop_domain") or raw.get("shop") or raw.get("store") or "").strip()
    storefront_domain = str(raw.get("storefront_domain", "")).strip()
    client_id = str(raw.get("client_id", "")).strip()
    client_secret = str(raw.get("client_secret", "")).strip()
    admin_api_access_token = str(raw.get("admin_api_access_token", "")).strip()
    auth_mode = str(raw.get("auth_mode", "auto")).strip().lower() or "auto"
    api_version = str(raw.get("api_version", "2025-10")).strip() or "2025-10"
    legacy_redirect_host = str(raw.get("redirect_host", "127.0.0.1")).strip() or "127.0.0.1"
    legacy_redirect_port = int(raw.get("redirect_port", 8787))
    redirect_uri = str(raw.get("redirect_uri", "")).strip()
    callback_bind_host = str(raw.get("callback_bind_host") or raw.get("callback_host") or "127.0.0.1").strip() or "127.0.0.1"
    callback_bind_port = int(raw.get("callback_bind_port") or raw.get("callback_port") or legacy_redirect_port)
    auth_timeout_seconds = int(raw.get("auth_timeout_seconds", 240))

    raw_scopes = raw.get("scopes", [])
    scopes: list[str]
    if isinstance(raw_scopes, str):
        scopes = [part.strip() for part in raw_scopes.split(",") if part.strip()]
    elif isinstance(raw_scopes, list):
        scopes = [str(part).strip() for part in raw_scopes if str(part).strip()]
    else:
        scopes = []
    if not scopes:
        scopes = ["read_products", "write_products", "read_metafields", "write_metafields"]

    if not redirect_uri:
        redirect_uri = f"http://{legacy_redirect_host}:{legacy_redirect_port}/callback"

    if not shop_domain or not client_id or not client_secret:
        return None

    return ShopifyConfig(
        shop_domain=shop_domain,
        storefront_domain=storefront_domain,
        client_id=client_id,
        client_secret=client_secret,
        admin_api_access_token=admin_api_access_token,
        auth_mode=auth_mode,
        scopes=scopes,
        redirect_uri=redirect_uri,
        callback_bind_host=callback_bind_host,
        callback_bind_port=callback_bind_port,
        auth_timeout_seconds=auth_timeout_seconds,
        api_version=api_version,
    )


@dataclass
class ShopifyToken:
    access_token: str
    scope: str
    created_at_utc: str


def load_shopify_token() -> ShopifyToken | None:
    if not SHOPIFY_TOKEN_PATH.exists():
        return None
    try:
        raw = _load_json(SHOPIFY_TOKEN_PATH)
    except Exception:
        return None

    access_token = str(raw.get("access_token", "")).strip()
    if not access_token:
        return None
    return ShopifyToken(
        access_token=access_token,
        scope=str(raw.get("scope", "")).strip(),
        created_at_utc=str(raw.get("created_at_utc", "")).strip(),
    )


def save_shopify_token(access_token: str, scope: str) -> None:
    SHOPIFY_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "access_token": access_token,
        "scope": scope,
        "created_at_utc": datetime.now(UTC).isoformat(),
    }
    SHOPIFY_TOKEN_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def clear_shopify_token() -> None:
    if SHOPIFY_TOKEN_PATH.exists():
        SHOPIFY_TOKEN_PATH.unlink()


def check_domain_reachable(domain: str, timeout_seconds: int = 6) -> tuple[bool, str]:
    if not domain:
        return False, "Missing domain."

    url = f"https://{domain}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            code = int(getattr(response, "status", 200))
            if 200 <= code < 500:
                return True, f"HTTP {code}"
            return False, f"Unexpected HTTP status {code}"
    except urllib.error.HTTPError as exc:
        if 200 <= exc.code < 500:
            return True, f"HTTP {exc.code}"
        return False, f"HTTP error {exc.code}"
    except Exception as exc:
        return False, str(exc)
