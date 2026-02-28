from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer

from product_prospector.core.config_store import ShopifyConfig


@dataclass
class OAuthCallbackPayload:
    params: dict[str, str]
    raw_query: str


@dataclass
class OAuthHandshakeResult:
    success: bool
    access_token: str = ""
    scope: str = ""
    expires_in: int | None = None
    error: str = ""


def validate_access_token(config: ShopifyConfig, access_token: str) -> tuple[bool, str]:
    url = f"https://{config.shop_domain}/admin/api/{config.api_version}/shop.json"
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"X-Shopify-Access-Token": access_token, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            code = int(getattr(response, "status", 0))
            if code == 200:
                return True, "OK"
            return False, f"Unexpected status {code}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)


def perform_oauth_handshake(config: ShopifyConfig) -> OAuthHandshakeResult:
    state = secrets.token_urlsafe(24)
    redirect_uri = config.redirect_uri.strip()
    callback_path = _callback_path_from_uri(redirect_uri)
    scope_csv = ",".join(config.scopes)
    auth_url = (
        f"https://{config.shop_domain}/admin/oauth/authorize"
        f"?client_id={urllib.parse.quote(config.client_id)}"
        f"&scope={urllib.parse.quote(scope_csv)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&state={urllib.parse.quote(state)}"
    )

    opened = _open_auth_url(auth_url)
    if not opened:
        return OAuthHandshakeResult(success=False, error="Could not open browser for OAuth.")

    callback = _wait_for_callback(
        host=config.callback_bind_host,
        port=config.callback_bind_port,
        expected_path=callback_path,
        timeout_seconds=config.auth_timeout_seconds,
    )
    if callback is None:
        return OAuthHandshakeResult(success=False, error="Timed out waiting for Shopify OAuth callback.")

    params = callback.params
    if params.get("state", "") != state:
        return OAuthHandshakeResult(success=False, error="OAuth state mismatch.")
    if params.get("shop", "").strip().lower() != config.shop_domain.strip().lower():
        return OAuthHandshakeResult(success=False, error="OAuth callback shop domain mismatch.")
    if not _verify_hmac(raw_query=callback.raw_query, shared_secret=config.client_secret):
        return OAuthHandshakeResult(success=False, error="OAuth HMAC verification failed.")

    code = params.get("code", "").strip()
    if not code:
        return OAuthHandshakeResult(success=False, error="OAuth callback missing authorization code.")

    exchange = _exchange_code_for_token(config=config, code=code)
    if not exchange.success:
        return exchange

    valid, reason = validate_access_token(config=config, access_token=exchange.access_token)
    if not valid:
        return OAuthHandshakeResult(success=False, error=f"Token validation failed: {reason}")

    return exchange


def exchange_client_credentials_for_token(config: ShopifyConfig) -> OAuthHandshakeResult:
    url = f"https://{config.shop_domain}/admin/oauth/access_token"
    payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return OAuthHandshakeResult(success=False, error=f"Client credentials HTTP {exc.code}: {detail}")
    except Exception as exc:
        return OAuthHandshakeResult(success=False, error=str(exc))

    access_token = str(body.get("access_token", "")).strip()
    scope = str(body.get("scope", "")).strip()
    expires_in_raw = body.get("expires_in")
    expires_in: int | None = None
    try:
        if expires_in_raw is not None:
            expires_in = int(expires_in_raw)
    except Exception:
        expires_in = None

    if not access_token:
        return OAuthHandshakeResult(success=False, error="Client credentials response missing access_token.")
    return OAuthHandshakeResult(success=True, access_token=access_token, scope=scope, expires_in=expires_in)


def _exchange_code_for_token(config: ShopifyConfig, code: str) -> OAuthHandshakeResult:
    url = f"https://{config.shop_domain}/admin/oauth/access_token"
    payload = json.dumps(
        {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "code": code,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return OAuthHandshakeResult(success=False, error=f"Token exchange HTTP {exc.code}: {detail}")
    except Exception as exc:
        return OAuthHandshakeResult(success=False, error=str(exc))

    access_token = str(body.get("access_token", "")).strip()
    scope = str(body.get("scope", "")).strip()
    if not access_token:
        return OAuthHandshakeResult(success=False, error="Token exchange returned no access token.")
    return OAuthHandshakeResult(success=True, access_token=access_token, scope=scope)


def _verify_hmac(raw_query: str, shared_secret: str) -> bool:
    pairs = urllib.parse.parse_qsl(raw_query, keep_blank_values=True)
    hmac_value = ""
    message_parts: list[tuple[str, str]] = []
    for key, value in pairs:
        if key == "hmac":
            hmac_value = value
            continue
        if key == "signature":
            continue
        message_parts.append((key, value))
    if not hmac_value:
        return False

    message = "&".join(f"{k}={v}" for k, v in sorted(message_parts, key=lambda item: item[0]))
    digest = hmac.new(shared_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(digest, hmac_value)


def _open_auth_url(url: str) -> bool:
    try:
        if os.name == "nt":
            os.startfile(url)  # type: ignore[attr-defined]
            return True
    except Exception:
        pass
    try:
        return bool(webbrowser.open(url, new=1, autoraise=True))
    except Exception:
        return False


def _callback_path_from_uri(redirect_uri: str) -> str:
    parsed = urllib.parse.urlparse(redirect_uri)
    path = parsed.path.strip() or "/callback"
    if not path.startswith("/"):
        return f"/{path}"
    return path


def _wait_for_callback(host: str, port: int, expected_path: str, timeout_seconds: int) -> OAuthCallbackPayload | None:
    callback_holder: dict[str, OAuthCallbackPayload] = {}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != expected_path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            params_list = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            params = {key: value for key, value in params_list}
            callback_holder["payload"] = OAuthCallbackPayload(params=params, raw_query=parsed.query)

            body = (
                "<html><body style='font-family:Segoe UI,Arial,sans-serif;'>"
                "<h3>Product Prospector</h3>"
                "<p>Shopify authorization complete. You can close this window.</p>"
                "</body></html>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            done.set()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    try:
        server = HTTPServer((host, port), CallbackHandler)
    except Exception:
        return None

    server.timeout = 1
    try:
        deadline = time.time() + max(timeout_seconds, 30)
        while time.time() < deadline and not done.is_set():
            server.handle_request()
    finally:
        server.server_close()

    return callback_holder.get("payload")
