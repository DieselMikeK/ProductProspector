"""Helpers for app versioning and release-manifest based updates."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import ssl
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

try:
    import certifi
except Exception:  # pragma: no cover - optional TLS fallback
    certifi = None


APP_NAME = "Product Prospector"
MAIN_EXECUTABLE_NAME = "ProductProspector.exe"
UPDATER_EXECUTABLE_NAME = "ProductProspectorUpdater.exe"
VERSION_FILENAME = "VERSION"
PRIMARY_RELEASE_RELATIVE_PATH = MAIN_EXECUTABLE_NAME
UPDATE_FOLDER_NAME = "update"
DEFAULT_UPDATE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/DieselMikeK/ProductProspector/master/app/update/release.json"
)
LEGACY_UPDATE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/DieselMikeK/ProductProspector/master/app/docs/release.json"
)
CERTIFI_CA_BUNDLE = certifi.where() if certifi else ""


def get_update_dir(base_path: str | os.PathLike[str] | None = None) -> str:
    """Return the update folder path for the current runtime or a provided app/base folder."""
    root = os.fspath(base_path) if base_path else get_base_dir()
    root_path = Path(root)
    if root_path.name.lower() == "app":
        return str(root_path / UPDATE_FOLDER_NAME)
    return str(root_path / "app" / UPDATE_FOLDER_NAME)


def get_source_dir() -> str:
    """Return the folder containing the application source files."""
    return os.path.dirname(os.path.abspath(__file__))


def get_base_dir() -> str:
    """Return the app's runtime directory."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return get_source_dir()


def get_resource_path(relative_path: str) -> str:
    """Resolve a bundled resource path for source and PyInstaller builds."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(get_source_dir(), relative_path)


def normalize_version(value: object) -> str:
    """Normalize a version string for comparisons and display."""
    return str(value or "").strip().lstrip("vV")


def normalize_release_relative_path(value: object) -> str:
    """Normalize a manifest file path and reject traversal outside the install root."""
    raw_value = str(value or "").strip().replace("\\", "/")
    parts: list[str] = []
    for token in raw_value.split("/"):
        token = token.strip()
        if not token or token == ".":
            continue
        if token == "..":
            raise ValueError("Release manifest file path cannot contain '..'.")
        parts.append(token)
    if not parts:
        raise ValueError("Release manifest file path is missing.")
    return "/".join(parts)


def normalize_sha256(value: object) -> str:
    """Normalize and validate a SHA-256 string."""
    sha256 = str(value or "").strip().lower()
    if sha256:
        sha256 = "".join(ch for ch in sha256 if ch in "0123456789abcdef")
        if len(sha256) != 64:
            raise ValueError("Release manifest sha256 must be a 64-character hex string.")
    return sha256


def normalize_release_file(entry: object) -> dict[str, str]:
    """Normalize a single file entry from the release manifest."""
    if not isinstance(entry, dict):
        raise ValueError("Release manifest file entries must be JSON objects.")

    relative_path = normalize_release_relative_path(
        entry.get("relative_path") or entry.get("path")
    )
    download_url = str(entry.get("download_url") or "").strip()
    sha256 = normalize_sha256(entry.get("sha256"))

    return {
        "relative_path": relative_path,
        "download_url": download_url,
        "sha256": sha256,
    }


def parse_version_tuple(value: object) -> tuple[int, ...]:
    """Convert dotted version strings into comparable tuples."""
    version = normalize_version(value)
    if not version:
        return (0,)
    parts: list[int] = []
    for token in version.split("."):
        token = token.strip()
        if token.isdigit():
            parts.append(int(token))
            continue
        digits = "".join(ch for ch in token if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts or [0])


def load_app_version() -> str:
    """Read the current application version from the bundled VERSION file."""
    source_dir = Path(get_source_dir()).resolve()
    candidates = [
        Path(get_resource_path(os.path.join("app", UPDATE_FOLDER_NAME, VERSION_FILENAME))),
        Path(get_update_dir()) / VERSION_FILENAME,
        source_dir.parent / UPDATE_FOLDER_NAME / VERSION_FILENAME,
        source_dir.parents[1] / "app" / UPDATE_FOLDER_NAME / VERSION_FILENAME,
        Path(get_resource_path(VERSION_FILENAME)),
        Path(get_base_dir()) / VERSION_FILENAME,
        source_dir / VERSION_FILENAME,
        source_dir.parents[1] / VERSION_FILENAME,
    ]
    seen: set[Path] = set()
    for path in candidates:
        normalized = path.resolve(strict=False)
        if normalized in seen or not normalized.exists():
            continue
        seen.add(normalized)
        try:
            version = normalize_version(normalized.read_text(encoding="utf-8"))
        except OSError:
            continue
        if version:
            return version
    return "0.0.0"


def load_update_config(required_dir: str | os.PathLike[str] | None) -> dict[str, object]:
    """Load optional update configuration overrides from the runtime app data folder."""
    candidates: list[Path] = []
    if required_dir:
        required_path = Path(required_dir)
        candidates.extend(
            [
                required_path.parent / UPDATE_FOLDER_NAME / "update_config.json",
                required_path / "update_config.json",
            ]
        )
    candidates.extend(
        [
            Path(get_update_dir()) / "update_config.json",
            Path(get_source_dir()).resolve().parent / UPDATE_FOLDER_NAME / "update_config.json",
        ]
    )

    seen: set[Path] = set()
    for path in candidates:
        normalized = path.resolve(strict=False)
        if normalized in seen or not normalized.exists():
            continue
        seen.add(normalized)
        try:
            data = json.loads(normalized.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict):
            return data
    return {}


def get_update_manifest_url(required_dir: str | os.PathLike[str] | None = None) -> str:
    """Return the remote release manifest URL for update checks."""
    env_url = str(os.environ.get("PRODUCT_PROSPECTOR_UPDATE_MANIFEST_URL") or "").strip()
    if env_url:
        return env_url
    config = load_update_config(required_dir)
    config_url = str(config.get("manifest_url") or "").strip()
    if config_url:
        return config_url
    return DEFAULT_UPDATE_MANIFEST_URL


def _is_cert_verification_error(exc: Exception) -> bool:
    """Return True when an exception indicates TLS certificate verification failed."""
    if isinstance(exc, ssl.SSLCertVerificationError):
        return True
    if isinstance(exc, ssl.SSLError) and "CERTIFICATE_VERIFY_FAILED" in str(exc):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError):
            return True
        if isinstance(reason, ssl.SSLError) and "CERTIFICATE_VERIFY_FAILED" in str(reason):
            return True
        if "CERTIFICATE_VERIFY_FAILED" in str(exc):
            return True
    return "CERTIFICATE_VERIFY_FAILED" in str(exc)


def open_url_with_tls_fallback(request: urllib.request.Request, timeout: int = 5):
    """Open a URL, retrying with certifi's CA bundle when default TLS trust fails."""
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except Exception as exc:
        if not CERTIFI_CA_BUNDLE or not _is_cert_verification_error(exc):
            raise

    ssl_context = ssl.create_default_context(cafile=CERTIFI_CA_BUNDLE)
    return urllib.request.urlopen(request, timeout=timeout, context=ssl_context)


def normalize_release_manifest(data: object, source_url: str = "") -> dict[str, object]:
    """Normalize a release manifest payload into the fields the app expects."""
    if not isinstance(data, dict):
        raise ValueError("Release manifest must be a JSON object.")

    version = normalize_version(data.get("version") or data.get("tag_name"))
    if not version:
        raise ValueError("Release manifest is missing a version.")

    download_url = str(data.get("download_url") or "").strip()
    sha256 = normalize_sha256(data.get("sha256"))
    notes = str(data.get("notes") or data.get("body") or "").strip()
    published_at = str(data.get("published_at") or "").strip()
    files: list[dict[str, str]] = []

    raw_files = data.get("files")
    if raw_files is not None:
        if not isinstance(raw_files, list):
            raise ValueError("Release manifest files must be a JSON array.")
        seen_paths: set[str] = set()
        for entry in raw_files:
            normalized_entry = normalize_release_file(entry)
            key = normalized_entry["relative_path"].lower()
            if key in seen_paths:
                raise ValueError(
                    f"Release manifest contains duplicate file entry '{normalized_entry['relative_path']}'."
                )
            seen_paths.add(key)
            files.append(normalized_entry)

    if not files and download_url:
        files.append(
            {
                "relative_path": PRIMARY_RELEASE_RELATIVE_PATH,
                "download_url": download_url,
                "sha256": sha256,
            }
        )

    primary_file = next(
        (
            entry
            for entry in files
            if entry["relative_path"].lower() == PRIMARY_RELEASE_RELATIVE_PATH.lower()
        ),
        None,
    )
    if primary_file:
        if not download_url:
            download_url = primary_file["download_url"]
        if not sha256:
            sha256 = primary_file["sha256"]

    return {
        "version": version,
        "download_url": download_url,
        "sha256": sha256,
        "notes": notes,
        "published_at": published_at,
        "files": files,
        "source_url": source_url,
    }


def fetch_release_manifest(required_dir: str | os.PathLike[str] | None = None, timeout: int = 5) -> dict[str, object]:
    """Fetch and parse the remote release manifest."""
    primary_url = get_update_manifest_url(required_dir)
    candidate_urls = [primary_url]
    normalized_primary = primary_url.strip().lower()
    if normalized_primary == DEFAULT_UPDATE_MANIFEST_URL.lower():
        candidate_urls.append(LEGACY_UPDATE_MANIFEST_URL)

    last_error: Exception | None = None
    for url in candidate_urls:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": f"{APP_NAME.replace(' ', '')}/UpdateCheck"},
        )
        try:
            with open_url_with_tls_fallback(request, timeout=timeout) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                payload = response.read().decode(charset)
            data = json.loads(payload)
            return normalize_release_manifest(data, source_url=url)
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("Unable to load a release manifest.")


def compute_file_sha256(path: str | os.PathLike[str]) -> str:
    """Return the SHA-256 hash of a file."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def find_updater_source_path() -> str:
    """Locate the updater executable bundled with or beside the app."""
    candidates = [
        get_resource_path(os.path.join("app", UPDATE_FOLDER_NAME, UPDATER_EXECUTABLE_NAME)),
        os.path.join(get_update_dir(), UPDATER_EXECUTABLE_NAME),
        os.path.join(get_base_dir(), UPDATER_EXECUTABLE_NAME),
        os.path.join(get_base_dir(), UPDATE_FOLDER_NAME, UPDATER_EXECUTABLE_NAME),
        os.path.join(get_source_dir(), "update", UPDATER_EXECUTABLE_NAME),
        os.path.join(Path(get_source_dir()).resolve().parent, UPDATE_FOLDER_NAME, UPDATER_EXECUTABLE_NAME),
        os.path.join(get_source_dir(), "dist", UPDATER_EXECUTABLE_NAME),
    ]
    seen: set[str] = set()
    for path in candidates:
        normalized = os.path.abspath(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        if os.path.exists(normalized):
            return normalized
    raise FileNotFoundError(
        f"{UPDATER_EXECUTABLE_NAME} not found. Build it before shipping updates."
    )


def stage_updater_executable(current_version: object) -> str:
    """Copy the updater helper to a stable temp location and return that path."""
    source_path = find_updater_source_path()
    staged_dir = os.path.join(tempfile.gettempdir(), "ProductProspectorUpdater")
    os.makedirs(staged_dir, exist_ok=True)

    version_tag = normalize_version(current_version) or "dev"
    staged_path = os.path.join(staged_dir, f"{version_tag}-{UPDATER_EXECUTABLE_NAME}")

    needs_copy = True
    if os.path.exists(staged_path):
        try:
            needs_copy = (
                os.path.getsize(staged_path) != os.path.getsize(source_path)
                or int(os.path.getmtime(staged_path)) != int(os.path.getmtime(source_path))
            )
        except OSError:
            needs_copy = True

    if needs_copy:
        shutil.copy2(source_path, staged_path)

    return staged_path


def stage_release_manifest(manifest: object, current_version: object = "") -> str:
    """Write a normalized release manifest to a stable temp path for the updater helper."""
    normalized_manifest = normalize_release_manifest(manifest)
    staged_dir = os.path.join(tempfile.gettempdir(), "ProductProspectorUpdater")
    os.makedirs(staged_dir, exist_ok=True)

    version_tag = normalize_version(current_version or normalized_manifest.get("version")) or "dev"
    staged_path = os.path.join(staged_dir, f"{version_tag}-release-manifest.json")

    with open(staged_path, "w", encoding="utf-8") as handle:
        json.dump(normalized_manifest, handle, indent=2)

    return staged_path
