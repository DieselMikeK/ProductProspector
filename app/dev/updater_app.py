"""Standalone updater helper for swapping in a new ProductProspector.exe."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import tkinter as tk
import urllib.request
from tkinter import ttk

from update_utils import (
    APP_NAME,
    MAIN_EXECUTABLE_NAME,
    compute_file_sha256,
    get_resource_path,
    normalize_release_manifest,
    open_url_with_tls_fallback,
)

try:
    import ctypes
except Exception:  # pragma: no cover - Windows-only best effort
    ctypes = None


DOWNLOAD_CHUNK_SIZE = 1024 * 256
PROCESS_WAIT_TIMEOUT_SECONDS = 45
FILE_UNLOCK_TIMEOUT_SECONDS = 45
LAUNCH_VERIFY_TIMEOUT_SECONDS = 10
LAUNCH_VERIFY_POLL_INTERVAL_SECONDS = 0.25


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Product Prospector updater")
    parser.add_argument("--current-exe", required=True, help="Path to the installed ProductProspector.exe")
    parser.add_argument("--manifest-file", default="", help="Path to a release manifest file")
    parser.add_argument("--download-url", required=True, help="URL of the replacement executable")
    parser.add_argument("--target-version", required=True, help="Version being installed")
    parser.add_argument("--source-version", default="", help="Version currently installed")
    parser.add_argument("--sha256", default="", help="Expected SHA-256 hash for the downloaded executable")
    parser.add_argument("--wait-pid", type=int, default=0, help="PID of the app process that should exit first")
    return parser.parse_args(argv)


def wait_for_process_exit(pid: int, timeout_seconds: int) -> bool:
    """Wait for a process to exit on Windows. Returns True if it exited in time."""
    if not pid or os.name != "nt" or ctypes is None:
        return True

    synchronize = 0x00100000
    wait_object_0 = 0x00000000
    wait_timeout = 0x00000102

    kernel32 = ctypes.windll.kernel32
    process_handle = kernel32.OpenProcess(synchronize, False, pid)
    if not process_handle:
        return True

    try:
        result = kernel32.WaitForSingleObject(process_handle, int(timeout_seconds * 1000))
        return result == wait_object_0 or result != wait_timeout
    finally:
        kernel32.CloseHandle(process_handle)


class UpdaterWindow:
    def __init__(self, args) -> None:
        self.args = args
        self.target_exe = os.path.abspath(args.current_exe)
        self.install_root = os.path.dirname(self.target_exe)
        self.target_dir = self.install_root
        self.staging_dir = tempfile.mkdtemp(prefix="ProductProspectorUpdate-")
        self.release_files = self._load_release_files()
        self.applied_files: list[tuple[dict[str, str | bool], bool]] = []

        self.root = tk.Tk()
        self.root.title(f"{APP_NAME} Updater")
        self.root.resizable(False, False)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.configure(bg="#171717")

        try:
            icon_path = get_resource_path(os.path.join("app", "icon.ico"))
            if os.path.exists(icon_path):
                self.root.iconbitmap(icon_path)
        except Exception:
            pass

        self.status_var = tk.StringVar(value="Preparing update...")
        version_text = f"v{args.source_version or '?'} -> v{args.target_version}"
        self.detail_var = tk.StringVar(value=version_text)
        self.can_close = False

        self._build_ui()

    def _build_ui(self) -> None:
        width, height = 500, 210
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        self.root.geometry(f"{width}x{height}+{(screen_w-width)//2}+{(screen_h-height)//2}")

        container = tk.Frame(self.root, bg="#171717", padx=24, pady=20)
        container.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            container,
            text=f"{APP_NAME} Update",
            font=("Segoe UI", 14, "bold"),
            bg="#171717",
            fg="#f3f3f3",
        ).pack(anchor="w")

        tk.Label(
            container,
            textvariable=self.detail_var,
            font=("Segoe UI", 9),
            bg="#171717",
            fg="#a8a8a8",
        ).pack(anchor="w", pady=(4, 16))

        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "Updater.Horizontal.TProgressbar",
            troughcolor="#303030",
            background="#2ea043",
            bordercolor="#171717",
            lightcolor="#2ea043",
            darkcolor="#2ea043",
        )

        self.progress = ttk.Progressbar(
            container,
            orient="horizontal",
            mode="determinate",
            maximum=100,
            length=448,
            style="Updater.Horizontal.TProgressbar",
        )
        self.progress.pack(anchor="w")

        tk.Label(
            container,
            textvariable=self.status_var,
            font=("Segoe UI", 10),
            bg="#171717",
            fg="#d6d6d6",
            wraplength=450,
            justify="left",
        ).pack(anchor="w", pady=(16, 0))

        self.close_button = ttk.Button(container, text="Close", command=self.root.destroy)
        self.close_button.pack(anchor="e", pady=(18, 0))
        self.close_button.configure(state=tk.DISABLED)

    def _on_close(self) -> None:
        if self.can_close:
            self.root.destroy()

    def set_status(self, text: str) -> None:
        self.root.after(0, lambda: self.status_var.set(text))

    def set_progress(self, value: int) -> None:
        clamped = max(0, min(100, int(value)))
        self.root.after(0, lambda: self.progress.configure(value=clamped))

    def allow_close(self) -> None:
        self.can_close = True
        self.root.after(0, lambda: self.close_button.configure(state=tk.NORMAL))

    def run(self) -> None:
        self.root.after(100, self._start_update_thread)
        self.root.mainloop()

    def _start_update_thread(self) -> None:
        import threading

        threading.Thread(target=self._perform_update, daemon=True).start()

    def _perform_update(self) -> None:
        try:
            self._download_release_files()
            self._wait_for_app_exit()
            self._install_release_files()
            self._finish_success()
        except Exception as exc:
            self._cleanup_partial_files()
            self.set_status(f"Update failed: {exc}")
            self.allow_close()

    def _resolve_target_path(self, relative_path: str) -> str:
        normalized = str(relative_path or "").replace("\\", "/").strip()
        parts = [part for part in normalized.split("/") if part and part != "."]
        if not parts or any(part == ".." for part in parts):
            raise RuntimeError(f"Invalid release target path '{relative_path}'.")
        target_path = os.path.abspath(os.path.join(self.install_root, *parts))
        if os.path.commonpath([self.install_root, target_path]) != self.install_root:
            raise RuntimeError(f"Release target path '{relative_path}' is outside the install folder.")
        return target_path

    def _build_release_entry(self, relative_path: str, download_url: str, sha256: str) -> dict[str, str | bool]:
        target_path = self._resolve_target_path(relative_path)
        return {
            "relative_path": str(relative_path).replace("\\", "/"),
            "download_url": str(download_url or "").strip(),
            "sha256": str(sha256 or "").strip().lower(),
            "target_path": target_path,
            "staged_path": os.path.join(
                self.staging_dir,
                *str(relative_path).replace("\\", "/").split("/"),
            ),
            "backup_path": target_path + ".bak",
            "is_main_exe": os.path.abspath(target_path) == self.target_exe,
        }

    def _load_release_files(self) -> list[dict[str, str | bool]]:
        manifest_path = str(self.args.manifest_file or "").strip()
        release_files: list[dict[str, str | bool]] = []

        if manifest_path:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = normalize_release_manifest(json.load(handle), source_url=manifest_path)
            release_files.extend(
                self._build_release_entry(
                    entry["relative_path"],
                    str(entry.get("download_url") or ""),
                    str(entry.get("sha256") or ""),
                )
                for entry in manifest.get("files") or []
            )

        if not any(bool(entry["is_main_exe"]) for entry in release_files):
            release_files.append(
                self._build_release_entry(
                    MAIN_EXECUTABLE_NAME,
                    self.args.download_url,
                    self.args.sha256,
                )
            )

        return release_files

    def _download_one_file(self, entry: dict[str, str | bool], progress_start: int, progress_span: int) -> None:
        display_name = str(entry["relative_path"]).replace("/", "\\")
        self.set_status(f"Downloading {display_name}...")
        request = urllib.request.Request(
            str(entry["download_url"]),
            headers={"User-Agent": "ProductProspectorUpdater/1.0"},
        )
        os.makedirs(os.path.dirname(str(entry["staged_path"])), exist_ok=True)
        with open_url_with_tls_fallback(request, timeout=60) as response:
            total_bytes = int(response.headers.get("Content-Length") or 0)
            downloaded_bytes = 0
            with open(str(entry["staged_path"]), "wb") as handle:
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    handle.write(chunk)
                    downloaded_bytes += len(chunk)
                    if total_bytes > 0:
                        percent = min(
                            progress_start + progress_span,
                            progress_start + int(downloaded_bytes * progress_span / total_bytes),
                        )
                        self.set_progress(percent)
                        self.set_status(
                            f"Downloading {display_name}... "
                            f"{downloaded_bytes / 1_048_576:.1f} / {total_bytes / 1_048_576:.1f} MB"
                        )
                    else:
                        self.set_status(
                            f"Downloading {display_name}... {downloaded_bytes / 1_048_576:.1f} MB"
                        )

        expected_hash = str(entry.get("sha256") or "").strip().lower()
        if expected_hash:
            self.set_status(f"Verifying {display_name}...")
            actual_hash = compute_file_sha256(str(entry["staged_path"]))
            if actual_hash.lower() != expected_hash:
                raise RuntimeError(f"{display_name} hash does not match the release manifest")

    def _download_release_files(self) -> None:
        file_count = max(1, len(self.release_files))
        progress_span = max(1, 85 // file_count)
        for index, entry in enumerate(self.release_files):
            start = index * progress_span
            self._download_one_file(entry, start, progress_span)
        self.set_progress(88)

    def _wait_for_app_exit(self) -> None:
        self.set_status("Waiting for Product Prospector to close...")
        wait_for_process_exit(self.args.wait_pid, PROCESS_WAIT_TIMEOUT_SECONDS)

    def _install_one_file(self, entry: dict[str, str | bool]) -> bool:
        staged_path = str(entry["staged_path"])
        target_path = str(entry["target_path"])
        backup_path = str(entry["backup_path"])
        is_main_exe = bool(entry["is_main_exe"])
        if not os.path.exists(staged_path):
            raise RuntimeError(f"Downloaded file was not found for {entry['relative_path']}")

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        if os.path.exists(backup_path):
            os.remove(backup_path)

        had_existing_file = os.path.exists(target_path)
        deadline = time.time() + FILE_UNLOCK_TIMEOUT_SECONDS
        while True:
            try:
                if had_existing_file:
                    os.replace(target_path, backup_path)
                os.replace(staged_path, target_path)
                return had_existing_file
            except PermissionError:
                if not is_main_exe or time.time() >= deadline:
                    raise RuntimeError(
                        f"{str(entry['relative_path']).replace('/', chr(92))} is still locked after waiting"
                    )
                time.sleep(0.5)
            except Exception:
                if had_existing_file and os.path.exists(backup_path) and not os.path.exists(target_path):
                    os.replace(backup_path, target_path)
                raise

    def _restore_file(self, entry: dict[str, str | bool], had_existing_file: bool) -> None:
        target_path = str(entry["target_path"])
        backup_path = str(entry["backup_path"])
        if os.path.exists(target_path):
            try:
                os.remove(target_path)
            except OSError:
                pass
        if had_existing_file and os.path.exists(backup_path):
            try:
                os.replace(backup_path, target_path)
            except OSError:
                pass

    def _install_release_files(self) -> None:
        ordered_files = [
            entry for entry in self.release_files if not bool(entry["is_main_exe"])
        ] + [
            entry for entry in self.release_files if bool(entry["is_main_exe"])
        ]
        applied_files: list[tuple[dict[str, str | bool], bool]] = []

        try:
            for index, entry in enumerate(ordered_files, start=1):
                display_name = str(entry["relative_path"]).replace("/", "\\")
                self.set_status(f"Installing {display_name}...")
                self.set_progress(88 + min(11, index * 10 // max(1, len(ordered_files))))
                had_existing_file = self._install_one_file(entry)
                applied_files.append((entry, had_existing_file))
        except Exception:
            for entry, had_existing_file in reversed(applied_files):
                self._restore_file(entry, had_existing_file)
            raise

        self.applied_files = applied_files
        self._cleanup_partial_files()
        self.set_progress(100)

    def _cleanup_backups(self) -> None:
        for entry, _had_existing_file in self.applied_files:
            backup_path = str(entry["backup_path"])
            if os.path.exists(backup_path):
                try:
                    os.remove(backup_path)
                except OSError:
                    pass
        self.applied_files = []

    def _rollback_update(self) -> None:
        for entry, had_existing_file in reversed(self.applied_files):
            self._restore_file(entry, had_existing_file)
        self.applied_files = []

    def _finish_success(self) -> None:
        self.set_status("Update installed. Reopening app...")
        time.sleep(1.0)
        try:
            launched_process = subprocess.Popen([self.target_exe], cwd=self.target_dir)
        except Exception:
            self.set_status("Update installed. Please reopen Product Prospector manually.")
            self._cleanup_backups()
            self.allow_close()
            return

        launch_deadline = time.time() + LAUNCH_VERIFY_TIMEOUT_SECONDS
        while time.time() < launch_deadline:
            exit_code = launched_process.poll()
            if exit_code is None:
                time.sleep(LAUNCH_VERIFY_POLL_INTERVAL_SECONDS)
                continue
            if exit_code != 0:
                self.set_status("Updated app failed to start. Restoring previous version...")
                self._rollback_update()
                time.sleep(1.0)
                try:
                    subprocess.Popen([self.target_exe], cwd=self.target_dir)
                except Exception:
                    self.set_status(
                        "Updated app failed to start. Previous version was restored. "
                        "Please reopen Product Prospector manually."
                    )
                else:
                    self.set_status("Updated app failed to start. Previous version restored.")
                self.allow_close()
                return
            break

        self._cleanup_backups()
        self.can_close = True
        self.root.after(0, self.root.destroy)

    def _cleanup_partial_files(self) -> None:
        if self.staging_dir and os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app = UpdaterWindow(args)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
