from __future__ import annotations

import ctypes
import traceback
from datetime import datetime
from pathlib import Path

from product_prospector.desktop_app import main


def _show_error_dialog(message: str) -> None:
    try:
        ctypes.windll.user32.MessageBoxW(0, message, "Product Prospector Error", 0x10)
    except Exception:
        pass


def _write_error_log(message: str) -> None:
    base_dir = Path(__file__).resolve().parent
    app_dir = base_dir / "app"
    if app_dir.exists():
        log_path = app_dir / "ProductProspector_error.log"
    else:
        log_path = base_dir / "ProductProspector_error.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = f"[{timestamp}] {message}\n\n"
    try:
        log_path.write_text(content, encoding="utf-8")
    except Exception:
        pass


if __name__ == "__main__":
    try:
        exit_code = int(main())
    except Exception:
        error_text = traceback.format_exc()
        _write_error_log(error_text)
        _show_error_dialog(
            "Product Prospector failed to start.\n\n"
            "A crash log was written to ProductProspector_error.log in the app folder."
        )
        raise SystemExit(1)
    raise SystemExit(exit_code)
