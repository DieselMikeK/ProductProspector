#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
APP_DEV_DIR="$PROJECT_ROOT/app/dev"
VENV_DIR="$APP_DEV_DIR/.venv-mac-build"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"
ICON_ICO="$PROJECT_ROOT/app/icon.ico"
ICONSET_DIR="$APP_DEV_DIR/build/ProductProspector.iconset"
ICON_ICNS="$APP_DEV_DIR/build/ProductProspector.icns"
ROOT_APP="$PROJECT_ROOT/ProductProspector.app"
COPY_TO_ROOT="${COPY_TO_ROOT:-1}"
MAX_ROOT_BACKUPS="${MAX_ROOT_BACKUPS:-1}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: $PYTHON_BIN not found. Install Python 3.12+ or set PYTHON_BIN=<python_path>." >&2
  exit 1
fi

echo "Using Python: $PYTHON_BIN"
"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install -r "$APP_DEV_DIR/requirements.txt"

pushd "$APP_DEV_DIR" >/dev/null
rm -rf build dist
mkdir -p "$ICONSET_DIR"

if ! command -v iconutil >/dev/null 2>&1; then
  echo "Error: iconutil is required on macOS to generate .icns files." >&2
  exit 1
fi

if [[ ! -f "$ICON_ICO" ]]; then
  echo "Error: icon source not found at $ICON_ICO" >&2
  exit 1
fi

"$VENV_DIR/bin/python" - <<PY
from pathlib import Path
from PIL import Image, ImageOps

src = Path("$ICON_ICO")
dst = Path("$ICONSET_DIR")
img = Image.open(src).convert("RGBA")
if img.width != img.height:
    side = max(img.width, img.height)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    offset = ((side - img.width) // 2, (side - img.height) // 2)
    canvas.paste(img, offset)
    img = canvas

outputs = [
    ("icon_16x16.png", 16),
    ("icon_16x16@2x.png", 32),
    ("icon_32x32.png", 32),
    ("icon_32x32@2x.png", 64),
    ("icon_128x128.png", 128),
    ("icon_128x128@2x.png", 256),
    ("icon_256x256.png", 256),
    ("icon_256x256@2x.png", 512),
    ("icon_512x512.png", 512),
    ("icon_512x512@2x.png", 1024),
]
for name, size in outputs:
    rendered = ImageOps.fit(img, (size, size), method=Image.Resampling.LANCZOS)
    rendered.save(dst / name, format="PNG")
PY

iconutil -c icns "$ICONSET_DIR" -o "$ICON_ICNS"

"$VENV_DIR/bin/pyinstaller" \
  --noconfirm \
  --clean \
  --windowed \
  --name "ProductProspector" \
  --icon "$ICON_ICNS" \
  --paths "$APP_DEV_DIR" \
  --hidden-import "product_prospector" \
  --hidden-import "core" \
  --add-data "../required:app/required" \
  --add-data "../config:app/config" \
  --add-data "../video:app/video" \
  --add-data "../logo.png:app" \
  --add-data "../icon.ico:app" \
  --add-data "../product_prospector.settings.json:app" \
  run_product_prospector.pyw

popd >/dev/null

echo "Build complete:"
echo "  $APP_DEV_DIR/dist/ProductProspector.app"

if [[ "$COPY_TO_ROOT" == "1" ]]; then
  backup_ts="$(date +%Y%m%d-%H%M%S)"
  if [[ -d "$ROOT_APP" ]]; then
    mv "$ROOT_APP" "$PROJECT_ROOT/ProductProspector.app.backup-$backup_ts"
  fi

  cp -R "$APP_DEV_DIR/dist/ProductProspector.app" "$ROOT_APP"

  if [[ "$MAX_ROOT_BACKUPS" =~ ^[0-9]+$ ]]; then
    backup_index=0
    while IFS= read -r old_backup; do
      [[ -z "$old_backup" ]] && continue
      backup_index=$((backup_index + 1))
      if (( backup_index > MAX_ROOT_BACKUPS )); then
        rm -rf "$old_backup"
      fi
    done < <(ls -dt "$PROJECT_ROOT"/ProductProspector.app.backup-* 2>/dev/null || true)
  fi

  echo "Copied to root:"
  echo "  $ROOT_APP"
  echo "Kept root backups:"
  echo "  up to $MAX_ROOT_BACKUPS"
fi
