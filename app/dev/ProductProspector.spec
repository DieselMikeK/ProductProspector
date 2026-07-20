# -*- mode: python ; coding: utf-8 -*-

import json
import os
from pathlib import Path

import playwright
from PyInstaller.utils.hooks import collect_submodules


updater_asset = os.path.join('dist', 'ProductProspectorUpdater.exe')
if not os.path.exists(updater_asset):
    raise SystemExit(
        "Missing dist\\ProductProspectorUpdater.exe. "
        "Build the updater helper first with .\\build_windows_exe.ps1 or "
        "python -m PyInstaller ProductProspectorUpdater.spec."
    )

playwright_package_dir = Path(playwright.__file__).resolve().parent
playwright_browsers_manifest = playwright_package_dir / 'driver' / 'package' / 'browsers.json'
with playwright_browsers_manifest.open('r', encoding='utf-8') as handle:
    playwright_browser_entries = json.load(handle).get('browsers', [])
firefox_revision = next(
    str(item.get('revision', '')).strip()
    for item in playwright_browser_entries
    if str(item.get('name', '')).strip() == 'firefox'
)
playwright_browser_cache = Path(
    os.environ.get(
        'PLAYWRIGHT_BROWSERS_PATH',
        str(Path(os.environ.get('LOCALAPPDATA', '')) / 'ms-playwright'),
    )
)
firefox_browser_root = playwright_browser_cache / f'firefox-{firefox_revision}'
if not firefox_browser_root.exists():
    raise SystemExit(
        f'Missing Playwright Firefox revision {firefox_revision} at {firefox_browser_root}. '
        'Run python -m playwright install firefox before building.'
    )
playwright_firefox_data = [
    (
        str(firefox_browser_root),
        f'playwright/driver/package/.local-browsers/firefox-{firefox_revision}',
    )
]


a = Analysis(
    ['run_product_prospector.pyw'],
    pathex=[os.path.abspath('.')],
    binaries=[],
    datas=[
        ('..\\required', 'app\\required'),
        ('..\\config', 'app\\config'),
        ('..\\video', 'app\\video'),
        ('..\\logo.png', 'app'),
        ('..\\icon.ico', 'app'),
        ('..\\product_prospector.settings.json', 'app'),
        ('..\\update\\VERSION', 'app\\update'),
        (updater_asset, 'app\\update'),
    ] + playwright_firefox_data,
    hiddenimports=[
        'product_prospector',
        'product_prospector.core',
        'core',
    ] + collect_submodules('product_prospector') + collect_submodules('core'),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pandas.io.formats.style', 'pandas.io.formats.style_render'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='ProductProspector',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['..\\icon.ico'],
)
