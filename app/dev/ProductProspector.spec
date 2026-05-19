# -*- mode: python ; coding: utf-8 -*-

import os

from PyInstaller.utils.hooks import collect_submodules


updater_asset = os.path.join('dist', 'ProductProspectorUpdater.exe')
if not os.path.exists(updater_asset):
    raise SystemExit(
        "Missing dist\\ProductProspectorUpdater.exe. "
        "Build the updater helper first with .\\build_windows_exe.ps1 or "
        "python -m PyInstaller ProductProspectorUpdater.spec."
    )


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
    ],
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
