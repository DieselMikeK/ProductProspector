# Product Prospector Bundling Rules

Read this file before every bundle/publish action.

1. NEVER add anything new to repo root except `ProductProspector.exe`.
2. Windows root handoff must use a PyInstaller `--onefile` build.
3. Keep build artifacts inside `app/dev/dist/` and `app/dev/build/`.
4. Do not copy `_internal/` to repo root.
5. Do not copy `dist/`, `build/`, `.spec`, or temp files to repo root.
6. If root has bundle artifacts from a prior run, remove them before handing off.
7. After bundling, verify root output with:
   `Get-ChildItem -Force`
