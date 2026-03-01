$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir "..\.."))
$AppDevDir = Join-Path $ProjectRoot "app\dev"
$VenvDir = Join-Path $AppDevDir ".venv-win-build"
$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }

Write-Host "Using Python: $PythonBin"

& $PythonBin -m venv $VenvDir
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPyInstaller = Join-Path $VenvDir "Scripts\pyinstaller.exe"

& $VenvPython -m pip install --upgrade pip setuptools wheel
& $VenvPython -m pip install -r (Join-Path $AppDevDir "requirements.txt")

Push-Location $AppDevDir

if (Test-Path "build") { Remove-Item "build" -Recurse -Force }
if (Test-Path "dist") { Remove-Item "dist" -Recurse -Force }

& $VenvPyInstaller `
  --noconfirm `
  --clean `
  --windowed `
  --name "ProductProspector" `
  --icon "..\icon.ico" `
  --paths "$AppDevDir" `
  --hidden-import "product_prospector" `
  --hidden-import "core" `
  --add-data "..\required;app\required" `
  --add-data "..\config;app\config" `
  --add-data "..\video;app\video" `
  --add-data "..\logo.png;app" `
  --add-data "..\icon.ico;app" `
  --add-data "..\product_prospector.settings.json;app" `
  run_product_prospector.pyw

Pop-Location

Write-Host "Build complete:"
Write-Host "  $AppDevDir\dist\ProductProspector\ProductProspector.exe"
