$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir "..\.."))
$AppDevDir = Join-Path $ProjectRoot "app\dev"
$BundleRulesPath = Join-Path $ProjectRoot "app\BUNDLING_RULES.md"
$VenvDir = Join-Path $AppDevDir ".venv-win-build"
$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }

function Get-PyInstallerWarnModuleName {
  param([string]$Line)

  if ($Line -match "^(missing|excluded|runtime) module named (.+?) -") {
    return $Matches[2].Trim("'")
  }
  return ""
}

function Test-IsExpectedPyInstallerWarnModule {
  param([string]$ModuleName)

  if (-not $ModuleName) {
    return $false
  }

  $expectedExact = @(
    "_dummy_thread",
    "_frozen_importlib",
    "_frozen_importlib_external",
    "_posixshmem",
    "_posixsubprocess",
    "_scproxy",
    "_typeshed",
    "AppKit",
    "Foundation",
    "IPython",
    "PyQt4",
    "StringIO",
    "adbc_driver_manager",
    "botocore",
    "bs4",
    "charset_normalizer",
    "collections.abc",
    "dateutil.tz.tzfile",
    "defusedxml",
    "defusedxml.ElementTree",
    "fcntl",
    "fsspec",
    "grp",
    "java",
    "java.lang",
    "lxml",
    "lxml.etree",
    "lxml.html",
    "matplotlib",
    "numba",
    "numexpr",
    "numpy",
    "numpy.random.RandomState",
    "numpy_distutils",
    "odf",
    "olefile",
    "openpyxl.tests",
    "pandas.core.internals.Block",
    "pandas.io.formats.style",
    "pandas.io.formats.style_render",
    "playwright._impl._worker",
    "posix",
    "psutil",
    "pwd",
    "pyarrow",
    "pyimod02_importers",
    "pytest",
    "python_calamine",
    "qtpy",
    "readline",
    "resource",
    "scipy",
    "sqlalchemy",
    "sqlite3.Error",
    "tables",
    "termios",
    "threadpoolctl",
    "traitlets",
    "vms_lib",
    "win32pdh",
    "xlrd",
    "xlsxwriter",
    "yaml"
  )

  if ($expectedExact -contains $ModuleName) {
    return $true
  }

  $expectedPrefixes = @(
    "IPython.",
    "adbc_driver_manager.",
    "bs4.",
    "botocore.",
    "dateutil.tz.",
    "defusedxml.",
    "java.",
    "lxml.",
    "matplotlib.",
    "multiprocessing.",
    "numba.",
    "numpy.",
    "numpy_distutils.",
    "odf.",
    "openpyxl.tests.",
    "pandas.core.internals.",
    "playwright._impl._worker.",
    "pyarrow.",
    "scipy.",
    "six.moves",
    "sqlalchemy."
  )

  foreach ($prefix in $expectedPrefixes) {
    if ($ModuleName.StartsWith($prefix, [System.StringComparison]::Ordinal)) {
      return $true
    }
  }

  return $false
}

function Show-PyInstallerWarnSummary {
  param([string]$WarnPath)

  if (-not (Test-Path $WarnPath)) {
    return
  }

  $warnLines = Get-Content $WarnPath | Where-Object { $_ -match "^(missing|excluded|runtime) module named " }
  if (-not $warnLines) {
    Write-Host "PyInstaller warnings: none"
    return
  }

  $unexpected = @()
  foreach ($line in $warnLines) {
    $moduleName = Get-PyInstallerWarnModuleName -Line $line
    if (-not (Test-IsExpectedPyInstallerWarnModule -ModuleName $moduleName)) {
      $unexpected += [PSCustomObject]@{
        Module = $moduleName
        Line = $line
      }
    }
  }

  $expectedCount = $warnLines.Count - $unexpected.Count
  Write-Host "PyInstaller warning summary: total=$($warnLines.Count) expected=$expectedCount unexpected=$($unexpected.Count)"

  if ($unexpected.Count -gt 0) {
    Write-Warning "Unexpected PyInstaller warning modules detected:"
    $unexpected |
      Sort-Object Module -Unique |
      ForEach-Object { Write-Host "  $($_.Module)" }
  }
}

Write-Host "Using Python: $PythonBin"
if (Test-Path $BundleRulesPath) {
  Write-Host "Bundling rules: $BundleRulesPath"
}

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
  --onefile `
  --windowed `
  --name "ProductProspector" `
  --icon "..\icon.ico" `
  --paths "$AppDevDir" `
  --hidden-import "product_prospector" `
  --hidden-import "core" `
  --exclude-module "pandas.io.formats.style" `
  --exclude-module "pandas.io.formats.style_render" `
  --add-data "..\required;app\required" `
  --add-data "..\config;app\config" `
  --add-data "..\video;app\video" `
  --add-data "..\logo.png;app" `
  --add-data "..\icon.ico;app" `
  --add-data "..\product_prospector.settings.json;app" `
  run_product_prospector.pyw

Pop-Location

Write-Host "Build complete:"
Write-Host "  $AppDevDir\dist\ProductProspector.exe"
Show-PyInstallerWarnSummary -WarnPath (Join-Path $AppDevDir "build\ProductProspector\warn-ProductProspector.txt")
