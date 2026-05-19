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
    "certifi",
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

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPyInstaller = Join-Path $VenvDir "Scripts\pyinstaller.exe"

if (-not (Test-Path $VenvPython)) {
  if (Test-Path $VenvDir) {
    Remove-Item -LiteralPath $VenvDir -Recurse -Force
  }
  Write-Host "Creating build virtual environment..."
  & $PythonBin -m venv $VenvDir
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create build virtual environment."
  }
} else {
  Write-Host "Reusing existing build virtual environment..."
}

& $VenvPython -m pip install --upgrade pip setuptools wheel
if ($LASTEXITCODE -ne 0) {
  throw "Failed to upgrade pip/setuptools/wheel in build virtual environment."
}
& $VenvPython -m pip install -r (Join-Path $AppDevDir "requirements.txt")
if ($LASTEXITCODE -ne 0) {
  throw "Failed to install build requirements."
}

Push-Location $AppDevDir

Write-Host "Running syntax checks..."
& $VenvPython -m py_compile update_utils.py updater_app.py desktop_app.py run_product_prospector.pyw
if ($LASTEXITCODE -ne 0) {
  throw "Syntax checks failed."
}

if (Test-Path "build") { Remove-Item "build" -Recurse -Force }
if (Test-Path "dist") { Remove-Item "dist" -Recurse -Force }

Write-Host "Building updater helper..."
& $VenvPyInstaller `
  --noconfirm `
  --clean `
  ProductProspectorUpdater.spec
if ($LASTEXITCODE -ne 0) {
  throw "Updater helper build failed."
}

Write-Host "Building main application..."
& $VenvPyInstaller `
  --noconfirm `
  --clean `
  ProductProspector.spec
if ($LASTEXITCODE -ne 0) {
  throw "Main application build failed."
}

Pop-Location

$MainExe = Join-Path $AppDevDir "dist\ProductProspector.exe"
$UpdaterExe = Join-Path $AppDevDir "dist\ProductProspectorUpdater.exe"
$RuntimeUpdateDir = Join-Path $ProjectRoot "app\update"
$RuntimeUpdaterExe = Join-Path $RuntimeUpdateDir "ProductProspectorUpdater.exe"

if (-not (Test-Path $MainExe)) {
  throw "Main build did not produce $MainExe"
}
if (-not (Test-Path $UpdaterExe)) {
  throw "Updater build did not produce $UpdaterExe"
}

New-Item -ItemType Directory -Force -Path $RuntimeUpdateDir | Out-Null
Copy-Item -Force $MainExe (Join-Path $ProjectRoot "ProductProspector.exe")
Copy-Item -Force $UpdaterExe $RuntimeUpdaterExe

Write-Host "Build complete:"
Write-Host "  $MainExe"
Write-Host "Updater helper:"
Write-Host "  $RuntimeUpdaterExe"
Show-PyInstallerWarnSummary -WarnPath (Join-Path $AppDevDir "build\ProductProspector\warn-ProductProspector.txt")
