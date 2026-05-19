$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir "..\.."))
Set-Location $repoRoot

Write-Host "Building Product Prospector release artifacts..."
& .\app\dev\build_windows_exe.ps1

$versionPath = Join-Path $repoRoot "app\update\VERSION"
$version = (Get-Content $versionPath -Raw).Trim()
$mainExe = Join-Path $repoRoot "app\dev\dist\ProductProspector.exe"
$updaterExe = Join-Path $repoRoot "app\dev\dist\ProductProspectorUpdater.exe"
$releaseTemplatePath = Join-Path $repoRoot "app\update\release-template.json"

if (-not (Test-Path $mainExe)) {
    throw "Main build did not produce app\dev\dist\ProductProspector.exe"
}
if (-not (Test-Path $updaterExe)) {
    throw "Updater build did not produce app\dev\dist\ProductProspectorUpdater.exe"
}

$sha256 = (Get-FileHash -Algorithm SHA256 $mainExe).Hash.ToLowerInvariant()
$releaseFiles = @(
    [ordered]@{
        relative_path = "ProductProspector.exe"
        asset_name    = "ProductProspector.exe"
        sha256        = $sha256
    }
)

$releaseTemplate = [ordered]@{
    version      = $version
    download_url = ""
    sha256       = $sha256
    notes        = ""
    published_at = ""
    files        = $releaseFiles
}
$releaseTemplate | ConvertTo-Json -Depth 5 | Set-Content -Path $releaseTemplatePath -Encoding UTF8

Write-Host ""
Write-Host "Build complete."
Write-Host "Main app:       $mainExe"
Write-Host "Updater helper: $updaterExe"
Write-Host "Release JSON:   $releaseTemplatePath"
Write-Host "SHA-256:        $sha256"
