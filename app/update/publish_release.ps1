param(
    [string]$Version = "",
    [string]$Notes = "",
    [string]$Remote = "origin",
    [string]$Branch = "master",
    [switch]$NoPush
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir "..\.."))
Set-Location $repoRoot

$currentBranch = (git branch --show-current).Trim()
if ($LASTEXITCODE -ne 0) {
    throw "Failed to determine the current git branch."
}
if ($currentBranch -ne $Branch) {
    throw "Current branch is '$currentBranch'. Checkout '$Branch' before requesting a release."
}

$statusLines = @(git status --porcelain)
if ($LASTEXITCODE -ne 0) {
    throw "Failed to inspect git status."
}
if ($statusLines.Count -gt 0) {
    throw "Working tree must be clean before requesting a release. Push normal code changes first."
}

$versionPath = Join-Path $repoRoot "app\update\VERSION"
$releaseVersion = (Get-Content $versionPath -Raw).Trim()
if (-not [string]::IsNullOrWhiteSpace($Version)) {
    $releaseVersion = $Version.Trim()
    Set-Content -Path $versionPath -Value $releaseVersion -Encoding ascii
}
if ([string]::IsNullOrWhiteSpace($releaseVersion)) {
    throw "VERSION is empty."
}

$requestPath = Join-Path $repoRoot "app\update\release_request.json"
$requestPayload = [ordered]@{
    notes        = $Notes.Trim()
    requested_at = (Get-Date).ToUniversalTime().ToString("o")
}
$requestPayload | ConvertTo-Json | Set-Content -Path $requestPath -Encoding UTF8

git add app/update/VERSION app/update/release_request.json
if ($LASTEXITCODE -ne 0) {
    throw "Failed to stage the release request."
}

git commit -m "Request release v$releaseVersion"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create the release request commit."
}

if ($NoPush) {
    Write-Host "Release request commit created locally."
    exit 0
}

Write-Host "Pushing release request to $Remote/$Branch..."
git push $Remote $Branch
if ($LASTEXITCODE -ne 0) {
    throw "Failed to push the release request."
}

Write-Host "Release requested for v$releaseVersion."
Write-Host "GitHub Actions will build Product Prospector, create the release, and update app\update\release.json."
