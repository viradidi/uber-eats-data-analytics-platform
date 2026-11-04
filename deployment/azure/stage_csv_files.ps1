[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$DestinationPath
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$sourceFiles = @("drivers.csv", "promotions.csv", "zones.csv")

if (-not (Test-Path -LiteralPath $DestinationPath -PathType Container)) {
    throw "CSV destination is not reachable or does not exist: '$DestinationPath'. Create and authorize the SMB share first."
}

foreach ($fileName in $sourceFiles) {
    $source = Join-Path $repoRoot $fileName
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required committed CSV file was not found: '$source'."
    }

    $destination = Join-Path $DestinationPath $fileName
    Copy-Item -LiteralPath $source -Destination $destination -Force

    $sourceHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
    $destinationHash = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash
    if ($sourceHash -ne $destinationHash) {
        throw "SHA-256 verification failed after copying '$fileName'."
    }

    Write-Host "Staged and verified $fileName"
}

Write-Host "CSV share staging completed. Confirm the ADF File Server linked service uses this exact share."
