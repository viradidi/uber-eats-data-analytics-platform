[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[a-z0-9]{3,24}$")]
    [string]$StorageAccountName,

    [Parameter(Mandatory = $false)]
    [ValidatePattern("^[a-z0-9-]{3,63}$")]
    [string]$FileSystemName = "bronze",

    [Parameter(Mandatory = $false)]
    [ValidatePattern("^[a-z0-9_/-]{1,63}$")]
    [string]$FolderPath = "csv_source"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$sourceFiles = @("drivers.csv", "promotions.csv", "zones.csv")
$tempDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("uber-eats-csv-verify-" + [guid]::NewGuid().ToString("N"))

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required. Install it and sign in before running this script."
}

& az account show --only-show-errors --output none
if ($LASTEXITCODE -ne 0) {
    throw "Azure CLI is not signed in. Run 'az login' and select the intended subscription."
}

foreach ($fileName in $sourceFiles) {
    $source = Join-Path $repoRoot $fileName
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required committed CSV file was not found: '$source'."
    }
}

$filesystemExists = & az storage fs exists --name $FileSystemName --account-name $StorageAccountName --auth-mode login --query exists --output tsv --only-show-errors
if ($LASTEXITCODE -ne 0) {
    throw "Could not check ADLS filesystem '$FileSystemName' on '$StorageAccountName'."
}
if ($filesystemExists -ne "true") {
    & az storage fs create --name $FileSystemName --account-name $StorageAccountName --auth-mode login --only-show-errors --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create ADLS filesystem '$FileSystemName'."
    }
}

$directoryExists = & az storage fs directory exists --name $FolderPath --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --query exists --output tsv --only-show-errors
if ($LASTEXITCODE -ne 0) {
    throw "Could not check ADLS source directory '$FolderPath'."
}
if ($directoryExists -ne "true") {
    & az storage fs directory create --name $FolderPath --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --only-show-errors --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create ADLS source directory '$FolderPath'."
    }
}

New-Item -ItemType Directory -Path $tempDirectory -Force | Out-Null
try {
    foreach ($fileName in $sourceFiles) {
        $source = Join-Path $repoRoot $fileName
        $destinationPath = "$FolderPath/$fileName"
        Write-Host "Uploading $fileName -> $FileSystemName/$destinationPath"

        & az storage fs file upload --source $source --path $destinationPath --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --overwrite true --only-show-errors --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Upload failed for '$fileName'."
        }

        $remotePath = Join-Path $tempDirectory $fileName
        & az storage fs file download --path $destinationPath --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --destination $remotePath --only-show-errors --output none
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $remotePath -PathType Leaf)) {
            throw "Download verification failed for '$FileSystemName/$destinationPath'."
        }

        $sourceHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
        $remoteHash = (Get-FileHash -LiteralPath $remotePath -Algorithm SHA256).Hash
        if ($sourceHash -ne $remoteHash) {
            throw "SHA-256 verification failed after upload for '$fileName'."
        }

        Write-Host "Staged and SHA-256 verified $fileName"
    }
}
finally {
    if (Test-Path -LiteralPath $tempDirectory) {
        Remove-Item -LiteralPath $tempDirectory -Recurse -Force
    }
}

Write-Host "ADLS CSV staging completed. The ADF CSV source dataset reads from '$FileSystemName/$FolderPath'."
