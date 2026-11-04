[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[a-z0-9]{3,24}$")]
    [string]$StorageAccountName,
    [Parameter(Mandatory = $false)]
    [ValidatePattern("^[a-z0-9-]{3,63}$")]
    [string]$FileSystemName = "bronze"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$files = @(
    @{
        Source = Join-Path $repoRoot "api_metadata_v2 (2).json"
        Destination = "metadata/api_metadata_v2 (2).json"
    },
    @{
        Source = Join-Path $repoRoot "csv_metadata_proper (2).json"
        Destination = "metadata/csv_metadata_proper (2).json"
    }
)

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required. Install it and sign in before running this script."
}

& az account show --only-show-errors --output none
if ($LASTEXITCODE -ne 0) {
    throw "Azure CLI is not signed in. Run 'az login' and select the intended subscription."
}

foreach ($file in $files) {
    if (-not (Test-Path -LiteralPath $file.Source -PathType Leaf)) {
        throw "Required repository metadata file was not found: $($file.Source)"
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

$directoryExists = & az storage fs directory exists --name "metadata" --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --query exists --output tsv --only-show-errors
if ($LASTEXITCODE -ne 0) {
    throw "Could not check the metadata directory in '$FileSystemName'."
}
if ($directoryExists -ne "true") {
    & az storage fs directory create --name "metadata" --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --only-show-errors --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the ADLS metadata directory."
    }
}

foreach ($file in $files) {
    Write-Host "Uploading $($file.Source) -> $FileSystemName/$($file.Destination)"
    & az storage fs file upload --source $file.Source --path $file.Destination --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --overwrite true --only-show-errors --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Upload failed for '$($file.Source)'."
    }

    $exists = & az storage fs file exists --path $file.Destination --file-system $FileSystemName --account-name $StorageAccountName --auth-mode login --query exists --output tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $exists -ne "true") {
        throw "Upload verification failed for '$FileSystemName/$($file.Destination)'."
    }
}

Write-Host "ADLS metadata staging completed and both files were verified."
