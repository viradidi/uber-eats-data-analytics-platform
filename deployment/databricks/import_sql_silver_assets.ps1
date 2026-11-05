[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^https://[A-Za-z0-9.-]+$")]
    [string]$WorkspaceHost,

    [Parameter(Mandatory = $false)]
    [string]$ProfileName
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$WorkspaceRoot = "/Shared/UberEats"

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI is required. Install the current Databricks CLI before running this script."
}
if ($ProfileName) {
    $env:DATABRICKS_CONFIG_PROFILE = $ProfileName
}
$env:DATABRICKS_HOST = $WorkspaceHost.TrimEnd("/")

function Invoke-DatabricksCli {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )
    & databricks @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Databricks CLI failed while $Description (exit code $LASTEXITCODE)."
    }
}

function Import-Notebook {
    param(
        [Parameter(Mandatory = $true)][string]$RelativeSource,
        [Parameter(Mandatory = $true)][string]$WorkspacePath
    )
    $source = Join-Path $repoRoot ($RelativeSource -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required SQL Silver notebook export was not found: '$RelativeSource'."
    }
    Invoke-DatabricksCli -Arguments @(
        "workspace", "import", $WorkspacePath,
        "--file", $source,
        "--format", "SOURCE",
        "--language", "PYTHON",
        "--overwrite"
    ) -Description "importing SQL Silver notebook '$RelativeSource'"
}

function Import-PythonWorkspaceFile {
    param(
        [Parameter(Mandatory = $true)][string]$RelativeSource,
        [Parameter(Mandatory = $true)][string]$WorkspacePath
    )
    $source = Join-Path $repoRoot ($RelativeSource -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required SQL Silver Python module was not found: '$RelativeSource'."
    }
    Invoke-DatabricksCli -Arguments @(
        "workspace", "import", $WorkspacePath,
        "--file", $source,
        "--format", "RAW",
        "--overwrite"
    ) -Description "importing SQL Silver module '$RelativeSource'"
}

# Confirm CLI authentication and access before replacing workspace files.
Invoke-DatabricksCli -Arguments @("workspace", "list", "/Shared", "--output", "json") -Description "checking workspace access"
Invoke-DatabricksCli -Arguments @("workspace", "mkdirs", "$WorkspaceRoot/notebooks") -Description "creating notebook directory"
Invoke-DatabricksCli -Arguments @("workspace", "mkdirs", "$WorkspaceRoot/src") -Description "creating src directory"
Invoke-DatabricksCli -Arguments @("workspace", "mkdirs", "$WorkspaceRoot/src/common") -Description "creating common module directory"

Import-Notebook -RelativeSource "notebooks/Silver_SQL_Incremental.py" -WorkspacePath "$WorkspaceRoot/notebooks/Silver_SQL_Incremental"

$pythonFiles = @(
    "src/__init__.py",
    "src/common/__init__.py",
    "src/common/sql_silver.py"
)
foreach ($relativePath in $pythonFiles) {
    $workspacePath = "$WorkspaceRoot/$($relativePath -replace '\\', '/')"
    Import-PythonWorkspaceFile -RelativeSource $relativePath -WorkspacePath $workspacePath
}

$expectedPaths = @(
    "$WorkspaceRoot/notebooks/Silver_SQL_Incremental",
    "$WorkspaceRoot/src/__init__.py",
    "$WorkspaceRoot/src/common/__init__.py",
    "$WorkspaceRoot/src/common/sql_silver.py"
)
foreach ($workspacePath in $expectedPaths) {
    Invoke-DatabricksCli -Arguments @("workspace", "get-status", $workspacePath, "--output", "json") -Description "verifying workspace path '$workspacePath'"
}

Write-Host ""
Write-Host "SQL Silver notebook and required shared modules imported and verified."
Write-Host "This updates only the SQL Silver notebook/module paths; it does not run a job or modify other notebooks."
