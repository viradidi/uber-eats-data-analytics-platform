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
        [Parameter(Mandatory = $true)][string]$WorkspacePath,
        [Parameter(Mandatory = $true)][ValidateSet("SOURCE", "JUPYTER")][string]$Format
    )

    $source = Join-Path $repoRoot ($RelativeSource -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required notebook export was not found: '$RelativeSource'."
    }

    Invoke-DatabricksCli -Arguments @(
        "workspace", "import", $WorkspacePath,
        "--file", $source,
        "--format", $Format,
        "--language", "PYTHON",
        "--overwrite"
    ) -Description "importing notebook '$RelativeSource' to '$WorkspacePath'"

    Write-Host "Imported notebook $RelativeSource -> $WorkspacePath"
}

function Import-PythonWorkspaceFile {
    param(
        [Parameter(Mandatory = $true)][string]$RelativeSource,
        [Parameter(Mandatory = $true)][string]$WorkspacePath
    )

    $source = Join-Path $repoRoot ($RelativeSource -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required Python module was not found: '$RelativeSource'."
    }

    Invoke-DatabricksCli -Arguments @(
        "workspace", "import", $WorkspacePath,
        "--file", $source,
        "--format", "RAW",
        "--overwrite"
    ) -Description "importing workspace file '$RelativeSource' to '$WorkspacePath'"

    Write-Host "Imported workspace file $RelativeSource -> $WorkspacePath"
}

# Confirm identity and workspace access before modifying anything.
Invoke-DatabricksCli -Arguments @("workspace", "list", "/Shared", "--output", "json") -Description "checking workspace access"

$directories = @(
    "$WorkspaceRoot/notebooks",
    "$WorkspaceRoot/routing",
    "$WorkspaceRoot/src",
    "$WorkspaceRoot/src/common",
    "$WorkspaceRoot/src/gold"
)
foreach ($directory in $directories) {
    Invoke-DatabricksCli -Arguments @("workspace", "mkdirs", $directory) -Description "creating workspace directory '$directory'"
}

# Import exactly one canonical notebook source for each workspace notebook path.
# The paired .py/.ipynb exports are alternate serializations; importing both
# would make the final result depend on import order.
$notebooks = @(
    @{ Source = "notebooks/Silver_Deliveries.py"; Name = "Silver_Deliveries"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Drivers.py"; Name = "Silver_Drivers"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Order_Status.py"; Name = "Silver_Order_Status"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Payments.py"; Name = "Silver_Payments"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Promotions.py"; Name = "Silver_Promotions"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Ratings.py"; Name = "Silver_Ratings"; Format = "SOURCE" },
    @{ Source = "notebooks/Silver_Zones.ipynb"; Name = "Silver_Zones"; Format = "JUPYTER" },
    @{ Source = "notebooks/Gold_Analytics.py"; Name = "Gold_Analytics"; Format = "SOURCE" },
    @{ Source = "routing/api-dispacher-notebook.py"; Name = "api-dispacher-notebook"; Format = "SOURCE" },
    @{ Source = "routing/dispacher_csv_notebook.py"; Name = "dispacher_csv_notebook"; Format = "SOURCE" }
)

foreach ($notebook in $notebooks) {
    $target = "$WorkspaceRoot/"
    if ($notebook.Source.StartsWith("routing/")) {
        $target += "routing/"
    }
    else {
        $target += "notebooks/"
    }
    $target += $notebook.Name
    Import-Notebook -RelativeSource $notebook.Source -WorkspacePath $target -Format $notebook.Format
}

# Import shared Python code as RAW workspace files so .py extensions and package
# paths remain intact for imports from /Workspace/Shared/UberEats.
$pythonFiles = @(
    "src/__init__.py",
    "src/common/__init__.py",
    "src/common/incremental.py",
    "src/common/quality_events.py",
    "src/common/spark_utils.py",
    "src/gold/__init__.py",
    "src/gold/transformations.py"
)
foreach ($relativePath in $pythonFiles) {
    $target = "$WorkspaceRoot/$($relativePath -replace '\\', '/')"
    Import-PythonWorkspaceFile -RelativeSource $relativePath -WorkspacePath $target
}

# Read every deployed notebook and module path back from the workspace.
$expectedPaths = @(
    "$WorkspaceRoot/notebooks/Silver_Deliveries",
    "$WorkspaceRoot/notebooks/Silver_Drivers",
    "$WorkspaceRoot/notebooks/Silver_Order_Status",
    "$WorkspaceRoot/notebooks/Silver_Payments",
    "$WorkspaceRoot/notebooks/Silver_Promotions",
    "$WorkspaceRoot/notebooks/Silver_Ratings",
    "$WorkspaceRoot/notebooks/Silver_Zones",
    "$WorkspaceRoot/notebooks/Gold_Analytics",
    "$WorkspaceRoot/routing/api-dispacher-notebook",
    "$WorkspaceRoot/routing/dispacher_csv_notebook",
    "$WorkspaceRoot/src/common/incremental.py",
    "$WorkspaceRoot/src/common/quality_events.py",
    "$WorkspaceRoot/src/gold/transformations.py"
)
foreach ($workspacePath in $expectedPaths) {
    Invoke-DatabricksCli -Arguments @("workspace", "get-status", $workspacePath, "--output", "json") -Description "verifying workspace path '$workspacePath'"
}

Write-Host ""
Write-Host "Databricks notebooks, dispatchers, and shared Python modules imported and verified."
Write-Host "Workspace root: $WorkspaceRoot"
Write-Host "Configure the SQL Silver Job separately; it is not source-controlled in this repository."
