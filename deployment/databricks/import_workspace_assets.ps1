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
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("uber-eats-databricks-" + [guid]::NewGuid().ToString("N"))
$notebookStage = Join-Path $tempRoot "notebooks"
$routingStage = Join-Path $tempRoot "routing"

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI is required. Install the current Databricks CLI before running this script."
}

if ($ProfileName) {
    $env:DATABRICKS_CONFIG_PROFILE = $ProfileName
}

$env:DATABRICKS_HOST = $WorkspaceHost

New-Item -ItemType Directory -Path $notebookStage -Force | Out-Null
New-Item -ItemType Directory -Path $routingStage -Force | Out-Null

# Import one canonical source export per notebook name. The paired .py/.ipynb
# exports share names, so importing both into one workspace folder would cause
# one to overwrite the other nondeterministically.
$notebookFiles = @(
    "Silver_Deliveries.py",
    "Silver_Drivers.py",
    "Silver_Order_Status.py",
    "Silver_Payments.py",
    "Silver_Promotions.py",
    "Silver_Ratings.py",
    "Silver_Zones.ipynb",
    "Gold_Analytics.py"
)

foreach ($fileName in $notebookFiles) {
    $source = Join-Path (Join-Path $repoRoot "notebooks") $fileName
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required Databricks notebook export was not found: '$source'."
    }
    Copy-Item -LiteralPath $source -Destination (Join-Path $notebookStage $fileName) -Force
}

$routingFiles = @(
    "api-dispacher-notebook.py",
    "dispacher_csv_notebook.py"
)
foreach ($fileName in $routingFiles) {
    $source = Join-Path (Join-Path $repoRoot "routing") $fileName
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required Databricks dispatcher export was not found: '$source'."
    }
    Copy-Item -LiteralPath $source -Destination (Join-Path $routingStage $fileName) -Force
}

# Confirm the active CLI identity can reach the selected workspace.
& databricks workspace list "/Shared" --output json
if ($LASTEXITCODE -ne 0) {
    throw "Databricks CLI authentication or workspace access failed. Configure a profile for '$WorkspaceHost' and retry."
}

# Import notebooks and routing dispatchers as notebooks. import-dir strips the
# source extension, matching the ADF notebook paths.
& databricks workspace mkdirs "$WorkspaceRoot/notebooks"
if ($LASTEXITCODE -ne 0) { throw "Could not create workspace notebook directory." }
& databricks workspace mkdirs "$WorkspaceRoot/routing"
if ($LASTEXITCODE -ne 0) { throw "Could not create workspace routing directory." }

& databricks workspace import-dir $notebookStage "$WorkspaceRoot/notebooks" --overwrite
if ($LASTEXITCODE -ne 0) { throw "Databricks notebook import failed." }
& databricks workspace import-dir $routingStage "$WorkspaceRoot/routing" --overwrite
if ($LASTEXITCODE -ne 0) { throw "Databricks routing notebook import failed." }

# Import src/ as workspace files (RAW) so Python package paths and .py suffixes
# remain intact. Silver and Gold notebooks insert WorkspaceRoot into sys.path.
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
    $source = Join-Path $repoRoot ($relativePath -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required Python package source was not found: '$source'."
    }
    $target = "$WorkspaceRoot/$($relativePath -replace '\\', '/')"
    $targetDirectory = $target.Substring(0, $target.LastIndexOf("/"))
    & databricks workspace mkdirs $targetDirectory
    if ($LASTEXITCODE -ne 0) { throw "Could not create workspace package folder '$targetDirectory'." }
    & databricks workspace import $target --file $source --format RAW --overwrite
    if ($LASTEXITCODE -ne 0) { throw "Could not import workspace module '$relativePath'." }
}

# Verify ADF's direct notebook activity paths and Gold entrypoint.
$expectedPaths = @(
    "$WorkspaceRoot/notebooks/Silver_Payments",
    "$WorkspaceRoot/notebooks/Silver_Deliveries",
    "$WorkspaceRoot/notebooks/Silver_Order_Status",
    "$WorkspaceRoot/notebooks/Silver_Ratings",
    "$WorkspaceRoot/notebooks/Silver_Drivers",
    "$WorkspaceRoot/notebooks/Silver_Promotions",
    "$WorkspaceRoot/notebooks/Silver_Zones",
    "$WorkspaceRoot/notebooks/Gold_Analytics",
    "$WorkspaceRoot/routing/api-dispacher-notebook",
    "$WorkspaceRoot/routing/dispacher_csv_notebook"
)
foreach ($workspacePath in $expectedPaths) {
    & databricks workspace get-status $workspacePath --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Workspace import verification failed for '$workspacePath'."
    }
}

Remove-Item -LiteralPath $tempRoot -Recurse -Force
Write-Host "Databricks notebooks, dispatchers, and shared Python modules imported and verified."
Write-Host "Workspace root: $WorkspaceRoot"
Write-Host "Configure the SQL Silver Job separately; it is not source-controlled in this repository."
