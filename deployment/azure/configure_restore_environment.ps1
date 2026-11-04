[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[a-z0-9]{3,24}$")]
    [string]$StorageAccountName,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9.-]+$")]
    [string]$SourceSqlServerFqdn,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$SourceSqlDatabase,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9.-]+$")]
    [string]$ControlSqlServerFqdn,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ControlSqlDatabase,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^https://[A-Za-z0-9.-]+$")]
    [string]$DatabricksWorkspaceHost,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^/subscriptions/[0-9a-fA-F-]+/resourceGroups/[^/]+/providers/Microsoft.Databricks/workspaces/[^/]+$")]
    [string]$DatabricksWorkspaceResourceId,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^https://[A-Za-z0-9.-]+$")]
    [string]$KeyVaultUri,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^https://[A-Za-z0-9.-]+$")]
    [string]$ApiBaseUrl,

    [Parameter(Mandatory = $false)]
    [ValidatePattern("^[A-Za-z0-9][A-Za-z0-9-]{1,61}[A-Za-z0-9]$")]
    [string]$DataFactoryName = "ubereatssri",

    [Parameter(Mandatory = $false)]
    [ValidateNotNullOrEmpty()]
    [string]$AzureRegion = "eastus",

    [Parameter(Mandatory = $false)]
    [ValidateNotNullOrEmpty()]
    [string]$DatabricksNodeType = "Standard_DS3_v2",

    [Parameter(Mandatory = $false)]
    [ValidateRange(1, 100)]
    [int]$DatabricksWorkerCount = 1
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "PowerShell 7 or newer is required for ordered JSON editing."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$adlsUrl = "https://$StorageAccountName.dfs.core.windows.net/"
$bronzeRoot = "abfss://bronze@$StorageAccountName.dfs.core.windows.net"
$apiBase = $ApiBaseUrl.TrimEnd("/")
$keyVaultBase = $KeyVaultUri.TrimEnd("/") + "/"
$databricksHost = $DatabricksWorkspaceHost.TrimEnd("/")

function Get-RepoPath([string]$RelativePath) {
    return Join-Path $repoRoot ($RelativePath -replace "/", [System.IO.Path]::DirectorySeparatorChar)
}

function Read-RepoJson([string]$RelativePath) {
    $filePath = Get-RepoPath $RelativePath
    if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
        throw "Required configuration file was not found: '$RelativePath'."
    }
    return (Get-Content -LiteralPath $filePath -Raw | ConvertFrom-Json -AsHashtable)
}

function Write-RepoJson([string]$RelativePath, $Document) {
    $filePath = Get-RepoPath $RelativePath
    $json = ConvertTo-Json -InputObject $Document -Depth 100
    Set-Content -LiteralPath $filePath -Value ($json + [Environment]::NewLine) -Encoding utf8
    Write-Host "Configured $RelativePath"
}

function Set-SqlLinkedService([string]$RelativePath, [string]$Server, [string]$Database) {
    $document = Read-RepoJson $RelativePath
    $document.properties.typeProperties.server = $Server
    $document.properties.typeProperties.database = $Database
    Write-RepoJson $RelativePath $document
}

function Set-DatabricksLinkedService([string]$RelativePath) {
    $document = Read-RepoJson $RelativePath
    $properties = $document.properties.typeProperties
    $properties.domain = $databricksHost
    $properties.workspaceResourceId = $DatabricksWorkspaceResourceId
    $properties.newClusterNodeType = $DatabricksNodeType
    $properties.newClusterNumOfWorker = [string]$DatabricksWorkerCount
    # Keep the pinned, supported 16.4 LTS / Photon / Scala 2.12 runtime.
    $properties.newClusterVersion = "16.4.x-photon-scala2.12"
    Write-RepoJson $RelativePath $document
}

function Set-TextFile([string]$RelativePath, [string]$OldValue, [string]$NewValue) {
    $filePath = Get-RepoPath $RelativePath
    if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
        throw "Required file was not found: '$RelativePath'."
    }
    $original = Get-Content -LiteralPath $filePath -Raw
    $updated = $original.Replace($OldValue, $NewValue)
    if ($updated -ne $original) {
        Set-Content -LiteralPath $filePath -Value $updated -Encoding utf8
        Write-Host "Updated environment value in $RelativePath"
    }
}

# ADLS endpoint and the factory-level parameter used by every Silver notebook.
$adls = Read-RepoJson "linkedService/AzureDataLakeStorage1.json"
$adls.properties.typeProperties.url = $adlsUrl
Write-RepoJson "linkedService/AzureDataLakeStorage1.json" $adls

$factoryFiles = @(Get-ChildItem -LiteralPath (Get-RepoPath "factory") -Filter "*.json" -File)
if ($factoryFiles.Count -ne 1) {
    throw "Expected exactly one Data Factory export under factory/. Found $($factoryFiles.Count)."
}
$factoryRelativePath = "factory/$($factoryFiles[0].Name)"
$factory = Read-RepoJson $factoryRelativePath
$factory.name = $DataFactoryName
$factory.location = $AzureRegion
if (-not $factory.properties.globalParameters) {
    $factory.properties.globalParameters = @{}
}
$factory.properties.globalParameters.bronze_root = @{
    type = "string"
    value = $bronzeRoot
}
$factory.properties.globalParameters.api_base_url = @{
    type = "string"
    value = $apiBase
}
$factory.properties.globalParameters.key_vault_uri = @{
    type = "string"
    value = $keyVaultBase.TrimEnd("/")
}
$factory.identity = @{ type = "SystemAssigned" }
Write-RepoJson $factoryRelativePath $factory
$targetFactoryRelativePath = "factory/$DataFactoryName.json"
if ($factoryRelativePath -ne $targetFactoryRelativePath) {
    $targetFactoryPath = Get-RepoPath $targetFactoryRelativePath
    if (Test-Path -LiteralPath $targetFactoryPath) {
        throw "A factory export already exists at '$targetFactoryRelativePath'. Resolve the duplicate before continuing."
    }
    Move-Item -LiteralPath (Get-RepoPath $factoryRelativePath) -Destination $targetFactoryPath
    Write-Host "Renamed factory export to $targetFactoryRelativePath"
}

# Source and control SQL endpoints may be the same server/database or separate.
Set-SqlLinkedService "linkedService/ls_sql_database.json" $SourceSqlServerFqdn $SourceSqlDatabase
Set-SqlLinkedService "linkedService/ls_sql_control.json" $ControlSqlServerFqdn $ControlSqlDatabase

# All direct ADF notebook activities use these linked services.
Set-DatabricksLinkedService "linkedService/LS_db_silver.json"
Set-DatabricksLinkedService "linkedService/ls_api_adb.json"
Set-DatabricksLinkedService "linkedService/ls_silver_csv.json"

# API source and Key Vault references.
$rest = Read-RepoJson "linkedService/LS_restapi.json"
$rest.properties.typeProperties.url = $apiBase
$secretReference = $rest.properties.typeProperties.authHeaders["X-API-Key"]
if ($secretReference -and $secretReference.ContainsKey("secretVersion")) {
    $secretReference.Remove("secretVersion")
}
Write-RepoJson "linkedService/LS_restapi.json" $rest

$keyVault = Read-RepoJson "linkedService/LS_AzureKeyVault.json"
$keyVault.properties.typeProperties.baseUrl = $keyVaultBase
Write-RepoJson "linkedService/LS_AzureKeyVault.json" $keyVault

# Keep the API watermark endpoint aligned with the REST linked service, use the
# current Key Vault secret version, and keep the secret out of ADF activity logs.
$apiPipeline = Read-RepoJson "pipeline/api_src_inital load.json"
$apiLoop = $apiPipeline.properties.activities |
    Where-Object { $_.type -eq "ForEach" } |
    Select-Object -First 1

if (-not $apiLoop) {
    throw "Could not find the API endpoint ForEach activity."
}

$watermarkActivity = $apiLoop.typeProperties.activities |
    Where-Object { $_.name -eq "Web_Get_Api_Watermarks" } |
    Select-Object -First 1
$keyActivity = $apiLoop.typeProperties.activities |
    Where-Object { $_.name -eq "get_keyvalut_api _key" } |
    Select-Object -First 1

if (-not $watermarkActivity -or -not $keyActivity) {
    throw "Could not find the API watermark or Key Vault Web activity."
}

$watermarkActivity.typeProperties.url = @{
    value = "@concat(pipeline().globalParameters.api_base_url, '/watermarks')"
    type = "Expression"
}
$keyActivity.typeProperties.url = @{
    value = "@concat(pipeline().globalParameters.key_vault_uri, '/secrets/ubereatsapikey?api-version=7.5')"
    type = "Expression"
}
$keyActivity.policy.secureInput = $true
$keyActivity.policy.secureOutput = $true
$watermarkActivity.policy.secureInput = $true
Write-RepoJson "pipeline/api_src_inital load.json" $apiPipeline

# Update Silver notebook and dispatcher widget defaults as a convenience for
# manual execution; ADF still passes the factory-level bronze_root explicitly.
$oldBronzeRoot = "abfss://bronze@ubereatsrisstorage.dfs.core.windows.net"
$notebookDirectory = Get-RepoPath "notebooks"
Get-ChildItem -LiteralPath $notebookDirectory -File |
    Where-Object { $_.Name -like "Silver_*" -and $_.Extension -in @(".py", ".ipynb") } |
    ForEach-Object {
        $original = Get-Content -LiteralPath $_.FullName -Raw
        $updated = $original.Replace($oldBronzeRoot, $bronzeRoot)
        if ($updated -ne $original) {
            Set-Content -LiteralPath $_.FullName -Value $updated -Encoding utf8
            Write-Host "Updated Bronze root default in notebooks/$($_.Name)"
        }
    }

foreach ($relativePath in @(
    "routing/api-dispacher-notebook.py",
    "routing/api-dispacher-notebook.ipynb",
    "routing/dispacher_csv_notebook.py"
)) {
    Set-TextFile $relativePath $oldBronzeRoot $bronzeRoot
}

Write-Host ""
Write-Host "Restore configuration applied to the local repository working tree."
Write-Host "ADLS: $adlsUrl"
Write-Host "Source SQL: $SourceSqlServerFqdn / $SourceSqlDatabase"
Write-Host "Control SQL: $ControlSqlServerFqdn / $ControlSqlDatabase"
Write-Host "Databricks: $databricksHost"
Write-Host "Data Factory: $DataFactoryName ($AzureRegion)"
Write-Host "API: $apiBase"
Write-Host "Key Vault: $keyVaultBase"
Write-Host ""
Write-Warning "Review 'git diff' before publishing ADF artifacts. No secret values were requested or written."
Write-Warning "Create/update the 'ubereatsapikey' secret value in Key Vault and grant the new Data Factory managed identity access separately."
Write-Warning "This script changes local resource JSON and notebook default values; commit only the environment configuration you intend to publish."
