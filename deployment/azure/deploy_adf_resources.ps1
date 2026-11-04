[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-fA-F-]{36}$")]
    [string]$SubscriptionId,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9._()-]{1,90}$")]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9][A-Za-z0-9-]{1,61}[A-Za-z0-9]$")]
    [string]$FactoryName,

    [Parameter(Mandatory = $false)]
    [string]$ApiVersion = "2018-06-01"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path

foreach ($moduleName in @("Az.Accounts", "Az.Resources")) {
    if (-not (Get-Module -ListAvailable -Name $moduleName)) {
        throw "$moduleName is required. Install it with: Install-Module $moduleName -Scope CurrentUser"
    }
    Import-Module $moduleName -ErrorAction Stop
}

$context = Get-AzContext
if (-not $context) {
    throw "No Azure PowerShell session is active. Run Connect-AzAccount first."
}
if ($context.Subscription.Id -ne $SubscriptionId) {
    Set-AzContext -SubscriptionId $SubscriptionId -ErrorAction Stop | Out-Null
}
$context = Get-AzContext
if ($context.Subscription.Id -ne $SubscriptionId) {
    throw "Active Azure context does not match SubscriptionId '$SubscriptionId'."
}
if (-not (Get-Command Invoke-AzRestMethod -ErrorAction SilentlyContinue)) {
    throw "Invoke-AzRestMethod is unavailable. Update the Az.Accounts module."
}

$factoryDirectory = Join-Path $repoRoot "factory"
$factoryFiles = @(Get-ChildItem -LiteralPath $factoryDirectory -Filter "*.json" -File)
if ($factoryFiles.Count -ne 1) {
    throw "Expected exactly one factory JSON export under factory/. Found $($factoryFiles.Count). Run configure_restore_environment.ps1 first."
}

$factoryExport = Get-Content -LiteralPath $factoryFiles[0].FullName -Raw | ConvertFrom-Json -AsHashtable
if ($factoryExport.name -ne $FactoryName) {
    throw "Factory export name '$($factoryExport.name)' does not match requested FactoryName '$FactoryName'. Run configure_restore_environment.ps1 with the correct name."
}
if (-not $factoryExport.location -or -not $factoryExport.identity -or -not $factoryExport.properties) {
    throw "Factory export must include location, identity, and properties."
}

$resourceBase = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$FactoryName"
$factoryPath = "$resourceBase?api-version=$ApiVersion"

function Invoke-AdfPut {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)]$Payload
    )

    $resourcePath = "$resourceBase/$RelativePath?api-version=$ApiVersion"
    $body = ConvertTo-Json -InputObject $Payload -Depth 100 -Compress
    $response = Invoke-AzRestMethod -Path $resourcePath -Method PUT -Payload $body -ErrorAction Stop
    if ($response.StatusCode -notin @(200, 201, 202)) {
        throw "ADF deployment returned HTTP $($response.StatusCode) for $Description. Response: $($response.Content)"
    }
    Write-Host "PUT $Description -> HTTP $($response.StatusCode)"
}

function Invoke-AdfGet {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $resourcePath = "$resourceBase/$RelativePath?api-version=$ApiVersion"
    $response = Invoke-AzRestMethod -Path $resourcePath -Method GET -ErrorAction Stop
    if ($response.StatusCode -ne 200) {
        throw "ADF verification returned HTTP $($response.StatusCode) for $Description. Response: $($response.Content)"
    }
    Write-Host "Verified $Description"
}

function Get-ResourceExport {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    $fullPath = Join-Path $repoRoot ($RelativePath -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "Missing ADF export: $RelativePath"
    }
    return (Get-Content -LiteralPath $fullPath -Raw | ConvertFrom-Json -AsHashtable)
}

# This script deploys only Data Factory resources. The resource group, storage,
# SQL databases, Key Vault, API host, and Databricks workspace must exist first.
if (-not (Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue)) {
    throw "Resource group '$ResourceGroupName' does not exist in subscription '$SubscriptionId'. Create/approve it before deploying the Data Factory."
}

$factoryBody = [ordered]@{
    location = $factoryExport.location
    identity = $factoryExport.identity
    properties = $factoryExport.properties
}
if ($factoryExport.tags) {
    $factoryBody.tags = $factoryExport.tags
}

if ($PSCmdlet.ShouldProcess("Data Factory $FactoryName", "Create or update")) {
    $body = ConvertTo-Json -InputObject $factoryBody -Depth 100 -Compress
    $response = Invoke-AzRestMethod -Path $factoryPath -Method PUT -Payload $body -WaitForCompletion -ErrorAction Stop
    if ($response.StatusCode -notin @(200, 201, 202)) {
        throw "Data Factory deployment returned HTTP $($response.StatusCode). Response: $($response.Content)"
    }
    Write-Host "PUT Data Factory $FactoryName -> HTTP $($response.StatusCode)"
}

# Linked services are deployed before datasets and pipelines.
$linkedServiceFiles = @(Get-ChildItem -LiteralPath (Join-Path $repoRoot "linkedService") -Filter "*.json" -File | Sort-Object Name)
foreach ($file in $linkedServiceFiles) {
    $relative = "linkedService/$($file.Name)"
    $resource = Get-ResourceExport -RelativePath $relative
    if (-not $resource.name -or -not $resource.properties) {
        throw "Invalid linked-service export: $relative"
    }
    $payload = @{ name = $resource.name; properties = $resource.properties }
    $description = "linked service $($resource.name)"
    if ($PSCmdlet.ShouldProcess($description, "Create or update Azure Data Factory resource")) {
        Invoke-AdfPut -RelativePath "linkedservices/$([uri]::EscapeDataString($resource.name))" -Description $description -Payload $payload
        if (-not $WhatIfPreference) {
            Invoke-AdfGet -RelativePath "linkedservices/$([uri]::EscapeDataString($resource.name))" -Description "linked service $($resource.name)"
        }
    }
}

$datasetFiles = @(Get-ChildItem -LiteralPath (Join-Path $repoRoot "dataset") -Filter "*.json" -File | Sort-Object Name)
foreach ($file in $datasetFiles) {
    $relative = "dataset/$($file.Name)"
    $resource = Get-ResourceExport -RelativePath $relative
    if (-not $resource.name -or -not $resource.properties) {
        throw "Invalid dataset export: $relative"
    }
    $payload = @{ name = $resource.name; properties = $resource.properties }
    $description = "dataset $($resource.name)"
    if ($PSCmdlet.ShouldProcess($description, "Create or update Azure Data Factory resource")) {
        Invoke-AdfPut -RelativePath "datasets/$([uri]::EscapeDataString($resource.name))" -Description $description -Payload $payload
        if (-not $WhatIfPreference) {
            Invoke-AdfGet -RelativePath "datasets/$([uri]::EscapeDataString($resource.name))" -Description "dataset $($resource.name)"
        }
    }
}

# Deploy the master pipeline last, after its child ExecutePipeline targets.
$pipelineOrder = @(
    "api_src_inital load.json",
    "csv_initial load.json",
    "sql_incremental pipeline.json",
    "uber_eats_master.json"
)
foreach ($fileName in $pipelineOrder) {
    $relative = "pipeline/$fileName"
    $resource = Get-ResourceExport -RelativePath $relative
    if (-not $resource.name -or -not $resource.properties) {
        throw "Invalid pipeline export: $relative"
    }
    if ($resource.properties.Contains("lastPublishTime")) {
        $resource.properties.Remove("lastPublishTime")
    }
    $payload = @{ name = $resource.name; properties = $resource.properties }
    $description = "pipeline $($resource.name)"
    if ($PSCmdlet.ShouldProcess($description, "Create or update Azure Data Factory resource")) {
        Invoke-AdfPut -RelativePath "pipelines/$([uri]::EscapeDataString($resource.name))" -Description $description -Payload $payload
        if (-not $WhatIfPreference) {
            Invoke-AdfGet -RelativePath "pipelines/$([uri]::EscapeDataString($resource.name))" -Description "pipeline $($resource.name)"
        }
    }
}

# Every deployed factory child resource is read back immediately after its PUT.
Write-Host ""
Write-Host "ADF resource deployment and read-back verification completed."
Write-Host "The master pipeline was NOT started. Run it only after SQL source contracts, the external SQL Silver Job review, Key Vault secret, and permissions have passed the acceptance gates."
