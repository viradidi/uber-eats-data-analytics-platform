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
if (-not (Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue)) {
    throw "Resource group '$ResourceGroupName' does not exist in subscription '$SubscriptionId'."
}

$resourceBase = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$FactoryName"

function Read-PipelineExport {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    $path = Join-Path $repoRoot ($RelativePath -replace "/", [System.IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Missing pipeline export: '$RelativePath'."
    }
    return (Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable)
}

function Find-Activity {
    param(
        [Parameter(Mandatory = $false)]$Activities,
        [Parameter(Mandatory = $true)][string]$Name
    )

    foreach ($activity in @($Activities)) {
        if ($null -eq $activity) { continue }
        if ($activity.name -eq $Name) { return $activity }

        $properties = $activity.typeProperties
        foreach ($childName in @("activities", "ifTrueActivities", "ifFalseActivities")) {
            $found = Find-Activity -Activities $properties[$childName] -Name $Name
            if ($null -ne $found) { return $found }
        }
    }
    return $null
}

$master = Read-PipelineExport "pipeline/uber_eats_master.json"
$sqlPipeline = Read-PipelineExport "pipeline/sql_incremental pipeline.json"

if ($master.name -ne "uber_eats_master" -or $sqlPipeline.name -ne "sql_incremental pipeline") {
    throw "Pipeline export names do not match the expected run-correlation patch."
}

$masterSqlActivity = Find-Activity -Activities $master.properties.activities -Name "Run_SQL_Ingestion"
if ($null -eq $masterSqlActivity -or
    $masterSqlActivity.typeProperties.parameters.parent_pipeline_run_id.value -ne "@pipeline().RunId") {
    throw "Master pipeline does not pass @pipeline().RunId to the SQL child pipeline."
}

if ($sqlPipeline.properties.parameters.parent_pipeline_run_id.type -ne "String") {
    throw "SQL child pipeline is missing the parent_pipeline_run_id string parameter."
}

$sqlSilverActivity = Find-Activity -Activities $sqlPipeline.properties.activities -Name "Job_Silver_copy1"
if ($null -eq $sqlSilverActivity -or
    $sqlSilverActivity.type -ne "DatabricksNotebook" -or
    $sqlSilverActivity.typeProperties.notebookPath -ne "/Shared/UberEats/notebooks/Silver_SQL_Incremental") {
    throw "SQL pipeline is not wired to the repository-owned SQL Silver notebook."
}

$baseParameters = $sqlSilverActivity.typeProperties.baseParameters
if ($baseParameters.parent_pipeline_run_id.value -ne "@pipeline().parameters.parent_pipeline_run_id" -or
    $baseParameters.child_pipeline_run_id.value -ne "@pipeline().RunId") {
    throw "SQL Silver notebook run-correlation parameters are missing or incorrect."
}

$exports = @($master, $sqlPipeline)
foreach ($export in $exports) {
    $pipelineName = [string]$export.name
    $relativePath = "pipelines/$([uri]::EscapeDataString($pipelineName))"
    $resourcePath = "$resourceBase/$relativePath?api-version=$ApiVersion"
    $payload = @{ name = $pipelineName; properties = $export.properties }
    $body = ConvertTo-Json -InputObject $payload -Depth 100 -Compress

    if ($PSCmdlet.ShouldProcess("ADF pipeline $pipelineName", "Create or update run-correlation configuration")) {
        $response = Invoke-AzRestMethod -Path $resourcePath -Method PUT -Payload $body -WaitForCompletion -ErrorAction Stop
        if ($response.StatusCode -notin @(200, 201, 202)) {
            throw "ADF update failed for '$pipelineName'. HTTP $($response.StatusCode): $($response.Content)"
        }
        Write-Host "Updated ADF pipeline '$pipelineName' -> HTTP $($response.StatusCode)"

        if (-not $WhatIfPreference) {
            $readback = Invoke-AzRestMethod -Path $resourcePath -Method GET -ErrorAction Stop
            if ($readback.StatusCode -ne 200) {
                throw "ADF read-back failed for '$pipelineName'. HTTP $($readback.StatusCode)."
            }
            $deployed = $readback.Content | ConvertFrom-Json -AsHashtable
            if ($deployed.name -ne $pipelineName) {
                throw "ADF read-back returned an unexpected pipeline for '$pipelineName'."
            }
            Write-Host "Verified deployed pipeline '$pipelineName'."
        }
    }
}

Write-Host ""
Write-Host "Run-correlation patch processed for exactly two ADF pipelines."
Write-Host "No factory, linked service, dataset, storage, SQL, or Databricks resource was modified by this script."
