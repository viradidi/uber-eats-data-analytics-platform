[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^https://[A-Za-z0-9.-]+$")]
    [string]$WorkspaceHost,

    [Parameter(Mandatory = $false)]
    [string]$ProfileName,

    [Parameter(Mandatory = $false)]
    [long]$JobId = 121055824417886,

    [Parameter(Mandatory = $false)]
    [string]$OutputDirectory = (Join-Path $HOME "UberEats-SQL-Silver-Job-Export")
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI is required. Install the current Databricks CLI before exporting the SQL Silver Job."
}
if ($ProfileName) {
    $env:DATABRICKS_CONFIG_PROFILE = $ProfileName
}
$env:DATABRICKS_HOST = $WorkspaceHost.TrimEnd("/")

function Invoke-DatabricksJson {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $raw = & databricks @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Databricks CLI failed while $Description (exit code $LASTEXITCODE)."
    }
    $jsonText = $raw -join [Environment]::NewLine
    try {
        return ($jsonText | ConvertFrom-Json -AsHashtable -ErrorAction Stop)
    }
    catch {
        throw "Could not parse Databricks CLI JSON while $Description. CLI output was not saved."
    }
}

function Protect-JobConfiguration {
    param([Parameter(Mandatory = $false)]$Value)

    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $protected = [ordered]@{}
        foreach ($key in $Value.Keys) {
            if ([string]$key -match "(?i)(password|(^|[._-])token$|access[._-]?token|refresh[._-]?token|api[._-]?key|client[._-]?secret|secret_value|(^|[._-])secret$|private[._-]?key|authorization|connection[._-]?string|credential)") {
                $protected[$key] = "<REDACTED>"
            }
            else {
                $protected[$key] = Protect-JobConfiguration -Value $Value[$key]
            }
        }
        return $protected
    }
    if ($Value -is [System.Collections.IList]) {
        $protectedItems = @()
        foreach ($item in $Value) {
            $protectedItems += ,(Protect-JobConfiguration -Value $item)
        }
        return ,$protectedItems
    }
    if ($Value -is [string]) {
        return ($Value -replace "(?i)(https?://)[^/@:]+:[^/@]+@", "$1<REDACTED>@")
    }
    return $Value
}

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

function Export-WorkspaceSource {
    param(
        [Parameter(Mandatory = $true)][string]$WorkspacePath,
        [Parameter(Mandatory = $true)][string]$TaskKey
    )

    if (-not $WorkspacePath.StartsWith("/")) {
        return [ordered]@{ task_key = $TaskKey; workspace_path = $WorkspacePath; status = "not a workspace absolute path; inspect Git/DBFS/volume reference" }
    }

    $safeTask = $TaskKey -replace "[^A-Za-z0-9._-]", "_"
    $relativePath = $WorkspacePath.TrimStart("/") -replace "[/\\]", "__"
    $status = Invoke-DatabricksJson -Arguments @("workspace", "get-status", $WorkspacePath, "--output", "json") -Description "reading metadata for workspace source '$WorkspacePath'"
    $objectType = [string]$status.object_type
    $language = [string]$status.language

    $format = "RAW"
    $extension = [System.IO.Path]::GetExtension($WorkspacePath)
    if ($objectType -eq "NOTEBOOK") {
        $format = "SOURCE"
        switch ($language.ToUpperInvariant()) {
            "PYTHON" { $extension = ".py" }
            "SQL" { $extension = ".sql" }
            "SCALA" { $extension = ".scala" }
            "R" { $extension = ".r" }
            default { $extension = ".source" }
        }
    }
    elseif ([string]::IsNullOrEmpty($extension)) {
        $extension = ".raw"
    }

    $fileName = ($safeTask + "__" + $relativePath + $extension)
    $destination = Join-Path $sourceDirectory $fileName
    Invoke-DatabricksCli -Arguments @(
        "workspace", "export", $WorkspacePath,
        "--file", $destination,
        "--format", $format
    ) -Description "exporting workspace source '$WorkspacePath'"

    return [ordered]@{
        task_key = $TaskKey
        workspace_path = $WorkspacePath
        object_type = $objectType
        language = $language
        format = $format
        local_file = $destination
        status = "exported"
    }
}

New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$sourceDirectory = Join-Path $OutputDirectory "sources"
New-Item -ItemType Directory -Path $sourceDirectory -Force | Out-Null

# The CLI returns all job properties and may paginate very large task arrays.
$pages = @()
$pageToken = $null
do {
    $arguments = @("jobs", "get", [string]$JobId, "--output", "json")
    if ($pageToken) {
        $arguments += @("--page-token", [string]$pageToken)
    }
    $page = Invoke-DatabricksJson -Arguments $arguments -Description "retrieving job $JobId"
    $pages += ,$page
    $pageToken = $page.next_page_token
} while ($pageToken)

$firstPage = $pages[0]
$jobSettings = $firstPage.settings
if (-not $jobSettings) {
    throw "Databricks returned no settings for job $JobId."
}

$allTasks = @()
foreach ($page in $pages) {
    if ($page.settings.tasks) {
        $allTasks += $page.settings.tasks
    }
}
$taskSourceResults = @()
$nonWorkspaceReferences = @()
$childJobReferences = @()
$gitSource = $jobSettings.git_source

foreach ($task in $allTasks) {
    $taskKey = [string]$task.task_key
    if ($task.notebook_task.notebook_path) {
        if ($task.notebook_task.source -eq "GIT" -or $gitSource) {
            $nonWorkspaceReferences += [ordered]@{
                task_key = $taskKey
                source = "GIT"
                path = $task.notebook_task.notebook_path
                git_source = $gitSource
                status = "export the exact repository commit recorded by the job settings"
            }
        }
        else {
            $taskSourceResults += Export-WorkspaceSource -WorkspacePath $task.notebook_task.notebook_path -TaskKey $taskKey
        }
    }
    if ($task.spark_python_task.python_file) {
        $pythonFile = [string]$task.spark_python_task.python_file
        if ($pythonFile.StartsWith("/")) {
            $taskSourceResults += Export-WorkspaceSource -WorkspacePath $pythonFile -TaskKey $taskKey
        }
        else {
            $nonWorkspaceReferences += [ordered]@{ task_key = $taskKey; source = "spark_python_task"; path = $pythonFile; status = "DBFS/volume/URI reference; export separately from its storage source" }
        }
    }
    if ($task.sql_task.file.path) {
        $sqlFile = [string]$task.sql_task.file.path
        if ($sqlFile.StartsWith("/")) {
            $taskSourceResults += Export-WorkspaceSource -WorkspacePath $sqlFile -TaskKey $taskKey
        }
        else {
            $nonWorkspaceReferences += [ordered]@{ task_key = $taskKey; source = "sql_task"; path = $sqlFile; status = "non-workspace SQL file; export separately from its storage source" }
        }
    }
    if ($task.run_job_task.job_id) {
        $childJobReferences += [ordered]@{ task_key = $taskKey; job_id = $task.run_job_task.job_id }
    }
}

$protectedPages = @()
foreach ($page in $pages) {
    $protectedPages += ,(Protect-JobConfiguration -Value $page)
}
$jobExportPath = Join-Path $OutputDirectory ("job_" + $JobId + "_sanitized.json")
$protectedPages | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $jobExportPath -Encoding utf8

# Identify likely literal credentials in exported notebook/script sources. Do not
# alter code automatically; inspect every flagged file before sharing.
$secretPatterns = @(
    '(?i)(password|secret|client_secret|api_key|access_token|refresh_token)\s*[:=]\s*["''][^"'']+["'']',
    '(?i)(authorization|bearer)\s*[:=]\s*["''][^"'']+["'']',
    '(?i)(jdbc:[^"'']*(?:password|pwd)=)[^;"'']+'
)
$potentialSecrets = @()
foreach ($source in $taskSourceResults | Where-Object { $_.status -eq "exported" }) {
    $lineNumber = 0
    foreach ($line in Get-Content -LiteralPath $source.local_file) {
        $lineNumber++
        foreach ($pattern in $secretPatterns) {
            if ($line -match $pattern) {
                $potentialSecrets += [ordered]@{
                    file = $source.local_file
                    line = $lineNumber
                    reason = "Possible literal credential; inspect and redact before sharing"
                }
                break
            }
        }
    }
}

$manifest = [ordered]@{
    job_id = $JobId
    job_name = $jobSettings.name
    exported_at_utc = [DateTime]::UtcNow.ToString("o")
    settings_pages = $pages.Count
    tasks = @($allTasks | ForEach-Object {
        [ordered]@{
            task_key = $_.task_key
            task_type = if ($_.notebook_task) { "notebook" } elseif ($_.spark_python_task) { "spark_python" } elseif ($_.sql_task) { "sql" } elseif ($_.run_job_task) { "run_job" } else { "other" }
            depends_on = $_.depends_on
        }
    })
    workspace_sources = $taskSourceResults
    external_source_references = $nonWorkspaceReferences
    child_job_references = $childJobReferences
    potential_secret_matches = $potentialSecrets
    job_definition = $jobExportPath
    sharing_instruction = "Review every exported source file and redact secrets/customer data before uploading the folder."
}
$manifestPath = Join-Path $OutputDirectory "export_manifest.json"
$manifest | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $manifestPath -Encoding utf8

Write-Host ""
Write-Host "SQL Silver Job export package created at: $OutputDirectory"
Write-Host "Sanitized job definition: $jobExportPath"
Write-Host "Manifest: $manifestPath"
Write-Host "Workspace sources exported: $(($taskSourceResults | Where-Object { $_.status -eq 'exported' }).Count)"
Write-Host "External Git/DBFS/volume references: $($nonWorkspaceReferences.Count)"
Write-Host "Child job references: $($childJobReferences.Count)"
if ($potentialSecrets.Count -gt 0) {
    Write-Warning "$($potentialSecrets.Count) potential literal credential(s) were detected. Inspect and redact the flagged lines before sharing."
}
Write-Host "This script does not run, edit, or reset the Databricks Job."
