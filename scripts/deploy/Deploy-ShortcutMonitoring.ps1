<#
.SYNOPSIS
    One-shot, config-driven deployer for the Fabric Shortcuts Downstream Usage Monitoring solution.
    Creates/updates every Fabric artifact (Lakehouse, Environment, Eventstream, Notebooks, Pipeline,
    Semantic Model, Report, Data Agent) in a target workspace, in dependency order, from the
    fabric/** reference item definitions in this repo (token-substituted per scripts/deploy/parameters.json).

.DESCRIPTION
    Replaces Git-integration-based deployment for people who just want to run a script against a
    fresh (or existing) workspace. Reads scripts/deploy/deploy.config.json (copy from
    deploy.config.example.json), resolves ids as it creates items, and persists them to
    scripts/deploy/.deploy-state.<workspaceId>.json so a re-run (or -SkipSteps) doesn't need to
    recreate items it already deployed - it updates them in place instead.

.PARAMETER ConfigPath
    Path to your deploy.config.json (copy of deploy.config.example.json with real values filled in).

.PARAMETER SkipSteps
    Array of step numbers to skip (e.g. -SkipSteps 1,2,3,4,5,6,7,8,9 to redeploy only the analytics
    layer). Skipped steps still need their outputs in the state file from a prior run.

.PARAMETER WhatIf
    Print the planned steps without calling any Fabric API.

.EXAMPLE
    .\Deploy-ShortcutMonitoring.ps1 -ConfigPath .\deploy.config.json

.EXAMPLE
    .\Deploy-ShortcutMonitoring.ps1 -ConfigPath .\deploy.config.json -SkipSteps 1,2,3,4,5,6,7,8,9
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ConfigPath,
    [int[]]$SkipSteps = @(),
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # scripts/deploy -> scripts -> repo root
$DeployDir = $PSScriptRoot
$FabricDir = Join-Path $RepoRoot "fabric"

Import-Module (Join-Path $DeployDir "lib\FabricDeploy.psm1") -Force

if (-not (Test-Path $ConfigPath)) { throw "Config file not found: $ConfigPath. Copy deploy.config.example.json and fill it in." }
$Config = Get-Content $ConfigPath -Raw | ConvertFrom-Json
$ParametersManifest = Get-Content (Join-Path $DeployDir "parameters.json") -Raw | ConvertFrom-Json

# ---------------------------------------------------------------------------------------------
# State: resolved ids persist per-workspace so re-runs / -SkipSteps update in place instead of
# recreating, and so a failed run can be resumed from the step after the last success.
# ---------------------------------------------------------------------------------------------
function Get-StatePath {
    param([string]$WorkspaceId)
    return Join-Path $DeployDir ".deploy-state.$WorkspaceId.json"
}

function Load-State {
    param([string]$WorkspaceId)
    $path = Get-StatePath -WorkspaceId $WorkspaceId
    if (Test-Path $path) {
        $raw = Get-Content $path -Raw | ConvertFrom-Json
        $h = @{}
        $raw.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
        return $h
    }
    return @{}
}

function Save-State {
    param([string]$WorkspaceId, [hashtable]$State)
    $path = Get-StatePath -WorkspaceId $WorkspaceId
    $State | ConvertTo-Json -Depth 10 | Set-Content -Path $path -Encoding utf8
}

function Get-TokenMap {
    <# Builds the literal->resolved-value map from parameters.json + whatever is in $State so far. #>
    param([hashtable]$State)
    $map = @{}
    foreach ($t in $ParametersManifest.tokens) {
        if ($State.ContainsKey($t.name) -and $State[$t.name]) {
            $map[$t.literal] = $State[$t.name]
        }
    }
    return $map
}

function Write-StepBanner {
    param([int]$Number, [string]$Title)
    Write-Host ""
    Write-Host "==================================================================" -ForegroundColor Cyan
    Write-Host ("STEP {0:D2}: {1}" -f $Number, $Title) -ForegroundColor Cyan
    Write-Host "==================================================================" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------------------------
# Step implementations. Each returns the (possibly updated) $State hashtable.
# ---------------------------------------------------------------------------------------------

function Step01-Workspace {
    param($Config, $State, $Headers)
    Write-StepBanner 1 "Workspace"
    if ($Config.workspace.id) {
        $ws = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($Config.workspace.id)" -Headers $Headers -Method Get -UseBasicParsing
        $wsContent = $ws.Content | ConvertFrom-Json
        Write-Host "  Using existing workspace '$($wsContent.displayName)' ($($Config.workspace.id)), capacity state: $($wsContent.capacityId)"
        $State["WORKSPACE_ID"] = $Config.workspace.id
        $State["WORKSPACE_NAME"] = $wsContent.displayName
    } elseif ($Config.workspace.createIfMissing) {
        if (-not $Config.workspace.displayName -or -not $Config.workspace.capacityId) {
            throw "workspace.createIfMissing is true but workspace.displayName/capacityId are missing in config."
        }
        Write-Host "  Creating workspace '$($Config.workspace.displayName)'..."
        $body = @{ displayName = $Config.workspace.displayName; capacityId = $Config.workspace.capacityId } | ConvertTo-Json
        $result = Invoke-FabricRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $Headers -Body $body
        $State["WORKSPACE_ID"] = $result.Body.id
        $State["WORKSPACE_NAME"] = $Config.workspace.displayName
    } else {
        throw "config.workspace.id is empty and workspace.createIfMissing is false - nothing to target. Set one of them."
    }
    Write-Host "  [OK] workspace id = $($State['WORKSPACE_ID'])"
    return $State
}

function Step02-Folders {
    param($Config, $State, $Headers)
    Write-StepBanner 2 "Folders"
    $wsId = $State["WORKSPACE_ID"]
    $parentFolderId = $null
    if ($Config.folders.containerFolderName) {
        $parentFolderId = Get-OrNew-FabricFolder -WorkspaceId $wsId -Headers $Headers -DisplayName $Config.folders.containerFolderName
        Write-Host "  [OK] container folder '$($Config.folders.containerFolderName)' = $parentFolderId"
    }
    $folderNames = @("notebooks", "pipelines", "environment", "eventstreams", "semanticmodels", "reports", "dataagents")
    foreach ($name in $folderNames) {
        $id = Get-OrNew-FabricFolder -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ParentFolderId $parentFolderId
        $State["FOLDER_$($name.ToUpper())"] = $id
        Write-Host "  [OK] folder '$name' = $id"
    }
    return $State
}

function Step03-Lakehouse {
    param($Config, $State, $Headers)
    Write-StepBanner 3 "Lakehouse"
    $wsId = $State["WORKSPACE_ID"]
    $lhName = $Config.lakehouse.displayName
    $lhId = New-FabricLakehouse -WorkspaceId $wsId -Headers $Headers -DisplayName $lhName
    $State["LAKEHOUSE_ID"] = $lhId
    Write-Host "  [OK] lakehouse '$lhName' = $lhId"
    Write-Host "  Waiting for SQL analytics endpoint to provision..."
    $sqlEndpoint = Wait-LakehouseSqlEndpoint -WorkspaceId $wsId -Headers $Headers -LakehouseId $lhId
    $State["SQL_ENDPOINT"] = $sqlEndpoint
    Write-Host "  [OK] SQL endpoint = $sqlEndpoint"
    return $State
}

function Step04-Config {
    param($Config, $State, $Headers)
    Write-StepBanner 4 "Upload config.json"
    $wsId = $State["WORKSPACE_ID"]
    $lhId = $State["LAKEHOUSE_ID"]

    $clientSecret = ""
    if ($Config.auth.clientId) {
        $secure = Read-Host -Prompt "Enter the monitoring service principal's client secret (input hidden)" -AsSecureString
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
        $clientSecret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    } else {
        Write-Warning "  config.auth.clientId is empty - writing config.json with a blank clientSecret placeholder. Fill it in manually before this solution can run."
    }

    $configJson = [ordered]@{
        monitoredWorkspaces = $Config.monitoredWorkspaces
        detection = $Config.detection
        orchestration = $Config.orchestration
        auth = [ordered]@{
            tenantId = $Config.auth.tenantId
            clientId = $Config.auth.clientId
            clientSecret = $clientSecret
        }
    }
    $json = $configJson | ConvertTo-Json -Depth 10
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    Set-OneLakeFileContent -WorkspaceId $wsId -ItemId $lhId -RelativePath "Files/config/config.json" -Bytes $bytes
    Write-Host "  [OK] uploaded Files/config/config.json ($($bytes.Length) bytes)"
    return $State
}

function Step05-Environment {
    param($Config, $State, $Headers)
    Write-StepBanner 5 "Environment (ENV_OpenLineage)"
    $wsId = $State["WORKSPACE_ID"]
    $name = $Config.environment.displayName
    $templateDir = Join-Path $FabricDir "environment\$name.Environment"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "Environment"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "Environment" -Parts $parts -ExistingItemId $existing.id
    if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_ENVIRONMENT"] }
    $State["ENVIRONMENT_ID"] = $itemId
    Write-Host "  [OK] environment '$name' = $itemId"
    Write-Warning "  Manual step still required: open the environment in the portal, add/verify the Kafka secret under Public libraries / Spark properties (see README 'Building the ENV_OpenLineage environment'), and Publish it - this cannot be automated via the definition API."
    return $State
}

function Step06-Eventstream {
    param($Config, $State, $Headers)
    Write-StepBanner 6 "Eventstream (ES_OpenLineageEvents)"
    $wsId = $State["WORKSPACE_ID"]
    $name = $Config.eventstream.displayName
    $templateDir = Join-Path $FabricDir "eventstreams\$name.Eventstream"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "Eventstream"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "Eventstream" -Parts $parts -ExistingItemId $existing.id
    if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_EVENTSTREAMS"] }
    $State["EVENTSTREAM_ID"] = $itemId
    Write-Host "  [OK] eventstream '$name' = $itemId"
    return $State
}

function Step07-Notebooks {
    param($Config, $State, $Headers)
    Write-StepBanner 7 "Notebooks"
    $wsId = $State["WORKSPACE_ID"]
    $notebooks = @(
        @{ Name = "NB_ShortcutInventory_DuplicateDetection"; TokenKey = "NOTEBOOK_ID_DUPLICATE_DETECTION" },
        @{ Name = "NB_CopyEventDetection_Warehouse"; TokenKey = "NOTEBOOK_ID_WAREHOUSE" },
        @{ Name = "NB_CopyEventDetection_SparkKafka"; TokenKey = "NOTEBOOK_ID_SPARKKAFKA" },
        @{ Name = "NB_OpenLineage_Validate"; TokenKey = $null }
    )
    if ($Config.deployOptionalTestNotebook) {
        $notebooks += @{ Name = "NB_OpenLineage_SparkLineageTest"; TokenKey = $null }
    }

    foreach ($nb in $notebooks) {
        $templateDir = Join-Path $FabricDir "notebooks\$($nb.Name).Notebook"
        if (-not (Test-Path $templateDir)) { Write-Warning "  Skipping $($nb.Name) - template folder not found."; continue }
        $tokenMap = Get-TokenMap -State $State
        $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
        $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $nb.Name -Type "Notebook"
        $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $nb.Name -ItemType "Notebook" -Parts $parts -ExistingItemId $existing.id
        if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_NOTEBOOKS"] }
        if ($nb.TokenKey) { $State[$nb.TokenKey] = $itemId }
        Write-Host "  [OK] notebook '$($nb.Name)' = $itemId"
    }
    return $State
}

function Step08-Pipeline {
    param($Config, $State, $Headers)
    Write-StepBanner 8 "Pipeline (PL_ShortcutMonitoringOrchestrator)"
    $wsId = $State["WORKSPACE_ID"]
    $name = "PL_ShortcutMonitoringOrchestrator"
    $templateDir = Join-Path $FabricDir "pipelines\$name.DataPipeline"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "DataPipeline"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "DataPipeline" -Parts $parts -ExistingItemId $existing.id
    if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_PIPELINES"] }
    $State["PIPELINE_ID"] = $itemId
    Write-Host "  [OK] pipeline '$name' = $itemId"

    if ($Config.pipeline.scheduleEnabled) {
        Write-Host "  Setting schedule (cron: $($Config.pipeline.cronExpression))..."
        Set-FabricItemSchedule -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -JobType "Pipeline" `
            -CronExpression $Config.pipeline.cronExpression -Enabled $true -TimeZone $Config.pipeline.timeZone
        Write-Host "  [OK] schedule enabled"
    } else {
        Write-Host "  Schedule left disabled (config.pipeline.scheduleEnabled=false) - trigger manually or enable later."
    }
    return $State
}

function Step09-InitialRun {
    param($Config, $State, $Headers)
    Write-StepBanner 9 "Initial pipeline run (seeds Fact/Dim tables before the semantic model deploy)"
    if (-not $Config.runInitialPipelineJob) {
        Write-Host "  Skipped (config.runInitialPipelineJob=false). NOTE: the semantic model refresh in step 10 will fail until the pipeline has run at least once."
        return $State
    }
    $wsId = $State["WORKSPACE_ID"]
    $pipelineId = $State["PIPELINE_ID"]
    Write-Host "  Triggering pipeline run..."
    $jobUrl = Start-FabricPipelineRun -WorkspaceId $wsId -Headers $Headers -PipelineId $pipelineId
    Write-Host "  Polling job (this can take several minutes)..."
    $final = Wait-FabricJobInstance -JobInstanceUrl $jobUrl -Headers $Headers
    if ($final.status -ne "Completed") {
        throw "Initial pipeline run finished with status '$($final.status)'. Check the pipeline run details in the portal before continuing (resume with -SkipSteps up through 9)."
    }
    Write-Host "  [OK] initial pipeline run completed"
    return $State
}

function Step10-SemanticModel {
    param($Config, $State, $Headers)
    Write-StepBanner 10 "Semantic Model (SM_ShortcutMonitoring)"
    if (-not $Config.deploySemanticModel) { Write-Host "  Skipped (config.deploySemanticModel=false)"; return $State }
    $wsId = $State["WORKSPACE_ID"]
    $name = "SM_ShortcutMonitoring"
    $templateDir = Join-Path $FabricDir "semanticmodels\$name.SemanticModel"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap

    # Direct Lake models can't be converted in place - if one already exists here, recreate it rather
    # than update-in-place (see README "Deploying/updating the semantic model..." caveat).
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "SemanticModel"
    if ($existing) {
        Write-Host "  Existing semantic model found - deleting and recreating (Direct Lake mode can't be updated in place)..."
        Invoke-FabricRequest -Method Delete -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($existing.id)" -Headers $Headers | Out-Null
    }
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "SemanticModel" -Parts $parts
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_SEMANTICMODELS"]
    $State["SEMANTIC_MODEL_ID"] = $itemId
    Write-Host "  [OK] semantic model '$name' = $itemId"

    Write-Host "  Smoke-testing with a DAX query..."
    Start-Sleep -Seconds 15   # give Direct Lake framing a moment after creation
    try {
        $result = Invoke-DaxQuery -WorkspaceName $State["WORKSPACE_NAME"] -DatasetId $itemId -Headers $Headers -Dax "EVALUATE ROW(`"n`", COUNTROWS(DimShortcut))"
        Write-Host "  [OK] DAX smoke test succeeded: $($result.results[0].tables[0].rows | ConvertTo-Json -Compress)"
    } catch {
        Write-Warning "  DAX smoke test failed (this is expected if step 9 was skipped and the Lakehouse tables don't exist yet): $($_.Exception.Message)"
    }
    return $State
}

function Step11-Report {
    param($Config, $State, $Headers)
    Write-StepBanner 11 "Report (RPT_ShortcutMonitoring)"
    if (-not $Config.deployReport) { Write-Host "  Skipped (config.deployReport=false)"; return $State }
    $wsId = $State["WORKSPACE_ID"]
    $name = "RPT_ShortcutMonitoring"
    $templateDir = Join-Path $FabricDir "reports\$name.Report"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "Report"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "Report" -Parts $parts -ExistingItemId $existing.id
    if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_REPORTS"] }
    $State["REPORT_ID"] = $itemId
    Write-Host "  [OK] report '$name' = $itemId"

    $def = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$itemId/getDefinition" -Headers $Headers -Method Post -UseBasicParsing
    $defContent = $def.Content | ConvertFrom-Json
    Write-Host "  [OK] verified getDefinition returns $($defContent.definition.parts.Count) parts (expected $($parts.Count))"
    return $State
}

function Step12-DataAgent {
    param($Config, $State, $Headers)
    Write-StepBanner 12 "Data Agent (DA_ShortcutMonitoring)"
    if (-not $Config.deployDataAgent) { Write-Host "  Skipped (config.deployDataAgent=false)"; return $State }
    $wsId = $State["WORKSPACE_ID"]
    $name = "DA_ShortcutMonitoring"
    $templateDir = Join-Path $FabricDir "dataagents\$name.DataAgent"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "DataAgent"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "DataAgent" -Parts $parts -ExistingItemId $existing.id
    if (-not $existing) { Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_DATAAGENTS"] }
    $State["DATA_AGENT_ID"] = $itemId
    Write-Host "  [OK] data agent '$name' = $itemId"
    return $State
}

# ---------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------
$Steps = @(
    @{ Number = 1; Title = "Workspace"; Fn = ${function:Step01-Workspace} }
    @{ Number = 2; Title = "Folders"; Fn = ${function:Step02-Folders} }
    @{ Number = 3; Title = "Lakehouse"; Fn = ${function:Step03-Lakehouse} }
    @{ Number = 4; Title = "Upload config.json"; Fn = ${function:Step04-Config} }
    @{ Number = 5; Title = "Environment"; Fn = ${function:Step05-Environment} }
    @{ Number = 6; Title = "Eventstream"; Fn = ${function:Step06-Eventstream} }
    @{ Number = 7; Title = "Notebooks"; Fn = ${function:Step07-Notebooks} }
    @{ Number = 8; Title = "Pipeline"; Fn = ${function:Step08-Pipeline} }
    @{ Number = 9; Title = "Initial pipeline run"; Fn = ${function:Step09-InitialRun} }
    @{ Number = 10; Title = "Semantic Model"; Fn = ${function:Step10-SemanticModel} }
    @{ Number = 11; Title = "Report"; Fn = ${function:Step11-Report} }
    @{ Number = 12; Title = "Data Agent"; Fn = ${function:Step12-DataAgent} }
)

if ($WhatIf) {
    Write-Host "Planned steps:"
    foreach ($s in $Steps) {
        $skipMark = if ($SkipSteps -contains $s.Number) { " (SKIP)" } else { "" }
        Write-Host ("  {0:D2} - {1}{2}" -f $s.Number, $s.Title, $skipMark)
    }
    return
}

$token = Get-FabricToken
$headers = Get-FabricHeaders -Token $token

# Need a workspace id up front to key the state file - either from config, or (after step 1 on a
# create-new-workspace run) it will be added to $State and the state file re-keyed/saved then.
$initialWsId = if ($Config.workspace.id) { $Config.workspace.id } else { "pending" }
$State = Load-State -WorkspaceId $initialWsId

$failedAt = $null
foreach ($step in $Steps) {
    if ($SkipSteps -contains $step.Number) {
        Write-StepBanner $step.Number "$($step.Title) (SKIPPED)"
        continue
    }
    try {
        $State = & $step.Fn -Config $Config -State $State -Headers $headers
        $wsIdForSave = if ($State["WORKSPACE_ID"]) { $State["WORKSPACE_ID"] } else { $initialWsId }
        Save-State -WorkspaceId $wsIdForSave -State $State
    } catch {
        Write-Error "Step $($step.Number) ($($step.Title)) failed: $($_.Exception.Message)"
        $failedAt = $step.Number
        break
    }
}

Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
if ($failedAt) {
    Write-Host "DEPLOYMENT STOPPED at step $failedAt. Fix the issue above, then resume with:" -ForegroundColor Red
    $resumeSkip = (1..($failedAt - 1)) -join ","
    Write-Host "  .\Deploy-ShortcutMonitoring.ps1 -ConfigPath `"$ConfigPath`" -SkipSteps $resumeSkip" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "DEPLOYMENT COMPLETE" -ForegroundColor Green
    Write-Host "  Workspace:      $($State['WORKSPACE_ID'])"
    Write-Host "  Lakehouse:      $($State['LAKEHOUSE_ID'])"
    Write-Host "  Semantic Model: $($State['SEMANTIC_MODEL_ID'])"
    Write-Host "  Report:         $($State['REPORT_ID'])"
    Write-Host "  Data Agent:     $($State['DATA_AGENT_ID'])"
    Write-Host "State saved to: $(Get-StatePath -WorkspaceId $State['WORKSPACE_ID'])"
}
Write-Host "==================================================================" -ForegroundColor Cyan
