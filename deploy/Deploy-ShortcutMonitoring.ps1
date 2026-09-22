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
$RepoRoot = Split-Path -Parent $PSScriptRoot   # deploy/ -> repo root
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
    $folderNames = @("lakehouse", "notebooks", "pipelines", "environment", "eventstreams", "eventhouses", "semanticmodels", "reports", "dataagents")
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
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $lhId -FolderId $State["FOLDER_LAKEHOUSE"]
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

function Step05-Eventstream {
    param($Config, $State, $Headers)
    Write-StepBanner 5 "Eventstream (ES_OpenLineageEvents)"
    $wsId = $State["WORKSPACE_ID"]
    $name = $Config.eventstream.displayName

    # Raw-capture Eventhouse must exist (with its "accept literally everything" dynamic-column
    # table) before the Eventstream is published, since the Eventstream's DirectIngestion
    # destination references it by item id, table name, and mapping rule name.
    $ehName = $Config.eventhouse.displayName
    $ehResult = Confirm-FabricEventhouseAndRawTable -WorkspaceId $wsId -Headers $Headers -EventhouseName $ehName -KqlTableName "ol_raw_events" -KqlMappingName "ol_raw_events_map"
    $ehId = $ehResult.EventhouseId
    $State["EVENTHOUSE_ID"] = $ehId
    $State["KQL_DATABASE_ID"] = $ehResult.KqlDatabaseId
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $ehId -FolderId $State["FOLDER_EVENTHOUSES"]
    Write-Host "  [OK] eventhouse '$ehName' = $ehId (KQL database = $($ehResult.KqlDatabaseId))"

    $templateDir = Join-Path $FabricDir "eventstreams\$name.Eventstream"
    $tokenMap = Get-TokenMap -State $State
    $parts = Get-ItemDefinitionParts -TemplateDir $templateDir -TokenMap $tokenMap
    $existing = Get-FabricItemByName -WorkspaceId $wsId -Headers $Headers -DisplayName $name -Type "Eventstream"
    $itemId = Publish-FabricItem -WorkspaceId $wsId -Headers $Headers -DisplayName $name -ItemType "Eventstream" -Parts $parts -ExistingItemId $existing.id
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_EVENTSTREAMS"]
    $State["EVENTSTREAM_ID"] = $itemId
    Write-Host "  [OK] eventstream '$name' = $itemId"

    Write-Host "  Verifying the Eventstream is actually Running (not paused)..."
    Confirm-FabricEventstreamRunning -WorkspaceId $wsId -Headers $Headers -EventstreamId $itemId
    return $State
}

function Step06-Environment {
    param($Config, $State, $Headers)
    Write-StepBanner 6 "Environment (ENV_OpenLineage) - deployed into each monitored workspace"
    $wsId = $State["WORKSPACE_ID"]

    # Eventstream (step 5) must run before this step - we read its CustomEndpoint source's live
    # Kafka connection details here and bake them straight into every monitored workspace's
    # Sparkcompute.yml, instead of requiring the Kafka secret to be copy-pasted manually from the
    # portal N times. The Eventstream itself stays centralized in THIS (the solution's own)
    # workspace - only the environment/listener config is replicated out to monitored workspaces,
    # since Fabric environments can only be attached to notebooks in their own workspace, whereas
    # the Eventstream's Kafka custom endpoint is just a network endpoint any Spark session can
    # publish to regardless of which workspace it runs in.
    Write-Host "  Fetching live Kafka connection details from Eventstream '$($Config.eventstream.displayName)'..."
    $kafkaConn = Get-EventstreamCustomEndpointConnection -WorkspaceId $wsId -Headers $Headers -EventstreamId $State["EVENTSTREAM_ID"]
    $State["KAFKA_TOPIC_NAME"] = $kafkaConn.eventHubName
    $State["KAFKA_BOOTSTRAP_SERVERS"] = "$($kafkaConn.fullyQualifiedNamespace):9093"
    $State["KAFKA_SASL_JAAS_SECRET_PLACEHOLDER"] = "$($kafkaConn.accessKeys.primaryConnectionString)"
    Write-Host "  [OK] resolved Kafka topic '$($State['KAFKA_TOPIC_NAME'])' on '$($kafkaConn.fullyQualifiedNamespace)' (secret not logged)"

    if (-not $Config.monitoredWorkspaces -or $Config.monitoredWorkspaces.Count -eq 0) {
        Write-Warning "  config.monitoredWorkspaces is empty - nothing to deploy. Add entries there and re-run this step (-SkipSteps 1,2,3,4,5)."
        return $State
    }

    $envName = $Config.environment.displayName
    $envTemplateDir = Join-Path $FabricDir "environment\$envName.Environment"
    $nbTemplateDir = Join-Path $FabricDir "notebooks\NB_OpenLineage_Validate.Notebook"
    if (-not $State["MONITORED_ENVIRONMENTS"] -or -not ($State["MONITORED_ENVIRONMENTS"] -is [hashtable])) {
        # A reloaded state file deserializes nested objects as PSCustomObject, not hashtable -
        # indexed assignment below ($State["MONITORED_ENVIRONMENTS"][$mwId] = ...) requires a real
        # hashtable, so convert/re-seed it here regardless of what shape it came back as.
        $h = @{}
        if ($State["MONITORED_ENVIRONMENTS"]) {
            $State["MONITORED_ENVIRONMENTS"].PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
        }
        $State["MONITORED_ENVIRONMENTS"] = $h
    }

    foreach ($mw in $Config.monitoredWorkspaces) {
        $mwId = $mw.workspaceId
        $mwName = $mw.workspaceName
        Write-Host ""
        Write-Host "  --- Monitored workspace '$mwName' ($mwId) ---"

        # Per-iteration token map: same KAFKA_* values as the shared central Eventstream, but
        # WORKSPACE_ID/WORKSPACE_NAME point at THIS monitored workspace (not the solution's own) -
        # a local clone so the real $State (used by every later step for the solution's own
        # workspace) is never mutated.
        $mwState = $State.Clone()
        # Captured from the real $State BEFORE the WORKSPACE_ID/WORKSPACE_NAME override below, so
        # NB_OpenLineage_Validate can still reach back into the solution's own workspace/Lakehouse
        # (where the shared ol_lineage_events_v3 sink table lives) even while it is deployed here.
        $mwState["SOLUTION_WORKSPACE_ID"] = $State["WORKSPACE_ID"]
        $mwState["SOLUTION_LAKEHOUSE_ID"] = $State["LAKEHOUSE_ID"]
        $mwState["WORKSPACE_ID"] = $mwId
        $mwState["WORKSPACE_NAME"] = $mwName

        $envTokenMap = Get-TokenMap -State $mwState
        $envParts = Get-ItemDefinitionParts -TemplateDir $envTemplateDir -TokenMap $envTokenMap
        $existingEnv = Get-FabricItemByName -WorkspaceId $mwId -Headers $Headers -DisplayName $envName -Type "Environment"
        $envItemId = Publish-FabricItem -WorkspaceId $mwId -Headers $Headers -DisplayName $envName -ItemType "Environment" -Parts $envParts -ExistingItemId $existingEnv.id
        Write-Host "  [OK] environment '$envName' = $envItemId"
        Write-Host "  Publishing environment (applies staged Spark config)..."
        Publish-FabricEnvironment -WorkspaceId $mwId -Headers $Headers -EnvironmentId $envItemId
        Write-Host "  [OK] environment published"

        $nbItemId = $null
        if (Test-Path $nbTemplateDir) {
            $mwState["ENVIRONMENT_ID"] = $envItemId
            $nbTokenMap = Get-TokenMap -State $mwState
            $nbParts = Get-ItemDefinitionParts -TemplateDir $nbTemplateDir -TokenMap $nbTokenMap
            $existingNb = Get-FabricItemByName -WorkspaceId $mwId -Headers $Headers -DisplayName "NB_OpenLineage_Validate" -Type "Notebook"
            $nbItemId = Publish-FabricItem -WorkspaceId $mwId -Headers $Headers -DisplayName "NB_OpenLineage_Validate" -ItemType "Notebook" -Parts $nbParts -ExistingItemId $existingNb.id
            Assert-ItemDefinitionUploaded -WorkspaceId $mwId -Headers $Headers -ItemId $nbItemId -DisplayName "NB_OpenLineage_Validate" -PartPathLike "*notebook-content*"
            Write-Host "  [OK] notebook 'NB_OpenLineage_Validate' = $nbItemId"
        }

        $State["MONITORED_ENVIRONMENTS"][$mwId] = @{ workspaceName = $mwName; environmentId = $envItemId; validateNotebookId = $nbItemId }

        Write-Host "  ACTION NEEDED: in '$mwName', attach '$envName' to whichever notebook(s) you want" -ForegroundColor Yellow
        Write-Host "  monitored (Notebook > Environment dropdown), including 'NB_OpenLineage_Validate' if you" -ForegroundColor Yellow
        Write-Host "  want to self-test - update its Cell 1 with a real shortcut path in that workspace first." -ForegroundColor Yellow
    }
    return $State
}

function Step07-Notebooks {
    param($Config, $State, $Headers)
    Write-StepBanner 7 "Notebooks"
    $wsId = $State["WORKSPACE_ID"]
    # NB_OpenLineage_Validate is intentionally NOT deployed here - it belongs in each monitored
    # workspace (deployed by step 6, alongside that workspace's own ENV_OpenLineage), not in the
    # solution's own workspace, since it exists purely to self-test a monitored workspace's
    # Spark/Kafka lineage wiring using a real shortcut that lives there.
    $notebooks = @(
        @{ Name = "NB_ShortcutInventory_DuplicateDetection"; TokenKey = "NOTEBOOK_ID_DUPLICATE_DETECTION" },
        @{ Name = "NB_CopyEventDetection_Warehouse"; TokenKey = "NOTEBOOK_ID_WAREHOUSE" },
        @{ Name = "NB_CopyEventDetection_SparkKafka"; TokenKey = "NOTEBOOK_ID_SPARKKAFKA" }
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
        Assert-ItemDefinitionUploaded -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -DisplayName $nb.Name -PartPathLike "*notebook-content*"
        Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_NOTEBOOKS"]
        if ($nb.TokenKey) { $State[$nb.TokenKey] = $itemId }
        Write-Host "  [OK] notebook '$($nb.Name)' = $itemId"
    }

    Write-Host ""
    Write-Host "  NOTE: NB_OpenLineage_Validate is deployed per-monitored-workspace by step 6, not here -" -ForegroundColor Yellow
    Write-Host "  see that step's 'ACTION NEEDED' output for where to attach its environment and run it." -ForegroundColor Yellow
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
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_PIPELINES"]
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
    Write-Host "  Triggering pipeline run..."
    $wsId = $State["WORKSPACE_ID"]
    $pipelineId = $State["PIPELINE_ID"]
    $jobUrl = Start-FabricPipelineRun -WorkspaceId $wsId -Headers $Headers -PipelineId $pipelineId
    Write-Host "  Polling job (this can take several minutes)..."
    $final = Wait-FabricJobInstance -JobInstanceUrl $jobUrl -Headers $Headers
    if ($final.status -ne "Completed") {
        $reason = $null
        if ($final.failureReason) { $reason = $final.failureReason | ConvertTo-Json -Depth 10 -Compress }
        Write-Host "  Job instance details: $($final | ConvertTo-Json -Depth 10)"
        throw "Initial pipeline run finished with status '$($final.status)'.$(if ($reason) { " Reason: $reason" }) Check the pipeline run details in the portal (Monitor hub) before continuing (resume with -SkipSteps up through 9)."
    }
    Write-Host "  [OK] initial pipeline run completed"

    # The pipeline job instance reporting "Completed" only means the orchestrator's activities
    # returned without error - it does NOT guarantee the notebooks actually did real work (e.g. a
    # notebook silently deployed with stub/corrupted content, or that errors internally but returns
    # normally, would still show as a completed job). Verify the actual Fact/Dim tables the
    # notebooks are supposed to seed are present before moving on, since the semantic
    # model/report/data-agent steps that follow only upload item definitions - they never validate
    # against real Lakehouse data, so they'd "succeed" even with zero tables.
    Write-Host "  Verifying expected Fact/Dim tables were actually created..."
    $expectedTables = @("DimShortcut", "FactShortcutInventoryDiff", "FactDuplicateShortcutGroup")
    $actualTables = Get-FabricLakehouseTables -WorkspaceId $wsId -Headers $Headers -LakehouseId $State["LAKEHOUSE_ID"]
    $missing = $expectedTables | Where-Object { $_ -notin $actualTables }
    if ($missing) {
        throw "Initial pipeline run reported 'Completed' but the expected table(s) [$($missing -join ', ')] are missing from the Lakehouse (found: [$($actualTables -join ', ')]). The notebooks likely ran with empty/stub content or failed silently - check the notebook job run logs in the portal (Monitor hub) before continuing (resume with -SkipSteps up through 8, then rerun step 9)."
    }
    Write-Host "  [OK] confirmed tables present: $($actualTables -join ', ')"
    $State["STEP9_VERIFIED_TABLES"] = $true

    # Force a SQL analytics endpoint metadata sync right after the pipeline writes/alters tables -
    # the endpoint's own auto-sync can lag a schema change (e.g. a newly added column) by more than
    # the 15s pause Step 10 gives it, and Direct Lake (which queries through this same SQL endpoint)
    # will then fail with "We cannot access the source column ..." even though the Delta log/table
    # already has the column. Doing this explicitly here removes that race for Step 10/the Report.
    Write-Host "  Forcing SQL analytics endpoint metadata sync..."
    Sync-FabricLakehouseSqlEndpoint -WorkspaceId $wsId -Headers $Headers -LakehouseId $State["LAKEHOUSE_ID"]
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

    # Persist the new id to the state file NOW, before the smoke test below - if it throws, the
    # caller's `$State = & $step.Fn ...` never completes and its own Save-State call never runs,
    # which would otherwise silently strand the state file pointing at the OLD (just-deleted, or on
    # a prior run: about-to-be-deleted) semantic model id. That stranding is what broke Step 11
    # (Report) after a prior recreate-then-smoke-test-failure: it tried to bind the report to a
    # semantic model id that no longer existed. Existing item ids in $existing were already deleted
    # above regardless of what happens next, so the new id is the only usable value from this point on.
    Save-State -WorkspaceId $wsId -State $State

    Write-Host "  Refreshing (framing) the Direct Lake semantic model..."
    # A Direct Lake model created via the API has no framed data yet - it must be explicitly
    # refreshed once before it can serve ANY query. A successful refresh is itself the proof the
    # model is queryable, so it replaces the old DAX-smoke-test step below rather than being
    # followed by one - that separate query used to misdiagnose the pre-refresh 403 as an ACL-
    # propagation delay (a symptom that never resolves just by waiting/retrying) instead of what
    # it really is: the model simply hadn't been refreshed yet.
    try {
        $refreshId = Start-FabricDatasetRefresh -WorkspaceId $wsId -DatasetId $itemId
        Wait-FabricDatasetRefresh -WorkspaceId $wsId -DatasetId $itemId -RefreshId $refreshId | Out-Null
        Write-Host "  [OK] semantic model refreshed/framed."
    } catch {
        if (-not $State["STEP9_VERIFIED_TABLES"]) {
            Write-Warning "  Dataset refresh failed (expected - step 9's table verification didn't run this invocation, e.g. it was skipped via -SkipSteps or config.runInitialPipelineJob=false, so the Lakehouse tables may not exist yet): $($_.Exception.Message)"
        } else {
            # Step 9 already verified the Fact/Dim tables exist by this point, so a refresh failure
            # here is a real problem (e.g. a bad model definition/relationship) - don't let the run
            # report "COMPLETE" while masking it as a mere warning.
            throw "Dataset refresh failed against a Lakehouse that step 9 already confirmed has data - this indicates a real semantic model problem, not a missing-data timing issue: $($_.Exception.Message)"
        }
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
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_REPORTS"]
    $State["REPORT_ID"] = $itemId
    Write-Host "  [OK] report '$name' = $itemId"

    # Use Invoke-FabricRequest (not a raw Invoke-WebRequest) so a 202 LRO response is actually
    # polled to completion before reading the body - a raw call here previously reported a false
    # "0 parts" every time (the report was in fact deployed correctly; the immediate 202 response
    # body is empty until the operation finishes), which was misleading and indistinguishable from
    # a real corrupted-upload failure like the one Assert-ItemDefinitionUploaded now catches for notebooks.
    $def = Invoke-FabricRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$itemId/getDefinition" -Headers $Headers
    $actualParts = $def.Body.definition.parts.Count
    if ($actualParts -ne $parts.Count) {
        throw "Report '$name' getDefinition returned $actualParts parts but $($parts.Count) were uploaded - the definition may not have published correctly."
    }
    Write-Host "  [OK] verified getDefinition returns $actualParts parts (expected $($parts.Count))"

    Write-Host "  Validating the report actually renders..."
    # getDefinition above only proves the uploaded JSON parts round-trip correctly - it says nothing
    # about whether the report is actually bound to a live semantic model and can be opened/rendered.
    # Confirm-FabricReportRuns checks that via ordinary report-read endpoints (not executeQueries,
    # which needs a separate tenant setting - see its own comment for why).
    try {
        $reportCheck = Confirm-FabricReportRuns -WorkspaceId $wsId -ReportId $itemId -ExpectedDatasetId $State["SEMANTIC_MODEL_ID"]
        Write-Host "  [OK] report renders: $($reportCheck.PageCount) page(s), bound to dataset $($reportCheck.DatasetId)"
    } catch {
        throw "Report '$name' was published but does not appear to render correctly (not bound to a live/queryable semantic model, or has no pages): $($_.Exception.Message)"
    }
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
    Move-FabricItemToFolder -WorkspaceId $wsId -Headers $Headers -ItemId $itemId -FolderId $State["FOLDER_DATAAGENTS"]
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
    @{ Number = 5; Title = "Eventstream"; Fn = ${function:Step05-Eventstream} }
    @{ Number = 6; Title = "Environment"; Fn = ${function:Step06-Environment} }
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
        # Re-acquire a fresh token before every step rather than reusing one token for the whole
        # run - AAD access tokens typically expire after ~60-75 minutes, and long-running steps
        # (SQL endpoint provisioning, the manual environment-attach step, the initial pipeline run's
        # multi-minute poll) can easily push a single-token run past that lifetime, causing a 401
        # partway through. 'az account get-access-token' is fast/cheap (uses the cached refresh
        # token), so refreshing per step has no meaningful cost.
        $token = Get-FabricToken
        $headers = Get-FabricHeaders -Token $token
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
