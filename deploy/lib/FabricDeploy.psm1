# FabricDeploy.psm1
# Shared helper functions for the Shortcut Monitoring solution's config-driven deployment script
# (scripts/deploy/Deploy-ShortcutMonitoring.ps1). Not a general-purpose Fabric SDK - just what this
# solution's step scripts need: token acquisition, generic item CRUD via the Fabric REST API,
# LRO polling, folder management, token/placeholder substitution, and job (notebook/pipeline) runs.

$script:FabricBaseUri = "https://api.fabric.microsoft.com/v1"
$script:PowerBiBaseUri = "https://api.powerbi.com/v1.0/myorg"

function Get-FabricToken {
    [CmdletBinding()]
    param()
    $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
    if (-not $token) { throw "Failed to acquire an access token via 'az account get-access-token'. Run 'az login' first." }
    return $token
}

function Get-FabricHeaders {
    param([string]$Token)
    return @{ Authorization = "Bearer $Token"; "Content-Type" = "application/json" }
}

function Wait-FabricOperation {
    <#
      Polls a long-running-operation Location URL returned as a 202 response header until it
      reaches a terminal state. Returns the final operation status object (with .status and,
      once available, the operation's result via a separate GET .../result call for creates).
    #>
    param(
        [Parameter(Mandatory)][string]$OperationUrl,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$MaxAttempts = 60,
        [int]$DelaySeconds = 3
    )
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        Start-Sleep -Seconds $DelaySeconds
        $resp = Invoke-WebRequest -Uri $OperationUrl -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        if ($content.status -eq "Succeeded") { return $content }
        if ($content.status -eq "Failed") {
            throw "Fabric operation failed: $($content.error | ConvertTo-Json -Depth 10)"
        }
    }
    throw "Timed out waiting for Fabric operation at $OperationUrl"
}

function Get-FabricOperationResult {
    param([Parameter(Mandatory)][string]$OperationUrl, [Parameter(Mandatory)][hashtable]$Headers)
    $resultUrl = "$OperationUrl/result"
    try {
        $resp = Invoke-WebRequest -Uri $resultUrl -Headers $Headers -Method Get -UseBasicParsing
        return $resp.Content | ConvertFrom-Json
    } catch {
        return $null
    }
}

function Invoke-FabricRequest {
    <#
      Generic wrapper for Fabric REST calls that may return 200/201 (sync) or 202 (LRO).
      Retries transient 429s once with the Retry-After header's delay.
      Returns a hashtable: @{ StatusCode; Body; Location }
    #>
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        [hashtable]$Headers,
        [string]$Body
    )
    try {
        $params = @{ Uri = $Uri; Headers = $Headers; Method = $Method; UseBasicParsing = $true }
        if ($Body) { $params.Body = $Body }
        $resp = Invoke-WebRequest @params
    } catch {
        $we = $_.Exception
        if ($we.Response) {
            $status = [int]$we.Response.StatusCode
            if ($status -eq 429) {
                $retryAfter = 10
                try { $retryAfter = [int]$we.Response.Headers["Retry-After"] } catch {}
                Write-Warning "Throttled (429), retrying after $retryAfter s..."
                Start-Sleep -Seconds $retryAfter
                return Invoke-FabricRequest -Method $Method -Uri $Uri -Headers $Headers -Body $Body
            }
            $stream = $we.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            $errBody = $reader.ReadToEnd()
            throw "Fabric API $Method $Uri failed ($status): $errBody"
        }
        throw
    }

    $result = @{ StatusCode = [int]$resp.StatusCode; Body = $null; Location = $resp.Headers['Location'] }
    if ($resp.Content) { $result.Body = $resp.Content | ConvertFrom-Json }

    if ($result.StatusCode -eq 202 -and $result.Location) {
        $final = Wait-FabricOperation -OperationUrl $result.Location -Headers $Headers
        $opResult = Get-FabricOperationResult -OperationUrl $result.Location -Headers $Headers
        if ($opResult) { $result.Body = $opResult }
        $result.StatusCode = 200
    }
    return $result
}

function Get-FabricItems {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [string]$Type)
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/items"
    if ($Type) { $uri += "?type=$Type" }
    $items = @()
    do {
        $resp = Invoke-WebRequest -Uri $uri -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        $items += $content.value
        $uri = $content.continuationUri
    } while ($uri)
    return $items
}

function Get-FabricItemByName {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$DisplayName, [string]$Type)
    $items = Get-FabricItems -WorkspaceId $WorkspaceId -Headers $Headers -Type $Type
    return $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
}

function Get-FabricFolders {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers)
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/folders"
    $folders = @()
    do {
        $resp = Invoke-WebRequest -Uri $uri -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        $folders += $content.value
        $uri = $content.continuationUri
    } while ($uri)
    return $folders
}

function Get-FabricLakehouseTables {
    <# Lists Delta table names currently registered in a Lakehouse's SQL/Tables view. #>
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$LakehouseId)
    $tables = @()
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/lakehouses/$LakehouseId/tables"
    do {
        $resp = Invoke-WebRequest -Uri $uri -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        $tables += $content.data
        $uri = $content.continuationUri
    } while ($uri)
    return $tables | ForEach-Object { $_.name }
}

function Get-OrNew-FabricFolder {
    <# Idempotent: returns the existing folder's id if a folder with this name (and parent) already exists, else creates it. #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$DisplayName,
        [string]$ParentFolderId
    )
    $existing = Get-FabricFolders -WorkspaceId $WorkspaceId -Headers $Headers |
        Where-Object { $_.displayName -eq $DisplayName -and ($_.parentFolderId -eq $ParentFolderId -or (-not $_.parentFolderId -and -not $ParentFolderId)) } |
        Select-Object -First 1
    if ($existing) { return $existing.id }

    $bodyObj = @{ displayName = $DisplayName }
    if ($ParentFolderId) { $bodyObj.parentFolderId = $ParentFolderId }
    $body = $bodyObj | ConvertTo-Json
    $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/folders" -Headers $Headers -Body $body
    return $result.Body.id
}

function Move-FabricItemToFolder {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$ItemId, [Parameter(Mandatory)][string]$FolderId)
    # NOTE: the Fabric "Move Item" API expects the body key "targetFolderId", not "folderId" -
    # using the wrong key is accepted (200) but silently does nothing, leaving the item at the
    # workspace root with no error.
    $body = @{ targetFolderId = $FolderId } | ConvertTo-Json
    Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$ItemId/move" -Headers $Headers -Body $body | Out-Null
}

function Get-ItemDefinitionParts {
    <#
      Reads every file under a fabric/<type>/<Name> template folder, substitutes each
      $TokenMap[literal] -> replacement occurrence, and returns Fabric item-definition parts
      (path/payload/payloadType) ready to POST/updateDefinition.
      $TokenMap is a hashtable of literal-reference-value -> new-value (see parameters.json).
    #>
    param(
        [Parameter(Mandatory)][string]$TemplateDir,
        [hashtable]$TokenMap = @{}
    )
    if (-not (Test-Path $TemplateDir)) { throw "Template folder not found: $TemplateDir" }
    $parts = @()
    Get-ChildItem -Path $TemplateDir -Recurse -File | ForEach-Object {
        $rel = $_.FullName.Substring($TemplateDir.Length + 1).Replace('\', '/')
        $text = [System.IO.File]::ReadAllText($_.FullName)
        $text = $text.TrimStart([char]0xFEFF)
        foreach ($literal in $TokenMap.Keys) {
            if ($text.Contains($literal)) { $text = $text.Replace($literal, [string]$TokenMap[$literal]) }
        }
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
        $b64 = [Convert]::ToBase64String($bytes)
        $parts += @{ path = $rel; payload = $b64; payloadType = "InlineBase64" }
    }
    return ,$parts
}

function Publish-FabricItem {
    <#
      Create-or-update an item from a parts array. If -ExistingItemId is supplied, updates that
      item's definition in place; otherwise looks up an item with the same DisplayName+Type in the
      workspace (idempotent re-run) and updates it, or creates a brand-new item if none exists.
      Returns the item id.
    #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$DisplayName,
        [Parameter(Mandatory)][string]$ItemType,
        [Parameter(Mandatory)][array]$Parts,
        [string]$Description = "",
        [string]$ExistingItemId
    )
    $itemId = $ExistingItemId
    if (-not $itemId) {
        $existing = Get-FabricItemByName -WorkspaceId $WorkspaceId -Headers $Headers -DisplayName $DisplayName -Type $ItemType
        if ($existing) { $itemId = $existing.id }
    }

    if ($itemId) {
        Write-Host "  Updating existing $ItemType '$DisplayName' ($itemId)..."
        $body = @{ definition = @{ parts = $Parts } } | ConvertTo-Json -Depth 30
        Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$itemId/updateDefinition" -Headers $Headers -Body $body | Out-Null
        return $itemId
    }

    Write-Host "  Creating $ItemType '$DisplayName'..."
    $body = @{ displayName = $DisplayName; description = $Description; type = $ItemType; definition = @{ parts = $Parts } } | ConvertTo-Json -Depth 30
    $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items" -Headers $Headers -Body $body
    if (-not $result.Body.id) { throw "Create of '$DisplayName' did not return an item id. Response: $($result.Body | ConvertTo-Json -Depth 10)" }
    return $result.Body.id
}

function Assert-ItemDefinitionUploaded {
    <#
      Re-fetches an item's definition right after publish and checks the given part's payload is
      at least MinBytes long. Fabric's item create/update-definition calls have occasionally been
      observed to return 200/202-success while silently persisting only a stub definition (e.g. a
      Notebook ending up with just its header comment, ~26 bytes) - since that failure mode is
      indistinguishable from success at the API level, this catches it immediately instead of
      finding out later when a pipeline "completes" without doing any real work.
    #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][string]$DisplayName,
        [Parameter(Mandatory)][string]$PartPathLike,
        [int]$MinBytes = 200
    )
    $def = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$ItemId/getDefinition" -Headers $Headers
    $part = $def.Body.definition.parts | Where-Object { $_.path -like $PartPathLike } | Select-Object -First 1
    if (-not $part) { throw "Post-publish verification failed for '$DisplayName': no definition part matching '$PartPathLike' was found." }
    $bytes = [Convert]::FromBase64String($part.payload)
    if ($bytes.Length -lt $MinBytes) {
        throw "Post-publish verification failed for '$DisplayName': uploaded '$($part.path)' is only $($bytes.Length) bytes (expected at least $MinBytes). The Fabric API accepted the publish but persisted a stub/empty definition - re-run this step."
    }
}

function New-FabricLakehouse {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$DisplayName)
    $existing = Get-FabricItemByName -WorkspaceId $WorkspaceId -Headers $Headers -DisplayName $DisplayName -Type "Lakehouse"
    if ($existing) { return $existing.id }
    $body = @{ displayName = $DisplayName; type = "Lakehouse" } | ConvertTo-Json
    $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items" -Headers $Headers -Body $body
    return $result.Body.id
}

function Confirm-FabricEventhouseAndRawTable {
    <#
      Idempotently ensures the raw-capture Eventhouse + its default KQL Database + a single
      "accept literally everything" table exist, before the Eventstream (DirectIngestion
      destination) is published against them. The table has exactly one dynamic column mapped to
      the JSON root ("$"), so it can never suffer the Eventstream-style schema-lock-from-first-
      message data loss - every event, regardless of shape (START vs COMPLETE, future fields,
      etc.), lands verbatim. Filtering (eventType='COMPLETE' AND non-empty inputs/outputs) happens
      downstream in Spark, not here. `.create-merge table` / `.create-or-alter ... mapping` are
      both safe to re-run (idempotent), so this can run on every deploy.
    #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$EventhouseName,
        [Parameter(Mandatory)][string]$KqlTableName,
        [Parameter(Mandatory)][string]$KqlMappingName
    )
    $eh = Get-FabricItemByName -WorkspaceId $WorkspaceId -Headers $Headers -DisplayName $EventhouseName -Type "Eventhouse"
    if (-not $eh) {
        Write-Host "  Creating Eventhouse '$EventhouseName'..."
        $body = @{ displayName = $EventhouseName; type = "Eventhouse" } | ConvertTo-Json
        $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items" -Headers $Headers -Body $body
        $ehId = $result.Body.id
        Start-Sleep -Seconds 8  # give the default child KQL Database time to register as an item
    } else {
        $ehId = $eh.id
        Write-Host "  [OK] Eventhouse '$EventhouseName' already exists ($ehId)"
    }

    $kqlDb = $null
    for ($i = 0; $i -lt 6 -and -not $kqlDb; $i++) {
        $kqlDb = Get-FabricItemByName -WorkspaceId $WorkspaceId -Headers $Headers -DisplayName $EventhouseName -Type "KQLDatabase"
        if (-not $kqlDb) { Start-Sleep -Seconds 5 }
    }
    if (-not $kqlDb) { throw "Eventhouse '$EventhouseName' did not auto-create its default KQL Database - cannot proceed." }

    $schemaText = ".create-merge table $KqlTableName (EventTime: datetime, RawRecord: dynamic)`n" +
        ".create-or-alter table $KqlTableName ingestion json mapping '$KqlMappingName' " +
        "`"[{\`"column\`":\`"EventTime\`",\`"Properties\`":{\`"path\`":\`"`$.eventTime\`"}},{\`"column\`":\`"RawRecord\`",\`"Properties\`":{\`"path\`":\`"`$\`"}}]`""
    $propsText = "{`"databaseType`":`"ReadWrite`",`"parentEventhouseItemId`":`"$ehId`",`"oneLakeCachingPeriod`":`"P36500D`",`"oneLakeStandardStoragePeriod`":`"P36500D`"}"

    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    $b64Schema = [Convert]::ToBase64String($utf8NoBom.GetBytes($schemaText))
    $b64Props = [Convert]::ToBase64String($utf8NoBom.GetBytes($propsText))

    $body = @{
        definition = @{
            parts = @(
                @{ path = "DatabaseProperties.json"; payload = $b64Props; payloadType = "InlineBase64" },
                @{ path = "DatabaseSchema.kql"; payload = $b64Schema; payloadType = "InlineBase64" }
            )
        }
    } | ConvertTo-Json -Depth 10
    Write-Host "  Ensuring raw table '$KqlTableName' (+ JSON mapping '$KqlMappingName') exists in KQL Database '$EventhouseName'..."
    Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$($kqlDb.id)/updateDefinition" -Headers $Headers -Body $body | Out-Null
    Write-Host "  [OK] raw-capture table ready"

    return @{ EventhouseId = $ehId; KqlDatabaseId = $kqlDb.id }
}

function Get-EventstreamCustomEndpointConnection {
    <#
      Finds the CustomEndpoint source node on an Eventstream (via its topology) and returns its live
      Kafka connection details (fullyQualifiedNamespace, eventHubName, primaryConnectionString).
      This lets the deploy script auto-populate ENV_OpenLineage's Spark Kafka config instead of
      requiring the Kafka secret to be copy-pasted manually from the portal.
    #>
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$EventstreamId)
    $topology = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/eventstreams/$EventstreamId/topology" -Headers $Headers -Method Get -UseBasicParsing
    $topologyContent = $topology.Content | ConvertFrom-Json
    $source = $topologyContent.sources | Where-Object { $_.type -eq "CustomEndpoint" } | Select-Object -First 1
    if (-not $source) { throw "No CustomEndpoint source found on Eventstream $EventstreamId - cannot auto-configure the Kafka connection." }
    $conn = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/eventstreams/$EventstreamId/sources/$($source.id)/connection" -Headers $Headers -Method Get -UseBasicParsing
    return $conn.Content | ConvertFrom-Json
}

function Confirm-FabricEventstreamRunning {
    <# Ensures every source/destination node of an Eventstream is actually 'Running', not just
       'published'. An Eventstream can silently end up Paused (manual portal action, an underlying
       capacity pause/resume cycle, or a destination auto-pausing itself after an error) while the
       item itself still looks perfectly healthy from Get-FabricItemByName - the only way to see this
       is the topology endpoint's per-node 'status'. If any node isn't Running, this calls the
       whole-Eventstream resume endpoint and re-checks, so a stale-paused stream is self-healed on
       every deploy run instead of silently dropping all events (which manifests downstream as a
       confusing "table ol_lineage_events_v3 not found" in the destination preview / detection
       notebook, since a paused destination never gets the chance to auto-create its sink table).

       If the destination is STILL not Running after every automated resume attempt, that's not a
       simple pause - it's almost always the Eventhouse destination's one-time "dangling
       connectionName" issue (see NB_CopyEventDetection_SparkKafka.Notebook's header for the full
       root cause): a REST-created Eventhouse destination has no real Fabric Connection object
       backing it until an operator completes the portal's interactive "Configure"/Get data wizard
       (OAuth handshake) - there is no public REST API to do this. Rather than let the deploy
       continue on to Step 9's pipeline run (which fails with a confusing 404 from the Kusto query
       endpoint), this pauses and walks the operator through the one-time manual fix, then re-checks
       before continuing. This only happens the first time the destination is created - subsequent
       redeploys against the same Eventstream item keep the Connection object and won't re-prompt. #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$EventstreamId,
        [int]$MaxAttempts = 3,
        [int]$DelaySeconds = 15
    )
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $topology = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/eventstreams/$EventstreamId/topology" -Headers $Headers -Method Get -UseBasicParsing
        $topologyContent = $topology.Content | ConvertFrom-Json
        $nodes = @($topologyContent.sources) + @($topologyContent.destinations)
        $notRunning = $nodes | Where-Object { $_.status -and $_.status -notin @("Running", "Active") }
        if (-not $notRunning) {
            Write-Host "  [OK] Eventstream is Running ($($nodes.Count) node(s) checked)."
            return
        }
        $summary = ($notRunning | ForEach-Object { "$($_.name)=$($_.status)" }) -join ", "
        Write-Host "  Eventstream has node(s) not Running: $summary - attempting resume ($attempt/$MaxAttempts)..."
        try {
            Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/eventstreams/$EventstreamId/resume" -Headers $Headers -Body '{"startType":"Now"}' | Out-Null
        } catch {
            Write-Warning "  Resume call failed: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds $DelaySeconds
    }

    $portalUrl = "https://app.fabric.microsoft.com/groups/$WorkspaceId/eventstreams/$EventstreamId"
    while ($true) {
        Write-Warning "  Eventstream still has non-Running node(s) after $MaxAttempts automated resume attempt(s)."
        Write-Host ""
        Write-Host "  MANUAL STEP LIKELY REQUIRED (one-time only, first time this destination is created):"
        Write-Host "    1. Open: $portalUrl"
        Write-Host "    2. Click the 'RawCapture' destination node."
        Write-Host "    3. Confirm 'Eventhouse' and 'KQL Database' both resolve to a real item (not 'Item not found')."
        Write-Host "    4. If not, complete the destination wizard: Eventhouse -> KQL Database -> Get data -> select/inspect table 'ol_raw_events' -> Finish."
        Write-Host "    5. Click Publish if the canvas still shows 'Edit mode'."
        Write-Host ""
        $response = Read-Host "  Press Enter once done to re-check (or type 'skip' to continue without verifying)"
        if ($response -eq "skip") {
            Write-Warning "  Continuing without verifying the Eventstream is Running - Step 9's pipeline run may fail if the destination isn't actually configured."
            return
        }
        $topology = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/eventstreams/$EventstreamId/topology" -Headers $Headers -Method Get -UseBasicParsing
        $topologyContent = $topology.Content | ConvertFrom-Json
        $nodes = @($topologyContent.sources) + @($topologyContent.destinations)
        $notRunning = $nodes | Where-Object { $_.status -and $_.status -notin @("Running", "Active") }
        if (-not $notRunning) {
            Write-Host "  [OK] Eventstream is Running ($($nodes.Count) node(s) checked)."
            return
        }
        $summary = ($notRunning | ForEach-Object { "$($_.name)=$($_.status)" }) -join ", "
        Write-Host "  Still not Running: $summary"
    }
}

function Publish-FabricEnvironment {
    <# Applies a published environment's staged Spark settings/libraries - required after
       updateDefinition, since environment content changes only take effect once published. #>
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$EnvironmentId)
    Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/environments/$EnvironmentId/staging/publish" -Headers $Headers | Out-Null
}

function Wait-LakehouseSqlEndpoint {
    <# Polls the Lakehouse item until its SQL analytics endpoint is provisioned and returns its connection-string hostname. #>
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$LakehouseId, [int]$MaxAttempts = 40, [int]$DelaySeconds = 5)
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        $resp = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/lakehouses/$LakehouseId" -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        $sqlEp = $content.properties.sqlEndpointProperties
        if ($sqlEp -and $sqlEp.connectionString -and $sqlEp.provisioningStatus -eq "Success") {
            return $sqlEp.connectionString
        }
        Start-Sleep -Seconds $DelaySeconds
    }
    throw "Timed out waiting for the SQL analytics endpoint on Lakehouse $LakehouseId to provision."
}

function Sync-FabricLakehouseSqlEndpoint {
    <# Forces the Lakehouse's SQL analytics endpoint to re-sync its table metadata from the Delta
       log (POST .../sqlEndpoints/{id}/refreshMetadata). The endpoint normally syncs schema changes
       (e.g. newly ALTERed/added columns) automatically within about a minute, but that sync can lag
       noticeably behind a notebook write - and a Direct Lake semantic model (which reads through this
       same SQL endpoint, not directly against the Delta log) will fail with
       "We cannot access the source column ..." if it's queried/refreshed before the sync catches up.
       That failure mode is a real 400-level DAX/query error, NOT a 401/403, so Invoke-DaxQuery's own
       retry logic in Step 10 does NOT protect against it - this function must actually wait for a
       Success status itself, not just fire-and-forget once, to remove the race. #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$LakehouseId,
        [int]$MaxAttempts = 10,
        [int]$DelaySeconds = 10
    )
    $resp = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/lakehouses/$LakehouseId" -Headers $Headers -Method Get -UseBasicParsing
    $content = $resp.Content | ConvertFrom-Json
    $sqlEndpointId = $content.properties.sqlEndpointProperties.id
    if (-not $sqlEndpointId) { Write-Warning "  Could not resolve the Lakehouse's SQL endpoint id - skipping metadata sync."; return }

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/sqlEndpoints/$sqlEndpointId/refreshMetadata" -Headers $Headers
        $failed = $result.Body.value | Where-Object { $_.status -ne "Success" }
        if (-not $failed) {
            if ($attempt -gt 1) { Write-Host "  [OK] SQL endpoint metadata sync succeeded on attempt $attempt/$MaxAttempts." }
            return
        }
        if ($attempt -lt $MaxAttempts) {
            Write-Host "    SQL endpoint metadata sync attempt $attempt/$MaxAttempts still non-Success for: $(($failed | ForEach-Object { $_.tableName }) -join ', ') - retrying in ${DelaySeconds}s..."
            Start-Sleep -Seconds $DelaySeconds
        } else {
            Write-Warning "  SQL endpoint metadata sync still reported non-Success status for: $(($failed | ForEach-Object { $_.tableName }) -join ', ') after $MaxAttempts attempt(s) (~$($MaxAttempts * $DelaySeconds)s). Step 10's DAX smoke test may fail with a column-resolution error if this doesn't catch up - if so, just wait a bit and resume with -SkipSteps up through 9."
        }
    }
}

function Start-FabricNotebookRun {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$NotebookId)
    $resp = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances?jobType=RunNotebook" -Headers $Headers -Method Post -UseBasicParsing
    return $resp.Headers['Location']
}

function Start-FabricPipelineRun {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$PipelineId)
    $resp = Invoke-WebRequest -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$PipelineId/jobs/instances?jobType=Pipeline" -Headers $Headers -Method Post -UseBasicParsing
    return $resp.Headers['Location']
}

function Wait-FabricJobInstance {
    param([Parameter(Mandatory)][string]$JobInstanceUrl, [Parameter(Mandatory)][hashtable]$Headers, [int]$MaxAttempts = 100, [int]$DelaySeconds = 10)
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        Start-Sleep -Seconds $DelaySeconds
        $resp = Invoke-WebRequest -Uri $JobInstanceUrl -Headers $Headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        Write-Host "    job status: $($content.status)"
        if ($content.status -in @("Completed", "Failed", "Cancelled", "Deduped")) { return $content }
    }
    throw "Timed out waiting for job at $JobInstanceUrl"
}

function Get-OneLakeToken {
    [CmdletBinding()]
    param()
    $token = az account get-access-token --resource "https://storage.azure.com" --query accessToken -o tsv
    if (-not $token) { throw "Failed to acquire a storage.azure.com token via 'az account get-access-token'." }
    return $token
}

function Set-OneLakeFileContent {
    <#
      Writes (creates or overwrites) a single file under an item's OneLake Files/ path using the
      ADLS Gen2 DFS REST API (create -> append -> flush). Used to upload Files/config/config.json
      into the Lakehouse without needing it to be part of the item's git-tracked definition.
    #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][byte[]]$Bytes
    )
    $token = Get-OneLakeToken
    $headers = @{ Authorization = "Bearer $token" }
    $baseUri = "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId/$ItemId/$RelativePath"

    Invoke-WebRequest -Uri "$baseUri`?resource=file" -Headers $headers -Method Put -UseBasicParsing | Out-Null
    $appendHeaders = $headers.Clone()
    $appendHeaders["Content-Type"] = "application/octet-stream"
    Invoke-WebRequest -Uri "$baseUri`?action=append&position=0" -Headers $appendHeaders -Method Patch -Body $Bytes -UseBasicParsing | Out-Null
    Invoke-WebRequest -Uri "$baseUri`?action=flush&position=$($Bytes.Length)" -Headers $headers -Method Patch -UseBasicParsing | Out-Null
}

function Set-FabricItemSchedule {
    <# Enables/updates a Cron schedule for a pipeline (or notebook) job type. #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$ItemId,
        [string]$JobType = "Pipeline",
        [Parameter(Mandatory)][string]$CronExpression,
        [bool]$Enabled = $true,
        [string]$StartDateTime,
        [string]$EndDateTime,
        [string]$TimeZone = "UTC"
    )
    if (-not $StartDateTime) { $StartDateTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }
    if (-not $EndDateTime) { $EndDateTime = (Get-Date).ToUniversalTime().AddYears(5).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    $body = @{
        enabled = $Enabled
        configuration = @{
            type = "Cron"
            interval = $CronExpression
            startDateTime = $StartDateTime
            endDateTime = $EndDateTime
            localTimeZoneId = $TimeZone
        }
    } | ConvertTo-Json -Depth 10
    Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$ItemId/jobs/$JobType/schedules" -Headers $Headers -Body $body | Out-Null
}

function Get-PowerBiToken {
    <# The legacy Power BI REST API (api.powerbi.com) requires a token issued for its own
       resource/audience - a Fabric-audience token (api.fabric.microsoft.com) is rejected with a
       400 Bad Request even though many newer Fabric endpoints accept it. #>
    [CmdletBinding()]
    param()
    $token = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv
    if (-not $token) { throw "Failed to acquire a Power BI-scoped access token via 'az account get-access-token'." }
    return $token
}

function Start-FabricDatasetRefresh {
    <# A Direct Lake semantic model created via the REST API is NOT automatically "framed" - unlike
       Import mode, Direct Lake doesn't load any data on creation, and querying it before its first
       refresh can return a 403 that looks identical to (but is NOT) an ACL-propagation delay: no
       amount of waiting fixes it, only an actual refresh does, since that's the "reframe" operation
       that aligns the model's metadata with the current Delta tables and establishes the query-time
       security context. This calls the Enhanced Refresh API (POST .../datasets/{id}/refreshes) with
       type=full to trigger that reframe, retrying on 401/403 since even this trigger call can race
       the same brand-new-item ACL propagation as Invoke-DaxQuery. Returns the refresh request id
       (from the x-ms-request-id response header) for Wait-FabricDatasetRefresh to poll. #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DatasetId,
        [int]$MaxAttempts = 6,
        [int]$DelaySeconds = 15
    )
    $body = @{ type = "full"; commitMode = "transactional"; maxParallelism = 10; retryCount = 0 } | ConvertTo-Json
    $uri = "$script:PowerBiBaseUri/groups/$WorkspaceId/datasets/$DatasetId/refreshes"
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $token = Get-PowerBiToken
        $headers = Get-FabricHeaders -Token $token
        try {
            $resp = Invoke-WebRequest -Uri $uri -Headers $headers -Method Post -Body $body -UseBasicParsing
            $requestId = $resp.Headers['x-ms-request-id']
            if (-not $requestId) { $requestId = $resp.Headers['RequestId'] }
            if (-not $requestId) { throw "Refresh trigger succeeded but no request id was returned in the response headers." }
            return $requestId
        } catch {
            $statusCode = $_.Exception.Response.StatusCode.value__
            $isAuthTransient = ($statusCode -eq 401 -or $statusCode -eq 403)
            if ($isAuthTransient -and $attempt -lt $MaxAttempts) {
                Write-Host "    Refresh trigger attempt $attempt/$MaxAttempts got HTTP $statusCode (likely permission propagation delay for the new item) - retrying in ${DelaySeconds}s..."
                Start-Sleep -Seconds $DelaySeconds
                continue
            }
            throw
        }
    }
}

function Wait-FabricDatasetRefresh {
    <# Polls the Enhanced Refresh API's status endpoint until the refresh started by
       Start-FabricDatasetRefresh reaches a terminal state. "Unknown" means still in progress
       (Power BI's own term for "not yet finished", not an error). #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DatasetId,
        [Parameter(Mandatory)][string]$RefreshId,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 10
    )
    $uri = "$script:PowerBiBaseUri/groups/$WorkspaceId/datasets/$DatasetId/refreshes/$RefreshId"
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $token = Get-PowerBiToken
        $headers = Get-FabricHeaders -Token $token
        $resp = Invoke-WebRequest -Uri $uri -Headers $headers -Method Get -UseBasicParsing
        $content = $resp.Content | ConvertFrom-Json
        switch ($content.status) {
            "Completed" { return $content }
            "Failed" { throw "Dataset refresh failed: $($content.serviceExceptionJson)" }
            "Cancelled" { throw "Dataset refresh was cancelled." }
            "Disabled" { throw "Dataset refresh is disabled for this dataset." }
            default { Start-Sleep -Seconds $DelaySeconds }  # "Unknown" - still in progress
        }
    }
    throw "Timed out waiting for dataset refresh $RefreshId to complete after $MaxAttempts attempts."
}

function Confirm-FabricReportRuns {
    <# Validates that a just-published report actually renders, without using the executeQueries
       DAX API (that endpoint requires the separate tenant setting "Dataset Execute Queries REST
       API" under Admin Portal > Tenant settings > Integration settings to even be enabled, and its
       403 when disabled is indistinguishable from a real propagation-delay/permissions problem).
       Instead this uses two ordinary report-read endpoints that the Fabric portal itself relies on
       when opening a report, so they carry no such special gating:
         - GET reports/{id}          - confirms the report resolves and returns a live embedUrl
                                       and the expected bound dataset id (i.e., the binding is
                                       intact, not dangling).
         - GET reports/{id}/pages    - confirms the service can actually parse/serve the report's
                                       page list (a structural/rendering check beyond just
                                       "the definition JSON parsed", which Step 11 already checks
                                       separately via getDefinition). #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ReportId,
        [string]$ExpectedDatasetId,
        [int]$MaxAttempts = 6,
        [int]$DelaySeconds = 15
    )
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $token = Get-PowerBiToken
        $headers = Get-FabricHeaders -Token $token
        try {
            $reportResp = Invoke-WebRequest -Uri "$script:PowerBiBaseUri/groups/$WorkspaceId/reports/$ReportId" -Headers $headers -Method Get -UseBasicParsing
            $report = $reportResp.Content | ConvertFrom-Json
            if (-not $report.embedUrl) { throw "Report metadata returned no embedUrl - it may not be in a renderable state." }
            if ($ExpectedDatasetId -and $report.datasetId -ne $ExpectedDatasetId) {
                throw "Report is bound to dataset '$($report.datasetId)' but expected '$ExpectedDatasetId' - the semantic model binding looks stale/dangling."
            }
            $pagesResp = Invoke-WebRequest -Uri "$script:PowerBiBaseUri/groups/$WorkspaceId/reports/$ReportId/pages" -Headers $headers -Method Get -UseBasicParsing
            $pages = ($pagesResp.Content | ConvertFrom-Json).value
            if (-not $pages -or $pages.Count -eq 0) { throw "Report has no pages according to the pages API." }
            return @{ EmbedUrl = $report.embedUrl; DatasetId = $report.datasetId; PageCount = $pages.Count }
        } catch {
            $statusCode = $_.Exception.Response.StatusCode.value__
            $isAuthTransient = ($statusCode -eq 401 -or $statusCode -eq 403)
            if ($isAuthTransient -and $attempt -lt $MaxAttempts) {
                Write-Host "    Report validation attempt $attempt/$MaxAttempts got HTTP $statusCode (likely permission propagation delay for the new item) - retrying in ${DelaySeconds}s..."
                Start-Sleep -Seconds $DelaySeconds
                continue
            }
            throw
        }
    }
}

Export-ModuleMember -Function *
