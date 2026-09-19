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
    $body = @{ folderId = $FolderId } | ConvertTo-Json
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

function New-FabricLakehouse {
    param([Parameter(Mandatory)][string]$WorkspaceId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$DisplayName)
    $existing = Get-FabricItemByName -WorkspaceId $WorkspaceId -Headers $Headers -DisplayName $DisplayName -Type "Lakehouse"
    if ($existing) { return $existing.id }
    $body = @{ displayName = $DisplayName; type = "Lakehouse" } | ConvertTo-Json
    $result = Invoke-FabricRequest -Method Post -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items" -Headers $Headers -Body $body
    return $result.Body.id
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

function Invoke-DaxQuery {
    param([Parameter(Mandatory)][string]$WorkspaceName, [Parameter(Mandatory)][string]$DatasetId, [Parameter(Mandatory)][hashtable]$Headers, [Parameter(Mandatory)][string]$Dax)
    $body = @{ queries = @(@{ query = $Dax }); serializerSettings = @{ includeNulls = $true } } | ConvertTo-Json -Depth 10
    $uri = "$script:PowerBiBaseUri/datasets/$DatasetId/executeQueries"
    $resp = Invoke-WebRequest -Uri $uri -Headers $Headers -Method Post -Body $body -UseBasicParsing
    return $resp.Content | ConvertFrom-Json
}

Export-ModuleMember -Function *
