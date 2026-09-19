# Fabric REST API helper functions used throughout this build.
# Requires: az CLI authenticated to the target tenant (az login).
#
# Usage: dot-source this file, then call the functions, e.g.:
#   . .\01_fabric_rest_helpers.ps1
#   $headers = Get-FabricAuthHeaders
#   Invoke-FabricGet -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $headers

function Get-FabricAuthHeaders {
    param([string]$ContentType = "application/json")
    $token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
    return @{ Authorization = "Bearer $token"; "Content-Type" = $ContentType }
}

function Get-OneLakeAuthHeaders {
    # OneLake DFS API (blob/file operations on Files/Tables) uses the storage.azure.com resource
    $token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv
    return @{ Authorization = "Bearer $token" }
}

function Wait-FabricLro {
    # Polls a long-running-operation Location header until Succeeded/Failed.
    param(
        [Parameter(Mandatory)][string]$OperationUrl,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$MaxAttempts = 12,
        [int]$DelaySeconds = 5
    )
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        Start-Sleep -Seconds $DelaySeconds
        $op = Invoke-RestMethod -UseBasicParsing -Uri $OperationUrl -Headers $Headers -Method Get
        if ($op.status -in @('Succeeded', 'Failed')) { return $op }
    }
    return $op
}

function New-FabricItemDefinition {
    # Creates or updates an item's definition (base64 ipynb/yml/json parts) and republishes if needed.
    # $Parts: array of @{ path = "..."; payload = "<base64>"; payloadType = "InlineBase64" }
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemTypePlural,   # e.g. "notebooks", "environments", "eventstreams"
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][array]$Parts,
        [string]$Format = $null
    )
    $headers = Get-FabricAuthHeaders
    $def = @{ parts = $Parts }
    if ($Format) { $def.format = $Format }
    $body = @{ definition = $def } | ConvertTo-Json -Depth 20 -Compress
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/$ItemTypePlural/$ItemId/updateDefinition"
    $resp = Invoke-WebRequest -UseBasicParsing -Uri $uri -Headers $headers -Method Post -Body $body -TimeoutSec 60
    if ($resp.StatusCode -eq 202) {
        $op = Wait-FabricLro -OperationUrl $resp.Headers['Location'] -Headers $headers
        return $op
    }
    return $resp
}

function Publish-FabricEnvironment {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$EnvironmentId
    )
    $headers = Get-FabricAuthHeaders
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/environments/$EnvironmentId/staging/publish"
    $resp = Invoke-WebRequest -UseBasicParsing -Uri $uri -Headers $headers -Method Post -Body '{}' -TimeoutSec 60
    if ($resp.StatusCode -eq 202) {
        return Wait-FabricLro -OperationUrl $resp.Headers['Location'] -Headers $headers -MaxAttempts 24 -DelaySeconds 10
    }
    return $resp
}

function Invoke-FabricNotebookRun {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$NotebookId
    )
    $headers = Get-FabricAuthHeaders
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/notebooks/$NotebookId/jobs/execute/instances"
    $resp = Invoke-WebRequest -UseBasicParsing -Uri $uri -Headers $headers -Method Post -Body '{}' -TimeoutSec 60
    return $resp.Headers['Location']   # job instance URL
}

function Wait-FabricNotebookJob {
    param(
        [Parameter(Mandatory)][string]$JobInstanceUrl,
        [int]$MaxAttempts = 45,
        [int]$DelaySeconds = 15
    )
    $headers = Get-FabricAuthHeaders
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        Start-Sleep -Seconds $DelaySeconds
        try {
            $st = Invoke-RestMethod -UseBasicParsing -Uri $JobInstanceUrl -Headers $headers -Method Get
            Write-Output "iter=$i status=$($st.status)"
            if ($st.status -in @('Completed', 'Failed', 'Cancelled')) { return $st }
        } catch {
            Write-Output "iter=$i transient error: $($_.Exception.Message)"
        }
    }
    return $st
}

function Get-OneLakeFile {
    # Downloads a file from a Lakehouse's Files/ or Tables/ path via the OneLake DFS API.
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemId,      # Lakehouse item ID
        [Parameter(Mandatory)][string]$RelativePath # e.g. "Files/validation_result.json"
    )
    $headers = Get-OneLakeAuthHeaders
    $uri = "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId/$ItemId/$RelativePath"
    return Invoke-RestMethod -UseBasicParsing -Uri $uri -Headers $headers -Method Get
}

function Get-OneLakeDirectoryListing {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][string]$Directory,   # e.g. "<itemId>/Tables"
        [bool]$Recursive = $false
    )
    $headers = Get-OneLakeAuthHeaders
    $uri = "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId`?resource=filesystem&recursive=$($Recursive.ToString().ToLower())&directory=$Directory"
    return Invoke-RestMethod -UseBasicParsing -Uri $uri -Headers $headers -Method Get
}
