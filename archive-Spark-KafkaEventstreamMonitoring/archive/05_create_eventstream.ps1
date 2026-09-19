# Creates the ES_OpenLineageEvents Eventstream and retrieves its auto-provisioned
# Kafka connection details (bootstrap servers, topic name, SAS connection string).
# Prerequisite: run 04_build_eventstream_topology.py first.
. "$PSScriptRoot\01_fabric_rest_helpers.ps1"

$WorkspaceId = "c5c50c6e-30d1-4d2e-8766-d82917e13592"   # WS_SagarFabric01
$FolderId    = "e06c951e-bd85-4be6-9ee2-93f45627903d"   # Spark - Kafka based solution

$topoB64 = Get-Content "$env:TEMP\eventstream_topology_b64.txt" -Raw
$headers = Get-FabricAuthHeaders

# --- Create the Eventstream item (no separate publish step needed, unlike Environment) ---
$createBody = @{
    displayName = "ES_OpenLineageEvents"
    description = "Kafka-compatible custom endpoint ingesting OpenLineage run events, routed to a Lakehouse sink table"
    folderId    = $FolderId
    definition  = @{
        parts = @(
            @{ path = "eventstream.json"; payload = $topoB64; payloadType = "InlineBase64" }
        )
    }
} | ConvertTo-Json -Depth 10

$resp = Invoke-WebRequest -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams" -Headers $headers -Method Post -Body $createBody -TimeoutSec 60
if ($resp.StatusCode -eq 202) {
    $op = Wait-FabricLro -OperationUrl $resp.Headers['Location'] -Headers $headers
    $item = Invoke-RestMethod -Uri "$($resp.Headers['Location'])/result" -Headers $headers
} else {
    $item = $resp.Content | ConvertFrom-Json
}
$EventstreamId = $item.id
Write-Output "Eventstream created: $EventstreamId"

# --- Get the topology to find the source node ID ---
Start-Sleep -Seconds 5
$topo = Invoke-RestMethod -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId/topology" -Headers $headers -Method Get
$SourceId = $topo.sources[0].id
Write-Output "Source node ID: $SourceId"

# --- Retrieve the real Kafka connection details for the auto-provisioned Event Hub ---
$conn = Invoke-RestMethod -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId/sources/$SourceId/connection" -Headers $headers -Method Get
Write-Output "bootstrap.servers = $($conn.fullyQualifiedNamespace):9093"
Write-Output "topicName         = $($conn.eventHubName)"
Write-Output "primaryConnectionString (SAS) retrieved -- feed into 02_build_environment.py's SAS_CONNECTION_STRING"
# NOTE: do not write $conn.accessKeys.primaryConnectionString to disk in plaintext long-term;
# it is only needed transiently to build the Environment's Sparkcompute.yml.
