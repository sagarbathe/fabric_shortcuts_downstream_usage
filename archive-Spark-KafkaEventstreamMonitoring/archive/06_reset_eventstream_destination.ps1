# Fix for: Eventstream Lakehouse destination reports "Running" and messages are
# visibly flowing (confirmed in Eventstream portal "Data preview"), but zero rows
# ever land in the target Delta table and the table folder never appears.
#
# Root cause observed in this build: the destination's target Delta table
# (ol_lineage_events) was dropped externally via a notebook `DROP TABLE` while the
# Eventstream destination was still bound to it. The destination's internal state
# became stale/corrupted -- it kept reporting "Running" but silently stopped
# writing, and never recreated the table.
#
# Fix: update the Eventstream's eventstream.json definition to repoint the
# destination's `deltaTable` property at a brand-new table name, then push via
# updateDefinition. This cleanly reinitializes the destination's write path.
#
# DO NOT drop/delete an Eventstream's target Lakehouse table externally while the
# Eventstream is running -- if you need a schema reset, use this script (change
# the table name) instead of dropping the table out from under it.

. "$PSScriptRoot\01_fabric_rest_helpers.ps1"

$WorkspaceId   = "c5c50c6e-30d1-4d2e-8766-d82917e13592"   # WS_SagarFabric01
$EventstreamId = "92feabc7-d07f-449e-b713-64e53764ddf9"   # ES_OpenLineageEvents
$NewTableName  = "ol_lineage_events_v3"   # bump this each time you need a fresh reset

$headers = Get-FabricAuthHeaders

# 1. Fetch the current definition
$def = Invoke-WebRequest -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId/getDefinition" -Headers $headers -Method Post -Body '{}' -ContentType "application/json" -TimeoutSec 60
$defJson = $def.Content | ConvertFrom-Json

# 2. Edit the eventstream.json part: change destinations[0].properties.deltaTable
$esPart = $defJson.definition.parts | Where-Object { $_.path -eq 'eventstream.json' }
$topologyBytes = [Convert]::FromBase64String($esPart.payload)
$topology = [System.Text.Encoding]::UTF8.GetString($topologyBytes) | ConvertFrom-Json
$topology.destinations[0].properties.deltaTable = $NewTableName
$newPayload = $topology | ConvertTo-Json -Depth 20
$esPart.payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newPayload))

# 3. Push the updated definition
$body = @{ definition = $defJson.definition } | ConvertTo-Json -Depth 20 -Compress
$resp = Invoke-WebRequest -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId/updateDefinition" -Headers $headers -Method Post -Body $body -TimeoutSec 60
Write-Output "updateDefinition status: $($resp.StatusCode)"

# 4. Poll the destination status until it returns to "Running" with the new table name
for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep -Seconds 15
    $topoNow = Invoke-RestMethod -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId/topology" -Headers $headers -Method Get
    $st = $topoNow.destinations[0].status
    Write-Output "iter=$i destination status=$st"
    if ($st -eq 'Running') { break }
}

Write-Output "Done. New sink table: $NewTableName -- trigger a new notebook run (real Spark read/write) to generate fresh OpenLineage events, then check Tables/$NewTableName in the Lakehouse."
