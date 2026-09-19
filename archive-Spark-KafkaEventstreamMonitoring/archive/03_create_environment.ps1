# Creates (first time) or updates + publishes the ENV_OpenLineage Fabric Environment.
# Prerequisite: run 02_build_environment.py first to produce sparkcompute_final_b64.txt.
. "$PSScriptRoot\01_fabric_rest_helpers.ps1"

$WorkspaceId   = "c5c50c6e-30d1-4d2e-8766-d82917e13592"   # WS_SagarFabric01
$FolderId      = "e06c951e-bd85-4be6-9ee2-93f45627903d"   # Spark - Kafka based solution
$EnvironmentId = "6b3991b0-08cf-484b-bef3-beff3a6b8861"   # ENV_OpenLineage (already created)

$b64 = Get-Content "$env:TEMP\sparkcompute_final_b64.txt" -Raw

# --- To CREATE a brand-new Environment item (first time only) ---
# $headers = Get-FabricAuthHeaders
# $createBody = @{
#     displayName = "ENV_OpenLineage"
#     description = "OpenLineage + Kafka transport Spark environment for shortcut monitoring"
#     folderId    = $FolderId
#     definition  = @{
#         parts = @(
#             @{ path = "Setting/Sparkcompute.yml"; payload = $b64; payloadType = "InlineBase64" }
#         )
#     }
# } | ConvertTo-Json -Depth 10
# $resp = Invoke-WebRequest -UseBasicParsing -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/environments" -Headers $headers -Method Post -Body $createBody -TimeoutSec 60
# $op = Wait-FabricLro -OperationUrl $resp.Headers['Location'] -Headers $headers
# $EnvironmentId = (Invoke-RestMethod -Uri "$($resp.Headers['Location'])/result" -Headers $headers).id

# --- To UPDATE the existing Environment's Sparkcompute.yml ---
$parts = @(
    @{ path = "Setting/Sparkcompute.yml"; payload = $b64; payloadType = "InlineBase64" }
)
$op = New-FabricItemDefinition -WorkspaceId $WorkspaceId -ItemTypePlural "environments" -ItemId $EnvironmentId -Parts $parts
Write-Output "updateDefinition status: $($op.status)"

# Environments require an explicit publish step to activate the staged definition.
$pub = Publish-FabricEnvironment -WorkspaceId $WorkspaceId -EnvironmentId $EnvironmentId
Write-Output "publish status: $($pub.status)"
