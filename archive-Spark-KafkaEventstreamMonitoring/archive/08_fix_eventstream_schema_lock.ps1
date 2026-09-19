<#
.SYNOPSIS
  Fixes the "schema lock" bug where the Eventstream Lakehouse destination silently drops
  OpenLineage events whose inputs/outputs shape differs from the first-inferred Delta schema.

.PROBLEM
  ol_lineage_events / ol_lineage_events_v2: the Lakehouse destination auto-infers a Delta
  schema from the FIRST event it processes. OpenLineage inputs/outputs are recursive,
  highly variable array<struct<...>> shapes (depends on the source dataset's own schema).
  Once locked, any later event whose inputs/outputs shape doesn't match the original
  inferred struct is silently dropped with an internal OutputDataConversionError.
  Result: inputs/outputs columns always empty, or events missing entirely.

.FIX
  Add a SQL operator ("SqlFlatten") in the Eventstream topology that projects inputs/outputs
  through json_stringify(...) so they always land as a stable STRING type - no future event
  shape can ever break the destination's inferred schema again. Also filters to
  eventType = 'COMPLETE' only, since APPLICATION/START events are not useful for lineage matching.

  IMPORTANT: CAST(inputs AS NVARCHAR(MAX)) does NOT work in the Eventstream SQL engine for
  array/struct columns - it throws "Cannot cast type 'array' to type 'nvarchar(max)'".
  Use json_stringify(inputs) instead - confirmed working.

.NOTES
  Requires 01_fabric_rest_helpers.ps1 to be dot-sourced first (Get-FabricAuthHeaders, etc.)
  This script is a REFERENCE of the exact operator/topology JSON that was applied via
  updateDefinition to eventstream 92feabc7-d07f-449e-b713-64e53764ddf9 (ES_OpenLineageEvents)
  in workspace c5c50c6e-30d1-4d2e-8766-d82917e13592 (WS_SagarFabric01).
  See 07_eventstream_topology_FINAL_working.json for the complete, live-verified topology.
#>

$WorkspaceId   = "c5c50c6e-30d1-4d2e-8766-d82917e13592"   # WS_SagarFabric01
$EventstreamId = "92feabc7-d07f-449e-b713-64e53764ddf9"   # ES_OpenLineageEvents
$LakehouseId   = "c61c659f-ecba-4de8-a5b7-25885eb3021f"   # LH_ShortcutMonitoring

# The SQL operator that flattens inputs/outputs to stable JSON strings.
# NOTE: json_stringify(), not CAST(... AS NVARCHAR(MAX)) - CAST fails on array/struct types.
$sqlFlattenQuery = @"
SELECT eventTime, producer, eventType, run, job,
       json_stringify(inputs) AS inputs_json,
       json_stringify(outputs) AS outputs_json
INTO [FlattenedStream]
FROM [ES_OpenLineageEvents-stream]
WHERE eventType = 'COMPLETE'
"@

Write-Host "Reference query used in the SqlFlatten operator:"
Write-Host $sqlFlattenQuery

# Topology fragments added to the Eventstream definition (see 07_eventstream_topology_FINAL_working.json
# for the full, authoritative definition actually applied):
#
# operators: [{
#   "name": "SqlFlatten", "type": "SQL",
#   "inputNodes": [{ "name": "ES_OpenLineageEvents-stream" }],
#   "properties": {
#     "query": "<sqlFlattenQuery above, single line>",
#     "advancedSettings": {
#       "eventsOutOfOrderPolicy": "Adjust",
#       "eventsOutOfOrderMaxDelayInSeconds": 5,
#       "eventsLateArrivalMaxDelayInSeconds": 300
#     }
#   }
# }]
#
# streams: [{
#   "name": "FlattenedStream", "type": "DerivedStream",
#   "properties": { "inputSerialization": { "type": "Json", "properties": { "encoding": "UTF8" } } },
#   "inputNodes": [{ "name": "SqlFlatten" }]
# }]
#
# destinations[0].inputNodes -> [{ "name": "FlattenedStream" }]
# destinations[0].properties.deltaTable -> "ol_lineage_events_v3"  (NEW table - old v1/v2 tables
#   already had their schema permanently locked to the bad inferred shape and cannot be reused)

# To re-apply: dot-source 01_fabric_rest_helpers.ps1, load 07_eventstream_topology_FINAL_working.json,
# base64-encode it, and push via New-FabricItemDefinition -ItemTypePlural "eventstreams" with part
# path "eventstream.json" (payloadType InlineBase64), including the existing ".platform" part.
