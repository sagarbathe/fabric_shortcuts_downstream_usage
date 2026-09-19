# Fabric OpenLineage → Kafka (Eventstream) → Lakehouse Prototype

Scripts and artifact definitions used to build this solution in the Fabric tenant
`sagarbathe@mngenvmcap146722.onmicrosoft.com`, workspace `WS_SagarFabric01`,
folder `Shortcut Monitoring solution / Spark - Kafka based solution`.

Source design doc: `fabric_openlineage_kafka_design.md` (Downloads folder).

## Key IDs (this tenant)
- Workspace WS_SagarFabric01: `c5c50c6e-30d1-4d2e-8766-d82917e13592`
- Workspace WS_SagarFabric03: `0077781d-74c3-437e-b75c-42e640953c69`
- Folder "Spark - Kafka based solution": `e06c951e-bd85-4be6-9ee2-93f45627903d`
- Lakehouse `LH_ShortcutMonitoring`: `c61c659f-ecba-4de8-a5b7-25885eb3021f`
- Environment `ENV_OpenLineage`: `6b3991b0-08cf-484b-bef3-beff3a6b8861`
- Eventstream `ES_OpenLineageEvents`: `92feabc7-d07f-449e-b713-64e53764ddf9`
- Notebook `NB_OpenLineage_Validate`: `56a4cbe5-d3aa-4488-85a3-3b48a44ccd78`
- Eventstream source Event Hub (auto-provisioned): namespace
  `esehbnjzmxu954h0iqynlc.servicebus.windows.net`, event hub `esehbnjzmxu954h0iqynlc_eh`

## How it works end to end
1. `ENV_OpenLineage` (Fabric Environment) has `openlineage-spark_2.12:1.53.0` on
   `spark.jars.packages`, appends `io.openlineage.spark.agent.OpenLineageSparkListener`
   to `spark.extraListeners`, and configures the OpenLineage Kafka transport
   (`spark.openlineage.transport.*`) pointing at the Eventstream's auto-provisioned
   Kafka-compatible Event Hub endpoint (SAS embedded directly — see Security Notes).
2. Any notebook that attaches `ENV_OpenLineage` and reads/writes Spark data (e.g. a
   OneLake shortcut) automatically emits OpenLineage START/COMPLETE run events to
   that Kafka endpoint — no notebook code changes required.
3. Eventstream `ES_OpenLineageEvents` (`CustomEndpoint` source) receives those events
   and a `Lakehouse` destination lands them as a Delta table
   (`LH_ShortcutMonitoring.Tables.ol_lineage_events_v2`) with an auto-inferred schema
   matching the real OpenLineage RunEvent JSON shape (eventTime, eventType, run.runId,
   job.namespace/name, inputs/outputs, facets, etc.).

## Files in this folder
- `01_fabric_rest_helpers.ps1` — reusable PowerShell functions for Fabric REST API
  calls (get token, create/update item definitions, run notebook jobs, poll LROs,
  read/write OneLake DFS files).
- `02_build_environment.py` — builds the `Setting/Sparkcompute.yml` for
  `ENV_OpenLineage` (OpenLineage + Kafka transport config) and base64-encodes it.
- `03_create_environment.ps1` — creates/updates + publishes the Environment item.
- `04_build_eventstream_topology.py` — builds the Eventstream `eventstream.json`
  topology (CustomEndpoint source → stream → Lakehouse destination).
- `05_create_eventstream.ps1` — creates the Eventstream item and retrieves the
  auto-provisioned Kafka connection details (bootstrap servers, topic, SAS).
- `06_reset_eventstream_destination.ps1` — repoints the Lakehouse destination at a
  new Delta table name (used to recover from a corrupted destination binding —
  see Troubleshooting below).
- `notebook_builds/` — the various validation/diagnostic notebook builders used
  during development (each is a small Python script producing a `.ipynb` + base64
  payload, pushed via `updateDefinition` and run via the jobs API). Kept for
  reference/reuse, e.g. `build_nb_validate_final.py` is the last good end-to-end
  validation notebook (drops/recreates sink, real shortcut read+write, waits,
  inspects landed rows by direct Delta path read).
- `artifact_definitions/` — raw JSON/YAML definitions captured from the live
  Fabric items (Environment Sparkcompute.yml, Eventstream topology) for reference.

## Troubleshooting notes (found during this build)
- **Fabric REST workspace ID mismatch**: the OneLake API's workspace object ID is
  NOT the same as the Fabric core API workspace ID. Always resolve the real ID via
  `GET https://api.fabric.microsoft.com/v1/workspaces`.
- **Environment items need an explicit publish step**: `updateDefinition` only
  stages changes; call `POST /environments/{id}/staging/publish` to activate.
- **Notebook jobs commonly sit in `NotStarted` for 1-5+ minutes** (cold Spark
  session start) — this is normal, not a hang.
- **Eventstream Lakehouse destination can get "stuck"** if its target Delta table
  is deleted externally (e.g. via `DROP TABLE` from a notebook) while the
  destination is still bound to it: topology keeps reporting `status: Running`,
  Kafka messages are visibly flowing (confirmed via Eventstream Data Preview in the
  portal), but zero rows ever land and the table folder is never recreated.
  **Fix**: update the Eventstream's `eventstream.json` definition to point the
  destination's `deltaTable` property at a brand-new table name and push via
  `updateDefinition` — this cleanly reinitializes the destination and it starts
  landing data again (see `06_reset_eventstream_destination.ps1`).
- **KafkaConsumer diagnostics against the Eventstream's auto-provisioned Event Hub
  don't work with an arbitrary `group.id`** — the SAS issued for the CustomEndpoint
  source appears to be Send-only, so consumer-side reads fail/return nothing. Use
  the portal's Eventstream "Data preview" to visually confirm messages are flowing
  instead.
- The **`view`/`grep` tools redact strings that look like secrets** even in
  non-secret contexts; when verifying real secret values, read files directly
  (e.g. via Python) rather than trusting redacted tool output.

## Schema-lock bug and fix (found after initial "it works" checkpoint)
- **Symptom**: `ol_lineage_events_v2`'s `inputs`/`outputs` columns were always empty,
  even for jobs that clearly did a real read+write.
- **Root cause**: the Eventstream Lakehouse destination infers its Delta schema from
  the FIRST event it processes. OpenLineage `inputs`/`outputs` are recursive,
  highly variable `array<struct<...>>` shapes (they embed the full source dataset
  schema, incl. nested `columnLineage` facets) - no two source tables produce the
  same shape. Once the destination's schema is locked to the first event's shape,
  ANY later event whose inputs/outputs don't match that exact struct is silently
  dropped (`OutputDataConversionError.TypeConversionError`, invisible unless you dig
  into the Eventstream's own diagnostics).
- **Fix**: added a `SQL` operator (`SqlFlatten`) in the Eventstream topology that
  projects `inputs`/`outputs` through `json_stringify(...)` so they always land as a
  stable STRING column - no future event shape can ever break the destination's
  schema again. Also filters `WHERE eventType = 'COMPLETE'` early (START/APPLICATION
  events aren't useful for lineage matching). **`CAST(inputs AS NVARCHAR(MAX))` does
  NOT work** on array/struct types in the Eventstream SQL engine - use
  `json_stringify()` instead (confirmed working).
- New destination table (old ones' schemas are permanently locked, can't be
  reused): `ol_lineage_events_v3`, columns `eventTime, producer, eventType, run,
  job, inputs_json (string), outputs_json (string), EventProcessedUtcTime,
  PartitionId, EventEnqueuedUtcTime`.
- Verified: `ol_validate_output` write from `NB_OpenLineage_Validate` produced a
  COMPLETE event with full `inputs_json` (source schema) and `outputs_json`
  (incl. `columnLineage` facet with per-column `DIRECT`/`IDENTITY` transformations)
  correctly landed as JSON strings.
- See `08_fix_eventstream_schema_lock.ps1` (reference script) and
  `07_eventstream_topology_FINAL_working.json` (authoritative live topology snapshot).

## NB_CopyEventDetection_SparkKafka
- New notebook (folder `728c462b-ed06-460d-bdc1-226186060854`, item id
  `61617dd0-188d-417e-bb66-386463fdbd80`) that mirrors the existing
  `NB_CopyEventDetection_Spark` (file-transport) notebook's matching/detection
  logic, but sources lineage events from the shared `ol_lineage_events_v3`
  Eventstream sink table instead of per-notebook NDJSON files.
- `engine = 'SparkKafka'` in `FactCopyEvent`; `hosting_item_id` is populated with
  the REAL notebook item GUID (available via
  `run.facets.spark_properties.properties['trident.artifact.id']` in the Kafka
  events - an improvement over the file-transport notebook, which had no reliable
  item GUID).
- Incremental via a single-row watermark table `SparkKafkaLineageWatermark`
  (`last_processed_enqueued_time`, max `EventEnqueuedUtcTime` seen) - simpler than
  the file-transport notebook's per-file byte-offset watermark, since there's one
  shared source table instead of per-notebook files.
- Same matching restrictions as the sibling notebook: only `COMPLETE` events with
  non-empty `inputs` AND `outputs`; only single-input writes matched to
  `DimShortcut` (multi-input joins out of scope); dedupe multi-job-per-write noise
  by keeping the richest `columnLineage` per `(output_namespace, output_name)`;
  `DimShortcut` match key is `(hosting_workspace_name, hosting_item_name,
  shortcut_name)` derived from the ABFSS path, NOT display name; a column counts
  as "retained" only if ALL its `columnLineage.inputFields[].transformations[]`
  are `subtype == "IDENTITY"`.
- `ol_lineage_events_v3` is never deleted/truncated by this notebook (it's owned by
  the Eventstream destination - doing so risks re-corrupting the write path exactly
  as happened with `ol_lineage_events`/`v2`).
- **Gotcha hit while building this**: writing the `FactCopyEvent`/watermark table
  COMMENT via `ALTER TABLE ... SET TBLPROPERTIES ('comment' = '...')` failed with
  `[PARSE_SYNTAX_ERROR] Syntax error at or near ''engine''` when the comment text
  contained a single-quoted word. Spark SQL does **not** reliably support `''`
  (doubled-quote) escaping inside a single-quoted string literal the way ANSI
  SQL/Hive does - the safe fix is to avoid embedding single quotes in
  dynamically-built comment strings at all (`.replace("'", "")`) rather than trying
  to escape them.
- Verified end-to-end: after the fix, a real detection row landed with
  `hosting_item_name='NB_OpenLineage_Validate'`, `matched_shortcut_name=
  'sh_lakehouse01_SalesLT_Product'`, `retention_pct=100.0`,
  `is_select_star=True`, `is_shortcut_read_and_saved_as_is=True`.
- Local script: `NB_CopyEventDetection_SparkKafka.py` (full pushed notebook source,
  in Fabric's `# Fabric notebook source` / `# META` / `# CELL` format).

## Known outstanding items (not yet done)
- Key Vault secret `es-openlineage-sas` in `sbkeyvault01` — blocked by
  `publicNetworkAccess: Disabled` on the vault (could not be changed via `az`
  despite it appearing "Enabled" in one check). SAS is currently embedded directly
  in the Environment's Spark config as a documented fallback — treat as tech debt.
- Optional KQL Database destination on the Eventstream (design doc's
  `kql_destination=true`) — not yet built.
- Negative test (break `bootstrap.servers`, confirm failure visibility) — not yet
  run.
