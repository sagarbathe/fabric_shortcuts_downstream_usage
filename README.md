# Fabric Shortcuts Downstream Usage Monitoring

Microsoft Fabric solution that detects when a OneLake **shortcut** is read and then
**saved "as-is"** (copy/CTAS/INSERT-SELECT with no material transformation) by a
downstream Lakehouse or Warehouse item — and, as a Phase-2 enhancement, correlates that
with native Spark/OpenLineage lineage events. It flags duplicate shortcuts pointing at the
same source across the tenant.

See [`docs/architecture/Shortcut Monitoring Solution - Design Document.md`](docs/architecture/Shortcut%20Monitoring%20Solution%20-%20Design%20Document.md)
for the full design (architecture, data model, thresholds, rules).

## Repo layout

```
├── README.md
├── config/
│   └── config.example.json        # config.json schema doc (no secrets)
├── docs/
│   └── architecture/
│       └── Shortcut Monitoring Solution - Design Document.md
└── fabric/                        # Fabric-native items, grouped by type
    ├── notebooks/
    │   ├── NB_ShortcutInventory_DuplicateDetection.Notebook/
    │   ├── NB_CopyEventDetection_Warehouse.Notebook/
    │   ├── NB_CopyEventDetection_SparkKafka.Notebook/
    │   ├── NB_OpenLineage_SparkLineageTest.Notebook/
    │   └── NB_OpenLineage_Validate.Notebook/
    ├── pipelines/
    │   └── PL_ShortcutMonitoringOrchestrator.DataPipeline/
    ├── environment/
    │   └── ENV_OpenLineage.Environment/
    └── eventstreams/
        └── ES_OpenLineageEvents.Eventstream/
```

> **Fabric Git sync note:** each Fabric item folder (`<DisplayName>.<ItemType>`) must still sit
> directly inside the folder that the Fabric workspace's Git connection points at — Fabric mirrors
> folder structure 1:1 between the repo and the workspace. This repo's items were regrouped by type
> under `fabric/notebooks/`, `fabric/pipelines/`, `fabric/environment/`, `fabric/eventstreams/`; to
> keep the live `Shortcut Monitoring solution` workspace in sync, create matching folders
> (`notebooks`, `pipelines`, `environment`, `eventstreams`) inside that workspace folder, move each
> item into its corresponding folder there, then reconnect/sync — otherwise Git sync will show these
> items as moved/conflicting until the workspace side matches.

## Item reference

| Item | Type | Purpose |
|---|---|---|
| `LH_ShortcutMonitoring` | Lakehouse | Hosts the solution's own config (`Files/config/config.json`), dimension/fact tables (`DimShortcut`, `FactShortcutInventoryDiff`, `FactDuplicateShortcutGroup`, `FactCopyEvent`), and error logs. |
| `NB_ShortcutInventory_DuplicateDetection.Notebook` | Notebook | Phase-1 MVP. Enumerates Lakehouse/Warehouse shortcuts across monitored workspaces via the monitoring service principal, upserts `DimShortcut`, computes shortcut add/remove lifecycle events, and flags duplicate shortcuts (same hosting item = High severity; same hosting workspace/different item = Medium; cross-workspace same-source is not flagged). |
| `NB_CopyEventDetection_Warehouse.Notebook` | Notebook | Phase-1 MVP, **Warehouse engine**. Mines Fabric Warehouse Query Insights (`exec_requests_history`) for CTAS/INSERT-SELECT statements sourced from a known shortcut and records them to `FactCopyEvent` (`engine = 'Warehouse'`). |
| `NB_CopyEventDetection_SparkKafka.Notebook` | Notebook | Kafka/Eventstream-based variant of the Spark copy-event rule; writes to `FactCopyEvent` with `engine = 'SparkKafka'`, fed by the `ES_OpenLineageEvents` Eventstream. |
| `NB_OpenLineage_SparkLineageTest.Notebook` | Notebook | Phase-2 spike/prototype — read-only investigation of Spark/OpenLineage event capture; does not touch production tables/config. |
| `NB_OpenLineage_Validate.Notebook` | Notebook | Validation notebook exercising a real shortcut read + save so the `ENV_OpenLineage` environment's `OpenLineageSparkListener` emits START/COMPLETE lineage events to the Kafka transport for testing. |
| `ENV_OpenLineage.Environment` | Environment | Spark environment with the OpenLineage listener configured, attached to the OpenLineage-instrumented notebooks. |
| `ES_OpenLineageEvents.Eventstream` | Eventstream | Ingests OpenLineage events (emitted by `ENV_OpenLineage`) for downstream copy-event correlation. |
| `PL_ShortcutMonitoringOrchestrator.DataPipeline` | Data Pipeline | Orchestrates the solution: runs `NB_ShortcutInventory_DuplicateDetection` first, then fans out to `NB_CopyEventDetection_Warehouse` and `NB_CopyEventDetection_SparkKafka` in parallel (each only does work if enabled in config). Has a 15-minute Cron schedule, **created disabled** — enable it in the Fabric portal (Settings → Schedule) once the solution is validated. |

## Configuration (`config.json`)

Each notebook reads `Files/config/config.json` from its own attached `LH_ShortcutMonitoring`
Lakehouse at runtime (resolved dynamically via `notebookutils.runtime.context`, so no workspace/
lakehouse IDs are hardcoded in the notebooks — reattaching to a different Lakehouse/workspace is
enough to retarget the whole solution). See [`config/config.example.json`](config/config.example.json) for the
full schema (**not** the live file — the real `config.json` with the live secret lives only in the
deployed Lakehouse, never in this repo). Key sections:

| Section | Purpose |
|---|---|
| `monitoredWorkspaces` | List of `{workspaceName, workspaceId}` — the workspaces scanned for shortcuts/copy events. Edit this to add/remove workspaces from monitoring without touching any notebook code. |
| `detection.columnRetentionThresholdPercent` | The "saved as-is" column-retention threshold used by both copy-event engines. |
| `detection.excludeWarehouseNamePatterns` | Auto-generated Dataflow staging warehouses to ignore during Warehouse-engine detection. |
| `orchestration.enabledEngines` | `["warehouse", "sparkKafka"]` by default — lets you disable an engine (e.g. no Eventstream wired up in a dev workspace) purely via config; the pipeline still calls both notebooks every run, and a disabled notebook exits immediately without doing work. |
| `auth` | Service principal `tenantId`/`clientId`/`clientSecret` used for Fabric Admin REST API calls. **`clientSecret` is entered manually into the deployed `config.json` on OneLake — it is plaintext, not Key Vault-backed.** This was a deliberate, temporary simplification; revisit before wider production rollout. |

Note: run **cadence** (how often the pipeline fires) is controlled solely by the Data Pipeline's own
schedule trigger — there is no `polling.*` config section, since a config value inside a notebook
can't retroactively change a platform-level schedule that already decided to run it.

## Prerequisites

- A Microsoft Fabric workspace (capacity must be running) with a monitoring service
  principal granted read access to all monitored workspaces.
- `config.json` deployed to `LH_ShortcutMonitoring/Files/config/config.json` with the monitored
  workspace list, detection thresholds, and the service principal's `clientSecret` filled in
  manually (see `config.example.json`).

## Deployment

Import/sync these items into a Fabric workspace (e.g. via workspace Git integration), attach the
notebooks to `LH_ShortcutMonitoring`, populate `config.json`, and enable
`PL_ShortcutMonitoringOrchestrator`'s schedule (Fabric portal → pipeline → Settings → Schedule) once
validated.
