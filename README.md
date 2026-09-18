# Fabric Shortcuts Downstream Usage Monitoring

Microsoft Fabric solution that detects when a OneLake **shortcut** is read and then
**saved "as-is"** (copy/CTAS/INSERT-SELECT with no material transformation) by a
downstream Lakehouse or Warehouse item — and correlates that with native Spark/OpenLineage lineage
events. It flags duplicate shortcuts pointing at the
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
| `NB_ShortcutInventory_DuplicateDetection.Notebook` | Notebook | Enumerates Lakehouse/Warehouse shortcuts across monitored workspaces via the monitoring service principal, upserts `DimShortcut`, computes shortcut add/remove lifecycle events, and flags duplicate shortcuts (same hosting item = High severity; same hosting workspace/different item = Medium; cross-workspace same-source is not flagged). |
| `NB_CopyEventDetection_Warehouse.Notebook` | Notebook | **Warehouse engine**. Mines Fabric Warehouse Query Insights (`exec_requests_history`) for CTAS/INSERT-SELECT statements sourced from a known shortcut and records them to `FactCopyEvent` (`engine = 'Warehouse'`). |
| `NB_CopyEventDetection_SparkKafka.Notebook` | Notebook | Kafka/Eventstream-based variant of the Spark copy-event rule; writes to `FactCopyEvent` with `engine = 'SparkKafka'`, fed by the `ES_OpenLineageEvents` Eventstream. |
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
- At least one existing OneLake **shortcut** in a monitored workspace that has already been read and
  saved as-is (via Spark or Warehouse) — this is what the copy-event engines actually detect. If you
  don't already have such a scenario to test against, see **Optional: simulate a test scenario** below.

### Optional: simulate a test scenario

If no shortcut read + save-as-is has happened yet in a monitored workspace, you can manufacture one
per engine so you have something for the detection notebooks to find:

- **Spark engine:** run `fabric/notebooks/NB_OpenLineage_Validate.Notebook`. It reads an existing
  OneLake shortcut and writes it out unmodified via Spark, which is exactly the "read and saved as-is"
  pattern the Spark/OpenLineage-based engine (`NB_CopyEventDetection_SparkKafka`) looks for. Update the
  hardcoded source shortcut path in its first cell to point at a real shortcut in your tenant before
  running.
- **Warehouse engine:** open the SQL query editor against a monitored Warehouse (that hosts, or has a
  shortcut to, a source table) and run a CTAS statement reading straight from the shortcut with no
  column reduction, e.g.:

  ```sql
  CREATE TABLE dbo.wh_validate_shortcut_copy
  AS
  SELECT * FROM dbo.<your_shortcut_table_name>;
  ```

  This is a genuine "read and saved as-is" copy — it will show up in that Warehouse's Query Insights
  (`queryinsights.exec_requests_history`) as a `CREATE TABLE AS SELECT`, which `NB_CopyEventDetection_Warehouse`
  picks up on its next run. Replace `dbo.<your_shortcut_table_name>` with the actual schema/name of the
  shortcut table, and drop `dbo.wh_validate_shortcut_copy` afterward if you don't want to keep the test
  artifact around.

## Deployment

### Option A: Fabric Git integration (recommended, used to build this solution)

Connect the target Fabric workspace directly to this repo (or a branch/fork of it) so items sync
in both directions through the Fabric portal — this is how the items in this repo were originally
authored and kept in sync.

1. Follow [Get started with Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started)
   to connect the workspace (Workspace settings → Git integration), pointing the connection at the
   folder in this repo containing the `fabric/` item folders (see the Fabric Git sync note above —
   the workspace's own folder structure must mirror `fabric/notebooks/`, `fabric/pipelines/`,
   `fabric/environment/`, `fabric/eventstreams/`).
2. Sync from Git into the workspace.
3. Attach the notebooks to `LH_ShortcutMonitoring`, populate `config.json`, and enable
   `PL_ShortcutMonitoringOrchestrator`'s schedule (Fabric portal → pipeline → Settings → Schedule)
   once validated.

### Option B: Scripted deployment via the Fabric REST API / `fabric-cicd`

For CI/CD (GitHub Actions/Azure DevOps) or environments where a live Git-connected workspace isn't
practical, deploy the same item folders programmatically instead of through the portal's Git pane:

- **[`fabric-cicd`](https://github.com/microsoft/fabric-cicd)** — Microsoft's open-source Python
  library purpose-built for this: point it at a workspace ID and this repo's `fabric/` directory and
  it publishes (or removes orphaned) Notebook/DataPipeline/Environment/Eventstream items directly via
  the Fabric REST API, with YAML-based parameterization for per-environment values (dev/test/prod
  workspace IDs, connection strings, etc.). See the
  [tutorial](https://learn.microsoft.com/en-us/fabric/cicd/tutorial-fabric-cicd-local) for a working
  example, and the [official CI/CD article](https://learn.microsoft.com/en-us/rest/api/fabric/articles/fabric-ci-cd) for how it relates to the raw Items/Folders REST APIs.
- **Raw Fabric REST API** — the same approach used to build this solution's pipeline/folders in this
  session: authenticate (Azure CLI / service principal token for `https://api.fabric.microsoft.com`),
  then call the [Items](https://learn.microsoft.com/en-us/rest/api/fabric/core/items) and
  [Folders](https://learn.microsoft.com/en-us/rest/api/fabric/core/folders) APIs directly
  (create/update item definitions, create folders, move items) from a script — more control, more
  boilerplate than `fabric-cicd`, useful for one-off automation or when you need something the
  library doesn't yet support.
- **Fabric Deployment Pipelines** — once the solution is deployed once, Fabric's built-in
  [deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
  feature can promote it across Dev → Test → Prod workspaces with environment-specific rules, as an
  alternative/complement to re-running Git sync or `fabric-cicd` per environment.
