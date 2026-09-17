# Fabric Shortcuts Downstream Usage Monitoring

Microsoft Fabric solution that detects when a OneLake **shortcut** is read and then
**saved "as-is"** (copy/CTAS/INSERT-SELECT with no material transformation) by a
downstream Lakehouse or Warehouse item — and, as a Phase-2 enhancement, correlates that
with native Spark/OpenLineage lineage events. It flags duplicate shortcuts pointing at the
same source across the tenant.

See [`docs/Shortcut Monitoring Solution - Design Document.md`](docs/Shortcut%20Monitoring%20Solution%20-%20Design%20Document.md)
for the full design (architecture, data model, thresholds, rules).

## Solution layout

This repo mirrors the Fabric workspace **git-connected item layout** (each top-level
folder is one Fabric item, named `<DisplayName>.<ItemType>`), synced from the
`Shortcut Monitoring solution` workspace folder.

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

## Prerequisites

- A Microsoft Fabric workspace (capacity must be running) with a monitoring service
  principal granted read access to all monitored workspaces.
- `config.json` populated with the monitored workspace list and detection thresholds
  (see design doc §5).

## Deployment

Import/sync these items into a Fabric workspace (e.g. via workspace Git integration),
attach the notebooks to `LH_ShortcutMonitoring`, and schedule
`NB_ShortcutInventory_DuplicateDetection` and the engine-specific `NB_CopyEventDetection_*`
notebooks per the cadence in the design document.
