# Data Dictionary

Authoritative reference for every table/view in the solution's `LH_ShortcutMonitoring` Lakehouse
(workspace `WS_SagarFabric01`). All descriptions below are pulled verbatim from the `TABLE_COMMENTS`/
`COLUMN_COMMENTS` dictionaries already embedded in the pipeline notebooks (see `fabric/notebooks/`) —
those are the single source of truth and are applied live as Delta table/column `COMMENT` metadata on
every run (`ALTER TABLE ... SET TBLPROPERTIES ('comment' = ...)` / `ALTER TABLE ... ALTER COLUMN ...
COMMENT ...`), so they are also visible directly in the Lakehouse SQL analytics endpoint, Power BI,
and any Data Agent built on top — this document just collects them in one place for readability.

See also [`docs/architecture/Shortcut Monitoring Solution - Design Document.md`](../architecture/Shortcut%20Monitoring%20Solution%20-%20Design%20Document.md)
§13.1 for how these tables fit into the overall pipeline.

## Table of contents

- [DimShortcut](#dimshortcut) — current shortcut inventory
- [FactShortcutInventoryDiff](#factshortcutinventorydiff) — shortcut created/removed history
- [FactDuplicateShortcutGroup](#factduplicateshortcutgroup) — duplicate shortcut groups
- [FactCopyEvent](#factcopyevent) — detected "read shortcut, saved as-is" copy events (core fact table)
- [vw_FactCopyEvent_SourceStatus](#vw_factcopyevent_sourcestatus) — view enriching FactCopyEvent
- [Watermark tables (internal, not user-facing)](#watermark-tables-internal-not-user-facing)
- [Which tables feed the semantic model](#which-tables-feed-the-semantic-model)

---

## `DimShortcut`

**Grain:** one row per currently-existing shortcut. **Written by:** `NB_ShortcutInventory_DuplicateDetection`
(full overwrite every run — a snapshot of shortcuts that exist *right now*, not a history).

> Current-state snapshot of every OneLake shortcut discovered across the monitored workspaces (see
> `Files/config/config.json`). Fully replaced on every scheduled run - this is a dimension of
> shortcuts that exist right now, not a history. Row grain: one row per shortcut.

| Column | Type | Description |
|---|---|---|
| `shortcut_sk` | `BIGINT` | Deterministic surrogate key (`xxhash64` of `shortcut_key`). Prefer this over `shortcut_key` for joins to Fact tables. |
| `shortcut_key` | `STRING` | Natural key: `hosting_workspace_id\|hosting_item_id\|shortcut_path\|shortcut_name`. Kept for readability/debugging. |
| `shortcut_name` | `STRING` | The shortcut display name as it appears in the hosting Lakehouse/Warehouse. |
| `hosting_workspace_id` | `STRING` | Workspace GUID where this shortcut physically lives (the workspace being monitored). |
| `hosting_workspace_name` | `STRING` | Display name of `hosting_workspace_id`, resolved at scan time. |
| `hosting_item_id` | `STRING` | Lakehouse/Warehouse item GUID that hosts (contains) this shortcut. |
| `hosting_item_name` | `STRING` | Display name of `hosting_item_id`. |
| `hosting_item_type` | `STRING` | Fabric item type of the hosting item, e.g. `Lakehouse` or `Warehouse`. |
| `shortcut_path` | `STRING` | Path within the hosting item where the shortcut is placed, e.g. `/Tables` or `/Files`. |
| `target_type` | `STRING` | Type of the shortcut target, e.g. `OneLake` (internal, tracked for duplicates), `AmazonS3`, `ADLS`, `GCS`, `Dataverse` (external). |
| `source_workspace_id` | `STRING` | For OneLake targets: workspace GUID of the shortcut source item. Null for external (non-OneLake) targets. |
| `source_workspace_name` | `STRING` | Display name of `source_workspace_id`, resolved at scan time. |
| `source_item_id` | `STRING` | For OneLake targets: item GUID the shortcut points to (its source Lakehouse/Warehouse/KQL DB). |
| `source_item_name` | `STRING` | Display name of `source_item_id`. |
| `source_path` | `STRING` | Path within the source item that the shortcut points to (excludes the shortcut's own name, by design, so two shortcuts with different names to the same path are still recognized as duplicates). |
| `is_internal_onelake` | `BOOLEAN` | True if `target_type = OneLake` (i.e. this shortcut is eligible for duplicate-shortcut detection); false for external targets. |
| `snapshot_ts` | `TIMESTAMP` | UTC timestamp of the scan run that produced this row. |

**Used by:** both copy-event detection notebooks as the cross-reference lookup — the Warehouse
notebook matches a parsed SQL statement's source table name against `DimShortcut`; the Spark/Kafka
notebook matches an OpenLineage input dataset's real OneLake path against it.

---

## `FactShortcutInventoryDiff`

**Grain:** one row per inventory-diff run × shortcut that changed. **Written by:** same notebook,
append-only.

> Append-only lifecycle event log of shortcut creations and deletions detected between consecutive
> scheduled runs (every 15 minutes). This is what makes the solution incremental: consumers read only
> the new rows appended since they last looked, instead of re-diffing the full DimShortcut snapshot.
> Row grain: one row per shortcut per change event (new = created since last run, removed = no longer
> present since last run).

| Column | Type | Description |
|---|---|---|
| `shortcut_sk` | `BIGINT` | Deterministic surrogate key (`xxhash64` of `shortcut_key`). Prefer this over `shortcut_key` for joins. |
| `shortcut_key` | `STRING` | Natural key of the shortcut this event refers to (see `DimShortcut.shortcut_key`). |
| `change_type` | `STRING` | `new` = this shortcut was created since the prior scheduled run. `removed` = this shortcut existed at the prior run but no longer exists now (deleted, or its hosting item was deleted). |
| `shortcut_name` | `STRING` | Shortcut display name at the time of this event. |
| `hosting_workspace_id` | `STRING` | Workspace GUID hosting the shortcut at the time of this event. |
| `hosting_workspace_name` | `STRING` | Display name of `hosting_workspace_id`. |
| `hosting_item_id` | `STRING` | Hosting Lakehouse/Warehouse item GUID at the time of this event. |
| `hosting_item_name` | `STRING` | Display name of `hosting_item_id`. |
| `hosting_item_type` | `STRING` | Fabric item type of the hosting item. |
| `shortcut_path` | `STRING` | Path within the hosting item, e.g. `/Tables` or `/Files`. |
| `target_type` | `STRING` | Type of the shortcut target (`OneLake`, `AmazonS3`, `ADLS`, `GCS`, `Dataverse`, etc.). |
| `source_workspace_id` | `STRING` | For OneLake targets: source item workspace GUID. |
| `source_workspace_name` | `STRING` | Display name of `source_workspace_id`. |
| `source_item_id` | `STRING` | For OneLake targets: source item GUID. |
| `source_item_name` | `STRING` | Display name of `source_item_id`. |
| `source_path` | `STRING` | Path within the source item that the shortcut points to. |
| `is_internal_onelake` | `BOOLEAN` | True if this was an internal OneLake shortcut (duplicate-detection eligible). |
| `snapshot_ts` | `TIMESTAMP` | UTC timestamp of the scan run in which this change was detected. |

**Used by:** `vw_FactCopyEvent_SourceStatus` to enrich `FactCopyEvent` rows whose source shortcut has
since been deleted, and as a fallback lookup (alongside current `DimShortcut`) when reconciling
historical `FactCopyEvent` rows.

---

## `FactDuplicateShortcutGroup`

**Grain:** one row per shortcut, tagged with its duplicate group. **Written by:** same notebook.

> One row per shortcut that is a member of a duplicate-shortcut group, i.e. two or more shortcuts
> point at the exact same source item/path. High severity = duplicates live in the SAME hosting
> Lakehouse/Warehouse/KQL item (pure redundancy). Medium severity = duplicates live in the SAME
> hosting workspace but DIFFERENT hosting items (possibly intentional per-item security boundary,
> still worth reviewing). The same source shortcut reused in a DIFFERENT workspace is normal reuse
> and is never flagged here.

| Column | Type | Description |
|---|---|---|
| `duplicate_group_id` | `STRING` | Stable hash identifying one duplicate group (all shortcuts pointing at the same source, grouped per the High/Medium rule). Group all rows sharing this id to see the full duplicate set. |
| `shortcut_sk` | `BIGINT` | Deterministic surrogate key of the duplicated shortcut (`xxhash64` of `shortcut_key`) — join to `DimShortcut.shortcut_sk`. |
| `shortcut_key` | `STRING` | Natural key of the duplicated shortcut. |
| `shortcut_name` | `STRING` | Display name of this specific duplicate shortcut. |
| `hosting_workspace_id` | `STRING` | Workspace GUID hosting this specific duplicate shortcut. |
| `hosting_workspace_name` | `STRING` | Display name of `hosting_workspace_id`. |
| `hosting_item_id` | `STRING` | Lakehouse/Warehouse item GUID hosting this specific duplicate shortcut. |
| `hosting_item_name` | `STRING` | Display name of `hosting_item_id`. |
| `source_workspace_id` | `STRING` | Workspace GUID of the common source all group members point to. |
| `source_workspace_name` | `STRING` | Display name of `source_workspace_id`. |
| `source_item_id` | `STRING` | Item GUID of the common source all group members point to. |
| `source_item_name` | `STRING` | Display name of `source_item_id`. |
| `source_path` | `STRING` | Common source path all group members point to (this is the field that makes them duplicates). |
| `group_member_count` | `STRING` | Total number of shortcuts in this duplicate group. |
| `severity` | `STRING` | `High` = duplicates share the same hosting item (pure redundancy). `Medium` = duplicates share the hosting workspace but live in different hosting items. |
| `explanation` | `STRING` | Human-readable reason for the assigned severity, for display in reports/alerts. |
| `detected_ts` | `STRING` | UTC timestamp when this duplicate group was last (re)computed. |

**Not consumed** by either copy-event detection notebook — purely a control-plane governance signal.

---

## `FactCopyEvent`

**Grain:** one row per detected copy event (one job/statement × one matched shortcut × one
destination). **Written by:** both `NB_CopyEventDetection_Warehouse` (`engine='Warehouse'`) and
`NB_CopyEventDetection_SparkKafka` (`engine='SparkKafka'`) — **one shared table, not duplicated per
engine.** This is the core fact table the whole solution exists to populate.

> One row per detected read-from-shortcut copy event, across the implemented engines: Warehouse
> (CTAS/INSERT-SELECT via Query Insights) and Spark Kafka (OpenLineage column-lineage detection
> sourced from the shared `ol_lineage_events_v3` Eventstream sink table) — distinguished by the
> `engine` column. `is_shortcut_read_and_saved_as_is` = TRUE when the write retained ALL source
> columns (`SELECT *` equivalent) OR `retention_pct > threshold_pct_at_detection`, even if extra new
> columns were also added. Both engines are incremental (their own watermark tables) — never a full
> rescan.

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` | Natural key. Warehouse: `{item_id}\|{distributed_statement_id}`. Spark/Kafka: `{lineage_file_path}\|{openlineage_run_id}`-style identifier unique per engine. |
| `hosting_workspace_id` | `STRING` | Workspace GUID where the copy statement/notebook ran. |
| `hosting_workspace_name` | `STRING` | Display name of `hosting_workspace_id`. |
| `hosting_item_id` | `STRING` | Warehouse item GUID (Warehouse engine) or notebook item GUID via `trident.artifact.id` (Spark/Kafka engine) where the copy ran. |
| `hosting_item_name` | `STRING` | Warehouse: the Warehouse item name. Spark/Kafka: the monitored notebook's name. |
| `engine` | `STRING` | Fabric engine that executed the copy: `Warehouse` or `SparkKafka` (Dataflow Gen2 is a separate, not-yet-implemented future engine). |
| `matched_shortcut_name` | `STRING` | Name of the OneLake shortcut the statement/notebook read from (matched against `DimShortcut`). |
| `matched_shortcut_database` | `STRING` | Name of the hosting Lakehouse/Warehouse item where the matched shortcut lives (may differ from `hosting_item_name` for cross-item copies). |
| `shortcut_sk` | `BIGINT` | Deterministic surrogate key of the matched shortcut, looked up from `DimShortcut.shortcut_sk` at detection time and stored as a real, materialized column (not derived at query time) — this is what lets the semantic model relate `FactCopyEvent` to `DimShortcut` under Direct Lake, which cannot use a calculated column as a relationship key. NULL if the matched shortcut couldn't be resolved to a `shortcut_sk` at detection time. Existing tables are migrated/backfilled automatically (see the migration-guard cell in both copy-event notebooks). |
| `dest_table` | `STRING` | Destination table name the SELECT/write was saved into. |
| `source_column_count` | `INT` | Total column count of the shortcut source table. |
| `dest_column_count` | `INT` | Column count actually written to the destination table. |
| `retained_column_count` | `INT` | Number of destination columns considered retained from the source (capped at `source_column_count`; Spark/Kafka bases this on `DIRECT`/`IDENTITY` OpenLineage column-lineage facets, a stricter definition than the Warehouse engine's name-based match). |
| `retention_pct` | `DOUBLE` | `retained_column_count / source_column_count * 100`. |
| `is_select_star` | `BOOLEAN` | TRUE if the statement used `SELECT *` from the shortcut (Warehouse engine) / retained all columns (Spark/Kafka engine). |
| `is_shortcut_read_and_saved_as_is` | `BOOLEAN` | TRUE if `is_select_star` OR `retention_pct > threshold_pct_at_detection` — the flag this whole solution exists to raise. |
| `threshold_pct_at_detection` | `DOUBLE` | Configurable retention threshold (`config.detection.columnRetentionThresholdPercent`) in effect when this row was computed. |
| `copy_event_starttime` | `STRING` | Start time of the source query/write (kept as STRING, not TIMESTAMP, to avoid a Delta schema-merge conflict encountered during implementation). |
| `copy_event_detected_time` | `STRING` | UTC timestamp this notebook run detected/computed this row. |
| `username` | `STRING` | Identity that executed the copy. Warehouse: `login_name` from `queryinsights.exec_requests_history` (no extra correlation needed). SparkKafka: resolved by joining the exact `JobInstanceId` embedded in the OpenLineage `job.name` against the monitored workspace's Monitoring KQL database `ItemJobEventLogs.ExecutingPrincipalId`, then resolving that AAD object id to a friendly UPN/display name via Microsoft Graph `directoryObjects/{id}` (falls back to the raw AAD object id if Graph resolution fails or lacks permission). NULL for rows written before this column existed (no historical backfill) or if the SparkKafka correlation itself could not find a match (e.g. Monitoring KQL DB unreachable, or the job.name pattern didn't yield a matching `JobInstanceId`). |

---

## `vw_FactCopyEvent_SourceStatus`

A SQL **view** (not a materialized table) over `FactCopyEvent`, built by `NB_CopyEventDetection_Warehouse`
but automatically reflecting rows from both engines since it simply selects over `FactCopyEvent`.

> Reporting view over FactCopyEvent enriched with the CURRENT existence status of the source shortcut
> it was copied from. `source_shortcut_exists_now` = FALSE means the shortcut that fed this copy
> event has since been deleted (per `DimShortcut`/`FactShortcutInventoryDiff`); `source_removed_ts` is
> when that deletion was detected. Recomputed fresh on every read — not a materialized/incremental
> table.

| Column | Type | Description |
|---|---|---|
| *(all `FactCopyEvent` columns)* | — | Passed through unchanged (`f.*`). |
| `source_shortcut_exists_now` | `BOOLEAN` | TRUE if the shortcut this copy event read from still exists in the current `DimShortcut` snapshot. |
| `source_removed_ts` | `TIMESTAMP` | If `source_shortcut_exists_now = FALSE`, the `snapshot_ts` of the most recent `removed` event for that shortcut in `FactShortcutInventoryDiff`. Null if it still exists or was never seen being removed. |

Zero incremental-processing cost (plain SQL view, joins `DimShortcut` + `FactShortcutInventoryDiff` on
`hosting_workspace_id` + `matched_shortcut_database`/`hosting_item_name` + `matched_shortcut_name`/`shortcut_name`
at query time) — this is how the solution answers "does this copy event's source shortcut still
exist, and if not, when was it deleted" without a dedicated reconciliation pipeline.

---

## Watermark tables (internal, not user-facing)

Not commented/described the same way as the tables above since they're purely operational plumbing,
not analytical data — excluded from the semantic model and reports.

| Table | Grain | Columns | Purpose |
|---|---|---|---|
| `CopyEventWatermark` | one row per Warehouse item | `item_id STRING`, `last_processed_start_time STRING` | Incremental watermark for the Warehouse engine — the last Query Insights `start_time` processed per Warehouse item. Ensures each run only scans NEW query-history rows (pushed into the SQL `WHERE` clause), never a full rescan. |
| `RawCaptureKqlWatermark` | single row | `last_ingest_ts` | Watermark for the raw-capture step that pulls already-landed OpenLineage events out of the solution's own Eventhouse KQL database (`ol_raw_events`, populated by the Eventstream's Eventhouse DirectIngestion destination) via the Kusto REST query API. Keyed on Kusto's own `ingestion_time()` (monotonic, immune to Kafka-side offset/retention semantics) rather than the event's own `eventTime`. Advances independently of `RawCaptureWatermark` below — this one governs "how far into `ol_raw_events` have we read," not "how far into staging have we synced." |
| `RawCaptureWatermark` | single row | `last_processed_event_time` | Watermark for the step that syncs newly-captured rows from the schema-free `ol_raw_kafka_staging` table into `ol_lineage_events_v3`, keyed on the staging table's `kafka_timestamp`. Deliberately a separate watermark from `RawCaptureKqlWatermark` and `SparkKafkaLineageWatermark` — all three advance independently because each governs a different stage of the same pipeline (KQL→staging, staging→`ol_lineage_events_v3`, `ol_lineage_events_v3`→`FactCopyEvent`), so a failure/retry at one stage cannot silently skip or double-process rows at another. |
| `SparkKafkaLineageWatermark` | single row (shared across all monitored notebooks) | `last_processed_enqueued_time TIMESTAMP` | Incremental watermark for the Spark/Kafka engine's detection step — max `EventEnqueuedUtcTime` already processed from `ol_lineage_events_v3`. Ensures each run only reads newly-appended rows. |

---

## Which tables feed the semantic model

The semantic model (`SM_ShortcutMonitoring`, see `fabric/semanticmodels/`) is built on the **final,
user-facing analytical tables only**: `DimShortcut`, `FactCopyEvent`, `FactDuplicateShortcutGroup`,
and `FactShortcutInventoryDiff`, in **Direct Lake** mode (reads Delta files directly — no import/
refresh needed). `vw_FactCopyEvent_SourceStatus`'s two enrichment columns
(`source_shortcut_exists_now`, `source_removed_ts`) are reproduced as calculated columns *inside* the
semantic model instead of importing the view as a fifth table — this avoids a second, overlapping
copy of the `FactCopyEvent` grain inside the model. `FactCopyEvent`'s `shortcut_sk` column (see above)
is a **real, materialized Lakehouse column**, not a semantic-model calculated column — Direct Lake
relationships cannot use a calculated column as a join key, so the lookup against `DimShortcut` is
done once by the copy-event notebooks at write time instead of at query time in DAX. Watermark tables
are excluded entirely — they carry no analytical value.
