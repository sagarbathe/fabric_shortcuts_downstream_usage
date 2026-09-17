# Shortcut "Read-and-Save-As-Is" Monitoring Solution — Design Document

**Status:** Living document. Sections 0–12 are the original solution design (target-state
architecture). Section 13 records the **actual implementation** built and validated in the tenant
(`mngenvmcap146722.onmicrosoft.com`, workspace `WS_SagarFabric01`), which deliberately diverged from
the original Real-Time Intelligence/Eventhouse architecture in favor of a simpler, notebook-based MVP
once implementation began — Section 13 explains why and documents the resulting data model precisely
as built. Section 14 records an alternate approach (OneLake Diagnostics + Delta History correlation)
that was researched as an alternative to OpenLineage for Spark-engine detection but was **not** the
approach ultimately implemented (kept for reference/comparison only).

---

## 0. Context and Scope

This design responds to **Part 2 ("Tracking and monitoring shortcut reads/copies")** of the attached
*Reframing the problem in Fabric terms* document, which established that:

- Prevention alone (Contributor-role restriction, Private Link, Outbound Access Protection) narrows the
  door but cannot stop a legitimately authorized user from reading data again through a second pointer.
- Monitoring must therefore cover **both** the control plane (who created a shortcut) and the **data
  plane** (who actually read through it, and — critically for this ask — who then *persisted* that data
  into a new table, effectively defeating the purpose of a shortcut and creating an uncontrolled copy).

This document designs the system that answers, continuously and with low latency:

> "Which jobs read data through a OneLake shortcut and then saved it into a new table essentially
> as-is (all columns, or ≥ a configurable % of columns), as opposed to genuinely transforming/reducing it?"

**Confirmed scope (from stakeholder answers):**

| Decision | Answer |
|---|---|
| Engines to cover | Spark notebooks/pipelines, Warehouse T-SQL, Dataflows Gen2 (all three) |
| Purview column lineage available? | No — must be reconstructed from engine-native logs |
| Lineage depth | Single hop only (shortcut read → immediate write). Multi-hop is out of scope for MVP |
| Same-workspace copies | In scope (flagged, but with lower severity than cross-workspace) |
| Real-time backbone | Fabric Real-Time Intelligence (Eventstream + Eventhouse/KQL DB) is licensed and available |
| Consumption | Power BI report + Fabric Data Agent (Copilot-style NL Q&A) |

---

## 1. Definitions

### 1.1 "Shortcut read-and-saved-as-is" (the core business rule)

For a given **copy event** (one job run that reads from at least one OneLake shortcut and writes to a
new/existing table as its primary output):

```
columns_available   = number of columns exposed by the shortcut's target table at read time
columns_saved        = number of those *same* (name+lineage-matched) columns present in the
                        destination table's schema immediately after the write
pct_saved            = columns_saved / columns_available

is_select_star       = true if the read operation had no explicit projection
                        (SELECT * / no column pruning / Dataflow step with no Table.SelectColumns
                        or Table.RemoveColumns on the source step)

is_read_and_saved_as_is =
        is_select_star == true
     OR pct_saved >= ConfigThreshold.percentage   -- default 80%, tenant-configurable
```

- Extra derived/computed columns added on top of the source columns do **not** reduce, dilute, or mask
  `pct_saved` — both the numerator (`columns_saved`) and the denominator (`columns_available`) count
  **source-origin columns only**; the destination table's total column count (source + new) is
  irrelevant to the ratio. Concretely, for a shortcut with 20 source columns:
  - 17 of the 20 source columns saved, plus 10 brand-new calculated columns added → destination has 27
    columns total, but `pct_saved = 17/20 = 85%` → **flagged** (≥ 80% threshold), regardless of how many
    extra columns dilute the destination schema.
  - 1 of the 20 source columns saved, plus several new columns → `pct_saved = 1/20 = 5%` → **not
    flagged**, per the ask's example, since the point isn't columns count in the output, it's how much
    of the *original* dataset was reproduced.
- This guarantees requirement consistency: **any copy that retains ≥ 80% (configurable) of the
  source's columns is flagged as "read-and-saved-as-is," even if additional columns were created** —
  adding columns can never be used to dodge detection by "diluting" the percentage.
- The threshold is stored in a config table (`ConfigThresholds`), versioned by `effective_from`, so
  policy changes apply going forward without needing to touch historical raw data (see §6.3).

### 1.1a "Duplicate shortcut" (control-plane rule, in addition to read-and-saved-as-is)

A separate but complementary check: two or more shortcut *definitions*, **hosted in the same
workspace**, that point to the **exact same underlying source location** — regardless of what each
shortcut is individually named. Two severity tiers, based on how tightly the duplicates are hosted:

```
resolved_target_key   = (source_workspace_id, source_item_id, source_path)     -- what it points TO (WS1.T1)

item_group_key        = (hosting_workspace_id, hosting_item_id, resolved_target_key)   -- WS2 + Item + WS1.T1
workspace_group_key    = (hosting_workspace_id, resolved_target_key)                    -- WS2 (any item) + WS1.T1
```

- `hosting_workspace_id`/`hosting_item_id` is the workspace **and specific Lakehouse/Warehouse/KQL
  Database** where the shortcut object itself lives (WS2 in the examples below);
  `source_workspace_id/source_item_id/source_path` is what it **points to** (WS1.T1). `source_path` is
  the path/location of the source data only — it explicitly **excludes** the shortcut's own name in the
  workspace where it was created.
- **Worked examples (confirmed with stakeholder):**
  - Workspace **WS1** contains table `T1`.
  - **High severity** — same hosting item: a user creates shortcut `Shortcut_A` in
    **`Lakehouse_X` in WS2** pointing to `WS1.T1`. Another (or the same) user creates shortcut
    `Shortcut_B`, **also in `Lakehouse_X` in WS2**, also pointing to `WS1.T1` →
    `item_group_key` = `(WS2, Lakehouse_X, WS1, T1)` matches for both, `COUNT(DISTINCT shortcut_id) = 2`.
    There is no legitimate reason for the *same* Lakehouse/Warehouse/KQL Database to contain two
    shortcuts to the identical source — this is pure redundancy (naming confusion, forgotten cleanup,
    or an attempt to route around a permission/label applied to the other shortcut).
  - **Medium severity** — same workspace, different hosting item: a user creates shortcut
    `Shortcut_C` in **`Lakehouse_Y` in WS2** (a *different* item than `Lakehouse_X`, still in WS2),
    also pointing to `WS1.T1` → does **not** match any `item_group_key`, but **does** match
    `workspace_group_key` = `(WS2, WS1, T1)` shared with Shortcut_A/B. Flagged as duplicate, but at
    **Medium** rather than High, because different Lakehouses/Warehouses/KQL Databases within the same
    workspace can legitimately serve different teams, domains, or security boundaries (e.g., each with
    its own OneLake data access roles) — so it is not automatically pure redundancy the way same-item
    duplicates are. Still worth surfacing, since it's unnecessary proliferation of pointers to the same
    data within one workspace and increases the governance/cleanup surface.
  - **Not flagged**: a user creates shortcut `Shortcut_D` in a **different workspace WS3**, also
    pointing to `WS1.T1`. Shortcutting the same certified source into multiple consuming workspaces is
    the normal, intended use of OneLake shortcuts — tracked in the inventory (and its reads/copies are
    still subject to the read-and-saved-as-is check, §1.1), but **not** flagged by this rule.
- Detection (two-pass, both incremental over the same `DimShortcut` snapshot, no data-plane telemetry
  needed since this depends only on shortcut *definitions* already collected in the inventory, §5):
  1. `GROUP BY hosting_workspace_id, hosting_item_id, resolved_target_key HAVING COUNT(DISTINCT shortcut_id) > 1` → **High**.
  2. `GROUP BY hosting_workspace_id, resolved_target_key HAVING COUNT(DISTINCT shortcut_id) > 1`, **excluding** shortcuts already captured by pass 1 → **Medium**, tagged with an explanation string (e.g., *"Duplicate source shortcuted from N different items within workspace WS2 — review whether consolidation is possible; not High because items may have distinct security boundaries"*).
- Every shortcut in a flagged duplicate group is surfaced together in the report/alert (not just the
  newest one), and its `duplicate_group_id` (plus severity and explanation) is carried onto
  `FactCopyEvent` rows (via lookup on `shortcut_id`), so a read-and-saved-as-is event on one duplicate
  immediately surfaces its sibling(s)
  in the same workspace.

### 1.2 "Copy event" grain

One row = one (job run, source shortcut, destination table) triple. A single job that reads two
shortcuts and writes two tables produces two copy-event rows.

### 1.3 Severity

| Condition | Severity |
|---|---|
| `is_read_and_saved_as_is` = true AND destination workspace ≠ source shortcut's target workspace | High (true cross-workspace duplication — the risk called out in Part 1 of the reference doc) |
| `is_read_and_saved_as_is` = true AND destination workspace = source workspace | Medium |
| `is_read_and_saved_as_is` = false | Informational only (not alerted, still recorded for trend/audit) |

---

## 2. Why "real one architecture" isn't a single log source

No single Fabric log gives us everything we need simultaneously:

| Log source | Gives us | Misses |
|---|---|---|
| Fabric Admin Activity Events (`/admin/activityevents`) / Purview Audit | Control-plane: shortcut create/update/delete, by whom, when | No column-level detail, no data-plane read info |
| OneLake diagnostics (streamed to a Lakehouse) | Data-plane: actual API-level reads through a shortcut (who, when, how much) | No column/schema detail — it's file/API-call telemetry, not query semantics |
| Fabric Workspace Monitoring (Spark app logs, Warehouse Query Insights) — landed in a per-workspace **KQL database (Eventhouse)** | Job/query-level detail: Spark SQL execution plans (incl. `ReadSchema`/`WriteSchema`), Warehouse query text | Needs parsing/interpretation per engine; not natively "is this a shortcut" aware |
| Dataflow Gen2 refresh history + mashup definition (Fabric REST API) | M-query steps (column selection intent), refresh completion events | No native streaming; only available post-refresh (poll-based) |
| Fabric REST API: list/get shortcuts, get table schema (Lakehouse/Warehouse) | Ground truth for "what is the shortcut's target schema right now" | Point-in-time only; must be called incrementally/cached |
| **OpenLineage (Spark listener, `file` transport)** — added during implementation, see §13 | **True column-level lineage**: exact per-output-column mapping back to per-input-column, with transformation type (DIRECT/IDENTITY vs. derived) — the most precise signal of all sources above for the Spark engine specifically | Requires enabling a Spark listener per notebook (`%%configure`); file transport has a known concurrent-write risk; not a Microsoft-native/managed telemetry feed (self-hosted library) |

The design therefore **fuses** these sources into one unified event/fact model rather than relying on
any single one.

---

## 3. High-Level Architecture (Target State)

```
                    ┌───────────────────────────────────────────────────────────────┐
                    │                     SOURCE SIGNALS (per workspace)             │
                    │                                                                │
  Spark notebooks/  │  Fabric Workspace Monitoring KQL DB (Eventhouse)               │
  pipelines ───────►│    - SparkExecutionEvents (physical plan, ReadSchema/WriteSchema)│
                    │                                                                │
  Warehouse/SQL ───►│    - QueryInsights (exec_requests_history: full SQL text,      │
  endpoint          │      source/target objects)                                   │
                    │                                                                │
  Dataflow Gen2 ───►│  Dataflow Refresh History + Mashup Definition (poll via API)   │
                    │                                                                │
  OneLake ─────────►│  OneLake diagnostics → landing Lakehouse (data-plane reads)    │
  diagnostics       │                                                                │
                    │  Fabric Admin Activity Events (control-plane: shortcut CRUD)   │
                    └──────────────────────────┬──────────────────────────────────────┘
                                               │  Eventstream (near-real-time, one per
                                               │  workspace/tenant-scoped source)
                                               ▼
                    ┌───────────────────────────────────────────────────────────────┐
                    │      CENTRAL EVENTHOUSE (KQL Database) — "raw event zone"      │
                    │  Tables: RawSparkPlanEvents, RawQueryInsightEvents,            │
                    │          RawDataflowRefreshEvents, RawOneLakeReadEvents,       │
                    │          RawShortcutActivityEvents                            │
                    │  (update policies do lightweight JSON-shred/parse on ingest)   │
                    └──────────────────────────┬──────────────────────────────────────┘
                                               │  KQL update policy / materialized view
                                               │  triggers "Data Activator" (Reflex) rule
                                               ▼
                    ┌───────────────────────────────────────────────────────────────┐
                    │           ENRICHMENT & DETECTION LAYER (near-real-time)         │
                    │  Fabric notebook (mini-batch, triggered by Eventstream/         │
                    │  Activator on each new raw event, effectively seconds-to-       │
                    │  low-minutes latency):                                         │
                    │   1. Resolve source shortcut → target table + schema           │
                    │      (cached ShortcutInventory + SchemaSnapshot dim, refreshed  │
                    │       incrementally — see §5)                                  │
                    │   2. Parse engine-specific read/write column lists              │
                    │      (see §4)                                                  │
                    │   3. Compute pct_saved, is_select_star, is_read_and_saved_as_is │
                    │   4. Idempotent MERGE/upsert into FactCopyEvent (keyed by       │
                    │      unique job-run-id + destination table) — safe re-runs      │
                    └──────────────────────────┬──────────────────────────────────────┘
                                               ▼
                    ┌───────────────────────────────────────────────────────────────┐
                    │        CURATED LAKEHOUSE / WAREHOUSE (Delta) — star schema      │
                    │  Dim: DimWorkspace, DimItem, DimUser, DimShortcut,              │
                    │       DimSourceSchemaVersion, ConfigThresholds                  │
                    │  Fact: FactCopyEvent, FactShortcutReadEvent (data-plane detail),│
                    │        FactShortcutInventoryDiff (control-plane detail)        │
                    └──────────────┬───────────────────────────────┬─────────────────┘
                                   │                                │
                                   ▼                                ▼
                    ┌───────────────────────┐        ┌───────────────────────────────┐
                    │   Power BI Report      │        │  Fabric Data Agent (Copilot)   │
                    │ (Direct Lake semantic  │        │  grounded on the same semantic │
                    │  model over the Fact/  │        │  model — NL Q&A: "who copied   │
                    │  Dim tables)           │        │  Shortcut X as-is last week?"  │
                    └───────────────────────┘        └───────────────────────────────┘
                                   ▲
                                   │  Data Activator alert rule (High severity, cross-
                                   │  workspace, is_read_and_saved_as_is = true)
                                   │
                    ┌───────────────────────────────────────────────────────────────┐
                    │  Teams / Email notification to Fabric admins / data owners     │
                    └───────────────────────────────────────────────────────────────┘
```

**Why this satisfies "real-time where possible, batch where not" (requirement #3):**

- Spark and Warehouse activity land in Workspace Monitoring's Eventhouse essentially as the job/query
  completes → streamed via Eventstream → detection notebook triggers within seconds/low minutes. This
  is effectively real-time.
- Dataflow Gen2 has no push/streaming event for refresh completion at the required granularity; it is
  polled every N minutes (configurable, default 5) via the refresh-history API — a small, tightly
  scoped batch step, not a full rescan.
- Control-plane shortcut inventory (which shortcuts exist, and where) is also poll-based against the
  Admin API (default every 15 min), because that API itself is not a push/event source.

> **Note (see §13):** the Eventstream/Eventhouse/Data Activator backbone described above is the
> **target-state** architecture. The implementation actually built (Phase 1 + Phase 2 MVP) uses plain
> scheduled Fabric notebooks writing directly to Delta tables instead — functionally equivalent for
> the MVP's near-real-time-enough latency needs, at a fraction of the setup complexity, and easily
> upgraded to the full Eventstream backbone later without changing the underlying data model.

---

## 4. Per-Engine Column-Level Detection Logic (Target-State Design)

### 4.1 Spark (Notebooks / Spark Job Definitions / Pipelines)

- Workspace Monitoring's Eventhouse captures Spark application telemetry, including SQL execution
  plans. Each `SparkListenerSQLExecutionStart`-derived record contains the physical plan description,
  which — for a scan against a shortcut path — includes a `ReadSchema: struct<col1:..,col2:..>` node,
  and for the terminal write action, the output `WriteSchema`/`Sink` node with its column list.
- Detection notebook logic:
  1. Filter Spark execution events where any scanned path resolves to a known shortcut
     (`DimShortcut.sourcePath` match against the plan's file path/table reference).
  2. Extract `ReadSchema` column names → `columns_read_from_shortcut`.
  3. Extract the write node's column list → `columns_written`.
  4. `columns_available` = current column count of the shortcut's target table (from
     `DimSourceSchemaVersion`, not from the plan, since the plan only shows what was *touched*, and a
     `SELECT *`/no-projection read may show all columns anyway — the two together let us detect both
     explicit `SELECT *` and "explicit list that happens to equal ~100%").
  5. `is_select_star` = true if no `Project`/column-pruning node exists between the scan and the sink
     for that source (i.e., the scan's output schema already equals the full source schema).
- Fallback if plan parsing is incomplete (e.g., complex plans): compare `columns_written ∩
  columns_available` (matched by name, case-insensitive) to get `columns_saved`, independent of what
  the plan claims was "read" — this makes the rule robust even when the read-side projection can't be
  parsed cleanly, since ultimately what matters for policy purposes is what ended up saved.
- **Superseded in implementation** by OpenLineage-based detection (§13.4) — Workspace Monitoring's
  Spark execution-plan parsing described above was found, during implementation, to only expose
  job-level telemetry (status/duration), not the per-statement `ReadSchema`/`WriteSchema` detail this
  design assumed would be available; OpenLineage was adopted instead as it provides genuine
  column-level lineage facets natively. This section is retained to document the original design
  intent and the reasoning trail.

### 4.2 Warehouse / SQL Analytics Endpoint (T-SQL)

- Source: Warehouse **Query Insights** (`queryinsights.exec_requests_history` and related DMVs),
  which retains full query text, submitting user, and duration; streamed/polled into the Eventhouse.
- Detection notebook logic:
  1. Identify statements whose `FROM`/source object resolves to a shortcut table (via
     `DimShortcut.qualifiedName` match against parsed `FROM` clause) and whose statement type is
     `CREATE TABLE AS SELECT` / `INSERT INTO ... SELECT` / `SELECT INTO`.
  2. Parse the SQL text with a lightweight T-SQL parser (e.g., `sqlglot`/`ANTLR` grammar for T-SQL) to
     extract the `SELECT` column list.
     - `SELECT *` (or `SELECT t.*`) → `is_select_star = true` directly.
     - Explicit column list → count against `columns_available` (from `INFORMATION_SCHEMA.COLUMNS` /
       Fabric REST API schema snapshot of the source object) to get `pct_saved`.
  3. `columns_saved` is cross-checked against the actual destination object's `INFORMATION_SCHEMA`
     after the statement commits, to guard against parser edge cases (e.g., `SELECT *` on a view that
     itself already excludes columns).
- **This is what was actually implemented** (Phase 1, §13.2/§13.3) — a regex-based (not full-grammar)
  T-SQL parser proved sufficient in practice for the CTAS/INSERT-SELECT shapes actually observed.

### 4.3 Dataflow Gen2 (Power Query / M)

- Source: Dataflow **refresh history** (completion events, polled) + the dataflow's **mashup
  definition** (retrieved via Fabric REST API `Get Dataflow Definition`), which contains the M query
  steps as text/JSON.
- Detection notebook logic:
  1. For each query in the mashup whose source step references a shortcut (`Lakehouse.Contents` /
     `AzureStorage`/`OneLake` navigation resolving to a known shortcut path).
  2. Walk the step list for that query: if it contains `Table.SelectColumns`, `Table.RemoveColumns`, or
     equivalent projection/aggregation steps that reduce column count, treat as an explicit
     projection; compute the resulting column count from the step's arguments (statically) or, more
     robustly, from the actual output schema captured in the refresh history / destination table after
     the refresh completes.
  3. If no such reducing step exists before the sink → `is_select_star = true` (functionally
     equivalent to a full unfiltered read).
  4. `columns_saved` = compare the **destination table's** actual schema after refresh (Lakehouse/
     Warehouse destination configured for the dataflow) against `columns_available`.
- **Not yet implemented** — deferred per the rollout plan (§11); still Phase-2/3 scope.

### 4.4 Common cross-check (safety net for all three engines)

Regardless of parser confidence, a scheduled lightweight reconciliation job re-derives `columns_saved`
purely from **actual destination table schema vs. source shortcut schema at read time**, using name
matching (with a configurable case-insensitive/whitespace-normalized comparator). This guarantees the
core metric is correct even if plan/SQL/M parsing has gaps — parsing is used primarily to get
`is_select_star` and the *read-side* intent, while `columns_saved`/`pct_saved` always has a
schema-diff-based ground truth.

### 4.5 Role of Fabric Admin Activity Events / Purview Audit (control-plane only)

Unlike §4.1–§4.4, this source contributes **nothing** to column-level detection — it only ever
carries `Create/Update/DeleteShortcut` events (actor, timestamp, workspace/item IDs). It is deliberately
kept as a separate, parallel pipeline with three specific jobs:

1. **Fast-path new-shortcut discovery.** Polled via the Fabric Admin REST API
   (`GET /admin/activityevents?activityType=CreateShortcut|UpdateShortcut|DeleteShortcut`) — or
   equivalently `Search-UnifiedAuditLog`/the Office 365 Management Activity API if the tenant's
   auditing is centralized through Purview — on a short interval (default every 5 min), watermarked on
   the event's `CreationTime`. This is faster than the full shortcut-inventory poll (§5, every 15 min),
   so a brand-new shortcut can be evaluated for duplicates (§1.1a) and attributed sooner than waiting
   for the next full inventory snapshot.
2. **Attribution.** `DimShortcut.created_by`/`created_at` come **only** from this source — the
   shortcut Get/List API returns current-state metadata, not who created it or when. Without this feed,
   the report/Data Agent could not answer "who created this shortcut."
3. **Independent, immediate alerting.** Wired directly into its own Data Activator rule — the exact
   pattern the reference document recommended: *"`CreateShortcut` event where target item ID = [a
   designated sensitive source] and actor is not in an approved group → alert."* This does **not**
   wait for the duplicate-detection or copy-detection pipelines; it is the single fastest signal in
   the whole architecture (an alert can fire the moment a shortcut is created, before anyone has even
   read through it).

Events land in `RawShortcutActivityEvents` (see architecture diagram, §3) and are consumed by both
`DimShortcut` (attribution merge) and the dedicated Activator rule above — they do **not** feed
`FactCopyEvent` or `FactShortcutReadEvent`, which come exclusively from the data-plane/engine sources.

> **Not yet implemented** — the current build's `DimShortcut` (§13.1) is populated purely from
> point-in-time Fabric REST API snapshots (list/get shortcuts), not the Admin Activity Events feed;
> `created_by`/`created_at` attribution and the independent fast-path CreateShortcut alert are Phase-2/3
> follow-ups.

---

## 5. Incremental Processing (Requirement #4)

Nothing in this design ever re-scans the full shortcut/table estate. Every component is
watermark-driven:

| Component | Incremental mechanism |
|---|---|
| Eventstream ingestion into Eventhouse | Native Eventstream checkpointing (offset-based, at-least-once delivery); consumer-side dedup via unique event ID |
| Shortcut inventory (`DimShortcut`) | Poll `/admin/items` + shortcut-get APIs every 15 min; **diff** against the last snapshot (`FactShortcutInventoryDiff`) and upsert only new/changed/removed shortcuts — never a full table rebuild |
| Source schema snapshots (`DimSourceSchemaVersion`) | Only re-fetched for a shortcut's target when (a) it's newly discovered, or (b) a schema-change signal is seen (Delta log `metaData` version bump / Warehouse `sys.columns` change), captured via a lightweight per-target watermark (`last_known_delta_version`) |
| Dataflow refresh polling | `WatermarkState(dataflowId, last_refresh_end_time)`; only refresh-history entries after the watermark are pulled |
| Detection notebook (fact table load) | Processes only newly landed rows in `RawSparkPlanEvents`/`RawQueryInsightEvents`/`RawDataflowRefreshEvents` since `WatermarkState(source, last_ingestion_time/offset)`; writes via `MERGE` keyed on `(job_run_id, destination_table_id)` for idempotency, so replays/late-arriving data never duplicate |
| Config threshold changes | Never require reprocessing raw facts — `pct_saved` and `columns_available/saved` are stored as-is; `is_read_and_saved_as_is` is computed **at query/report time** (or in a cheap nightly re-flag pass) against the currently effective `ConfigThresholds` row, so changing 80%→70% doesn't require touching historical raw grain data |

All watermark state lives in a single small `WatermarkState(source_name, key, watermark_value,
updated_at)` control table, checked/updated transactionally as part of each incremental job.

> **As implemented (§13.5):** the same watermark *principle* is used, but as two separate,
> purpose-specific tables rather than one generic `WatermarkState` table — `CopyEventWatermark`
> (per-Warehouse-item, keyed by `item_id`, watermarking on Query Insights `start_time`) and
> `SparkLineageWatermark` (per-lineage-file, keyed by `file_path`, watermarking on a byte offset). This
> was simpler to implement given each source's very different notion of "position" (a timestamp vs. a
> byte offset) and was not judged worth generalizing into one polymorphic table for a 2-engine MVP.

---

## 6. Data Model (Target-State Design)

### 6.1 Dimension tables

- **DimWorkspace**(workspace_id, workspace_name, capacity_id, is_monitored)
- **DimItem**(item_id, workspace_id, item_type [Lakehouse/Warehouse/Table/Shortcut], item_name, sensitivity_label)
- **DimUser**(user_id, upn, display_name, is_service_principal)
- **DimShortcut**(shortcut_id, hosting_workspace_id, hosting_item_id, shortcut_name, source_workspace_id, source_item_id, source_path, shortcut_type [internal/external/cross-tenant], created_by, created_at, is_active) — `hosting_*` = where the shortcut object lives (e.g., WS2); `source_*` = what it points to (e.g., WS1.T1)
- **DimSourceSchemaVersion**(shortcut_id, schema_version, column_name, ordinal, effective_from, effective_to) — slowly changing, versioned so historical `columns_available` is always computed against the schema *as of the read event*, not today's schema
- **ConfigThresholds**(threshold_pct, effective_from, effective_to, updated_by) — versioned, admin-editable

### 6.2 Fact tables

- **FactShortcutReadEvent** (grain: one data-plane read call) — from OneLake diagnostics: shortcut_id, reader_user_id, read_timestamp, bytes_read, api_operation. Used for "shortcut touched but nothing saved yet" visibility and anomaly/spike detection.
- **FactCopyEvent** (grain: one job-run × source shortcut × destination table) — the core fact:
  `job_run_id, engine_type [Spark/Warehouse/DataflowGen2], executed_by, execution_ts, shortcut_id,
  destination_workspace_id, destination_item_id, columns_available, columns_saved, pct_saved,
  is_select_star, is_cross_workspace, severity, is_read_and_saved_as_is (derived)`
- **FactShortcutInventoryDiff** (grain: one inventory diff run × shortcut) — control-plane change history: `snapshot_ts, shortcut_id, change_type [new/removed/target_changed]`
- **FactDuplicateShortcutGroup** (grain: one row per shortcut, tagged with its duplicate group) —
  `duplicate_group_id (hash of group key), shortcut_id, hosting_workspace_id, hosting_item_id,
  source_workspace_id, source_item_id, source_path, group_member_count, severity [High/Medium],
  explanation, first_detected_ts, last_confirmed_ts`. Two group levels, computed as two passes over the
  same `DimShortcut` snapshot: **High** = same `(hosting_workspace_id, hosting_item_id,
  resolved_target_key)`; **Medium** = same `(hosting_workspace_id, resolved_target_key)` but different
  `hosting_item_id` (i.e., not already covered by a High group). Cross-workspace shortcuts to the same
  source are excluded from both passes — that's normal reuse, not duplication. Recomputed incrementally
  as part of every shortcut-inventory diff cycle (§5), not a new capture pipeline. A shortcut's
  `duplicate_group_id`/`severity` is also carried onto `FactCopyEvent` rows (via a lookup on
  `shortcut_id`) so a report/alert on one duplicate can immediately surface its sibling(s).

### 6.3 Derived flag, not baked-in

`is_read_and_saved_as_is` is exposed as a **calculated column/measure** in the semantic model
(`pct_saved >= currently-effective ConfigThresholds.threshold_pct OR is_select_star`), so:
- Threshold changes apply retroactively across all history automatically in the report, with no
  reprocessing.
- An audit trail of *what the threshold was when* is preserved for compliance narrative if needed
  (via `ConfigThresholds` history + optionally storing `threshold_pct_at_evaluation` on the fact row
  too, for point-in-time reproducibility).

> **As implemented (§13.1):** the flag is instead computed and **materialized directly onto the
> `FactCopyEvent` row** at detection time (`is_shortcut_read_and_saved_as_is`, alongside
> `threshold_pct_at_detection` recording what threshold was in effect) rather than left as a
> query-time measure over a separate `ConfigThresholds` history table — simpler for the MVP's single
> current-threshold config.json, at the cost of needing a reprocessing pass (not yet built) if the
> threshold is changed and historical rows need to be re-flagged retroactively. See §13.6 for the
> concrete schema actually created.

---

## 7. Alerting

Fabric **Data Activator** (Reflex) rule(s) watch `FactCopyEvent` (or the pre-aggregation Eventhouse
materialized view feeding it) for:
- `severity = High` (cross-workspace + as-is) → immediate Teams/email to the data-owner group and
  Fabric admins.
- `severity = Medium` → optional daily digest instead of immediate ping, to avoid alert fatigue for
  same-workspace copies.

> **Not yet implemented** — no Data Activator rule exists yet; alerting is Phase-2/3 scope per the
> rollout plan (§11). Currently, results are visible only by querying `FactCopyEvent` directly or via
> the JSON run-summary artifacts each detection notebook writes to `Files/reports/`.

---

## 8. Consumption Layer (Requirement #5)

### 8.1 Power BI Report
- Built on a **Direct Lake** semantic model over the curated Lakehouse (near-real-time refresh,
  minimal duplication).
- Pages: (1) Executive KPI — count/trend of as-is copies, % cross-workspace; (2) Top offending
  workspaces/users; (3) Shortcut-level drill-down (all copy events per shortcut, with column diff
  detail); (4) Control-plane view (new shortcuts created, by whom, week-over-week diff).
- Row-level security scoped so workspace/data owners only see events relevant to items they own;
  full view reserved for Fabric admins/security team.

### 8.2 Fabric Data Agent
- Grounded on the same semantic model (Fact/Dim tables), enabling natural-language questions such as
  *"Which shortcuts were copied as-is into another workspace last week?"* or *"Who has copied
  Shortcut X more than once?"*.
- Reuses the report's RLS so answers respect the same access boundaries.

> **Not yet implemented** — neither the Power BI semantic model/report nor the Fabric Data Agent has
> been built yet; both remain open work items (see §13.7, Next Steps).

---

## 9. Non-Functional Requirements

| Aspect | Target |
|---|---|
| Detection latency (Spark/Warehouse) | Seconds to low single-digit minutes from job completion |
| Detection latency (Dataflow Gen2) | ≤ polling interval (default 5 min) after refresh completion |
| Shortcut inventory freshness | ≤ 15 min |
| Idempotency | All fact writes are MERGE/upsert keyed by natural keys; safe to replay any stage |
| Retention | Raw Eventhouse tables: 30–90 days (configurable); curated Lakehouse facts: retained per governance policy (e.g., 2 years) |
| Access | Monitoring workspace restricted to Fabric admins/security/data-governance roles only |

---

## 10. Prerequisites

Before implementation can begin, the following must be in place. Nothing below is provisioned *by*
this solution — they are dependencies on the tenant/platform.

### 10.1 Licensing / capacity
| Requirement | Why it's needed |
|---|---|
| A Microsoft Fabric capacity (F SKU, e.g., F64 or higher, or an equivalent P SKU) assigned to every monitored workspace **and** the monitoring workspace itself | Workspace Monitoring, Real-Time Intelligence (Eventstream/Eventhouse), Data Activator, and OneLake diagnostics are all Fabric-capacity features — they do not run on a plain Power BI Pro-only workspace |
| Fabric Copilot / Data Agent enabled at the capacity + tenant level (requires a qualifying capacity SKU, currently F64+/P1+) | Needed specifically for the Fabric Data Agent (Copilot-style NL Q&A) in §8.2; the Power BI report itself does not need this |
| Power BI Pro or PPU license for report authors; report *viewers* can consume via the Fabric capacity (Free) license as long as the report workspace is on Fabric capacity | Standard Power BI licensing model — confirm your tenant's viewer licensing approach before rollout |
| Microsoft Purview Audit (Standard is included in most M365 plans; Purview Audit **Premium**/E5 only needed if you require >180 days audit retention or higher audit bandwidth) | Only Standard is required for this solution's use case (recent shortcut CRUD events); Premium is an optional enhancement, not a hard requirement |

### 10.2 Tenant/platform settings to be enabled up front
| Setting | Where | Purpose |
|---|---|---|
| Fabric Admin Portal → Tenant settings → "Service principals can access read-only admin APIs" (and related admin API switches) | Fabric Admin Portal | Required for the service principal that polls `/admin/activityevents`, `/admin/items`, and shortcut-get APIs (§4.5, §5) |
| A registered Microsoft Entra app / service principal, granted Fabric **Administrator** role (or delegated tenant-scoped read access) and consented for Fabric REST API scopes | Entra ID + Fabric Admin Portal | Identity used by all polling jobs (inventory diff, activity events) — must be tenant-wide, since shortcuts must be inventoried across all workspaces, not just ones the running user happens to have access to |
| Unified Audit Log turned on in the Microsoft Purview compliance portal (on by default in most tenants, but must be verified) | Purview compliance portal | Prerequisite for `Search-UnifiedAuditLog`/Activity Events API to return any shortcut CRUD events at all |
| Workspace Monitoring enabled on every in-scope workspace (workspace Settings → Monitoring) | Per-workspace setting | Source of Spark execution-plan and Warehouse Query Insights telemetry (§4.1, §4.2) — currently an explicit opt-in per workspace |
| OneLake diagnostics enabled on every in-scope workspace, with a designated target Lakehouse for the diagnostic stream | Per-workspace setting | Source of `FactShortcutReadEvent` (data-plane read telemetry, §6.2); confirm current GA/preview status and regional availability in your tenant before committing to the timeline |
| Real-Time Intelligence items (Eventstream, Eventhouse/KQL Database) provisioned in the monitoring workspace | Fabric workspace | Backbone for the near-real-time ingestion path (§3) |
| Data Activator (Reflex) item provisioned in the monitoring workspace | Fabric workspace | Backbone for alerting (§7, §4.5) |

### 10.3 Network/security considerations
- If any in-scope workspace has **Outbound Access Protection (OAP)** enabled (a control recommended
  in Part 1 of the reference document), an explicit allow-list exception must be added for this
  monitoring solution's own outbound calls (Eventstream connectors, notebook calls to Admin REST API
  endpoints) — otherwise OAP will silently block the very telemetry this solution depends on.
- If any monitored workspace has **inbound Private Link/restricted workspace** protection enabled,
  confirm the monitoring pipeline's managed identity/service principal is pre-approved, so its
  read-only polling calls aren't blocked at the network layer.

### 10.4 Organizational/process prerequisites
- Agreement on the initial `ConfigThresholds.threshold_pct` value (default proposed: 80%) and who
  owns changing it going forward (governance/data-security team, not engineering).
- An approved-group definition for the "CreateShortcut ... actor not in an approved group" alert rule
  (§4.5) — i.e., who is allowed to create shortcuts against sensitive sources without triggering an
  alert.
- Designation of the monitoring workspace's owning team and its access list (§9, "Access" row) before
  go-live, since the workspace will contain sensitive metadata about who is copying what.

---

## 11. Rollout Plan

1. **MVP (Phase 1):** Spark + Warehouse engines only, single-hop, same-tenant shortcuts, Power BI
   report, config threshold table, batch detection notebook on a 5-minute schedule (defer full
   Eventstream/Activator real-time wiring).
2. **Phase 2:** Add Dataflow Gen2 detection; wire Eventstream + Data Activator for true near-real-time
   Spark/Warehouse detection; add high-severity alerting.
3. **Phase 3:** Add Fabric Data Agent; add cross-tenant/external shortcut coverage; add anomaly
   detection on `FactShortcutReadEvent` (read spikes / first-time access).

> **Actual progress vs. this plan, as of this document's last update:** Warehouse-engine detection
> (Phase 1, first half) is fully built and validated. Spark-engine detection (also nominally Phase 1)
> was delayed pending a viable column-lineage source, then completed using OpenLineage instead of the
> originally-envisioned Workspace Monitoring Spark-plan parsing (§4.1 note, §13.4). Power BI report,
> Fabric Data Agent, Dataflow Gen2, Eventstream/Data Activator real-time wiring, and alerting are all
> still open (Phase 2/3, not started). See §13.7 for the precise open item list.

---

## 12. Assumptions & Open Risks

- Assumes Workspace Monitoring is enabled (or can be enabled) on all in-scope workspaces; this is a
  prerequisite, not something this solution provisions itself — recommend a companion governance
  task to enforce it via tenant policy.
- SQL/M parsing (§4.2, §4.3) will not be 100% perfect for all query shapes; the schema-diff cross-check
  (§4.4) is the safety net that guarantees `pct_saved` correctness even when intent-parsing is imperfect.
- OneLake diagnostics and Admin Activity Events APIs are subject to Fabric-side ingestion delay/
  throttling; polling intervals should be tuned to tenant size and API quota.
- External/cross-tenant shortcuts (ADLS/S3/GCS, cross-tenant sharing) are read-tracked differently
  (no OneLake diagnostics equivalent in all cases) — flagged as a Phase 3 investigation item, not
  solved by this MVP.
- Column name-based matching (rather than true lineage IDs) can mis-attribute if the destination
  table happens to have same-named-but-different-meaning columns; acceptable false-positive risk for
  a monitoring/detective control, but should be documented for stakeholders.

---

## 13. Implementation As Built

This section documents the solution **actually implemented and validated** in the tenant, which
diverges from Sections 2–9's target-state Eventstream/Eventhouse/Data Activator architecture in favor
of a simpler MVP: plain Fabric notebooks, scheduled or run on demand, reading/writing directly to
Delta tables in one Lakehouse. This was a deliberate simplification once implementation began — the
Eventstream/Eventhouse backbone adds real value at scale (true push-based low-latency ingestion,
native replay/dedup), but for a 2-engine MVP with a handful of monitored workspaces, direct
notebook-to-Delta-table processing on a short polling interval delivers effectively the same
detection latency (single-digit minutes) with far less platform surface area to build/operate. The
data model (facts/dims) is intentionally very close to Section 6's design so upgrading to the full
Eventstream backbone later is additive, not a rewrite.

### 13.1 Solution Lakehouse and tables (ground truth, as built)

All tables live in a single Lakehouse, **`LH_ShortcutMonitoring`**, in workspace `WS_SagarFabric01`
(workspace id `c5c50c6e-30d1-4d2e-8766-d82917e13592`, lakehouse id
`c61c659f-ecba-4de8-a5b7-25885eb3021f`). Monitored workspaces (configured in `config.json`,
`monitoredWorkspaces`): `WS_SagarFabric01`, `WS_SagarFabric03`, `WS_AutoClaimsPOC`, `WS_AutoFNOL`.

| Table | Grain | Written by | Purpose / how it's used |
|---|---|---|---|
| **`DimShortcut`** | one row per currently-existing shortcut | `NB_ShortcutInventory_DuplicateDetection` (full-overwrite each run, idempotent) | The ground-truth shortcut inventory across all monitored workspaces: `shortcut_sk` (deterministic BIGINT surrogate key, `xxhash64` of the natural key), `shortcut_name`, `hosting_workspace_id/name`, `hosting_item_id/name/type`, `shortcut_path`, `source_workspace_id/name`, `source_item_id/name`, `source_path`, `is_internal_onelake`, `snapshot_ts`. **Used by both copy-event detection notebooks** as the cross-reference lookup: the Warehouse notebook matches a parsed SQL statement's source table name against `DimShortcut` (keyed by `hosting_workspace_id` + `hosting_item_name`, spanning both Lakehouse- and Warehouse-hosted shortcuts); the Spark notebook matches an OpenLineage input dataset's real OneLake path against `DimShortcut` (keyed by `hosting_workspace_name` + `hosting_item_name` + `shortcut_name`, since OpenLineage reports the physical path, not the shortcut's display name, but the path's trailing segment is the shortcut name). |
| **`FactShortcutInventoryDiff`** | one row per inventory diff run × shortcut that changed | Same notebook, append-only | Control-plane change history: `change_type` ∈ {new, removed, target_changed}. Answers "when was this shortcut created/deleted." **Used** by `vw_FactCopyEvent_SourceStatus` (below) to enrich `FactCopyEvent` rows whose source shortcut has since been deleted, and as a fallback lookup (alongside current `DimShortcut`) when backfilling `matched_shortcut_database` on historical `FactCopyEvent` rows. |
| **`FactDuplicateShortcutGroup`** | one row per shortcut, tagged with its duplicate group | Same notebook | Two-pass duplicate detection per §1.1a (High = same hosting item, Medium = same workspace/different item). **Not consumed** by either copy-event detection notebook — purely a control-plane governance signal, surfaced on its own in the (not-yet-built) Power BI report. |
| **`FactCopyEvent`** | one row per detected copy event (one job/statement × one matched shortcut × one destination) | **Both** `NB_CopyEventDetection_Warehouse` (`engine='Warehouse'`, append) and `NB_CopyEventDetection_Spark` (`engine='Spark'`, append) — **shared table, not duplicated per engine** | The core fact table this whole solution exists to populate: `event_id`, `hosting_workspace_id/name`, `hosting_item_id/name`, `engine`, `matched_shortcut_name`, `matched_shortcut_database` (the shortcut's hosting item name — may differ from `hosting_item_name` for cross-item copies), `dest_table`, `source_column_count`, `dest_column_count`, `retained_column_count`, `retention_pct`, `is_select_star`, `is_shortcut_read_and_saved_as_is`, `threshold_pct_at_detection`, `query_start_time`, `detected_ts`. Both engines write the identical schema so downstream reporting/Data Agent queries never need to special-case engine type except to filter/group by it. |
| **`CopyEventWatermark`** | one row per Warehouse item | `NB_CopyEventDetection_Warehouse` only | Incremental watermark for the Warehouse engine: `item_id -> last_processed_start_time` (a Query Insights `start_time` value). Ensures each run only scans NEW query-history rows per warehouse (pushed down into the `WHERE start_time > ...` SQL clause, not filtered client-side) — never a full rescan. Advances only past rows that were actually written to `FactCopyEvent`, so a row that fails parsing/matching is retried next run rather than silently lost. |
| **`SparkLineageWatermark`** | one row per lineage NDJSON file | `NB_CopyEventDetection_Spark` only | Incremental watermark for the Spark engine: `file_path -> last_processed_byte_offset`. Ensures each run only reads the bytes appended to a lineage file since the last run (via an OneLake DFS HTTP `Range` request), not the whole file — never a full rescan. Only advances past complete, successfully-read lines; a trailing partial line (file still being written) is re-read next run. |
| **`vw_FactCopyEvent_SourceStatus`** (view, not a table) | derived, recomputed on every query | Built once by `NB_CopyEventDetection_Warehouse`, but reflects rows from BOTH engines automatically since it selects over `FactCopyEvent` | Enriches every `FactCopyEvent` row with `source_shortcut_exists_now` (bool) and `source_removed_ts`, by joining current `DimShortcut` and `FactShortcutInventoryDiff`'s most recent `removed` event for that shortcut. Zero incremental-processing cost (plain SQL view); this is how the solution answers "does this copy event's source shortcut still exist, and if not, when was it deleted" for reporting, without needing a dedicated reconciliation pipeline. |

**Config** (`Files/config/config.json`, read by every notebook): `monitoredWorkspaces` (workspace
id/name pairs), `detection.columnRetentionThresholdPercent` (default 80, the single current-effective
threshold — see §6.3's note on this being simpler than the target-state versioned `ConfigThresholds`
table), `detection.excludeWarehouseNamePatterns` (auto-generated Dataflow staging warehouses to
ignore), `polling.*` (intended schedule intervals, not yet wired to actual Fabric schedules for the
copy-event notebooks), `auth.*` (service principal credentials used by all notebooks for Fabric REST
API + SQL/storage token acquisition).

### 13.2 `NB_ShortcutInventory_DuplicateDetection` (control-plane, built first)

Enumerates every shortcut across the 4 monitored workspaces via the Fabric REST API
(`/workspaces/{id}/items/{itemId}/shortcuts`), overwrites `DimShortcut` (full snapshot, idempotent),
diffs against the prior snapshot into `FactShortcutInventoryDiff` (append, new/removed/changed rows
only), and computes the two-pass duplicate-group detection into `FactDuplicateShortcutGroup`. Scheduled
every 15 minutes.

### 13.3 `NB_CopyEventDetection_Warehouse` (Warehouse engine, Phase 1)

Discovers every Warehouse item in the monitored workspaces (excluding Fabric's own auto-generated
Dataflow-staging warehouses), and for each one:
1. Reads its incremental watermark from `CopyEventWatermark`.
2. Queries `<warehouse>.queryinsights.exec_requests_history` (**not** `sys.dm_exec_requests_history` —
   that DMV name does not resolve on Fabric Warehouse SQL endpoints) with the watermark pushed into the
   SQL `WHERE start_time > ...` clause, filtering to `CREATE TABLE AS SELECT`/`INSERT...SELECT`-shaped
   statements.
3. Regex-parses each statement's destination table + column list (or detects `SELECT *`) and its
   source table reference(s), correctly handling 3-part qualified names (`[db].[schema].[table]`,
   bracket-stripped per-segment) since a copy is frequently **cross-item** (e.g. a CTAS in `warehouse03`
   reading a shortcut hosted in `lakehouse03`).
4. Cross-references the parsed source table against `DimShortcut` (spanning both Lakehouse- and
   Warehouse-hosted shortcuts, resolved by the referenced database name, not just the querying item's
   own name).
5. Gets the true source column count via a 3-part-qualified `INFORMATION_SCHEMA.COLUMNS` query against
   the shortcut's actual hosting item (case-sensitive — the warehouse's collation is
   `Latin1_General_100_BIN2_UTF8`), computes `retention_pct`, and flags
   `is_shortcut_read_and_saved_as_is` per the §1.1 rule.
6. Appends detected rows to `FactCopyEvent` (`engine='Warehouse'`), advances `CopyEventWatermark`
   (merged, only past successfully-processed rows), and writes a human-readable JSON run summary to
   `Files/reports/copyevent_summary_*` for quick manual verification (the SQL analytics endpoint can lag
   behind the underlying Delta tables by several minutes).

**Validated**: 2/2 real test copy events correctly detected in the live tenant, including a genuine
cross-item case (shortcut hosted in `lakehouse03`, copied via CTAS run from `warehouse03`), both with
100% retention and correctly flagged.

### 13.4 `NB_CopyEventDetection_Spark` (Spark engine, Phase 1/2 boundary)

**Why OpenLineage, and not Workspace Monitoring's Spark execution-plan parsing (§4.1's original
design):** investigation during implementation found Workspace Monitoring's Spark telemetry exposes
only job-level status/duration, not the per-statement `ReadSchema`/`WriteSchema` physical-plan detail
the original design assumed. **OpenLineage** (an open-source lineage-emission Spark listener,
`io.openlineage.spark.agent.OpenLineageSparkListener`, bundled/available in the Fabric Spark runtime)
was spiked instead and validated end-to-end in `NB_OpenLineage_SparkLineageTest`: enabled via a
`%%configure -f` cell (must be the interactive session's first cell/first line — the Job Scheduler API
does not support `%%configure`; scheduled/API-triggered runs must instead pass the same Spark conf via
the run request's `executionData.configuration.conf`), configured with the **file transport**
(`spark.openlineage.transport.type=file`), it emits real per-write `COMPLETE` events containing:
- `outputs[].facets.schema.fields[]` — the real destination column list.
- `outputs[].facets.columnLineage.fields.<outCol>.inputFields[].transformations[]` — exact per-output-
  column lineage back to specific input columns, each tagged `DIRECT`/`IDENTITY` (a straight copy-
  through) vs. a derived/computed transform — genuinely more precise than name-matching, since it
  tracks actual data flow rather than coincidental name equality.
- `inputs[].namespace`/`.name` — the source dataset's real underlying OneLake ABFSS path (not the
  shortcut's display name).

This was confirmed with real tenant data: a full "read shortcut and save as-is" test produced a
17-column COMPLETE event with 100% `DIRECT`/`IDENTITY` retention; a deliberately-reduced 2-column copy
of the same source produced a matching 2-column event — proving the facet correctly distinguishes the
two cases.

**Production notebook design** (`NB_CopyEventDetection_Spark`, built following this validation):
1. **Prerequisite** (one-time setup per monitored Spark notebook, outside this notebook's scope):
   every monitored notebook enables OpenLineage via its own `%%configure -f` cell, with
   `spark.openlineage.transport.location` pointed at a **per-workspace, per-notebook** file under this
   solution's own lakehouse, following this naming convention:
   `Files/lineage/{workspace_name}/{notebook_name}_ol_events.ndjson`. This
   convention (one file per monitored notebook, not one shared file) avoids concurrent-write
   contention on a single file and lets the incremental watermark be tracked per file independently.
2. Discovers every file matching `Files/lineage/*/*_ol_events.ndjson`.
3. Reads only the bytes appended since each file's watermark (`SparkLineageWatermark`, byte-offset
   based, via an OneLake DFS `Range` HTTP request) — incremental, never a full file rescan.
4. Parses each new-line `COMPLETE` event with exactly one input dataset (multi-input joins are
   out-of-scope for this single-hop rule, intentionally skipped to avoid false positives).
5. Resolves the input's real OneLake path back to `DimShortcut` (matching on hosting workspace/item/
   shortcut name derived from the path's trailing segments).
6. Computes `retained_column_count` as the count of output columns whose `columnLineage.inputFields`
   are ALL `DIRECT`/`IDENTITY` transforms (a stricter, lineage-based definition of "retained" than the
   Warehouse notebook's name-based approach — correctly excludes a derived column even if it happens
   to share a name with a source column).
7. Computes `retention_pct` and flags `is_shortcut_read_and_saved_as_is` using the identical rule and
   the same `THRESHOLD_PCT` config value as the Warehouse notebook.
8. Appends to the **same** `FactCopyEvent` table (`engine='Spark'`), advances
   `SparkLineageWatermark`, and writes the same JSON-summary-to-Files pattern.

**Status:** notebook content built (`notebook_copyevent_spark_content.py`); not yet deployed/tested
end-to-end in Fabric as of this document's last update (deployment + a live test run with a source
notebook emitting to the new per-notebook lineage path are the immediate next steps).

### 13.5 Incremental design, as actually implemented (see also §5's note)

Two separate, purpose-built watermark tables instead of one generic `WatermarkState` table:
`CopyEventWatermark` (per-Warehouse-item, timestamp-based) and `SparkLineageWatermark` (per-lineage-
file, byte-offset-based). Both follow the same principle — merge defensively (only ever advance,
never regress an item/file's watermark) and only advance past units of work that were **successfully
written** to `FactCopyEvent`, so a row that fails parsing/matching is retried on the next run rather
than silently and permanently skipped.

**Lineage file layout and addressing (as hardened during live testing).** Each monitored Spark
notebook's OpenLineage file-transport listener (configured once via `%%configure` at session start)
appends NDJSON lineage events, forever, to a single fixed path per notebook:

```
/lakehouse/LH_ShortcutMonitoring/Files/lineage/{workspace_name}/{notebook_name}/ol_events.ndjson
```

The path is addressed using Fabric's portable `/lakehouse/<lakehouseName>/...` mount syntax rather
than an absolute ABFSS URL with an embedded workspace/lakehouse GUID. This requires each monitored
notebook to have `LH_ShortcutMonitoring` attached as a **known lakehouse** (not necessarily its
default), and resolves the shared monitoring lakehouse by name at runtime regardless of which
workspace the monitored notebook happens to run in — avoiding hardcoded GUIDs and ensuring lineage
always lands in one central location instead of each notebook's own default lakehouse (an earlier
bug: `/lakehouse/default/...` resolves to the *running* notebook's own default lakehouse, not the
shared one).

`NB_CopyEventDetection_Spark`'s discovery cell walks this structure two levels deep
(`workspace → notebook`) under `Files/lineage/` to enumerate all lineage files, then applies the
byte-offset watermark per file as described above.

**Multiple OpenLineage job-events per logical write (dedupe).** Delta's write path (e.g. `saveAsTable`
in overwrite/replace mode) emits *multiple* Spark-job-level `COMPLETE` events for a single logical
write — typically `atomic_replace_table_as_select` (metadata-only, no column lineage) →
`append_data_exec_v1` (the real data-movement job, carries `columnLineage`) →
`atomic_replace_table_as_select` again. Treating each event as an independent copy event would
over-count `FactCopyEvent` rows (e.g. 3 rows for one logical write). The detection notebook filters
candidate write events to those with a non-empty `columnLineage.fields`, then **dedupes by output
dataset** (keeping the richest/most complete event per dataset) before emitting to `FactCopyEvent`,
so one logical write always yields exactly one fact row.

**Lineage file cleanup (configurable, default off).** Because the lineage file grows unbounded for
the life of a monitored notebook's Spark session, an optional cleanup step runs at the end of each
successful detection run, controlled by two config keys (`detection.lineageFileCleanupMode`:
`"none"` | `"delete"` | `"archive"`, default `"none"`; `detection.lineageFileCleanupGraceMinutes`,
default 60). Files are only ever deleted/archived **as a whole** (never truncated or rewritten
in-place, to avoid any read-modify-write race with an actively-appending OpenLineage session), and
only when both are true: (1) the file is **fully consumed** — the run's final watermark offset equals
the file's current size in bytes, and (2) the file has been **idle past the grace period**, checked
via the file's `modifyTime` from the directory listing (if `modifyTime` is unavailable, cleanup is
skipped for that file — a conservative default). `"delete"` removes the file outright; `"archive"`
moves it to a parallel `Files/lineage_archive/{workspace}/{notebook}/` path. In either case the file's
`SparkLineageWatermark` row is dropped so a new file later created at the same path restarts cleanly
at offset 0. Cleanup activity (deleted/archived/skipped files) is included in both the run's console
summary and its JSON report artifact.

> **v2 enhancement (not yet implemented):** the `modifyTime` + grace-period check is a heuristic, not
> a hard guarantee that a monitored notebook's Spark session has ended — it only infers idleness from
> recent write activity. A stronger design would additionally query Fabric's session/Livy API to check
> whether the source notebook's Spark session is still active before deleting/archiving its lineage
> file, removing reliance on a timing heuristic entirely. Deferred as a future hardening step; the
> grace-period approach is considered acceptable for now given cleanup defaults to `"none"` and, when
> enabled, is expected to use a conservative grace period (60+ minutes, longer for interactive/idle-
> prone monitored notebooks).

### 13.6 `FactCopyEvent` schema, exactly as created (both engines write this identical `StructType`)

```
event_id                        STRING   -- Warehouse: {item_id}|{distributed_statement_id}
                                          -- Spark:     {lineage_file_path}|{openlineage_run_id}
hosting_workspace_id             STRING
hosting_workspace_name           STRING
hosting_item_id                  STRING   -- NULL for Spark rows (no stable per-run item GUID)
hosting_item_name                STRING   -- Warehouse: the Warehouse item name
                                          -- Spark:     the monitored notebook's name
engine                           STRING   -- 'Warehouse' | 'Spark'
matched_shortcut_name            STRING
matched_shortcut_database        STRING   -- the shortcut's hosting item name (may differ from hosting_item_name for cross-item copies)
dest_table                       STRING
source_column_count              INT
dest_column_count                INT
retained_column_count            INT
retention_pct                    DOUBLE
is_select_star                   BOOLEAN
is_shortcut_read_and_saved_as_is BOOLEAN
threshold_pct_at_detection       DOUBLE
query_start_time                 STRING   -- kept as STRING (not TIMESTAMP) to avoid a Delta schema-merge conflict encountered during implementation
detected_ts                      STRING
```

### 13.7 Open items / next steps (as of this document's last update)

- Deploy `NB_CopyEventDetection_Spark` to Fabric and validate end-to-end against a real monitored
  notebook emitting lineage to the new per-notebook path convention.
- Clean up throwaway debugging notebooks (`NB_MinimalSanityTest`, `NB_ShortcutReadIsolationTest`) and
  reset the OpenLineage spike's test lineage file (currently mixed with earlier stale test events).
- Schedule both copy-event detection notebooks on a recurring interval (currently manual/on-demand
  runs only).
- Build the Power BI semantic model + report (§8.1) — not started.
- Build the Fabric Data Agent (§8.2) — not started.
- Dataflow Gen2 engine detection (§4.3) — not started.
- Eventstream/Eventhouse/Data Activator real-time backbone and alerting (§3 note, §7) — not started;
  current MVP relies on notebook run frequency alone for latency.
- Admin Activity Events-based shortcut attribution (`created_by`/`created_at`) and the independent
  fast-path CreateShortcut alert (§4.5) — not started.
- Decide whether the OpenLineage `file` transport is acceptable for production long-term, or whether a
  custom OneLake-backed transport (previously researched as "Option 2") is worth building to remove
  the file transport's known concurrent-write-drop risk.
- **v2:** replace the lineage-file cleanup's `modifyTime` + grace-period idleness heuristic (§13.5)
  with an active check against Fabric's session/Livy API to confirm the monitored notebook's Spark
  session has actually ended before deleting/archiving its lineage file.

---

## 14. Alternate Approach Considered But Not Implemented: OneLake Diagnostics + Delta History Correlation

The following design was researched as an alternative path to Spark-engine detection (an alternative
to OpenLineage, §13.4) using OneLake's built-in diagnostic logging instead of a Spark listener. It was
**not** the approach ultimately implemented — OpenLineage was adopted instead, since it provides true
column-level lineage natively, whereas this approach only provides file/API-level access telemetry
that must be heuristically correlated with Delta table history to *infer* a copy event, with no
column-level detail at all without significant additional work. Retained here for reference/comparison
only, and because emerging OneLake diagnostics-based tooling elsewhere may make it more attractive in
the future.

**Approach:** OneLake Diagnostics + Delta Table History correlation
**Scope:** Spark/Lakehouse workloads (Warehouse and KQL use a parallel pattern, noted below)

### 14.1 Objective

Determine, after the fact and on a recurring basis:
1. **Which OneLake shortcuts were read**, by whom, and when.
2. **Whether that read was followed by a write** that materialized the shortcut's data into a new,
   physically-copied Delta table.

This is a **detective control**, not a preventive one.

### 14.2 Prerequisites

| Requirement | Detail |
|---|---|
| Fabric capacity | Any capacity that supports OneLake diagnostics (current GA feature) |
| Workspace admin role | On every **source** workspace whose shortcuts/data you want to monitor |
| Contributor+ role | On the workspace hosting the **destination diagnostics Lakehouse** |
| Tenant setting | Admin Portal → Tenant settings → OneLake → enable "diagnostic logs capture end-user identifiable information (EUII)" — without this, `executingUPN`/identity fields will be blank or hashed |
| Dedicated diagnostics workspace (recommended) | Isolates audit data from operational workloads; separation of duties |
| A notebook-executing identity (user or service principal) | Needs Read access broad enough to run `DESCRIBE HISTORY` on candidate destination tables tenant-wide — treat as a sensitive, audit-only credential |

### 14.3 Architecture Overview

```
 ┌─────────────────────┐        ┌──────────────────────────┐
 │ Source Workspace(s)  │  JSON  │  Diagnostics Lakehouse    │
 │ (sensitive shortcuts)│ ─────▶ │  Files/DiagnosticLogs/... │
 └─────────────────────┘  ~1hr  └──────────┬───────────────┘
                                            │
                                            ▼
                              ┌───────────────────────────┐
                              │ Bronze: raw JSON → Delta   │
                              └──────────┬────────────────┘
                                         ▼
                       ┌─────────────────────────────────┐
                       │ Silver: typed events              │
                       │  - shortcut_reads (isShortcut=T)  │
                       │  - write_events (FabricWorkload/  │
                       │    CreateFile/AppendData/etc.)    │
                       └──────────┬─────────────────────┘
                                  ▼
                    ┌───────────────────────────────────┐
                    │ Correlation: read → candidate write │
                    │  (same identity, time window)       │
                    └──────────┬──────────────────────────┘
                               ▼
                 ┌─────────────────────────────────────┐
                 │ Confirmation: DESCRIBE HISTORY on the │
                 │ candidate destination table            │
                 └──────────┬────────────────────────────┘
                             ▼
               ┌───────────────────────────────────────┐
               │ Gold: ShortcutAuditFindings (Delta)      │
               │  → Power BI report / alerting            │
               └───────────────────────────────────────┘
```

### 14.4 Step 1 — Enable OneLake Diagnostics

**Manual (per workspace):** Workspace Settings → OneLake tab → toggle "Add diagnostic events to a
lakehouse" → select destination Lakehouse (same capacity; same VNet if Private Link inbound
protection is used) → optionally set an immutability retention period. Allow up to **one hour** for
events to begin flowing (not a real-time feed).

**Programmatic:**
```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/onelake/settings/modifyDiagnostics
Content-Type: application/json
Authorization: ******

{
  "status": "Enabled",
  "destination": {
    "type": "Lakehouse",
    "lakehouse": {
      "referenceType": "ById",
      "itemId": "{diagnosticsLakehouseItemId}",
      "workspaceId": "{diagnosticsWorkspaceId}"
    }
  }
}
```
Check status: `GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/onelake/settings`.
Set an immutability policy via `.../onelake/settings/modifyImmutabilityPolicy`.

### 14.5 Step 2 — Log Schema

Events land as append-only JSON files, Hive-style partitioned, under
`Files/DiagnosticLogs/OneLake/Workspaces/<WorkspaceId>/...` (verify exact partitioning/casing
empirically per tenant before building production code).

| Field | Purpose |
|---|---|
| `isShortcut` | Boolean — was this access through a shortcut |
| `accessedViaResource` | The shortcut's own location, when `isShortcut = true` |
| `Resource` | The resource path actually being accessed |
| `operationName` / `operationCategory` | e.g. `ReadFileOrGetBlob` (Read), `CreateFile`/`AppendDataToFile`/`FlushDataToFile` (Write), or `FabricWorkloadAccess` (engine-mediated grant — see limitation below) |
| `executingUPN` / `executingPrincipalId` | Who did it (requires the EUII tenant setting) |
| `accessStartTime` | Timestamp |
| `itemId` / `itemType` / `workspaceId` | Which Fabric item/workspace was involved |
| `correlationId` | Per-operation correlation GUID — not confirmed to span a whole Spark session; validate empirically |
| `callerIpAddress`, `originatingApp` | Additional context |

### 14.6 Step 3 — Bronze Layer: Ingest Raw Logs

```python
DIAG_LOG_PATH = "Files/DiagnosticLogs/OneLake/Workspaces/"  # adjust to your tenant's actual layout
BRONZE_TABLE = "onelake_diag_bronze"

df_raw = (
    spark.read
    .option("multiLine", False)   # OneLake diagnostics writes newline-delimited JSON
    .json(DIAG_LOG_PATH)
)

(
    df_raw.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
```

### 14.7 Step 4 — Silver Layer: Shortcut Reads and Candidate Writes

```python
from pyspark.sql import functions as F

bronze = spark.table(BRONZE_TABLE)

shortcut_reads = (
    bronze
    .filter(F.col("isShortcut") == True)
    .filter(F.col("operationCategory") == "Read")
    .select(
        F.col("executingPrincipalId").alias("reader_id"),
        F.col("executingUPN").alias("reader_upn"),
        F.col("accessedViaResource").alias("shortcut_path"),
        F.col("Resource").alias("target_resource"),
        F.col("workspaceId").alias("source_workspace_id"),
        F.col("itemId").alias("source_item_id"),
        F.col("accessStartTime").alias("read_time"),
        F.col("correlationId").alias("read_correlation_id"),
    )
)
shortcut_reads.write.format("delta").mode("overwrite").saveAsTable("shortcut_reads_silver")

write_events = (
    bronze
    .filter(
        (F.col("operationCategory") == "Write") |
        (F.col("operationName") == "FabricWorkloadAccess")
    )
    .select(
        F.col("executingPrincipalId").alias("writer_id"),
        F.col("executingUPN").alias("writer_upn"),
        F.col("Resource").alias("destination_resource"),
        F.col("workspaceId").alias("destination_workspace_id"),
        F.col("itemId").alias("destination_item_id"),
        F.col("accessStartTime").alias("write_time"),
        F.col("correlationId").alias("write_correlation_id"),
    )
)
write_events.write.format("delta").mode("overwrite").saveAsTable("write_events_silver")
```

### 14.8 Step 5 — Correlate Reads to Candidate Writes

```python
CORRELATION_WINDOW_MINUTES = 30

reads = spark.table("shortcut_reads_silver")
writes = spark.table("write_events_silver")

# Strategy A (strongest, if correlationId is confirmed to span a session):
candidates_by_correlation = (
    reads.alias("r")
    .join(writes.alias("w"), F.col("r.read_correlation_id") == F.col("w.write_correlation_id"), "inner")
    .withColumn("confidence", F.lit("HIGH_CORRELATION_ID"))
)

# Strategy B (fallback, identity + time-window heuristic):
candidates_by_window = (
    reads.alias("r")
    .join(
        writes.alias("w"),
        (F.col("r.reader_id") == F.col("w.writer_id")) &
        (F.col("w.write_time").between(
            F.col("r.read_time"),
            F.col("r.read_time") + F.expr(f"INTERVAL {CORRELATION_WINDOW_MINUTES} MINUTES")
        )),
        "inner"
    )
    .withColumn("confidence", F.lit("MEDIUM_TIME_WINDOW"))
)

candidates = candidates_by_correlation.unionByName(candidates_by_window, allowMissingColumns=True).dropDuplicates()
candidates.write.format("delta").mode("overwrite").saveAsTable("materialization_candidates")
```

A same-identity, close-in-time read+write is a strong hint, not proof.

### 14.9 Step 6 — Confirm Materialization via Delta Table History

```python
from delta.tables import DeltaTable
from pyspark.sql import Row

def check_delta_history(destination_path, write_time, window_minutes=5):
    try:
        hist_df = spark.sql(f"DESCRIBE HISTORY delta.`{destination_path}`")
    except Exception:
        return None  # not a Delta table at that path, or no access

    matches = (
        hist_df
        .filter(F.col("operation").isin("WRITE", "CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"))
        .filter(F.col("timestamp").between(
            F.lit(write_time) - F.expr(f"INTERVAL {window_minutes} MINUTES"),
            F.lit(write_time) + F.expr(f"INTERVAL {window_minutes} MINUTES")
        ))
    )
    if matches.count() == 0:
        return None
    row = matches.orderBy(F.col("timestamp").desc()).first()
    return {
        "delta_operation": row["operation"],
        "delta_version": row["version"],
        "delta_user": row["userName"] if "userName" in row.asDict() else None,
        "delta_timestamp": row["timestamp"],
        "operation_parameters": str(row["operationParameters"]),
    }

candidates_pd = candidates.toPandas()  # fine at audit-log scale; use mapInPandas for very large volumes
enriched_rows = []
for _, c in candidates_pd.iterrows():
    enrichment = check_delta_history(c["destination_resource"], c["write_time"])
    if enrichment:
        enriched_rows.append(Row(
            shortcut_path=c["shortcut_path"], reader_upn=c["reader_upn"], read_time=c["read_time"],
            source_workspace_id=c["source_workspace_id"], destination_resource=c["destination_resource"],
            destination_workspace_id=c["destination_workspace_id"], write_time=c["write_time"],
            confidence=c["confidence"], **enrichment
        ))
confirmed_df = spark.createDataFrame(enriched_rows)
```

### 14.10 Step 7 — Gold Layer & Step 8/9 — Orchestration + Alerting

Append `confirmed_df` (with `detected_at = current_timestamp()`) to `ShortcutAuditFindings`; wrap the
whole pipeline in a scheduled notebook (daily, or hourly at most — the diagnostics feed itself lags
up to an hour); alert via a simple end-of-notebook Teams/email webhook, native Data Activator on the
Gold table, or SIEM integration (land `ShortcutAuditFindings` in Sentinel/Log Analytics alongside
other security signals).

### 14.11 Known Limitations

1. **`FabricWorkloadAccess` is coarse** — Spark-mediated access logs as "temporary access was
   granted," not a full per-call trace; a Spark session touching several shortcuts/tables in one job
   may not disambiguate cleanly.
2. **`correlationId` scope is unverified** — validate empirically whether it spans a whole Spark
   session or is per-call; if per-call, the time-window strategy becomes primary.
3. **Identity capture is opt-in** — no EUII tenant setting, no attribution.
4. **Detection, not prevention** — runs after the fact, on an hours-old feed.
5. **Engine coverage** — this design covers Spark/Lakehouse; Warehouse and KQL/Eventhouse need the
   parallel pattern (their native query/command-text logs are actually more direct, since read+write
   typically appear in a single statement's text).
6. **False negatives from dynamic code** — destination path comes from the runtime access record (not
   static code analysis), so this is more robust than pure static analysis in this one respect, but
   any supplementary plan-based tooling would be weaker against dynamic names.
7. **Audit credential scope** — the service principal running `DESCRIBE HISTORY` tenant-wide needs
   broad read access; treat as a privileged, audit-only identity.

### 14.12 Rollout Plan (as originally proposed for this alternate approach)

| Phase | Scope | Goal |
|---|---|---|
| 1 — Pilot | One or two workspaces with known sensitive shortcuts | Validate schema field names, `correlationId` behavior, time-window tuning |
| 2 — Expand | All workspaces holding sensitive data | Full Bronze→Gold pipeline running daily, dashboard live |
| 3 — Harden | Tenant-wide | Automate diagnostics enablement via REST API, add alerting, integrate with SIEM, add Warehouse/KQL parallel pipelines |

### 14.13 Open Items to Validate Before Production (had this approach been chosen)

- Exact JSON field casing and partition layout in your tenant.
- Whether `correlationId` spans a Spark session.
- Retention needs vs. the diagnostics immutability policy window.
- Whether Workspace Monitoring's expanding engine coverage eventually subsumes part of this pipeline.
