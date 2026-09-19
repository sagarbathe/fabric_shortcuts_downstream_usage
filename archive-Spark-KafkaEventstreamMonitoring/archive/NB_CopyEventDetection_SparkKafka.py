# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c61c659f-ecba-4de8-a5b7-25885eb3021f",
# META       "default_lakehouse_name": "LH_ShortcutMonitoring",
# META       "default_lakehouse_workspace_id": "c5c50c6e-30d1-4d2e-8766-d82917e13592",
# META       "known_lakehouses": [
# META         {
# META           "id": "c61c659f-ecba-4de8-a5b7-25885eb3021f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## NB_CopyEventDetection_SparkKafka
# Kafka/Eventstream-based variant of the "shortcut read and saved as is" rule (design doc §1.1, §4.1).
# Companion to `NB_CopyEventDetection_Spark` (file-transport engine) and `NB_CopyEventDetection_Warehouse`
# (Warehouse engine) - writes to the SAME `FactCopyEvent` table, distinguished by `engine = 'SparkKafka'`.
# This is NEW, additive code - it does not modify either sibling notebook or its tables.
#
# --- Prerequisite (out of scope for this notebook) ---
# Every monitored Spark notebook must have the OpenLineage listener enabled via the attached
# `ENV_OpenLineage` environment (Kafka transport -> `ES_OpenLineageEvents` Eventstream ->
# `LH_ShortcutMonitoring.ol_lineage_events_v3` Lakehouse sink table). See
# `fabric_openlineage_kafka_design.md` and the `Spark-KafkaEventstreamMonitoring` reference folder
# for how that pipeline was built. Unlike the file-transport notebook, there is no per-notebook
# lineage file to discover - ALL monitored notebooks' events land in the SAME shared sink table,
# distinguished per-event by `run.facets.spark_properties.properties` (workspace id, notebook item id
# and name are embedded there by Fabric's Spark runtime).
#
# --- What this notebook does ---
#   1. Reads only the NEW rows appended to `ol_lineage_events_v3` since the last run (incremental -
#      requirement #4), using a single-row watermark table `SparkKafkaLineageWatermark` keyed on the
#      Eventstream-injected `EventEnqueuedUtcTime` column (append-only/monotonic per commit batch -
#      the Kafka/Eventstream analogue of the file-transport notebook's per-file byte offset).
#   2. Parses each COMPLETE event's `inputs_json`/`outputs_json` (stored as STRING columns, not
#      structs - see "IMPORTANT" note below) to recover `outputs[].facets.schema` (destination
#      columns), `outputs[].facets.columnLineage.fields` (per-output-column source mapping), and
#      `inputs[].namespace`/`.name` (source dataset's real OneLake ABFSS path).
#   3. Resolves each input's ABFSS path back to a known shortcut in `DimShortcut` (matches on
#      hosting_workspace + hosting_item + shortcut's real underlying OneLake path, NOT the shortcut's
#      logical display name - identical matching logic to the file-transport notebook).
#   4. Computes column-retention % and flags is_shortcut_read_and_saved_as_is using the SAME rule and
#      SAME configurable threshold (config.detection.columnRetentionThresholdPercent) as both sibling
#      notebooks.
#   5. Appends one row per detected event to the shared `FactCopyEvent` table (engine = 'SparkKafka').
#      Unlike the file-transport notebook, `hosting_item_id` IS populated here with the real notebook
#      item GUID (available from the event's `run.facets.spark_properties.properties['trident.artifact.id']`
#      field - the file-transport notebook could not do this since it only had a lineage-file FOLDER
#      NAME to go on, not a stable GUID).
#   6. Advances the single-row `SparkKafkaLineageWatermark` watermark to the max `EventEnqueuedUtcTime`
#      seen this run (across ALL rows read, whether or not they matched a shortcut - mirrors the
#      file-transport notebook's "advance past every byte read, not just matched ones" semantics).
#
# IMPORTANT - why inputs/outputs are STRING (JSON-encoded) columns, not native structs:
# The Eventstream Lakehouse destination auto-infers the sink table's Delta schema from whichever
# event batch it processes first. OpenLineage events carry wildly different `inputs`/`outputs` shapes
# per event (a job-span event has empty arrays; a real dataset read/write event has a deeply nested,
# per-source-table-varying `array<struct<namespace,name,facets,...>>`). Locking either shape in as
# the column's real Delta type causes every event of the OTHER shape to be silently rejected
# (`OutputDataConversionError.TypeConversionError`) and never land at all. The fix (see this
# notebook's sibling Eventstream `ES_OpenLineageEvents`, operator `SqlFlatten`) uses the Eventstream
# SQL operator's `json_stringify()` function to serialize `inputs`/`outputs` to a STRING before they
# reach the destination - this is schema-stable regardless of shape, at the cost of needing a
# `json.loads()` in THIS notebook to get back to structured data. This notebook does that parsing.
#
# NOT covered by this notebook (unchanged from the design doc):
#   - Enabling OpenLineage / the Kafka transport / the Eventstream pipeline itself (prerequisite).
#   - Dataflow Gen2 engine (deferred per rollout plan).
#   - Any cleanup/truncation of `ol_lineage_events_v3` - THAT TABLE IS OWNED BY THE EVENTSTREAM
#     DESTINATION. Deleting/dropping/truncating it out from under a running Eventstream corrupts the
#     destination's write path (confirmed during this build - see 00_README.md troubleshooting notes);
#     if it ever needs to be reset, use the documented `06_reset_eventstream_destination.ps1`-style fix
#     (repoint the destination at a new table name), never touch the old table directly.


# CELL ********************

import json, traceback
from datetime import datetime, timezone

# Resolve the solution's own lakehouse (where config.json/DimShortcut/FactCopyEvent/etc. live) from
# the notebook's runtime attachment context, same pattern as both sibling notebooks.
_ctx = notebookutils.runtime.context
LAKEHOUSE_WORKSPACE_ID = _ctx["defaultLakehouseWorkspaceId"]
LAKEHOUSE_ID = _ctx["defaultLakehouseId"]
LAKEHOUSE_ABFSS = f"abfss://{LAKEHOUSE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"

# The Eventstream Lakehouse destination's sink table (see ES_OpenLineageEvents topology). If the
# destination is ever reset to a new table name (per the destination-corruption fix documented in
# 00_README.md), update this constant to match - it is intentionally NOT auto-discovered, since
# there is no reliable way to distinguish "the current live sink table" from stale/abandoned ones
# left behind by prior resets (ol_lineage_events, ol_lineage_events_v2, ...) purely from the
# Lakehouse's table listing.
SOURCE_EVENTS_TABLE = "ol_lineage_events_v3"

def log_error_to_lakehouse(stage, exc):
    try:
        err_text = f"STAGE: {stage}\nTIME: {datetime.now(timezone.utc).isoformat()}\n\n{traceback.format_exc()}"
        rdd = spark.sparkContext.parallelize([err_text], 1)
        rdd.saveAsTextFile(f"{LAKEHOUSE_ABFSS}/Files/logs/copyevent_sparkkafka_error_{int(datetime.now().timestamp())}.log")
    except Exception:
        pass

try:
    config_df = spark.read.text(f"{LAKEHOUSE_ABFSS}/Files/config/config.json")
    config_text = "\n".join([r["value"] for r in config_df.collect()])
    # Defensive: tolerate a stray BOM/prefix before the first '{' (same guard as the sibling
    # notebooks, in case a historical save of this file picked one up).
    first_brace = config_text.find("{")
    if first_brace > 0:
        config_text = config_text[first_brace:]
    config = json.loads(config_text)
except Exception as e:
    log_error_to_lakehouse("load_config", e)
    raise

MONITORED_WORKSPACES = config["monitoredWorkspaces"]
THRESHOLD_PCT = float(config["detection"]["columnRetentionThresholdPercent"])
WORKSPACE_ID_TO_NAME = {w["workspaceId"]: w["workspaceName"] for w in MONITORED_WORKSPACES}

print(f"Loaded config. Monitored workspaces: {list(WORKSPACE_ID_TO_NAME.values())}")
print(f"Column-retention threshold: {THRESHOLD_PCT}%")
print(f"Source events table: Tables/{SOURCE_EVENTS_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Read the incremental watermark FIRST (before reading any new events), so this run only
#     considers rows appended after the last run's max EventEnqueuedUtcTime (requirement #4: no full
#     rescan). Single-row table (unlike the file-transport notebook's per-file watermark) since there
#     is exactly one shared source table here.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql import functions as F

WATERMARK_TABLE = "SparkKafkaLineageWatermark"

try:
    watermark_table_exists = spark.catalog.tableExists(WATERMARK_TABLE)
    if watermark_table_exists:
        wm_row = spark.read.table(WATERMARK_TABLE).collect()
        last_processed_enqueued_time = wm_row[0]["last_processed_enqueued_time"] if wm_row else None
    else:
        last_processed_enqueued_time = None
    print(f"Watermark (last_processed_enqueued_time): {last_processed_enqueued_time}")
except Exception as e:
    log_error_to_lakehouse("read_watermark", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Read only NEW rows from the sink table (direct Delta path read - avoids the Spark catalog,
#     which showed intermittent flakiness during this build; see 00_README.md). ---
try:
    if not spark.catalog.tableExists(SOURCE_EVENTS_TABLE):
        print(f"Source table '{SOURCE_EVENTS_TABLE}' does not exist yet - nothing to process this run.")
        new_events_df = None
    else:
        src_df = spark.read.format("delta").load(f"Tables/{SOURCE_EVENTS_TABLE}")
        if last_processed_enqueued_time is not None:
            src_df = src_df.filter(F.col("EventEnqueuedUtcTime") > F.lit(last_processed_enqueued_time))
        new_events_df = src_df

    new_event_rows = new_events_df.collect() if new_events_df is not None else []
    print(f"New event row(s) read this run: {len(new_event_rows)}")
except Exception as e:
    log_error_to_lakehouse("read_new_events", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Parse each row's inputs_json/outputs_json back into structured data, and pull the notebook's
#     real workspace id / item id / display name out of run.facets.spark_properties.properties
#     (embedded by Fabric's Spark runtime on every Spark job, regardless of which notebook it is). ---
all_new_events = []  # list of dicts: hosting_workspace_id/name, hosting_item_id/name, event(dict), enqueued_time
max_enqueued_time_seen = last_processed_enqueued_time

try:
    for r in new_event_rows:
        rd = r.asDict(recursive=True)
        enqueued_time = rd.get("EventEnqueuedUtcTime")
        if enqueued_time is not None and (max_enqueued_time_seen is None or enqueued_time > max_enqueued_time_seen):
            max_enqueued_time_seen = enqueued_time

        try:
            inputs = json.loads(rd.get("inputs_json") or "[]")
        except Exception:
            inputs = []
        try:
            outputs = json.loads(rd.get("outputs_json") or "[]")
        except Exception:
            outputs = []

        run_facets = (rd.get("run") or {}).get("facets") or {}
        spark_props = (run_facets.get("spark_properties") or {}).get("properties") or {}
        hosting_workspace_id = spark_props.get("trident.artifact.workspace.id") or spark_props.get("trident.workspace.id")
        hosting_item_id = spark_props.get("trident.artifact.id")
        hosting_item_name = spark_props.get("trident.artifact.name") or spark_props.get("spark.synapse.context.notebookname")
        hosting_workspace_name = WORKSPACE_ID_TO_NAME.get(hosting_workspace_id, hosting_workspace_id)

        ev = {
            "eventType": rd.get("eventType"),
            "eventTime": rd.get("eventTime"),
            "run": rd.get("run"),
            "job": rd.get("job"),
            "inputs": inputs,
            "outputs": outputs,
        }

        all_new_events.append({
            "hosting_workspace_id": hosting_workspace_id,
            "hosting_workspace_name": hosting_workspace_name,
            "hosting_item_id": hosting_item_id,
            "hosting_item_name": hosting_item_name,
            "event": ev,
            "enqueued_time": enqueued_time,
        })

    print(f"Parsed {len(all_new_events)} event(s). New max EventEnqueuedUtcTime seen: {max_enqueued_time_seen}")
except Exception as e:
    log_error_to_lakehouse("parse_new_events", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Extract per-output-column write detail from each COMPLETE event with schema/columnLineage
#     facets, then dedupe multiple COMPLETE events targeting the same output dataset within this
#     run down to the richest one - IDENTICAL logic/rationale to NB_CopyEventDetection_Spark (a
#     single logical write can emit several Spark-job-level COMPLETE events, only one of which
#     carries the columnLineage facet). ---
candidate_writes_raw = []
for item in all_new_events:
    ev = item["event"]
    if ev.get("eventType") != "COMPLETE":
        continue
    inputs = ev.get("inputs", [])
    outputs = ev.get("outputs", [])
    if not inputs or not outputs:
        continue

    for out in outputs:
        schema_facet = out.get("facets", {}).get("schema", {})
        out_columns = [f.get("name") for f in schema_facet.get("fields", [])]
        col_lineage_fields = out.get("facets", {}).get("columnLineage", {}).get("fields", {})

        if not col_lineage_fields:
            continue

        candidate_writes_raw.append({
            "hosting_workspace_id": item["hosting_workspace_id"],
            "hosting_workspace_name": item["hosting_workspace_name"],
            "hosting_item_id": item["hosting_item_id"],
            "hosting_item_name": item["hosting_item_name"],
            "job_name": ev.get("job", {}).get("name"),
            "event_time": ev.get("eventTime"),
            "run_id": ev.get("run", {}).get("runId"),
            "output_namespace": out.get("namespace"),
            "output_name": out.get("name"),
            "output_columns": out_columns,
            "column_lineage_fields": col_lineage_fields,
            "inputs": inputs,
        })

best_by_output = {}
for cw in candidate_writes_raw:
    key = (cw["output_namespace"], cw["output_name"])
    existing = best_by_output.get(key)
    if existing is None:
        best_by_output[key] = cw
        continue
    existing_richness = len(existing["column_lineage_fields"])
    cw_richness = len(cw["column_lineage_fields"])
    if cw_richness > existing_richness or (
        cw_richness == existing_richness and (cw["event_time"] or "") > (existing["event_time"] or "")
    ):
        best_by_output[key] = cw

candidate_writes = list(best_by_output.values())

print(f"Raw COMPLETE write event(s) with non-empty columnLineage: {len(candidate_writes_raw)}")
print(f"Candidate write event(s) after dedupe by output dataset: {len(candidate_writes)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Cross-reference each write's INPUT(s) against DimShortcut - IDENTICAL matching logic to
#     NB_CopyEventDetection_Spark (OpenLineage reports the shortcut's real underlying OneLake path,
#     not its logical display name, so the match is on hosting workspace + hosting item + trailing
#     path segment, not a name-string comparison). Only single-input writes are considered in scope
#     (multi-input joins are intentionally excluded to avoid false positives). ---
try:
    dim_shortcut_df = spark.read.table("DimShortcut")
    shortcut_lookup = {}
    for r in dim_shortcut_df.collect():
        key = (r["hosting_workspace_name"].lower(), r["hosting_item_name"].lower(), r["shortcut_name"].lower())
        shortcut_lookup[key] = r

    def resolve_input_to_shortcut(input_ds):
        namespace = (input_ds.get("namespace") or "")
        name = (input_ds.get("name") or "")
        ws_part = namespace.split("//")[-1].split("@")[0] if "//" in namespace else namespace
        name_segments = [seg for seg in name.strip("/").split("/") if seg]
        if len(name_segments) < 3:
            return None
        item_part = name_segments[0].split(".")[0]
        leaf_name = name_segments[-1]
        key = (ws_part.lower(), item_part.lower(), leaf_name.lower())
        return shortcut_lookup.get(key)

    matched_writes = []
    for cw in candidate_writes:
        if len(cw["inputs"]) != 1:
            continue
        matched_shortcut = resolve_input_to_shortcut(cw["inputs"][0])
        if matched_shortcut is None:
            continue
        cw["matched_shortcut_name"] = matched_shortcut["shortcut_name"]
        cw["matched_shortcut_hosting_workspace_id"] = matched_shortcut["hosting_workspace_id"]
        cw["matched_shortcut_hosting_workspace_name"] = matched_shortcut["hosting_workspace_name"]
        cw["matched_shortcut_hosting_item_name"] = matched_shortcut["hosting_item_name"]
        input_schema_fields = cw["inputs"][0].get("facets", {}).get("schema", {}).get("fields", [])
        cw["input_column_count"] = len(input_schema_fields)
        matched_writes.append(cw)

    print(f"Of {len(candidate_writes)} candidate write(s), {len(matched_writes)} matched to a single known shortcut input.")
except Exception as e:
    log_error_to_lakehouse("crossref_dimshortcut", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Compute column retention % and decide is_shortcut_read_and_saved_as_is - IDENTICAL rule to
#     both sibling notebooks: flagged if the write used ALL source columns (SELECT * equivalent) OR
#     retention% > THRESHOLD_PCT, even if extra new columns were added. Retained = output columns
#     whose columnLineage traces back via a DIRECT/IDENTITY transformation from the matched
#     shortcut's input (excludes derived/computed columns even if same-named). ---
fact_copy_event_rows = []
now_ts = datetime.now(timezone.utc).isoformat()

try:
    for cw in matched_writes:
        input_col_count = cw["input_column_count"]
        if input_col_count == 0:
            print(f"  WARN: input_column_count was 0 for shortcut '{cw['matched_shortcut_name']}' "
                  f"(job {cw['job_name']}) - skipping (source schema facet missing/empty).")
            continue

        col_lineage_fields = cw["column_lineage_fields"]
        retained_count = 0
        for out_col, lineage_info in col_lineage_fields.items():
            input_fields = lineage_info.get("inputFields", [])
            is_direct_copy = bool(input_fields) and all(
                any(t.get("subtype") == "IDENTITY" for t in f.get("transformations", []))
                for f in input_fields
            )
            if is_direct_copy:
                retained_count += 1

        dest_col_count = len(cw["output_columns"])
        is_select_star = (retained_count == input_col_count) and (dest_col_count == input_col_count)
        retention_pct = round((retained_count / input_col_count) * 100, 1)
        is_flagged = is_select_star or (retention_pct > THRESHOLD_PCT)

        fact_copy_event_rows.append({
            "event_id": f"kafka|{cw['run_id']}|{cw['output_namespace']}|{cw['output_name']}",
            "hosting_workspace_id": cw["hosting_workspace_id"],
            "hosting_workspace_name": cw["hosting_workspace_name"],
            "hosting_item_id": cw["hosting_item_id"],
            "hosting_item_name": cw["hosting_item_name"],
            "engine": "SparkKafka",
            "matched_shortcut_name": cw["matched_shortcut_name"],
            "matched_shortcut_database": cw["matched_shortcut_hosting_item_name"],
            "dest_table": cw["output_name"],
            "source_column_count": input_col_count,
            "dest_column_count": dest_col_count,
            "retained_column_count": retained_count,
            "retention_pct": retention_pct,
            "is_select_star": is_select_star,
            "is_shortcut_read_and_saved_as_is": is_flagged,
            "threshold_pct_at_detection": THRESHOLD_PCT,
            "query_start_time": cw["event_time"],
            "detected_ts": now_ts,
        })

    print(f"Computed retention % for {len(fact_copy_event_rows)} copy event(s); "
          f"{sum(1 for r in fact_copy_event_rows if r['is_shortcut_read_and_saved_as_is'])} flagged as read-and-saved-as-is.")
except Exception as e:
    log_error_to_lakehouse("compute_column_retention", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema matches NB_CopyEventDetection_Warehouse/_Spark's FACT_COPY_EVENT_SCHEMA exactly - this
# notebook appends to the SAME shared FactCopyEvent table (all three engines' rows coexist,
# distinguished by the 'engine' column). Unlike the file-transport Spark notebook, hosting_item_id
# IS populated here (real notebook GUID available from the Kafka event's spark_properties facet).
FACT_COPY_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("hosting_workspace_id", StringType()),
    StructField("hosting_workspace_name", StringType()),
    StructField("hosting_item_id", StringType()),
    StructField("hosting_item_name", StringType()),
    StructField("engine", StringType()),
    StructField("matched_shortcut_name", StringType()),
    StructField("matched_shortcut_database", StringType()),
    StructField("dest_table", StringType()),
    StructField("source_column_count", IntegerType()),
    StructField("dest_column_count", IntegerType()),
    StructField("retained_column_count", IntegerType()),
    StructField("retention_pct", DoubleType()),
    StructField("is_select_star", BooleanType()),
    StructField("is_shortcut_read_and_saved_as_is", BooleanType()),
    StructField("threshold_pct_at_detection", DoubleType()),
    StructField("query_start_time", StringType()),
    StructField("detected_ts", StringType()),
])

try:
    if fact_copy_event_rows:
        fact_copy_event_df = spark.createDataFrame(fact_copy_event_rows, schema=FACT_COPY_EVENT_SCHEMA)
        fact_copy_event_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("FactCopyEvent")
        print(f"Appended {fact_copy_event_df.count()} rows to FactCopyEvent (engine=SparkKafka).")
        display(fact_copy_event_df)
    else:
        if not spark.catalog.tableExists("FactCopyEvent"):
            spark.createDataFrame([], schema=FACT_COPY_EVENT_SCHEMA).write.mode("overwrite").format("delta").saveAsTable("FactCopyEvent")
        print("No new SparkKafka-engine copy events detected this run.")
except Exception as e:
    log_error_to_lakehouse("write_fact_copy_event", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Advance the single-row watermark to the max EventEnqueuedUtcTime seen across ALL rows read
#     this run (requirement #4: never a full rescan) - whether or not they resulted in a matched
#     FactCopyEvent row, mirroring the file-transport notebook's "advance past every byte read"
#     semantics. Only advances forward, never backward. ---
try:
    if max_enqueued_time_seen is not None and (
        last_processed_enqueued_time is None or max_enqueued_time_seen > last_processed_enqueued_time
    ):
        wm_schema = StructType([
            StructField("last_processed_enqueued_time", TimestampType()),
        ])
        spark.createDataFrame([{"last_processed_enqueued_time": max_enqueued_time_seen}], schema=wm_schema) \
            .write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(WATERMARK_TABLE)
        print(f"Watermark advanced to {max_enqueued_time_seen}.")
    else:
        print("No watermark advancement needed (no new rows read this run).")
except Exception as e:
    log_error_to_lakehouse("advance_watermark", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table/column descriptions - extend FactCopyEvent's comment to also mention the SparkKafka engine,
# and add SparkKafkaLineageWatermark's own comment (first time this notebook creates/touches it).
# Idempotent: safe to run every time.
try:
    if spark.catalog.tableExists("FactCopyEvent"):
        table_comment = (
            "One row per detected read-from-shortcut copy event, across THREE engines: Warehouse "
            "(CTAS/INSERT-SELECT via Query Insights, written by NB_CopyEventDetection_Warehouse), "
            "Spark file-transport (notebook read+save via OpenLineage column lineage read from "
            "per-notebook NDJSON files, written by NB_CopyEventDetection_Spark), and Spark Kafka "
            "(identical column-lineage detection, but sourced from the shared "
            "ol_lineage_events_v3 Eventstream sink table instead of per-notebook files, written by "
            "NB_CopyEventDetection_SparkKafka) - distinguished by the engine column. "
            "is_shortcut_read_and_saved_as_is = TRUE when the write retained ALL source columns "
            "(SELECT * equivalent) OR retention_pct > threshold_pct_at_detection, even if extra new "
            "columns were also added. All three engines are incremental (Warehouse: per-item query-"
            "history watermark; Spark file-transport: per-lineage-file byte-offset watermark; Spark "
            "Kafka: single EventEnqueuedUtcTime watermark) - never a full rescan."
        )
        # No single quotes remain in the comment text (Spark SQL string literals do not reliably
        # support '' escaping like ANSI SQL); strip defensively in case future edits add one.
        safe_table_comment = table_comment.replace("'", "")
        spark.sql(f"ALTER TABLE FactCopyEvent SET TBLPROPERTIES ('comment' = '{safe_table_comment}')")

    if spark.catalog.tableExists(WATERMARK_TABLE):
        wm_table_comment = (
            "Incremental watermark for NB_CopyEventDetection_SparkKafka: single row holding the max "
            "EventEnqueuedUtcTime already processed from the shared ol_lineage_events_v3 Eventstream "
            "sink table. Ensures each run only reads newly-appended rows, never a full table rescan."
        ).replace("'", "")
        spark.sql(f"ALTER TABLE {WATERMARK_TABLE} SET TBLPROPERTIES ('comment' = '{wm_table_comment}')")
        wm_col_comment = (
            "Max EventEnqueuedUtcTime already processed from ol_lineage_events_v3. Next run reads "
            "only rows at/after this timestamp."
        ).replace("'", "")
        spark.sql(f"ALTER TABLE {WATERMARK_TABLE} ALTER COLUMN last_processed_enqueued_time COMMENT '{wm_col_comment}'")
except Exception as e:
    log_error_to_lakehouse("write_table_comments", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== Run summary (SparkKafka copy-event detection via Eventstream) ===")
print(f"Source table: Tables/{SOURCE_EVENTS_TABLE}")
print(f"New event rows read this run: {len(all_new_events)}")
print(f"Candidate write events (input+output detail): {len(candidate_writes)}")
print(f"Matched to a known shortcut: {len(matched_writes)}")
print(f"Written to FactCopyEvent: {len(fact_copy_event_rows)}")
print(f"Flagged read-and-saved-as-is: {sum(1 for r in fact_copy_event_rows if r['is_shortcut_read_and_saved_as_is'])}")
print(f"Watermark advanced to: {max_enqueued_time_seen}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write a human-readable JSON summary artifact to Files/reports, same pattern as both sibling
# notebooks (SQL analytics endpoint can lag; this file is the fastest way to verify a run's outcome).
import json as _json2

copy_event_summary = {
    "run_ts": now_ts,
    "source_table": SOURCE_EVENTS_TABLE,
    "new_event_rows_this_run": len(all_new_events),
    "candidate_write_events": len(candidate_writes),
    "matched_to_known_shortcut": len(matched_writes),
    "written_to_fact_copy_event": len(fact_copy_event_rows),
    "flagged_read_and_saved_as_is": sum(1 for r in fact_copy_event_rows if r["is_shortcut_read_and_saved_as_is"]),
    "watermark_advanced_to": str(max_enqueued_time_seen),
    "copy_event_details": fact_copy_event_rows,
}

summary_json2 = _json2.dumps(copy_event_summary, indent=2, default=str)
rdd_out2 = spark.sparkContext.parallelize([summary_json2], 1)
out_path2 = f"{LAKEHOUSE_ABFSS}/Files/reports/copyevent_sparkkafka_summary_{int(datetime.now().timestamp())}"
rdd_out2.saveAsTextFile(out_path2)
print(f"Copy-event summary written to: {out_path2}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
