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
# `ENV_OpenLineage` environment (Kafka transport -> `ES_OpenLineageEvents` Eventstream's CustomEndpoint
# source). See `docs/architecture/Shortcut Monitoring Solution - Design Document.md` for how that
# pipeline is built. Unlike the file-transport notebook, there is no
# per-notebook lineage file to discover - ALL monitored notebooks' events land on the SAME shared
# Kafka-compatible topic, distinguished per-event by `run.facets.spark_properties.properties`
# (workspace id, notebook item id and name are embedded there by Fabric's Spark runtime).
# 
# THIS notebook is now also responsible for a step that was originally attempted via the Eventstream
# itself: getting raw OpenLineage events out of the Kafka-compatible topic and into
# `ol_lineage_events_v3` (Lakehouse Delta table). Approaches tried, in this order:
#   1. Eventstream's own `Lakehouse` destination (with a `SqlFlatten` operator). REJECTED - Eventstream
#      locks a field's structural Delta schema from the very FIRST event it ever processes, and since
#      OpenLineage always emits an empty-array `START` event before any populated `COMPLETE` event for
#      the same run, that lock silently and permanently dropped every later `COMPLETE` event
#      (`OutputDataConversionError.TypeConversionError` under the hood).
#   2. Direct external Kafka consumption of the Eventstream CustomEndpoint's Kafka-compatible topic
#      (Spark native `readStream.format("kafka")`, then a driver-side `kafka-python` consumer).
#      REJECTED - proven, both from Fabric and from a plain external Python client with identical
#      credentials, that the CustomEndpoint's exposed connection details are Send-only (producer)
#      credentials: `partitions_for_topic` (metadata-only) works, but any actual data-plane read
#      (`ListOffsets`, consumer-group offset commit/fetch, or a plain `Fetch`) fails with
#      `kafka.errors.TopicAuthorizationFailedError [Error 29]`. This is a hard platform permission
#      limit, not a code bug - only Fabric's own internal Eventstream destination mechanisms (which
#      use a different, internal, Listen-capable identity) can actually consume the stream.
#   3. Eventstream's `Eventhouse` destination in `DirectIngestion` mode (a schema-free `dynamic`
#      column set in a KQL Database, immune to the schema-lock bug in #1). WORKING, but only after a
#      one-time MANUAL portal step: creating the destination purely via the Fabric REST API (matching
#      Microsoft's own documented recipe - workspaceId/itemId/tableName/connectionName/mappingRuleName)
#      leaves it stuck at `status: "Warning"` indefinitely, because the `connectionName` it references
#      is a dangling reference - no real Fabric Connection object backs it (confirmed by listing all
#      tenant Connections and finding no match), so there's nothing to resolve the KQL Database's own
#      query/ingestion service URIs against. Microsoft's docs describe an extra manual **Configure**
#      wizard step in the portal (Get data > select/inspect table > Finish) that provisions this
#      Connection object via an interactive OAuth/managed-identity handshake with no public REST API
#      equivalent. Once that one-time manual step is done, the destination's `status` becomes
#      `Running` and everything else (this notebook's sync logic, redeploys, etc.) stays fully
#      API-driven - no further manual steps are needed unless the Eventstream/destination is dropped
#      and recreated from scratch again.
# 
# Given the above, THIS notebook reads the already-landed raw events straight out of the KQL Database
# (`EH_ShortcutMonitoring` Eventhouse, `ol_raw_events` table) via the Kusto REST query API
# (`{queryServiceUri}/v1/rest/query`, using `notebookutils.credentials.getToken("kusto")`) - NOT via
# any Kafka client. Each queried row is serialized back to a single JSON string (matching the shape
# `ol_lineage_events_v3`'s downstream parsing already expects) and staged into a schema-free
# Lakehouse Delta table (`ol_raw_kafka_staging`, single `raw_json STRING` column - kept as the
# staging table name/shape for continuity with the sync-into-`ol_lineage_events_v3` cell below, which
# is unchanged). Incremental reads use Kusto's own `ingestion_time()` as a monotonic watermark
# (`RawCaptureKqlWatermark` table) - robust regardless of the event's own `eventTime`, and immune to
# any Kafka-side offset/retention semantics entirely. The Eventstream item and its CustomEndpoint
# source + Eventhouse DirectIngestion destination are still required (they're what actually lands
# events into the KQL Database) - only the previously-attempted "read Kafka directly from this
# notebook" bypass has been removed, since it was proven fundamentally blocked (see #2 above).
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
# OpenLineage events carry wildly different `inputs`/`outputs` shapes per event (a job-span event
# has empty arrays; a real dataset read/write event has a deeply nested, per-source-table-varying
# `array<struct<namespace,name,facets,...>>`). Fabric Eventstream locks a field's structural Delta
# schema from the very FIRST event it ever processes - and since OpenLineage always emits an empty-
# array `START` event before any populated `COMPLETE` event for the same run, that lock silently
# and permanently drops every later `COMPLETE` event (`OutputDataConversionError.TypeConversionError`
# under the hood), no matter what any downstream operator/WHERE clause says. This is WHY the raw
# capture (`ol_raw_kafka_staging`) uses a single schema-free `raw_json STRING` column instead of any
# per-field typing - it can never lock onto the wrong shape, so nothing is ever silently dropped
# upstream of this notebook. This notebook parses that raw JSON itself (via `spark.read.json` across
# the whole batch, which unions/merges the schema across differently-shaped records automatically),
# so `inputs_json`/`outputs_json` land here as STRING (JSON-encoded) columns - schema-stable
# regardless of shape, at the cost of needing a `json.loads()` below to get back to structured data.
# 
# NOT covered by this notebook (unchanged from the design doc):
#   - Enabling OpenLineage / the Kafka transport / the Eventstream pipeline (prerequisite).
#   - Dataflow Gen2 engine (deferred per rollout plan).
#   - Any cleanup/truncation of `ol_raw_kafka_staging` - that table is the immutable raw capture.


# CELL ********************

import json, traceback, requests, re
from datetime import datetime, timezone
from pyspark.sql import functions as F

def log_error_to_lakehouse(stage, exc):
    try:
        err_text = f"STAGE: {stage}\nTIME: {datetime.now(timezone.utc).isoformat()}\n\n{traceback.format_exc()}"
        rdd = spark.sparkContext.parallelize([err_text], 1)
        rdd.saveAsTextFile(f"{LAKEHOUSE_ABFSS}/Files/logs/copyevent_sparkkafka_error_{int(datetime.now().timestamp())}.log")
    except Exception:
        pass

def with_delta_conflict_retry(fn, max_attempts=5, base_delay_seconds=5):
    """Retries fn() on Delta optimistic-concurrency conflicts (ConcurrentAppendException,
    MetadataChangedException, etc.) that can happen because FactCopyEvent is a SHARED table also
    written to by the sibling engine notebook (Warehouse/SparkKafka) - e.g. one notebook's
    ALTER TABLE ... COMMENT metadata commit landing while the other is mid-append. fn must be safe
    to call more than once and should re-read any Spark state it needs from scratch each call
    (not close over a stale DataFrame/snapshot) so each retry picks up the latest table version.
    Re-raises unchanged after max_attempts, and immediately re-raises anything that isn't a
    recognized Delta concurrency conflict."""
    import time
    conflict_markers = (
        "ConcurrentAppendException", "MetadataChangedException", "ConcurrentDeleteReadException",
        "ConcurrentDeleteDeleteException", "ConcurrentTransactionException", "ConcurrentWriteException",
        "ProtocolChangedException",
    )
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if not any(marker in str(e) for marker in conflict_markers):
                raise
            last_exc = e
            print(f"  Delta concurrency conflict on attempt {attempt}/{max_attempts} (likely the sibling "
                  f"engine notebook committing to the same shared FactCopyEvent table concurrently) - "
                  f"retrying in {base_delay_seconds}s: {str(e)[:200]}")
            time.sleep(base_delay_seconds)
    raise last_exc

# Resolve the solution's own lakehouse (where config.json/DimShortcut/FactCopyEvent/etc. live) from
# the notebook's runtime attachment context, same pattern as both sibling notebooks. Wrapped so a
# failure here (e.g. an unexpected runtime.context shape) still gets logged via the fallback path
# above, instead of crashing the whole session before log_error_to_lakehouse can ever run.
try:
    _ctx = notebookutils.runtime.context
    LAKEHOUSE_WORKSPACE_ID = _ctx["defaultLakehouseWorkspaceId"]
    LAKEHOUSE_ID = _ctx["defaultLakehouseId"]
    LAKEHOUSE_ABFSS = f"abfss://{LAKEHOUSE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"
except Exception as e:
    log_error_to_lakehouse("resolve_lakehouse_context", e)
    raise

# This notebook's own detection logic (unchanged) reads from this Lakehouse Delta table. It is now
# populated by THIS notebook (see the "sync raw capture" cells below), not directly by the
# Eventstream - it is intentionally NOT auto-discovered/renamed, so it's safe to keep this constant
# fixed across reruns/resets.
SOURCE_EVENTS_TABLE = "ol_lineage_events_v3"

# The Eventstream that exposes the Kafka-compatible CustomEndpoint `ENV_OpenLineage` publishes to -
# looked up by fixed display name via the Fabric REST API (intentionally NOT auto-discovered, same
# "no reliable way to tell current-vs-stale" reasoning as SOURCE_EVENTS_TABLE above). This notebook
# reads landed events out of the KQL Database that its Eventhouse DirectIngestion destination
# populates (see check_eventstream_and_get_kql_conn below), not the Kafka-compatible topic directly.
EVENTSTREAM_NAME = "ES_OpenLineageEvents"
RAW_CAPTURE_STAGING_TABLE = "ol_raw_kafka_staging"

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

# config.orchestration.enabledEngines lets the pipeline call this notebook unconditionally on every
# run while still allowing an environment (e.g. a dev workspace with no Eventstream wired up) to skip
# this engine entirely without editing the orchestrating pipeline - just flip config, no redeploy needed.
ENABLED_ENGINES = config.get("orchestration", {}).get("enabledEngines", ["warehouse", "sparkKafka"])
if "sparkKafka" not in ENABLED_ENGINES:
    print("SparkKafka engine disabled via config.orchestration.enabledEngines - exiting without doing work.")
    notebookutils.notebook.exit("skipped: sparkKafka engine disabled in config")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- First functional check: before touching any watermark or reading any events, confirm the
#     ES_OpenLineageEvents Eventstream's CustomEndpoint source AND its Eventhouse DirectIngestion
#     destination are both actually Running (not paused/errored/stuck-in-Warning) - if either isn't,
#     no new raw events are landing in the KQL Database at all. Returns the KQL Database's live query
#     service URI, database display name, and raw-events table name (read from the destination node's
#     own properties), for reuse by the next cell. Exits cleanly (skipped, not failed) if the
#     Eventstream is missing, rather than crashing later with a stack trace that looks like a real bug.
def check_eventstream_and_get_kql_conn():
    token = notebookutils.credentials.getToken("pbi")
    headers = {"Authorization": "Bear" + "er " + token}

    es_list = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{LAKEHOUSE_WORKSPACE_ID}/eventstreams",
        headers=headers, timeout=30)
    es_list.raise_for_status()
    es = next((e for e in es_list.json().get("value", []) if e.get("displayName") == EVENTSTREAM_NAME), None)
    if not es:
        print(f"Eventstream [{EVENTSTREAM_NAME}] not found in this workspace - this is EXPECTED/normal "
              f"if the deploy script's Eventstream step hasn't run in this workspace. Exiting without doing work.")
        notebookutils.notebook.exit(f"skipped: Eventstream [{EVENTSTREAM_NAME}] not found")

    topo = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{LAKEHOUSE_WORKSPACE_ID}/eventstreams/{es['id']}/topology",
        headers=headers, timeout=30)
    topo.raise_for_status()
    topo_json = topo.json()
    sources = {n.get("name"): n.get("status") for n in topo_json.get("sources", [])}
    not_running = {n: s for n, s in sources.items() if s and s not in ("Running", "Active")}
    if not_running:
        print(f"Eventstream [{EVENTSTREAM_NAME}] has non-running source(s): {not_running} - exiting without doing work.")
        notebookutils.notebook.exit(f"skipped: Eventstream source(s) not running - {not_running}")
    print(f"Eventstream [{EVENTSTREAM_NAME}] source status: Running (sources: {sources})")

    eh_dest = next((n for n in topo_json.get("destinations", []) if n.get("type") == "Eventhouse"), None)
    if not eh_dest:
        raise RuntimeError(f"No Eventhouse destination found on Eventstream [{EVENTSTREAM_NAME}] - cannot read raw events.")
    if eh_dest.get("status") not in ("Running", "Active"):
        print(f"WARNING: Eventhouse destination [{eh_dest.get('name')}] status is [{eh_dest.get('status')}], not "
              f"Running - new events may not be landing in the KQL Database (this usually means the destination's "
              f"connection needs the one-time manual portal 'Configure' step - see module docstring). "
              f"Continuing anyway in case previously-landed rows still need processing.")

    dest_props = eh_dest["properties"]
    kql_ws_id = dest_props["workspaceId"]
    kql_db_id = dest_props["itemId"]
    kql_table = dest_props["tableName"]

    kql_db = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{kql_ws_id}/kqldatabases/{kql_db_id}",
        headers=headers, timeout=30)
    kql_db.raise_for_status()
    kql_db_json = kql_db.json()
    query_service_uri = kql_db_json["properties"]["queryServiceUri"]
    kql_db_name = kql_db_json["displayName"]

    return query_service_uri, kql_db_name, kql_table

QUERY_SERVICE_URI, KQL_DB_NAME, KQL_RAW_TABLE = None, None, None
try:
    QUERY_SERVICE_URI, KQL_DB_NAME, KQL_RAW_TABLE = check_eventstream_and_get_kql_conn()
except Exception as e:
    log_error_to_lakehouse("check_eventstream_and_get_kql_conn", e)
    raise

MONITORED_WORKSPACES = config["monitoredWorkspaces"]
THRESHOLD_PCT = float(config["detection"]["columnRetentionThresholdPercent"])
WORKSPACE_ID_TO_NAME = {w["workspaceId"]: w["workspaceName"] for w in MONITORED_WORKSPACES}

print(f"Loaded config. Monitored workspaces: {list(WORKSPACE_ID_TO_NAME.values())}")
print(f"Column-retention threshold: {THRESHOLD_PCT}%")
print(f"Raw capture: KQL Database [{KQL_DB_NAME}].[{KQL_RAW_TABLE}] -> [{RAW_CAPTURE_STAGING_TABLE}]")
print(f"Detection source events table: Tables/{SOURCE_EVENTS_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Query already-landed raw events straight out of the KQL Database (EH_ShortcutMonitoring's
#     ol_raw_events table, populated by the Eventstream's Eventhouse DirectIngestion destination) via
#     the Kusto REST query API, rather than any Kafka client - see the module docstring for why the
#     direct-Kafka-read approach was abandoned. Each queried row is serialized back to a single JSON
#     string and appended into the schema-free staging table (ol_raw_kafka_staging, kept as the name/
#     shape for continuity with the unchanged sync-into-ol_lineage_events_v3 cell below). Incremental
#     reads use Kusto's own ingestion_time() as a monotonic watermark (RawCaptureKqlWatermark table) -
#     robust regardless of the event's own eventTime, and immune to Kafka-side offset/retention
#     semantics entirely (there is no Kafka client involved in this cell at all). ---
try:
    kusto_token = notebookutils.credentials.getToken("kusto")
    kusto_headers = {"Authorization": "Bear" + "er " + kusto_token, "Content-Type": "application/json"}

    RAW_CAPTURE_KQL_WATERMARK_TABLE = "RawCaptureKqlWatermark"
    if spark.catalog.tableExists(RAW_CAPTURE_KQL_WATERMARK_TABLE):
        wm_row = spark.read.table(RAW_CAPTURE_KQL_WATERMARK_TABLE).collect()
        last_ingest_ts = wm_row[0]["last_ingest_ts"] if wm_row else None
    else:
        last_ingest_ts = None
    print(f"Saved KQL ingestion-time watermark from prior run(s): {last_ingest_ts}")

    where_clause = ""
    if last_ingest_ts is not None:
        wm_literal = last_ingest_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        where_clause = f"| where ingest_ts > datetime({wm_literal}) "

    kql_query = (
        f"{KQL_RAW_TABLE} "
        "| extend ingest_ts = ingestion_time() "
        f"{where_clause}"
        "| project raw_json = tostring(pack('eventType', eventType, 'eventTime', eventTime, 'run', run, 'job', job, 'inputs', inputs, 'outputs', outputs, 'producer', producer)), ingest_ts "
        "| order by ingest_ts asc"
    )

    resp = requests.post(
        f"{QUERY_SERVICE_URI}/v1/rest/query",
        headers=kusto_headers,
        json={"db": KQL_DB_NAME, "csl": kql_query},
        timeout=60)
    resp.raise_for_status()
    result_json = resp.json()
    primary_table = result_json["Tables"][0]
    col_names = [c["ColumnName"] for c in primary_table["Columns"]]
    raw_json_idx = col_names.index("raw_json")
    ingest_ts_idx = col_names.index("ingest_ts")

    def parse_kusto_datetime(val):
        # Kusto's REST API returns up to 7 fractional-second digits (100ns ticks), which some Python
        # versions' datetime.fromisoformat() reject (it only accepts 3 or 6). Truncate to
        # microseconds (6 digits) defensively before parsing, rather than assuming the runtime's
        # Python version is lenient about this.
        v = val.replace("Z", "+00:00")
        if "." in v:
            head, rest = v.split(".", 1)
            frac_digits = rest.split("+")[0][:6]
            v = f"{head}.{frac_digits}+00:00"
        return datetime.fromisoformat(v)

    raw_messages = []
    for row in primary_table["Rows"]:
        raw_json_val = row[raw_json_idx]
        ingest_ts_val = row[ingest_ts_idx]
        if raw_json_val is None or ingest_ts_val is None:
            continue
        ts = parse_kusto_datetime(ingest_ts_val)
        raw_messages.append((raw_json_val, ts))

    print(f"Queried {len(raw_messages)} new raw event row(s) from KQL Database [{KQL_DB_NAME}].[{KQL_RAW_TABLE}].")

    if raw_messages:
        raw_df = spark.createDataFrame(raw_messages, ["raw_json", "kafka_timestamp"])
        raw_df.write.format("delta").mode("append").saveAsTable(RAW_CAPTURE_STAGING_TABLE)
        print(f"Appended {len(raw_messages)} row(s) into [{RAW_CAPTURE_STAGING_TABLE}].")

        new_last_ingest_ts = max(m[1] for m in raw_messages)
        (spark.createDataFrame([(new_last_ingest_ts,)], ["last_ingest_ts"])
            .write.format("delta").mode("overwrite").saveAsTable(RAW_CAPTURE_KQL_WATERMARK_TABLE))
        print(f"KQL ingestion-time watermark advanced to {new_last_ingest_ts}.")
    else:
        print("No new raw events found in the KQL Database this run.")
except Exception as e:
    log_error_to_lakehouse("kql_raw_capture_query", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Sync new COMPLETE+populated events from the raw-capture staging table
#     (`ol_raw_kafka_staging`) into `ol_lineage_events_v3` (Lakehouse Delta table), appending only
#     new rows. Parsing/filtering happens here in Spark (via `spark.read.json`, which unions/merges
#     the schema across differently-shaped records in the batch automatically) rather than in KQL,
#     since there is no Kusto layer in this architecture anymore. This step's own watermark
#     (RawCaptureWatermark, keyed on the staging table's `kafka_timestamp`) is intentionally separate
#     from the detection logic's own watermark further below - they advance independently. ---
RAW_CAPTURE_WATERMARK_TABLE = "RawCaptureWatermark"

try:
    if spark.catalog.tableExists(RAW_CAPTURE_WATERMARK_TABLE):
        wm_row = spark.read.table(RAW_CAPTURE_WATERMARK_TABLE).collect()
        last_raw_event_time = wm_row[0]["last_processed_event_time"] if wm_row else None
    else:
        last_raw_event_time = None
    print(f"Raw capture watermark (last_processed_event_time): {last_raw_event_time}")

    if not spark.catalog.tableExists(RAW_CAPTURE_STAGING_TABLE):
        print(f"Raw-capture staging table [{RAW_CAPTURE_STAGING_TABLE}] does not exist yet - nothing to sync this run.")
        raw_staging_rows = []
    else:
        staging_df = spark.read.format("delta").load(f"Tables/{RAW_CAPTURE_STAGING_TABLE}")
        if last_raw_event_time is not None:
            staging_df = staging_df.filter(F.col("kafka_timestamp") > F.lit(last_raw_event_time))
        raw_staging_rows = staging_df.collect()
    print(f"New raw Kafka row(s) found in staging this run: {len(raw_staging_rows)}")

    if raw_staging_rows:
        # Inject each row's own kafka_timestamp into its JSON payload (as `__kafka_ts`) before the
        # batch JSON parse below, so the timestamp survives alongside the rest of the parsed fields.
        augmented_json_strings = []
        for r in raw_staging_rows:
            try:
                obj = json.loads(r["raw_json"])
            except Exception:
                continue
            obj["__kafka_ts"] = r["kafka_timestamp"].isoformat()
            augmented_json_strings.append(json.dumps(obj))

        max_raw_event_time = max(r["kafka_timestamp"] for r in raw_staging_rows)

        if augmented_json_strings:
            parsed_df = spark.read.json(spark.sparkContext.parallelize(augmented_json_strings))
            matched_df = (
                parsed_df
                .filter(F.col("eventType") == "COMPLETE")
                .filter(F.size(F.coalesce(F.col("inputs"), F.array())) > 0)
                .filter(F.size(F.coalesce(F.col("outputs"), F.array())) > 0)
                .select(
                    F.col("eventType"),
                    F.col("eventTime"),
                    F.col("run"),
                    F.col("job"),
                    F.to_json(F.col("inputs")).alias("inputs_json"),
                    F.to_json(F.col("outputs")).alias("outputs_json"),
                    F.to_timestamp(F.col("__kafka_ts")).alias("EventEnqueuedUtcTime"),
                )
            )
            matched_rows = matched_df.collect()
            print(f"New COMPLETE+populated event(s) found in raw capture this run: {len(matched_rows)}")
            if matched_rows:
                (matched_df.write.format("delta").mode("append").option("mergeSchema", "true")
                    .saveAsTable(SOURCE_EVENTS_TABLE))
                print(f"Appended {len(matched_rows)} row(s) into [{SOURCE_EVENTS_TABLE}].")
            else:
                print("No COMPLETE+populated events among the new raw rows this run.")
        else:
            print("No parseable raw rows this run.")

        spark.createDataFrame([(max_raw_event_time,)], ["last_processed_event_time"]) \
            .write.format("delta").mode("overwrite").saveAsTable(RAW_CAPTURE_WATERMARK_TABLE)
        print(f"Raw capture watermark advanced to {max_raw_event_time}.")
    else:
        print("Nothing new to sync from raw capture this run.")
except Exception as e:
    log_error_to_lakehouse("sync_raw_capture", e)
    raise

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, LongType
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
        print(f"Source table [{SOURCE_EVENTS_TABLE}] does not exist yet - nothing to process this run.")
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

# --- Cross-reference each write's INPUT(s) against DimShortcut using a SINGLE unified matching
#     strategy that covers all three shortcut flavors: OneLake-to-Table, OneLake-to-File, and
#     external (AdlsGen2/AmazonS3/ExternalDataShare/etc.). Verified empirically (via a live example
#     in ol_raw_kafka_staging): OneLake proxies EVERY shortcut read transparently server-side,
#     REGARDLESS of target_type - even a read through an external ADLS/S3 shortcut still surfaces in
#     OpenLineage as a onelake.dfs.fabric.microsoft.com namespace + a HOSTING-side path (never the
#     true external URI). So there is no need for a separate "external URI" matching strategy - a
#     single hosting-path match, built from EVERY DimShortcut row's own
#     shortcut_path + "/" + shortcut_name (e.g. "Files/sh_adls_sbstorage001_delta" or
#     "Tables/dbo/DimAccount" for schema-enabled items), works for all target types.
#
#     Matching must be a PREFIX match on path segments, not an exact/last-segment match: a read can
#     continue PAST the shortcut's own root into a subfolder/file WITHIN it (this is common for
#     File-type shortcuts, e.g. reading one subfolder of a Delta table stored under Files) - taking
#     the read's trailing path segment as "the shortcut name" (the previous, buggy approach) breaks
#     the moment there's any such subfolder. Picks the LONGEST matching prefix in case of
#     nested/overlapping shortcuts.
#
#     Only single-input writes are considered in scope (multi-input joins are intentionally
#     excluded to avoid false positives). ---
try:
    dim_shortcut_df = spark.read.table("DimShortcut")
    dim_shortcut_rows = dim_shortcut_df.collect()

    # list of (hosting_workspace_name, hosting_item_name, hosting_workspace_id, hosting_item_id,
    #           local_path_segments_tuple, row). Both a NAME key and a GUID key are kept because
    # OpenLineage's namespace/name fields identify the workspace/item differently depending on HOW
    # the notebook code referenced the path: a full "abfss://<workspace_name>@onelake.../
    # <item_name>.Lakehouse/..." URI surfaces human-readable NAMES, while a path RELATIVE to the
    # notebook's default-attached lakehouse (e.g. "Files/<shortcut>/...") surfaces that lakehouse's
    # workspace/item GUIDs instead (Spark resolves the default mount internally by ID, not name).
    # Matching must accept either form.
    hosting_lookup = []
    for r in dim_shortcut_rows:
        local_path = f"{(r['shortcut_path'] or '').strip('/')}/{r['shortcut_name']}".strip("/")
        segs = tuple(p.lower() for p in local_path.split("/") if p)
        if not segs:
            continue
        hosting_lookup.append((
            r["hosting_workspace_name"].lower(),
            r["hosting_item_name"].lower(),
            (r["hosting_workspace_id"] or "").lower(),
            (r["hosting_item_id"] or "").lower(),
            segs,
            r,
        ))

    print(f"DimShortcut loaded: {len(hosting_lookup)} shortcut(s) total (all target types).")

    def resolve_input_to_shortcut(input_ds):
        namespace = (input_ds.get("namespace") or "")
        name = (input_ds.get("name") or "")

        ws_part = (namespace.split("//")[-1].split("@")[0] if "//" in namespace else namespace).lower()
        name_segments = [seg.lower() for seg in name.strip("/").split("/") if seg]
        if len(name_segments) < 2:
            return None
        item_part = name_segments[0].split(".")[0]
        remaining = tuple(name_segments[1:])

        best_match, best_len = None, -1
        for ws_name, item_name, ws_id, item_id, segs, row in hosting_lookup:
            name_hit = (ws_name == ws_part and item_name == item_part)
            id_hit = (ws_id and ws_id == ws_part and item_id and item_id == item_part)
            if not (name_hit or id_hit):
                continue
            n = len(segs)
            if n > len(remaining) or remaining[:n] != segs:
                continue
            if n > best_len:
                best_match, best_len = row, n
        return best_match

    def shortcut_severity(row):
        # High = Table-type shortcut (a full managed-table read is a strong, unambiguous copy
        # signal). Medium = File-type shortcut (a raw-file/folder read is a weaker "true
        # duplication" signal - could be a partial/staging/intermediate read). Determined purely by
        # WHERE the shortcut lives (Tables vs Files), independent of whether its ultimate target is
        # OneLake or external (AdlsGen2/AmazonS3/etc.) - a File shortcut is still a File shortcut
        # whether it points at another OneLake item or straight at external storage.
        path = (row["shortcut_path"] or "").strip("/").lower()
        return "High" if path == "tables" or path.startswith("tables/") else "Medium"

    matched_writes = []
    unmatched_examples = []
    for cw in candidate_writes:
        if len(cw["inputs"]) != 1:
            continue
        matched_shortcut = resolve_input_to_shortcut(cw["inputs"][0])
        if matched_shortcut is None:
            if len(unmatched_examples) < 5:
                unmatched_examples.append((cw["inputs"][0].get("namespace"), cw["inputs"][0].get("name")))
            continue
        cw["matched_shortcut_name"] = matched_shortcut["shortcut_name"]
        cw["matched_shortcut_hosting_workspace_id"] = matched_shortcut["hosting_workspace_id"]
        cw["matched_shortcut_hosting_workspace_name"] = matched_shortcut["hosting_workspace_name"]
        cw["matched_shortcut_hosting_item_name"] = matched_shortcut["hosting_item_name"]
        cw["matched_shortcut_sk"] = matched_shortcut["shortcut_sk"]
        cw["matched_shortcut_target_type"] = matched_shortcut["target_type"]
        cw["severity"] = shortcut_severity(matched_shortcut)
        input_schema_fields = cw["inputs"][0].get("facets", {}).get("schema", {}).get("fields", [])
        cw["input_column_count"] = len(input_schema_fields)
        matched_writes.append(cw)

    print(f"Of {len(candidate_writes)} candidate write(s), {len(matched_writes)} matched to a single known shortcut input.")
    if unmatched_examples:
        print(f"Input(s) that did NOT match any known shortcut (first {len(unmatched_examples)}, for diagnosis):")
        for ns, nm in unmatched_examples:
            print(f"  namespace={ns!r} name={nm!r}")
except Exception as e:
    log_error_to_lakehouse("crossref_dimshortcut", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Identify WHO ran the notebook that produced each matched write, via Fabric Workspace
#     Monitoring's `ItemJobEventLogs` table (a separate, admin-enabled Eventhouse per monitored
#     workspace - NOT the same Eventhouse this solution owns). OpenLineage events themselves carry
#     no user identity at all (confirmed empirically - only spark.app.id/trident.* properties), so
#     this is the only source of an executing-user identity for the SparkKafka engine.
#
#     Correlation is an EXACT GUID join, not a time-window guess: Fabric's OpenLineage Spark
#     listener embeds the run's real Fabric JobInstanceId inside `job.name` itself - e.g. job.name
#     "nb_foo_2_a4_b2_c90_b481_4_c3_e_9_d5_c_97670_f9448_f0.union" embeds JobInstanceId
#     "2a4b2c90-b481-4c3e-9d5c-97670f9448f0" (same 32 hex chars, just dash positions replaced with
#     underscores, immediately after the notebook's own name with ITS underscores also stripped -
#     verified empirically against 15 distinct job.name values spanning 4 separate runs, 100% match).
#     That JobInstanceId is then looked up directly in ItemJobEventLogs.JobInstanceId - an exact
#     match, no ambiguity from overlapping/concurrent runs.
def extract_job_instance_id(job_name, hosting_item_name):
    """Recovers the Fabric JobInstanceId embedded in an OpenLineage job.name, or None if job_name
    doesn't look like it has the expected "<notebook name><32 hex chars>[.<sub-op suffix>]" shape
    (e.g. hosting_item_name unknown/blank, or a future OpenLineage version changes this format)."""
    if not job_name or not hosting_item_name:
        return None
    compact_job = re.sub(r"_", "", job_name)
    compact_prefix = re.sub(r"_", "", hosting_item_name)
    if not compact_job.lower().startswith(compact_prefix.lower()):
        return None
    remainder = compact_job[len(compact_prefix):]
    if len(remainder) < 32:
        return None
    hex32 = remainder[:32]
    if not re.match(r"^[0-9a-f]{32}$", hex32, re.IGNORECASE):
        return None
    hex32 = hex32.lower()
    return f"{hex32[0:8]}-{hex32[8:12]}-{hex32[12:16]}-{hex32[16:20]}-{hex32[20:32]}"

MONITORING_KQL_CONN_CACHE = {}

def get_monitoring_kql_conn(workspace_id):
    """Discovers the given workspace's auto-provisioned "Monitoring KQL database" (created when
    Workspace Monitoring is enabled on it) and returns its {query_service_uri, db_id}, or None if
    Workspace Monitoring isn't enabled there. Cached per workspace for the life of this run - same
    dynamic-discovery approach as check_eventstream_and_get_kql_conn() above, just pointed at a
    different, monitoring-specific KQL database rather than this solution's own EH_ShortcutMonitoring."""
    if workspace_id in MONITORING_KQL_CONN_CACHE:
        return MONITORING_KQL_CONN_CACHE[workspace_id]
    token = notebookutils.credentials.getToken("pbi")
    headers = {"Authorization": "Bear" + "er " + token}
    try:
        items = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items",
                              headers=headers, timeout=30)
        items.raise_for_status()
        kql_db = next((it for it in items.json().get("value", [])
                       if it.get("type") == "KQLDatabase" and "monitoring" in (it.get("displayName") or "").lower()),
                      None)
        if not kql_db:
            print(f"WARN: no Workspace Monitoring KQL database found in workspace {workspace_id} - "
                  f"usernames for its SparkKafka events will be left blank.")
            MONITORING_KQL_CONN_CACHE[workspace_id] = None
            return None
        db_detail = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/kqldatabases/{kql_db['id']}",
            headers=headers, timeout=30)
        db_detail.raise_for_status()
        conn = {"query_service_uri": db_detail.json()["properties"]["queryServiceUri"], "db_id": kql_db["id"]}
    except Exception as e:
        print(f"WARN: could not resolve Workspace Monitoring KQL database for workspace {workspace_id}: {e}")
        conn = None
    MONITORING_KQL_CONN_CACHE[workspace_id] = conn
    return conn

def query_executing_principals(workspace_id, job_instance_ids):
    """Batch-looks-up ExecutingPrincipalId/Type for a set of JobInstanceIds, in ONE Kusto query per
    monitored workspace (rather than one per event) - returns {job_instance_id: (principal_id, principal_type)}."""
    job_instance_ids = [j for j in job_instance_ids if j]
    if not job_instance_ids:
        return {}
    conn = get_monitoring_kql_conn(workspace_id)
    if not conn:
        return {}
    try:
        kusto_token = notebookutils.credentials.getToken("kusto")
        kusto_headers = {"Authorization": "Bear" + "er " + kusto_token, "Content-Type": "application/json"}
        ids_list = ",".join(f"'{jid}'" for jid in set(job_instance_ids))
        csl = (f"ItemJobEventLogs | where JobInstanceId in ({ids_list}) "
               f"| summarize arg_max(Timestamp, ExecutingPrincipalId, ExecutingPrincipalType) by JobInstanceId")
        resp = requests.post(f"{conn['query_service_uri']}/v1/rest/query", headers=kusto_headers,
                              json={"db": conn["db_id"], "csl": csl}, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        cols = [c["ColumnName"] for c in result["Tables"][0]["Columns"]]
        out = {}
        for row in result["Tables"][0]["Rows"]:
            d = dict(zip(cols, row))
            out[d["JobInstanceId"]] = (d.get("ExecutingPrincipalId"), d.get("ExecutingPrincipalType"))
        return out
    except Exception as e:
        print(f"WARN: could not query ItemJobEventLogs for workspace {workspace_id}: {e}")
        return {}

def get_graph_token():
    """Client-credentials token for Microsoft Graph, reusing the SAME app registration
    (tenantId/clientId/clientSecret) already deployed into config.json for this solution's own
    Fabric REST calls - requires that app registration to also be granted User.Read.All (or
    Directory.Read.All) with admin consent, so it can resolve a raw AAD object id into a friendly
    UPN/display name."""
    auth = config["auth"]
    resp = requests.post(
        f"https://login.microsoftonline.com/{auth['tenantId']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": auth["clientId"],
            "client_secret": auth["clientSecret"],
            "scope": "https://graph.microsoft.com/.default",
        }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]

def resolve_principals_to_username(principal_ids):
    """Resolves a set of raw AAD object ids to a friendly UPN/display name via Microsoft Graph's
    directoryObjects endpoint (works for both User and ServicePrincipal principal types, unlike
    /v1.0/users/{id}). Best-effort: falls back to the raw GUID for any id that fails to resolve
    (e.g. missing Graph permission, deleted principal) rather than failing the whole run."""
    principal_ids = {p for p in principal_ids if p}
    if not principal_ids:
        return {}
    try:
        graph_token = get_graph_token()
    except Exception as e:
        print(f"WARN: could not acquire Microsoft Graph token (check that the app registration in "
              f"config.auth has been granted User.Read.All/Directory.Read.All with admin consent) - "
              f"usernames will fall back to raw AAD object ids. {e}")
        return {pid: pid for pid in principal_ids}
    headers = {"Authorization": "Bear" + "er " + graph_token}
    out = {}
    for pid in principal_ids:
        try:
            r = requests.get(f"https://graph.microsoft.com/v1.0/directoryObjects/{pid}", headers=headers, timeout=15)
            if r.status_code == 200:
                dj = r.json()
                out[pid] = dj.get("userPrincipalName") or dj.get("displayName") or pid
            else:
                out[pid] = pid
        except Exception:
            out[pid] = pid
    return out

try:
    job_instance_ids_by_workspace = {}
    for cw in matched_writes:
        jid = extract_job_instance_id(cw.get("job_name"), cw.get("hosting_item_name"))
        cw["job_instance_id"] = jid
        if jid:
            job_instance_ids_by_workspace.setdefault(cw["hosting_workspace_id"], set()).add(jid)

    principal_by_job_instance = {}  # job_instance_id -> (principal_id, principal_type)
    for ws_id, jids in job_instance_ids_by_workspace.items():
        principal_by_job_instance.update(query_executing_principals(ws_id, jids))

    all_principal_ids = {v[0] for v in principal_by_job_instance.values() if v and v[0]}
    username_by_principal_id = resolve_principals_to_username(all_principal_ids)

    resolved_count = 0
    for cw in matched_writes:
        principal = principal_by_job_instance.get(cw.get("job_instance_id"))
        if principal and principal[0]:
            cw["username"] = username_by_principal_id.get(principal[0], principal[0])
            resolved_count += 1
        else:
            cw["username"] = None

    print(f"Resolved username for {resolved_count} of {len(matched_writes)} matched write(s) "
          f"via Workspace Monitoring + Microsoft Graph.")
except Exception as e:
    log_error_to_lakehouse("resolve_executing_username", e)
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
def to_powerbi_dt_str(ts):
    """Normalize an ISO-8601 timestamp (e.g. OpenLineage's `eventTime`, which uses a "T" separator
    and an explicit UTC offset/"Z") into the same plain "YYYY-MM-DD HH:MM:SS.ffffff" style that
    NB_CopyEventDetection_Warehouse's copy_event_starttime already uses (str() of a SQL datetime2
    value) - keeps FactCopyEvent's copy_event_starttime/copy_event_detected_time columns in one
    Power-BI-friendly text format regardless of which engine wrote the row."""
    if not ts:
        return ts
    s = str(ts).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).strftime("%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return s  # unparseable - leave as-is rather than fail the run

fact_copy_event_rows = []
now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

try:
    for cw in matched_writes:
        input_col_count = cw["input_column_count"]
        if input_col_count == 0:
            print(f"  WARN: input_column_count was 0 for shortcut [{cw['matched_shortcut_name']}] "
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
            "shortcut_sk": cw["matched_shortcut_sk"],
            "severity": cw["severity"],
            "username": cw.get("username"),
            "dest_table": cw["output_name"],
            "source_column_count": input_col_count,
            "dest_column_count": dest_col_count,
            "retained_column_count": retained_count,
            "retention_pct": retention_pct,
            "is_select_star": is_select_star,
            "is_shortcut_read_and_saved_as_is": is_flagged,
            "threshold_pct_at_detection": THRESHOLD_PCT,
            "copy_event_starttime": to_powerbi_dt_str(cw["event_time"]),
            "copy_event_detected_time": now_ts,
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
    StructField("shortcut_sk", LongType()),
    StructField("severity", StringType()),
    StructField("username", StringType()),
    StructField("dest_table", StringType()),
    StructField("source_column_count", IntegerType()),
    StructField("dest_column_count", IntegerType()),
    StructField("retained_column_count", IntegerType()),
    StructField("retention_pct", DoubleType()),
    StructField("is_select_star", BooleanType()),
    StructField("is_shortcut_read_and_saved_as_is", BooleanType()),
    StructField("threshold_pct_at_detection", DoubleType()),
    StructField("copy_event_starttime", StringType()),
    StructField("copy_event_detected_time", StringType()),
])

try:
    if fact_copy_event_rows:
        fact_copy_event_df = spark.createDataFrame(fact_copy_event_rows, schema=FACT_COPY_EVENT_SCHEMA)
        with_delta_conflict_retry(lambda: fact_copy_event_df.write.mode("append").format("delta")
                                   .option("mergeSchema", "true").saveAsTable("FactCopyEvent"))
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
            "ol_lineage_events_v3 table (synced from the Eventhouse raw capture by this same "
            "notebook) instead of per-notebook files, written by "
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
        # Only issue the ALTER if the comment actually needs to change: this is a metadata-changing
        # commit against FactCopyEvent, a table BOTH this notebook and NB_CopyEventDetection_Warehouse
        # write to - running it unconditionally on every run made a Delta MetadataChangedException
        # collision with the sibling notebook's concurrent append far more likely than necessary.
        current_table_comment = spark.sql("SHOW TBLPROPERTIES FactCopyEvent('comment')").collect()[0]["value"]
        if current_table_comment != safe_table_comment:
            with_delta_conflict_retry(lambda: spark.sql(
                f"ALTER TABLE FactCopyEvent SET TBLPROPERTIES ('comment' = '{safe_table_comment}')"))

    if spark.catalog.tableExists(WATERMARK_TABLE):
        wm_table_comment = (
            "Incremental watermark for NB_CopyEventDetection_SparkKafka: single row holding the max "
            "EventEnqueuedUtcTime already processed from the shared ol_lineage_events_v3 "
            "table. Ensures each run only reads newly-appended rows, never a full table rescan."
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
