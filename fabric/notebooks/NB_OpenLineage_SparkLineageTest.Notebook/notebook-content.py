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
# META         },
# META         {
# META           "id": "5849f467-dd0a-4e9c-be00-b744441fe7c7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## NB_OpenLineage_SparkLineageTest
# Phase-2 SPIKE / prototype notebook - **read-only investigation, does not touch any existing
# tables, notebooks, or config used by NB_ShortcutInventory_DuplicateDetection or
# NB_CopyEventDetection_Warehouse.**
# 
# Purpose: verify, with real emitted data in this tenant, whether Fabric's built-in OpenLineage
# Spark listener produces column-level lineage detail sufficient to detect the Spark-engine
# equivalent of "shortcut read and saved as is" (design doc §4.1), using the **file transport**
# (Option 1 from the OpenLineage research: zero extra infrastructure, writes NDJSON straight to
# this Lakehouse's own Files area).
# 
# What this notebook does:
#   1. Enables OpenLineage via a `%%configure` cell (must be the FIRST cell run, before any Spark
#      code, since it restarts the Spark session with the new conf).
#   2. Performs a real "shortcut read and saved as is" operation: reads the existing shortcut
#      `sh_azuresql_import_SalesLT_Customer` (hosted in lakehouse01, WS_SagarFabric01) via its
#      OneLake ABFSS path and writes it, unmodified (SELECT * equivalent), to a throwaway test
#      Delta table `_ol_test_customer_full_copy` in THIS lakehouse (LH_ShortcutMonitoring) - never
#      touching production tables.
#   3. Performs a second, partial-column write (a deliberately-reduced column subset) to a second
#      throwaway table `_ol_test_customer_partial_copy`, so we can compare the emitted lineage
#      JSON for a "read-and-saved-as-is" case vs. a genuinely-reduced case.
#   4. Reads back the emitted OpenLineage NDJSON file(s) from this Lakehouse's `Files/lineage_test/`
#      folder and prints the raw JSON plus a parsed summary of input/output columns and any
#      columnLineage facet, so we can inspect real content before deciding how (or whether) to
#      build this into the production Spark-engine detection notebook.
#   5. Cleans up: drops both throwaway test tables at the end (the raw lineage JSON files are left
#      in place under Files/lineage_test/ for further manual inspection).
# 
# This notebook intentionally does NOT modify notebook_copyevent_content.py / notebook_content_v2.py
# or their deployed Fabric notebooks - per instruction, this is new, additive code only.


# MARKDOWN ********************

# **IMPORTANT (interactive use only):** the cell below enables OpenLineage for THIS interactive
# session. `%%configure` only works when run interactively in the Fabric notebook UI (it is NOT
# supported when the notebook is invoked via the Job Scheduler REST API - see the note in the next
# code cell for how that path passes the same conf via `executionData.configuration.conf` instead).
# Run this cell FIRST, before any other Spark code - `%%configure` must be the very first line of
# the cell (no leading comments) or Fabric's magic parser fails with
# `UsageError: Line magic function` `%%configure` `not found`.

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "conf": {
# MAGIC         "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
# MAGIC         "spark.openlineage.transport.type": "file",
# MAGIC         "spark.openlineage.transport.location": "/lakehouse/default/Files/lineage_test/ol_events.ndjson",
# MAGIC         "spark.openlineage.namespace": "shortcut_monitoring_spike",
# MAGIC         "spark.openlineage.facets.disabled": "[]"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# NOTE: the Spark conf that enables OpenLineage (spark.extraListeners, spark.openlineage.transport.*,
# spark.openlineage.namespace) is intentionally NOT set here via a "%%configure" cell. "%%configure"
# only works for interactive/UI notebook sessions - the Fabric Job Scheduler REST API
# (jobs/instances?jobType=RunNotebook) rejects it with
# "System_Cancelled_Session_Statements_Failed", since scheduled/API-triggered runs don't support
# in-notebook session-restart magics. Instead, the same conf is passed in the RunNotebook API call's
# executionData.configuration.conf payload (see how this notebook is actually invoked, documented in
# the markdown cell above / the deployment script) - this works for BOTH interactive AND
# API/scheduled runs, so it's the only approach compatible with eventually scheduling this in
# production. This cell just verifies the conf actually took effect this session.
try:
    ol_listener_conf = spark.conf.get("spark.extraListeners")
    ol_transport_type = spark.conf.get("spark.openlineage.transport.type")
    ol_transport_location = spark.conf.get("spark.openlineage.transport.location")
    print(f"spark.extraListeners = {ol_listener_conf}")
    print(f"spark.openlineage.transport.type = {ol_transport_type}")
    print(f"spark.openlineage.transport.location = {ol_transport_location}")
except Exception as e:
    print(f"WARNING: could not read back OpenLineage spark conf - it may not have been applied "
          f"via executionData.configuration.conf on this run: {e}")

# Extra diagnostic: check the JVM's actual list of registered SparkListeners (via
# ListenerBus) to confirm OpenLineageSparkListener really attached - if the class was
# missing/failed to construct, extraListeners silently no-ops instead of erroring, which
# would explain events not being emitted despite the conf reading back correctly above.
try:
    listeners_field = None
    lb = spark.sparkContext._jsc.sc().listenerBus()
    # Walk the class hierarchy looking for a "listeners" field (Spark's internal ListenerBus
    # stores listeners in a CopyOnWriteArrayList on a superclass - exact class varies by version).
    klass = lb.getClass()
    while klass is not None and listeners_field is None:
        try:
            listeners_field = klass.getDeclaredField("listenersPlusTimers")
        except Exception:
            try:
                listeners_field = klass.getDeclaredField("listeners")
            except Exception:
                klass = klass.getSuperclass()
    if listeners_field is None:
        raise Exception("no 'listeners' field found on any superclass")
    listeners_field.setAccessible(True)
    listeners = listeners_field.get(lb)
    listener_classes = [str(l) for l in listeners.toArray()]
    print("Registered Spark listeners:")
    for lc in listener_classes:
        print(" -", lc)
    print("OpenLineage listener attached:", any("OpenLineage" in lc for lc in listener_classes))
except Exception as e:
    print("Could not introspect listener bus:", repr(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 1: perform a genuine "shortcut read and saved as is" operation (SELECT * equivalent) ---
# Source: the REAL, pre-existing shortcut sh_azuresql_import_SalesLT_Customer, hosted in
# lakehouse01 (WS_SagarFabric03, workspace 0077781d-74c3-437e-b75c-42e640953c69). Reading it via
# its own OneLake ABFSS path (not this notebook's default lakehouse) so the lineage event's INPUT
# dataset correctly reflects the shortcut's real hosting item, exactly as a real user's notebook
# would reference it.
LAKEHOUSE03_WORKSPACE_ID = "0077781d-74c3-437e-b75c-42e640953c69"
LAKEHOUSE03_ITEM_ID = "5849f467-dd0a-4e9c-be00-b744441fe7c7"  # lakehouse03, confirmed via Fabric API
# SHORTCUT_TABLE_PATH = (
#     f"abfss://{LAKEHOUSE01_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
#     f"{LAKEHOUSE01_ITEM_ID}/Tables/sh_lakehouse01_SalesLT_Product"
# )

SHORTCUT_TABLE_PATH=f"abfss://WS_SagarFabric03@onelake.dfs.fabric.microsoft.com/lakehouse03.Lakehouse/Tables/sh_lakehouse01_SalesLT_Product"

df_source = spark.read.format("delta").load(SHORTCUT_TABLE_PATH)
print("Source shortcut columns:", df_source.columns)
print("Source shortcut row count:", df_source.count())

# "Read and saved as is" - full SELECT *, all columns retained, written to a throwaway PATH-BASED
# Delta location in THIS (LH_ShortcutMonitoring) lakehouse's Tables/ area (NOT saveAsTable/catalog
# registration - testing whether OpenLineage's per-job COMPLETE event (with real inputs/outputs/
# columnLineage) fires for a path-based .save() the same way it may not be firing for saveAsTable).
FULL_COPY_PATH = "Files/lineage_test/_ol_test_customer_full_copy_pathwrite"
df_source.write.mode("overwrite").format("delta").save(FULL_COPY_PATH)
print(f"Wrote {FULL_COPY_PATH} (full column set - the 'read and saved as is' case, path-based write).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 2: a genuinely-reduced copy (small column subset), for contrast ---
# Keep only ~2 of the source's columns (a low retention % case) so we can compare the emitted
# columnLineage facet against the full-copy case above and confirm OpenLineage actually
# distinguishes "select * / high retention" from "true partial read" at the column level.
all_cols = df_source.columns
partial_cols = all_cols[:2] if len(all_cols) >= 2 else all_cols
df_partial = df_source.select(*partial_cols)
PARTIAL_COPY_PATH = "Files/lineage_test/_ol_test_customer_partial_copy_pathwrite"
df_partial.write.mode("overwrite").format("delta").save(PARTIAL_COPY_PATH)
print(f"Wrote {PARTIAL_COPY_PATH} (columns retained: {partial_cols}) - the 'true partial read' case, path-based write.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 3: read back the raw OpenLineage NDJSON events and inspect them ---
# Give the async OpenLineage file writer a moment to flush (it writes on each Spark job
# start/complete event; by the time we get here both writes above have already completed their
# jobs, but a short pause avoids any race on the file's last flush).
import time
time.sleep(5)

import json

LINEAGE_FILE_PATH = "Files/lineage_test/ol_events.ndjson"
try:
    lineage_lines_df = spark.read.text(LINEAGE_FILE_PATH)
    raw_lines = [r["value"] for r in lineage_lines_df.collect()]
    print(f"Found {len(raw_lines)} raw OpenLineage event line(s) in {LINEAGE_FILE_PATH}.")
except Exception as e:
    raw_lines = []
    print(f"Could not read lineage file yet (may not exist / not flushed): {e}")

parsed_events = []
for line in raw_lines:
    try:
        parsed_events.append(json.loads(line))
    except Exception as e:
        print(f"  WARN: could not parse a line as JSON: {e}")

print(f"Parsed {len(parsed_events)} valid JSON event(s).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(parsed_events)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 4: summarize what each COMPLETE event tells us about inputs/outputs/column lineage ---
# We only care about eventType == "COMPLETE" events that have both inputs and outputs (a
# START event or a read-only job won't have the write-side detail we need).
summary_rows = []
for ev in parsed_events:
    if ev.get("eventType") != "COMPLETE":
        continue
    inputs = ev.get("inputs", [])
    outputs = ev.get("outputs", [])
    if not outputs:
        continue

    for out in outputs:
        out_name = out.get("name")
        out_namespace = out.get("namespace")
        schema_facet = out.get("facets", {}).get("schema", {})
        out_columns = [f.get("name") for f in schema_facet.get("fields", [])]

        col_lineage_facet = out.get("facets", {}).get("columnLineage", {})
        col_lineage_fields = col_lineage_facet.get("fields", {})

        input_names = [f"{i.get('namespace')}::{i.get('name')}" for i in inputs]
        input_col_counts = []
        for i in inputs:
            in_schema = i.get("facets", {}).get("schema", {})
            input_col_counts.append(len(in_schema.get("fields", [])))

        summary_rows.append({
            "job_name": ev.get("job", {}).get("name"),
            "output_dataset": f"{out_namespace}::{out_name}",
            "output_column_count": len(out_columns),
            "output_columns": out_columns,
            "input_datasets": input_names,
            "input_column_counts": input_col_counts,
            "has_column_lineage_facet": bool(col_lineage_fields),
            "column_lineage_field_count": len(col_lineage_fields),
            "column_lineage_sample": {
                k: v.get("inputFields", []) for k, v in list(col_lineage_fields.items())[:5]
            } if col_lineage_fields else None,
        })

print(f"\n=== Summary: {len(summary_rows)} write event(s) with output detail found ===\n")
for i, row in enumerate(summary_rows):
    print(f"--- Event {i+1} ---")
    print(json.dumps(row, indent=2, default=str))
    print()

if not summary_rows:
    print("No COMPLETE events with output/schema detail were found. Possible causes:")
    print("  - OpenLineage listener didn't attach (check the %%configure cell ran FIRST, with no prior Spark activity).")
    print("  - File transport hasn't flushed yet (try re-running this cell after a short wait).")
    print("  - Facet name/casing differs from assumed 'schema'/'columnLineage' - inspect parsed_events raw content directly below.")
    print("\nRaw parsed events (first 3, for manual inspection):")
    for ev in parsed_events[:3]:
        print(json.dumps(ev, indent=2, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Step 5: cleanup - drop both throwaway test outputs (leave the raw lineage JSON in place for
#     further manual inspection / re-running the summary cell above without redoing the writes). ---
notebookutils.fs.rm("Files/lineage_test/_ol_test_customer_full_copy_pathwrite", True)
notebookutils.fs.rm("Files/lineage_test/_ol_test_customer_partial_copy_pathwrite", True)
print("Removed both throwaway test outputs. Raw lineage events remain at Files/lineage_test/ol_events.ndjson for reference.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
