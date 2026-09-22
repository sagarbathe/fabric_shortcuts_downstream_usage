# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "6b3991b0-08cf-484b-bef3-beff3a6b8861",
# META       "workspaceId": "c5c50c6e-30d1-4d2e-8766-d82917e13592"
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: read + save-as-is, to generate a real Spark read/write for the
# OpenLineageSparkListener (registered via the attached ENV_OpenLineage
# environment) to automatically emit START/COMPLETE OpenLineage events for,
# over the Kafka transport. This notebook is deployed into THIS monitored
# workspace (not the solution's own workspace) specifically so it can validate
# against a REAL shortcut here - deliberately no default lakehouse is attached
# (fully-qualified abfss:// paths only), since every monitored workspace has
# different lakehouses/shortcuts.
#
# >>> BEFORE RUNNING: replace SOURCE_PATH below with a real OneLake shortcut in
#     THIS workspace (Lakehouse item > right-click the shortcut > Properties
#     for its path), and OUTPUT_PATH with any writable table location in a
#     lakehouse you own here. <<<
SOURCE_PATH = "abfss://<THIS-WORKSPACE-NAME-OR-ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE-NAME>.Lakehouse/Tables/<A-REAL-SHORTCUT-TABLE>"
OUTPUT_PATH = "abfss://<THIS-WORKSPACE-NAME-OR-ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE-NAME>.Lakehouse/Tables/ol_validate_output"

df = spark.read.format('delta').load(SOURCE_PATH)
df_out = df.limit(1000)
df_out.write.mode('overwrite').format('delta').save(OUTPUT_PATH)
print('Read + save complete.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: wait for the Eventstream (source -> SqlFlatten operator -> Lakehouse
# destination, batched at minimumRows=1 / maximumDurationInSeconds=60) to land
# the events. All monitored workspaces publish to the SAME shared Kafka topic
# and therefore the SAME central sink table in the solution's own Lakehouse -
# not a local one - so this reads/writes there via a fully-qualified path
# (SOLUTION_WORKSPACE_ID_TOKEN/SOLUTION_LAKEHOUSE_ID_TOKEN are substituted at
# deploy time to the solution's own workspace/lakehouse ids, regardless of
# which monitored workspace this copy of the notebook runs in).
import time, json
SOLUTION_LAKEHOUSE_ABFSS = "abfss://SOLUTION_WORKSPACE_ID_TOKEN@onelake.dfs.fabric.microsoft.com/SOLUTION_LAKEHOUSE_ID_TOKEN"
# Token-substituted at deploy time to this monitored workspace's display name, purely to name the
# output report file uniquely per workspace in the shared central Files area.
THIS_WORKSPACE_NAME = "WS_SagarFabric01"

time.sleep(90)
df2 = spark.read.format('delta').load(f"{SOLUTION_LAKEHOUSE_ABFSS}/Tables/ol_lineage_events_v3")
report = {
    'row_count': df2.count(),
    'schema_top_level': [f.simpleString()[:200] for f in df2.schema.fields],
}
rows = df2.select('eventTime', 'eventType', 'job', 'inputs_json', 'outputs_json').collect()
report['sample'] = [r.asDict(recursive=True) for r in rows]
out_path = f"{SOLUTION_LAKEHOUSE_ABFSS}/Files/validation_results/{THIS_WORKSPACE_NAME}_validation_result_v3.json"
notebookutils.fs.put(out_path, json.dumps(report, default=str), True)
print(f"Wrote validation report to {out_path}")
print(json.dumps(report, default=str, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
