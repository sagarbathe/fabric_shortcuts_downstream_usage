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
# META       "default_lakehouse_workspace_id": "c5c50c6e-30d1-4d2e-8766-d82917e13592"
# META     },
# META     "environment": {
# META       "environmentId": "6b3991b0-08cf-484b-bef3-beff3a6b8861",
# META       "workspaceId": "c5c50c6e-30d1-4d2e-8766-d82917e13592"
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: real shortcut read + save.
# No lineage-specific code needed -- the OpenLineageSparkListener registered
# via the attached ENV_OpenLineage environment automatically emits START and
# COMPLETE OpenLineage events for this read/write to the Kafka transport.
df = spark.read.format('delta').load(
    'abfss://WS_SagarFabric03@onelake.dfs.fabric.microsoft.com/lakehouse03.Lakehouse/Tables/sh_lakehouse01_SalesLT_Product'
)
df_out = df.limit(1000)
df_out.write.mode('overwrite').format('delta').save('Tables/ol_validate_output')
print('Read + save complete.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: wait for the Eventstream (source -> SqlFlatten operator -> Lakehouse
# destination, batched at minimumRows=1 / maximumDurationInSeconds=60) to land
# the events, then inspect the sink table by direct Delta path read and write
# a validation report.
import time, json
time.sleep(90)
df2 = spark.read.format('delta').load('Tables/ol_lineage_events_v3')
report = {
    'row_count': df2.count(),
    'schema_top_level': [f.simpleString()[:200] for f in df2.schema.fields],
}
rows = df2.select('eventTime', 'eventType', 'job', 'inputs_json', 'outputs_json').collect()
report['sample'] = [r.asDict(recursive=True) for r in rows]
notebookutils.fs.put('Files/validation_result_v3.json', json.dumps(report, default=str), True)
print(json.dumps(report, default=str, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
