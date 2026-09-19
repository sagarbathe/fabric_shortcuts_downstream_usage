"""
Final version of NB_OpenLineage_Validate.
End-to-end validation notebook: performs a real OneLake shortcut read + save
(this is what triggers genuine OpenLineage START/COMPLETE events via the
OpenLineageSparkListener registered in the attached ENV_OpenLineage environment),
waits for the Eventstream to land the events, then inspects the sink Delta table
by direct path read (avoids relying on the Spark catalog, which showed
intermittent flakiness during this build) and writes a JSON validation report to
Files/validation_result.json in the Lakehouse.
"""
import json
import base64

LAKEHOUSE_ID = "c61c659f-ecba-4de8-a5b7-25885eb3021f"        # LH_ShortcutMonitoring
WORKSPACE_ID = "c5c50c6e-30d1-4d2e-8766-d82917e13592"        # WS_SagarFabric01
ENVIRONMENT_ID = "6b3991b0-08cf-484b-bef3-beff3a6b8861"      # ENV_OpenLineage
SHORTCUT_PATH = (
    "abfss://WS_SagarFabric03@onelake.dfs.fabric.microsoft.com/"
    "lakehouse03.Lakehouse/Tables/sh_lakehouse01_SalesLT_Product"
)
SINK_TABLE = "ol_lineage_events_v2"   # current live Eventstream destination table

nb = {
    'nbformat': 4, 'nbformat_minor': 5,
    'metadata': {
        'language_info': {'name': 'python'},
        'dependencies': {
            'lakehouse': {
                'default_lakehouse': LAKEHOUSE_ID,
                'default_lakehouse_name': 'LH_ShortcutMonitoring',
                'default_lakehouse_workspace_id': WORKSPACE_ID
            },
            'environment': {
                'environmentId': ENVIRONMENT_ID,
                'workspaceId': WORKSPACE_ID
            }
        }
    },
    'cells': [
        {
            'cell_type': 'code',
            'source': [
                "# Cell 1: real shortcut read + save.\n",
                "# No lineage-specific code needed -- the OpenLineageSparkListener registered\n",
                "# via the attached ENV_OpenLineage environment automatically emits START and\n",
                "# COMPLETE OpenLineage events for this read/write to the Kafka transport.\n",
                f"df = spark.read.format('delta').load(\n",
                f"    '{SHORTCUT_PATH}'\n",
                ")\n",
                "df_out = df.limit(1000)\n",
                "df_out.write.mode('overwrite').format('delta').save('Tables/ol_validate_output')\n",
                "print('Read + save complete.')\n"
            ],
            'metadata': {}, 'execution_count': None, 'outputs': []
        },
        {
            'cell_type': 'code',
            'source': [
                "# Cell 2: wait for the Eventstream (source -> stream -> Lakehouse destination,\n",
                "# batched at minimumRows=1 / maximumDurationInSeconds=60) to land the events,\n",
                "# then inspect the sink table by direct Delta path read (avoids the Spark\n",
                "# catalog, which showed intermittent flakiness during this build) and write a\n",
                "# validation report.\n",
                "import time, json\n",
                "time.sleep(90)\n",
                f"df2 = spark.read.format('delta').load('Tables/{SINK_TABLE}')\n",
                "report = {\n",
                "    'row_count': df2.count(),\n",
                "    'schema_top_level': [f.simpleString()[:120] for f in df2.schema.fields],\n",
                "}\n",
                "rows = df2.select('eventTime', 'eventType', 'run.runId', 'job.namespace', 'job.name').limit(20).collect()\n",
                "report['sample'] = [r.asDict(recursive=True) for r in rows]\n",
                "notebookutils.fs.put('Files/validation_result.json', json.dumps(report, default=str), True)\n",
                "print(json.dumps(report, default=str, indent=2))\n"
            ],
            'metadata': {}, 'execution_count': None, 'outputs': []
        }
    ]
}

if __name__ == "__main__":
    path = r"C:\Users\sagarbathe\AppData\Local\Temp\nb_validate_final.ipynb"
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(nb))
    b64 = base64.b64encode(open(path, "rb").read()).decode()
    with open(r"C:\Users\sagarbathe\AppData\Local\Temp\nb_validate_final_b64.txt", "w") as f:
        f.write(b64)
    print("Wrote notebook, base64 length:", len(b64))
