import json, base64

nb = {
 'nbformat': 4, 'nbformat_minor': 5,
 'metadata': {
   'language_info': {'name': 'python'},
   'dependencies': {
     'lakehouse': {
        'default_lakehouse': 'c61c659f-ecba-4de8-a5b7-25885eb3021f',
        'default_lakehouse_name': 'LH_ShortcutMonitoring',
        'default_lakehouse_workspace_id': 'c5c50c6e-30d1-4d2e-8766-d82917e13592'
     },
     'environment': {
        'environmentId': '6b3991b0-08cf-484b-bef3-beff3a6b8861',
        'workspaceId': 'c5c50c6e-30d1-4d2e-8766-d82917e13592'
     }
   }
 },
 'cells': [
   {
     'cell_type': 'code',
     'source': [
        "import json\n",
        "report = {}\n",
        "conf = spark.sparkContext.getConf()\n",
        "keys = [k for k, v in conf.getAll() if 'openlineage' in k.lower() or 'extralisteners' in k.lower() or k.lower()=='spark.jars.packages']\n",
        "cfgdump = {}\n",
        "for k in sorted(keys):\n",
        "    v = conf.get(k)\n",
        "    if 'jaas' in k.lower() or 'password' in k.lower():\n",
        "        v = v[:40] + '...REDACTED'\n",
        "    cfgdump[k] = v\n",
        "report['config'] = cfgdump\n",
        "jvm = spark._jvm\n",
        "try:\n",
        "    cls = jvm.java.lang.Class.forName('io.openlineage.spark.agent.OpenLineageSparkListener')\n",
        "    report['listener_class_found'] = True\n",
        "except Exception as e:\n",
        "    report['listener_class_found'] = False\n",
        "    report['listener_class_error'] = repr(e)\n",
        "try:\n",
        "    lb = spark.sparkContext._jsc.sc().listenerBus()\n",
        "    m = lb.getClass().getMethod('listeners')\n",
        "    listeners = m.invoke(lb)\n",
        "    report['listeners'] = str(listeners)\n",
        "except Exception as e:\n",
        "    report['listeners_error'] = repr(e)\n",
        "notebookutils.fs.put('Files/diag_listener.json', json.dumps(report, default=str), True)\n",
        "print(json.dumps(report, default=str, indent=2))\n"
     ],
     'metadata': {}, 'execution_count': None, 'outputs': []
   }
 ]
}

path = r'C:\Users\sagarbathe\AppData\Local\Temp\nb_diag_listener.ipynb'
open(path, 'w', encoding='utf-8').write(json.dumps(nb))
b64 = base64.b64encode(open(path, 'rb').read()).decode()
open(r'C:\Users\sagarbathe\AppData\Local\Temp\nb_diag_listener_b64.txt', 'w').write(b64)
print('done', len(b64))
