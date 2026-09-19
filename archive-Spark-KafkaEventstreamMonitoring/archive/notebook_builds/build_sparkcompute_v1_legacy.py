import json, base64

with open(r'C:\Users\SAGARB~1\AppData\Local\Temp\es_conn.json', 'rb') as f:
    d = json.load(f)

ns = d['fullyQualifiedNamespace']
eh = d['eventHubName']
connstr = d['accessKeys']['primaryConnectionString']
jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" password="' + connstr + '";')

yaml = """enable_native_execution_engine: false
driver_cores: 4
driver_memory: 28g
executor_cores: 4
executor_memory: 28g
dynamic_executor_allocation:
  enabled: true
  min_executors: 1
  max_executors: 2
runtime_version: 1.3
spark_conf:
  spark.extraListeners: io.openlineage.spark.agent.OpenLineageSparkListener
  spark.openlineage.transport.type: kafka
  spark.openlineage.transport.topicName: {eh}
  spark.openlineage.transport.properties.bootstrap.servers: {ns}:9093
  spark.openlineage.transport.properties.security.protocol: SASL_SSL
  spark.openlineage.transport.properties.sasl.mechanism: PLAIN
  spark.openlineage.transport.properties.key.serializer: org.apache.kafka.common.serialization.StringSerializer
  spark.openlineage.transport.properties.value.serializer: org.apache.kafka.common.serialization.StringSerializer
  spark.openlineage.namespace: shortcut_monitoring
  spark.openlineage.facets.disabled: "[]"
  spark.jars.packages: io.openlineage:openlineage-spark_2.12:1.53.0
  spark.openlineage.transport.properties.sasl.jaas.config: '{jaas}'
""".format(eh=eh, ns=ns, jaas=jaas)

with open(r'C:\Users\SAGARB~1\AppData\Local\Temp\Sparkcompute_final.yml', 'w', newline='\r\n') as f:
    f.write(yaml)

b = open(r'C:\Users\SAGARB~1\AppData\Local\Temp\Sparkcompute_final.yml', 'rb').read()
b64 = base64.b64encode(b).decode()
with open(r'C:\Users\SAGARB~1\AppData\Local\Temp\sparkcompute_final_b64.txt', 'w') as f:
    f.write(b64)
print('done', len(b64))
