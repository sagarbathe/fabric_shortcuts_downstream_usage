"""
Builds the Sparkcompute.yml definition for the ENV_OpenLineage Fabric Environment.
Produces the final YAML + a base64-encoded payload ready for the Fabric REST
updateDefinition call (see 03_create_environment.ps1).

NOTE: this script embeds the Event Hub SAS connection string directly (documented
fallback because Key Vault `sbkeyvault01` blocked public network writes at build
time). Replace with a Key Vault-backed secret reference once that's resolved
(design doc Task 2 / Task 5c).
"""
import base64

RUNTIME_VERSION = "1.3"          # Fabric Spark runtime (Spark 3.5 / Scala 2.12)
OPENLINEAGE_SPARK_VERSION = "1.53.0"
OPENLINEAGE_SCALA_SUFFIX = "2.12"

BOOTSTRAP_SERVERS = "esehbnjzmxu954h0iqynlc.servicebus.windows.net:9093"
TOPIC_NAME = "esehbnjzmxu954h0iqynlc_eh"
# The SAS connection string retrieved from:
#   GET /v1/workspaces/{ws}/eventstreams/{esId}/sources/{sourceId}/connection
# Replace <SAS_CONNECTION_STRING> with the real value (do not commit the real value to source control).
SAS_CONNECTION_STRING = "<SAS_CONNECTION_STRING>"

JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{SAS_CONNECTION_STRING}";'
)

yaml_content = f"""enable_native_execution_engine: false
driver_cores: 4
driver_memory: 28g
executor_cores: 4
executor_memory: 28g
dynamic_executor_allocation:
  enabled: true
  min_executors: 1
  max_executors: 2
runtime_version: "{RUNTIME_VERSION}"
spark_conf:
  spark.jars.packages: "io.openlineage:openlineage-spark_{OPENLINEAGE_SCALA_SUFFIX}:{OPENLINEAGE_SPARK_VERSION}"
  spark.extraListeners: "io.openlineage.spark.agent.OpenLineageSparkListener"
  spark.openlineage.transport.type: "kafka"
  spark.openlineage.transport.topicName: "{TOPIC_NAME}"
  spark.openlineage.transport.properties.bootstrap.servers: "{BOOTSTRAP_SERVERS}"
  spark.openlineage.transport.properties.security.protocol: "SASL_SSL"
  spark.openlineage.transport.properties.sasl.mechanism: "PLAIN"
  spark.openlineage.transport.properties.sasl.jaas.config: '{JAAS_CONFIG}'
  spark.openlineage.transport.properties.key.serializer: "org.apache.kafka.common.serialization.StringSerializer"
  spark.openlineage.transport.properties.value.serializer: "org.apache.kafka.common.serialization.StringSerializer"
  spark.openlineage.namespace: "shortcut_monitoring"
  spark.openlineage.integration.spark.sql.enabled: "true"
"""

if __name__ == "__main__":
    out_path = r"C:\Users\sagarbathe\AppData\Local\Temp\Sparkcompute_final.yml"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)
    b64 = base64.b64encode(yaml_content.encode("utf-8")).decode("utf-8")
    with open(r"C:\Users\sagarbathe\AppData\Local\Temp\sparkcompute_final_b64.txt", "w") as f:
        f.write(b64)
    print("Wrote", out_path, "and base64 payload, length:", len(b64))
