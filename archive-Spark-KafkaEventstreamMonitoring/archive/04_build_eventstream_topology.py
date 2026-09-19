"""
Builds the Eventstream topology (eventstream.json) for ES_OpenLineageEvents:
  CustomEndpoint source -> DefaultStream -> Lakehouse destination.

The CustomEndpoint source needs no special properties at creation time; Fabric
auto-provisions a backing Kafka-compatible Event Hub for it. Retrieve the real
connection details afterwards via:
  GET /v1/workspaces/{ws}/eventstreams/{esId}/sources/{sourceId}/connection
"""
import json
import base64

WORKSPACE_ID = "c5c50c6e-30d1-4d2e-8766-d82917e13592"   # WS_SagarFabric01
LAKEHOUSE_ITEM_ID = "c61c659f-ecba-4de8-a5b7-25885eb3021f"  # LH_ShortcutMonitoring
DELTA_TABLE_NAME = "ol_lineage_events_v2"  # sink table name (auto-created by Eventstream)

topology = {
    "sources": [
        {
            "name": "OpenLineageCustomEndpoint",
            "type": "CustomEndpoint",
            "properties": {}
        }
    ],
    "destinations": [
        {
            "name": "LH_ShortcutMonitoring_Sink",
            "type": "Lakehouse",
            "properties": {
                "workspaceId": WORKSPACE_ID,
                "itemId": LAKEHOUSE_ITEM_ID,
                "schema": "",
                "deltaTable": DELTA_TABLE_NAME,
                "minimumRows": 1,
                "maximumDurationInSeconds": 60,
                "inputSerialization": {
                    "type": "Json",
                    "properties": {"encoding": "UTF8"}
                }
            },
            "inputNodes": [{"name": "ES_OpenLineageEvents-stream"}]
        }
    ],
    "streams": [
        {
            "name": "ES_OpenLineageEvents-stream",
            "type": "DefaultStream",
            "properties": {},
            "inputNodes": [{"name": "OpenLineageCustomEndpoint"}]
        }
    ],
    "operators": [],
    "compatibilityLevel": "1.1"
}

if __name__ == "__main__":
    payload = json.dumps(topology, indent=2)
    b64 = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
    with open(r"C:\Users\sagarbathe\AppData\Local\Temp\eventstream_topology.json", "w") as f:
        f.write(payload)
    with open(r"C:\Users\sagarbathe\AppData\Local\Temp\eventstream_topology_b64.txt", "w") as f:
        f.write(b64)
    print("Wrote topology, base64 length:", len(b64))
