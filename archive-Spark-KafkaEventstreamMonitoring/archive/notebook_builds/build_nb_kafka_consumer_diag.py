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
        "jvm = spark._jvm\n",
        "props = jvm.java.util.Properties()\n",
        "props.setProperty('bootstrap.servers', 'esehbnjzmxu954h0iqynlc.servicebus.windows.net:9093')\n",
        "props.setProperty('security.protocol', 'SASL_SSL')\n",
        "props.setProperty('sasl.mechanism', 'PLAIN')\n",
        "jaas = spark.conf.get('spark.openlineage.transport.properties.sasl.jaas.config')\n",
        "props.setProperty('sasl.jaas.config', jaas)\n",
        "props.setProperty('key.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')\n",
        "props.setProperty('value.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')\n",
        "props.setProperty('group.id', '$Default')\n",
        "props.setProperty('auto.offset.reset', 'earliest')\n",
        "props.setProperty('enable.auto.commit', 'false')\n",
        "consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)\n",
        "topic = 'esehbnjzmxu954h0iqynlc_eh'\n",
        "parts = consumer.partitionsFor(topic)\n",
        "print('partitions for topic:', parts)\n",
        "tp = jvm.org.apache.kafka.common.TopicPartition(topic, 0)\n",
        "from py4j.java_collections import ListConverter\n",
        "tp_list = ListConverter().convert([tp], spark.sparkContext._gateway._gateway_client)\n",
        "consumer.assign(tp_list)\n",
        "consumer.poll(0)\n",
        "consumer.seekToBeginning(tp_list)\n",
        "pos = consumer.position(tp)\n",
        "print('position after seek:', pos)\n",
        "end_offsets = consumer.endOffsets(tp_list)\n",
        "print('end offsets:', end_offsets)\n",
        "import time as t\n",
        "end = t.time() + 15\n",
        "messages = []\n",
        "while t.time() < end:\n",
        "    records = consumer.poll(2000)\n",
        "    it = records.iterator()\n",
        "    while it.hasNext():\n",
        "        r = it.next()\n",
        "        messages.append({'offset': r.offset(), 'key': str(r.key()), 'value': str(r.value())[:2000]})\n",
        "consumer.close()\n",
        "report = {'message_count': len(messages), 'messages': messages, 'partitions': str(parts), 'position_after_seek': str(pos), 'end_offsets': str(end_offsets)}\n",
        "notebookutils.fs.put('Files/kafka_consume_result.json', json.dumps(report, default=str), True)\n",
        "print('message_count:', len(messages))\n"
     ],
     'metadata': {}, 'execution_count': None, 'outputs': []
   }
 ]
}

path = r'C:\Users\sagarbathe\AppData\Local\Temp\nb_consume.ipynb'
open(path, 'w', encoding='utf-8').write(json.dumps(nb))
b64 = base64.b64encode(open(path, 'rb').read()).decode()
open(r'C:\Users\sagarbathe\AppData\Local\Temp\nb_consume_b64.txt', 'w').write(b64)
print('done', len(b64))
