import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'sf.stats.crimes',
    group_id='MyKafkaConsumer',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)

for message in consumer:
    print(f"[#{message.offset}]", message.value)
