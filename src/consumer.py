import json
from kafka import KafkaConsumer
consumer = KafkaConsumer(
    "sensor-events",
    bootstrap_servers=["localhost:9092", "localhost:9094", "localhost:9096"],
    group_id="exam-debug-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    key_deserializer=lambda b: b.decode("utf-8") if b else None,
    consumer_timeout_ms=15000,
)
for msg in consumer:
    print(f"partition={msg.partition} offset={msg.offset} key={msg.key} value={msg.value}")
consumer.close()
