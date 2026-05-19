import json
from kafka import KafkaConsumer, KafkaProducer
BROKERS=["localhost:9092","localhost:9094","localhost:9096"]
TOPIC="sensor-events"
def latest_reading(sensor_type, timeout_ms=5000):
    c=KafkaConsumer(TOPIC,bootstrap_servers=BROKERS,auto_offset_reset="earliest",enable_auto_commit=False,consumer_timeout_ms=timeout_ms,value_deserializer=lambda b: json.loads(b.decode("utf-8")),key_deserializer=lambda b: b.decode("utf-8") if b else None)
    rows=[]
    try:
        for m in c:
            v=m.value
            if v.get("sensor")==sensor_type:
                v["_kafka"]={"partition":m.partition,"offset":m.offset,"key":m.key}; rows.append(v)
    finally: c.close()
    rows.sort(key=lambda r:r.get("timestamp") or 0, reverse=True)
    return rows[0] if rows else None
def publish_reading(reading):
    p=KafkaProducer(bootstrap_servers=BROKERS,acks="all",retries=5,key_serializer=lambda k:k.encode("utf-8"),value_serializer=lambda v:json.dumps(v,separators=(",",":")).encode("utf-8"))
    try:
        md=p.send(TOPIC,key=reading["sensor"],value=reading).get(timeout=10); p.flush()
        return {"topic":md.topic,"partition":md.partition,"offset":md.offset}
    finally: p.close()
