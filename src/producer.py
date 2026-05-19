import argparse, json, random, signal, sys, time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

BROKERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC = "sensor-events"
SENSORS = {
    "temperature": {"unit": "C", "normal": (15.0, 35.0), "anomaly": (35.1, 45.0)},
    "humidity": {"unit": "%", "normal": (30.0, 90.0), "anomaly": (90.1, 95.0)},
    "pressure": {"unit": "hPa", "normal": (990.0, 1030.0), "anomaly": [(980.0, 989.9), (1030.1, 1040.0)]},
}
running = True

def stop_handler(signum, frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)

def choose_value(sensor, anomaly):
    cfg = SENSORS[sensor]
    if anomaly:
        ranges = cfg["anomaly"]
        lo, hi = random.choice(ranges) if isinstance(ranges, list) else ranges
    else:
        lo, hi = cfg["normal"]
    return round(random.uniform(lo, hi), 2)

def make_event(source, i):
    sensor = random.choice(list(SENSORS))
    anomaly = (i % 10 == 0) or (random.random() < 0.08)
    return {
        "sensor": sensor,
        "value": choose_value(sensor, anomaly),
        "unit": SENSORS[sensor]["unit"],
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source": source,
        "anomaly": anomaly,
    }

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=32768,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--source", default="site-A-rack-12")
    args = parser.parse_args()
    delay = 1 / args.rate if args.rate > 0 else 0
    producer = create_producer()
    sent = 0
    try:
        for i in range(args.count):
            if not running:
                break
            event = make_event(args.source, i)
            try:
                md = producer.send(TOPIC, key=event["sensor"], value=event).get(timeout=15)
                sent += 1
                print(f"{sent:04d} key={event['sensor']} partition={md.partition} offset={md.offset} value={event['value']} anomaly={event['anomaly']}")
            except KafkaError as exc:
                print(f"Kafka send failed: {exc}", file=sys.stderr)
            if delay:
                time.sleep(delay)
    finally:
        producer.flush(); producer.close()
        print(f"Producer closed. Sent {sent} events.")

if __name__ == "__main__":
    main()
