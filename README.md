# AeroSense IoT Data Engineering Platform

## Overview

This project implements an end-to-end local data engineering platform for IoT sensor data: Python producer, Kafka ingestion, Spark Structured Streaming, local data lake, Spark SQL analytics and Flask REST API.

## Architecture

```text
Producer -> Kafka sensor-events -> Spark Streaming -> Data Lake -> Analytics/API
```

## Installation

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker compose up -d
```

Create/check the topic:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3 || true
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

## Run

```bash
python src/producer.py --count 500 --rate 50 --source site-A-rack-12
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py
python src/analytics.py
python api/app.py
bash tests/test_curl_commands.sh
```

## Technical choices

### Curated partitioning
The curated zone uses `sensor_type/year/month/day`. Sensor type is low-cardinality and commonly filtered. Date partitions support time-range analytics and retention. I avoided high-cardinality fields such as source or timestamp.

### Spark output mode
Raw and curated sinks use append mode because they write immutable records. The aggregate consumption sink uses `foreachBatch` with update mode because Parquet cannot directly update aggregate rows in streaming mode.

### Replication factor and min.insync.replicas
The Kafka topic uses RF=3 and min.insync.replicas=2. With producer `acks=all`, this tolerates one broker failure while maintaining reliable writes.

### Event time vs ingestion time
Raw uses ingestion time for audit and operational debugging. Curated uses event time because business analysis concerns when the measurement happened. A large gap indicates delay, backlog or network issues.

### Delivery semantics
The platform is at-least-once. Kafka retries and Spark checkpoints reduce loss, but duplicates remain possible around non-transactional file writes. A production system would add idempotent deduplication and a transactional table format such as Delta Lake or Iceberg.

## Results

After running, Kafka UI should show the `sensor-events` topic. Raw, curated and consumption zones should contain files under `/tmp/datalake`. Analytics CSVs are written under `outputs/analytics/`. The API responds at `/api/v1/health`.

## Limitations and improvements

This local version uses a single-machine Spark session and local filesystem. With more time, I would add automated tests, a quarantine zone, compaction jobs, OpenAPI docs, authentication and Delta/Iceberg support.
