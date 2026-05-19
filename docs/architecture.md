# Architecture

```text
Python producer -> Kafka topic sensor-events -> Spark Structured Streaming
                                      |                |
                                      |                +-> raw JSON zone
                                      |                +-> curated Parquet zone
                                      |                +-> consumption aggregate zone
                                      v
                              Flask REST API
```

Kafka is the ingestion bus with three brokers, RF=3 and min.insync.replicas=2. Spark consumes Kafka, validates JSON sensor events, detects anomalies and writes the data lake. The API exposes latest Kafka readings, Parquet statistics, recent anomalies and POST write-back.
