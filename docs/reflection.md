# Reflection Questions

## 1. Crash after raw write but before curated write

The raw zone already stores the original Kafka payload, so the event is not lost. The curated zone may be incomplete until restart. Separate checkpoint directories for raw, curated and consumption sinks prevent progress in one sink from hiding failure in another sink.

## 2. Bottlenecks at 50,000 messages/s

Likely bottlenecks are Kafka partition throughput, producer batching, Spark micro-batch duration, and small Parquet files. I would increase partitions, scale Spark resources, tune producer batching, and add compaction.

## 3. Kafka vs Parquet as historical source

Kafka is best for recent replay, ordering per key and event-driven consumers. Parquet is better for cheap historical storage, partition pruning, column pruning and analytics. Kafka should be the ingestion log; Parquet should be the long-term analytical source.

## 4. Broken sensor for 2 hours

Outlier validation rejects physically impossible values from curated data, while anomaly rules flag plausible but abnormal values. I would keep raw data unchanged and write rejected records to a quarantine path instead of deleting them.

## 5. Adding sensor type co2

Modify `src/producer.py` for generation and units, `src/spark_pipeline.py` for allowed sensor, validation and anomaly rule, `api/app.py` for allowed type, and documentation/tests for curl examples. Kafka topic schema remains flexible JSON.
