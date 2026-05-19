import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dayofmonth,
    from_json,
    from_unixtime,
    hour,
    max as spark_max,
    min as spark_min,
    month,
    sum as spark_sum,
    when,
    window,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "localhost:9092,localhost:9094,localhost:9096",
)

TOPIC = os.getenv("KAFKA_TOPIC", "sensor-events")

# Exam required data lake layout.
LAKE_ROOT = os.getenv("DATALAKE_ROOT", "/tmp/datalake")
CKPT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/datalake-checkpoints")

RAW_PATH = f"{LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
CONSUMPTION_PATH = f"{LAKE_ROOT}/consumption/use_case=sensor_averages"

CKPT_RAW = f"{CKPT_ROOT}/raw"
CKPT_CURATED = f"{CKPT_ROOT}/curated"
CKPT_CONSUMPTION = f"{CKPT_ROOT}/consumption"

for path in [
    RAW_PATH,
    CURATED_PATH,
    CONSUMPTION_PATH,
    CKPT_RAW,
    CKPT_CURATED,
    CKPT_CONSUMPTION,
]:
    Path(path).mkdir(parents=True, exist_ok=True)


SENSOR_SCHEMA = StructType([
    StructField("sensor", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("timestamp", LongType(), False),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("AeroSenseExamPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_streaming_dataframes(spark):
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_zone = raw_kafka.select(
        col("value").cast("string").alias("raw_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("ingestion_time"),
        year(col("timestamp")).alias("year"),
        month(col("timestamp")).alias("month"),
        dayofmonth(col("timestamp")).alias("day"),
        hour(col("timestamp")).alias("hour"),
    )

    parsed = (
        raw_kafka
        .select(
            from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("p"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_ingestion_time"),
        )
        .select(
            col("p.sensor").alias("sensor_type"),
            col("p.value").alias("value"),
            col("p.unit").alias("unit"),
            col("p.source").alias("source"),
            col("p.anomaly").alias("producer_anomaly"),
            from_unixtime((col("p.timestamp") / 1000).cast("long")).cast("timestamp").alias("event_time"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("kafka_ingestion_time"),
        )
        .filter(col("sensor_type").isin("temperature", "humidity", "pressure"))
        .filter(col("value").isNotNull())
    )

    curated = (
        parsed
        .filter(
            (
                (col("sensor_type") == "temperature")
                & col("value").between(0, 80)
            )
            |
            (
                (col("sensor_type") == "humidity")
                & col("value").between(0, 100)
            )
            |
            (
                (col("sensor_type") == "pressure")
                & col("value").between(900, 1100)
            )
        )
        .withColumn(
            "is_anomaly",
            when(
                (
                    (col("sensor_type") == "temperature")
                    & (col("value") > 35)
                )
                |
                (
                    (col("sensor_type") == "humidity")
                    & (col("value") > 90)
                )
                |
                (
                    (col("sensor_type") == "pressure")
                    & (
                        (col("value") < 990)
                        | (col("value") > 1030)
                    )
                ),
                True,
            ).otherwise(False),
        )
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
    )

    consumption = (
        curated
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor_type"),
        )
        .agg(
            avg("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_type"),
            col("mean_value"),
            col("min_value"),
            col("max_value"),
            col("observation_count"),
            col("anomaly_count"),
            year(col("window.start")).alias("year"),
            month(col("window.start")).alias("month"),
        )
    )

    return raw_zone, curated, consumption


def write_consumption_batch(batch_df, batch_id):
    print(f"Writing consumption batch {batch_id}")

    if batch_df.limit(1).count() == 0:
        print(f"Consumption batch {batch_id} is empty.")
        return

    batch_df.orderBy("window_start", "sensor_type").show(50, truncate=False)

    (
        batch_df
        .write
        .mode("append")
        .partitionBy("sensor_type", "year", "month")
        .parquet(CONSUMPTION_PATH)
    )

    print(f"Consumption batch {batch_id} written to {CONSUMPTION_PATH}")


def main():
    spark = create_spark_session()

    print(f"Kafka brokers: {KAFKA_BROKERS}")
    print(f"Kafka topic: {TOPIC}")
    print(f"Raw: {RAW_PATH}")
    print(f"Curated: {CURATED_PATH}")
    print(f"Consumption: {CONSUMPTION_PATH}")

    raw_zone, curated, consumption = create_streaming_dataframes(spark)

    raw_query = (
        raw_zone
        .writeStream
        .format("json")
        .outputMode("append")
        .option("path", RAW_PATH)
        .option("checkpointLocation", CKPT_RAW)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="20 seconds")
        .start()
    )

    curated_query = (
        curated
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", CURATED_PATH)
        .option("checkpointLocation", CKPT_CURATED)
        .option("compression", "snappy")
        .partitionBy("sensor_type", "year", "month", "day")
        .trigger(processingTime="20 seconds")
        .start()
    )

    consumption_query = (
        consumption
        .writeStream
        .outputMode("update")
        .foreachBatch(write_consumption_batch)
        .option("checkpointLocation", CKPT_CONSUMPTION)
        .trigger(processingTime="20 seconds")
        .start()
    )

    print("Spark Structured Streaming pipeline started.")
    print("Wait about 60 seconds, then press Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
        for query in [raw_query, curated_query, consumption_query]:
            if query.isActive:
                query.stop()
        spark.stop()
        print("Spark pipeline stopped.")


if __name__ == "__main__":
    main()
