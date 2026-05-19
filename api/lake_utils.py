import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,col,count,max as spark_max,min as spark_min,sum as spark_sum
LAKE_ROOT=os.getenv("DATALAKE_ROOT","/tmp/datalake")
CURATED_PATH=f"{LAKE_ROOT}/curated/domain=iot"
def _spark():
    s=SparkSession.builder.appName("AeroSenseApiLakeQueries").master("local[*]").config("spark.sql.shuffle.partitions","3").getOrCreate(); s.sparkContext.setLogLevel("WARN"); return s
def sensor_types():
    try:
        s=_spark(); df=s.read.parquet(CURATED_PATH); out=[r["sensor_type"] for r in df.select("sensor_type").distinct().collect()]; s.stop(); return sorted(out)
    except Exception: return ["humidity","pressure","temperature"]
def daily_stats(sensor_type, days):
    s=_spark()
    try:
        cutoff=datetime.now(timezone.utc)-timedelta(days=days)
        df=s.read.parquet(CURATED_PATH).filter(col("sensor_type")==sensor_type).filter(col("event_time")>=cutoff)
        res=(df.groupBy("sensor_type","year","month","day").agg(count("value").alias("record_count"),avg("value").alias("avg_value"),spark_min("value").alias("min_value"),spark_max("value").alias("max_value"),spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count")).orderBy("year","month","day"))
        return [r.asDict(recursive=True) for r in res.collect()]
    finally: s.stop()
def recent_anomalies(sensor_type=None, limit=20):
    s=_spark()
    try:
        df=s.read.parquet(CURATED_PATH).filter(col("is_anomaly")==True)
        if sensor_type: df=df.filter(col("sensor_type")==sensor_type)
        return [r.asDict(recursive=True) for r in df.orderBy(col("event_time").desc()).limit(limit).select("sensor_type","value","unit","event_time","source").collect()]
    finally: s.stop()
