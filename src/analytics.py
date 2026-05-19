import os, time
from pathlib import Path
from pyspark.sql import SparkSession
LAKE_ROOT = os.getenv("DATALAKE_ROOT", "/tmp/datalake")
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
OUT = Path("outputs/analytics"); OUT.mkdir(parents=True, exist_ok=True)

def save(df, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUT / name))
    print(f"CSV written: {OUT/name}")

def main():
    spark = SparkSession.builder.appName("AeroSenseAnalytics").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(CURATED_PATH); df.createOrReplaceTempView("sensor_events")
    q1 = spark.sql("""
        SELECT year, month, day, HOUR(event_time) AS hour,
               SUM(CAST(is_anomaly AS INT)) AS anomaly_count, COUNT(*) AS total_events
        FROM sensor_events GROUP BY year, month, day, HOUR(event_time)
        ORDER BY anomaly_count DESC, total_events DESC LIMIT 5
    """); print("Q1 top anomaly hours"); q1.show(truncate=False); save(q1, "q1_top_anomaly_hours")
    q2 = spark.sql("""
        SELECT sensor_type, ROUND(AVG(value),3) AS global_mean, ROUND(MIN(value),3) AS min_value,
               ROUND(MAX(value),3) AS max_value, ROUND(STDDEV(value),3) AS stddev_value,
               ROUND(100.0 * SUM(CAST(is_anomaly AS INT)) / COUNT(*), 2) AS anomaly_rate_pct,
               COUNT(*) AS total_records
        FROM sensor_events GROUP BY sensor_type ORDER BY sensor_type
    """); print("Q2 sensor stats"); q2.show(truncate=False); save(q2, "q2_sensor_statistics")
    q3 = spark.sql("""
        SELECT year, month, day, ROUND(AVG(value),3) AS temperature_mean,
               SUM(CAST(is_anomaly AS INT)) AS anomaly_count, COUNT(*) AS total_temperature_records
        FROM sensor_events WHERE sensor_type='temperature'
        GROUP BY year, month, day ORDER BY year, month, day
    """); print("Q3 temperature daily evolution"); q3.show(truncate=False); save(q3, "q3_temperature_daily_evolution")
    start=time.time(); full=spark.sql("SELECT COUNT(*) AS n FROM sensor_events").collect()[0]["n"]; ft=time.time()-start
    start=time.time(); pruned=spark.sql("SELECT COUNT(*) AS n FROM sensor_events WHERE sensor_type='temperature'").collect()[0]["n"]; pt=time.time()-start
    speed=ft/pt if pt>0 else 0.0
    print(f"Q4 partition pruning: full_count={full}, full_time={ft:.4f}s, pruned_count={pruned}, pruned_time={pt:.4f}s, speedup={speed:.2f}x")
    q4=spark.createDataFrame([(full,ft,pruned,pt,float(speed))],"full_count long, full_time_s double, pruned_count long, pruned_time_s double, speedup double")
    save(q4,"q4_partition_pruning")
    spark.sql("SELECT COUNT(*) FROM sensor_events WHERE sensor_type='temperature'").explain(True)
    spark.stop()
if __name__ == "__main__": main()
