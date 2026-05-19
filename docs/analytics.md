# Analytics Results

This document reports the results of the Spark SQL analytical queries executed on the curated Parquet data lake.

The analytical script reads the curated zone from:

```text
/tmp/datalake/curated/domain=iot
```

The results were also exported as CSV files under:

```text
outputs/analytics/
```

## 1. Top 5 Hours with the Highest Number of Anomalies

Query objective: find the hours with the highest number of detected anomalies across all sensor types.

```text
+----+-----+---+----+-------------+------------+
|year|month|day|hour|anomaly_count|total_events|
+----+-----+---+----+-------------+------------+
|2026|5    |19 |8   |167          |1000        |
+----+-----+---+----+-------------+------------+
```

The highest anomaly concentration occurred on 2026-05-19 at 08:00. During this hour, the platform processed 1000 events and detected 167 anomalies, an anomaly rate of 16.7%.

## 2. Sensor-Level Global Statistics

```text
+-----------+-----------+---------+---------+------------+----------------+-------------+
|sensor_type|global_mean|min_value|max_value|stddev_value|anomaly_rate_pct|total_records|
+-----------+-----------+---------+---------+------------+----------------+-------------+
|humidity   |66.107     |30.59    |94.94    |19.875      |16.23           |345          |
|pressure   |1009.018   |980.03   |1038.22  |15.204      |17.97           |306          |
|temperature|26.967     |15.06    |44.62    |7.77        |16.05           |349          |
+-----------+-----------+---------+---------+------------+----------------+-------------+
```

The three expected sensor types were correctly ingested and stored in the curated zone. The total number of curated records is 1000. Pressure has the highest anomaly rate at 17.97%.

## 3. Daily Evolution for the Temperature Sensor

The query executed successfully and exported a CSV result. The temperature daily result contains 349 temperature records and 56 anomalies on 2026-05-19.

## 4. Partition Pruning Demonstration

```text
full_count = 1000
full_time = 0.2317s
pruned_count = 349
pruned_time = 0.1036s
speedup = 2.24x
```

The filtered query using `sensor_type='temperature'` was faster than the full scan. Spark's physical plan showed `PartitionFilters`, validating the partitioning strategy by `sensor_type`, `year`, `month`, and `day`.

## Conclusion

The Spark SQL analytical layer successfully queried the curated Parquet data lake and produced anomaly summaries, per-sensor statistics, temperature daily evolution, and a quantified partition pruning comparison.