from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    when,
)

INPUT_PATH = "output/processed/sensor_events"
OUTPUT_PATH = "output/analytics"

spark = (
    SparkSession.builder
    .appName("IoTSensorAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read the processed sensor records
df = spark.read.json(INPUT_PATH)

print("\n========== PROCESSED DATA ==========")
df.select(
    "sensor_id",
    "temperature",
    "humidity",
    "temperature_alert",
).show(10, truncate=False)

# Overall alert and temperature summary
alert_summary = df.agg(
    count("*").alias("total_readings"),
    spark_sum(
        when(df.temperature_alert == "WARNING", 1).otherwise(0)
    ).alias("warning_readings"),
    spark_sum(
        when(df.temperature_alert == "NORMAL", 1).otherwise(0)
    ).alias("normal_readings"),
    spark_round(avg("temperature"), 2).alias("average_temperature"),
    spark_round(spark_max("temperature"), 2).alias("maximum_temperature"),
)

print("\n========== OVERALL SENSOR SUMMARY ==========")
alert_summary.show(truncate=False)

alert_summary.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(f"{OUTPUT_PATH}/alert_summary")

# Per-sensor statistics
sensor_summary = (
    df.groupBy("sensor_id")
    .agg(
        count("*").alias("total_readings"),
        spark_round(avg("temperature"), 2).alias("average_temperature"),
        spark_round(spark_max("temperature"), 2).alias("maximum_temperature"),
        spark_sum(
            when(df.temperature_alert == "WARNING", 1).otherwise(0)
        ).alias("warning_count"),
    )
    .orderBy("sensor_id")
)

print("\n========== SENSOR-WISE SUMMARY ==========")
sensor_summary.show(20, truncate=False)

sensor_summary.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(f"{OUTPUT_PATH}/sensor_summary")

print(f"\nAnalytics saved under: {OUTPUT_PATH}")

spark.stop()
