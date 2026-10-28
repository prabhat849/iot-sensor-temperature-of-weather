from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-data"

OUTPUT_PATH = "output/processed/sensor_events"
CHECKPOINT_PATH = "output/checkpoint_iot"

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("machine_status", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("IoTSensorDataPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Starting IoT Sensor Data Processing Pipeline...")
print(f"Kafka topic: {KAFKA_TOPIC}")
print("Temperature warning threshold: 80 C")

# Read events from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka's binary message into JSON columns
json_stream = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

# Validate records and classify temperatures
processed_stream = (
    json_stream
    .filter(
        col("sensor_id").isNotNull()
        & col("temperature").isNotNull()
        & col("humidity").isNotNull()
    )
    .withColumn(
        "temperature_alert",
        when(col("temperature") > 80, "WARNING")
        .otherwise("NORMAL")
    )
    .withColumn("processed_at", current_timestamp())
)

# Write processed events as JSON files
query = (
    processed_stream.writeStream
    .format("json")
    .outputMode("append")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print(f"Processed output directory: {OUTPUT_PATH}")
print("Streaming is running. Press Ctrl+C to stop when ready.")

query.awaitTermination()
