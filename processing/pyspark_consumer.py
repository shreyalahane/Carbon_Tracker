import os

if os.name == "nt":
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += ";C:\\hadoop\\bin"

from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from quality import (
    process_weather_message,
    process_air_quality_message,
    process_carbon_intensity_message,
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "myfile.env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka_carbon:9092")
DEFAULT_CHECKPOINT = "checkpoint" if os.name == "nt" else "/tmp/checkpoint"
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT)

spark = SparkSession.builder \
    .appName("CarbonTracker") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33"
    ) \
    .config(
        "spark.sql.streaming.checkpointLocation",
        CHECKPOINT_DIR
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully")

# ── WEATHER ─────────────────────────────────────────────────────

def process_weather():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "weather_data") \
        .option("startingOffsets", "earliest") \
        .load()

    weather_df = df.selectExpr("CAST(value AS STRING) as json_str")

    def save_weather(batch_df, batch_id):
        import json
        for row in batch_df.collect():
            data = json.loads(row.json_str)
            process_weather_message(data, batch_id)

    weather_df.writeStream \
        .foreachBatch(save_weather) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/weather") \
        .start()

# ── AIR QUALITY ──────────────────────────────────────────────────

def process_air_quality():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "air_quality_data") \
        .option("startingOffsets", "earliest") \
        .load()

    air_df = df.selectExpr("CAST(value AS STRING) as json_str")

    def save_air_quality(batch_df, batch_id):
        import json
        for row in batch_df.collect():
            data = json.loads(row.json_str)
            process_air_quality_message(data, batch_id)

    air_df.writeStream \
        .foreachBatch(save_air_quality) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/air") \
        .start()

# ── CARBON INTENSITY ─────────────────────────────────────────────

def process_carbon_intensity():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "carbon_intensity_data") \
        .option("startingOffsets", "earliest") \
        .load()

    carbon_df = df.selectExpr("CAST(value AS STRING) as json_str")

    def save_carbon_intensity(batch_df, batch_id):
        import json
        for row in batch_df.collect():
            data = json.loads(row.json_str)
            process_carbon_intensity_message(data, batch_id)

    carbon_df.writeStream \
        .foreachBatch(save_carbon_intensity) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/carbon") \
        .start()

# ── MAIN ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    process_weather()
    process_air_quality()
    process_carbon_intensity()
    spark.streams.awaitAnyTermination()
