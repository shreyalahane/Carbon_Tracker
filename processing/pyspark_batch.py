"""Batch Kafka consumer used by Airflow's nightly DAG.

Reads whatever is currently on each topic (earliest -> latest), processes
each message once, then terminates. Dedup relies on date_exists / INSERT
IGNORE so re-running is idempotent.
"""

import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'

import json
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from quality import (
    process_weather_message,
    process_air_quality_message,
    process_carbon_intensity_message,
)

# -----------------------
# Environment Detection
# -----------------------
if Path("/app").exists():
    # Docker
    load_dotenv("/app/myfile.env")
    KAFKA_BOOTSTRAP = "kafka_carbon:9092"
else:
    # Windows
    load_dotenv("myfile.env")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                                "localhost:29092")

spark = SparkSession.builder \
    .appName("CarbonTrackerBatch") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark batch session created successfully")


def process_topic(topic, message_fn):
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    rows = df.selectExpr("CAST(value AS STRING) as json_str").collect()
    print(f"Kafka topic {topic}: {len(rows)} message(s) to process")

    for row in rows:
        data = json.loads(row.json_str)
        message_fn(data, 0)

    print(f"Kafka topic {topic}: done")


if __name__ == "__main__":
    process_topic("weather_data", process_weather_message)
    process_topic("air_quality_data", process_air_quality_message)
    process_topic("carbon_intensity_data",
                  process_carbon_intensity_message)
    spark.stop()
    print("Batch processing complete")
