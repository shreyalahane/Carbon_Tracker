import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'

from pyspark.sql import SparkSession
from dotenv import load_dotenv
os.makedirs("C:/tmp/checkpoint", exist_ok=True)

load_dotenv("/app/myfile.env")

spark = SparkSession.builder \
    .appName("CarbonTracker") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33")  \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")  \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark session created successfully")

MYSQL_URL = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
MYSQL_PROPERTIES = {
    "user": os.getenv('MYSQL_USER'),
    "password": os.getenv('MYSQL_PASSWORD'),
    "driver": "com.mysql.cj.jdbc.Driver"
}

def process_weather():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_carbon:9092") \
        .option("subscribe", "weather_data") \
        .option("startingOffsets", "earliest") \
        .load()

    weather_df = df.selectExpr("CAST(value AS STRING) as json_str")
    
    def save_weather(batch_df, batch_id):
        import json
        import pymysql
        from datetime import datetime
        
        for row in batch_df.collect():
            data = json.loads(row.json_str)
            daily = data.get('daily', {})
            times = daily.get('time', [])
            temps = daily.get('temperature_2m_mean', [])
            precips = daily.get('precipitation_sum', [])
            winds = daily.get('windspeed_10m_max', [])
            solar = daily.get('shortwave_radiation_sum', [])
            
            conn = pymysql.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
)
            cursor = conn.cursor()
            
            for i in range(len(times)):
                cursor.execute("""
                    INSERT IGNORE INTO weather_data 
                    (date, temperature_mean, precipitation, wind_speed, solar_radiation, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    times[i],
                    temps[i] if i < len(temps) else None,
                    precips[i] if i < len(precips) else None,
                    winds[i] if i < len(winds) else None,
                    solar[i] if i < len(solar) else None,
                    datetime.now()
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Weather batch {batch_id} saved to MySQL")
    
    weather_df.writeStream \
    .foreachBatch(save_weather) \
    .option("checkpointLocation", "/tmp/checkpoint/weather") \
    .start()

def process_air_quality():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_carbon:9092") \
        .option("subscribe", "air_quality_data") \
        .option("startingOffsets", "earliest") \
        .load()

    air_df = df.selectExpr("CAST(value AS STRING) as json_str")

    def save_air_quality(batch_df, batch_id):
        import json
        import pymysql
        from datetime import datetime

        for row in batch_df.collect():
            data = json.loads(row.json_str)
            hourly = data.get('hourly', {})
            times = hourly.get('time', [])
            pm25 = hourly.get('pm2_5', [])
            no2 = hourly.get('nitrogen_dioxide', [])
            co = hourly.get('carbon_monoxide', [])

            conn = pymysql.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
)
            cursor = conn.cursor()

            for i in range(len(times)):
                date = times[i].split('T')[0]
                cursor.execute("""
                    INSERT IGNORE INTO air_quality_data
                    (date, pm2_5, nitrogen_dioxide, carbon_monoxide, fetched_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    date,
                    pm25[i] if i < len(pm25) else None,
                    no2[i] if i < len(no2) else None,
                    co[i] if i < len(co) else None,
                    datetime.now()
                ))

            conn.commit()
            cursor.close()
            conn.close()
            print(f"Air quality batch {batch_id} saved to MySQL")

    air_df.writeStream \
    .foreachBatch(save_air_quality) \
    .option("checkpointLocation", "/tmp/checkpoint/air") \
    .start()

def process_carbon_intensity():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_carbon:9092") \
        .option("subscribe", "carbon_intensity_data") \
        .option("startingOffsets", "earliest") \
        .load()

    carbon_df = df.selectExpr("CAST(value AS STRING) as json_str")

    def save_carbon_intensity(batch_df, batch_id):
        import json
        import pymysql
        from datetime import datetime

        for row in batch_df.collect():
            data = json.loads(row.json_str)
            intensity_data = data.get('data', [{}])[0]
            intensity = intensity_data.get('intensity', {})

            conn = pymysql.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO carbon_intensity_data
                (date, carbon_intensity, fossil_fuel_percentage, index_value, fetched_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.now().date(),
                intensity.get('actual'),
                intensity.get('forecast'),
                intensity.get('index'),
                datetime.now()
            ))

            conn.commit()
            cursor.close()
            conn.close()
            print(f"Carbon intensity batch {batch_id} saved to MySQL")

    carbon_df.writeStream \
    .foreachBatch(save_carbon_intensity) \
    .option("checkpointLocation", "/tmp/checkpoint/carbon") \
    .start()

if __name__ == "__main__":
    process_weather()
    process_air_quality()
    process_carbon_intensity()
    
    spark.streams.awaitAnyTermination()