import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv("/app/myfile.env")

spark = SparkSession.builder \
    .appName("CarbonTracker") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,mysql:mysql-connector-java:8.0.33") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully")

def get_mysql_connection():
    import pymysql
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )

def get_mongo_db():
    import pymongo
    client = pymongo.MongoClient("mongodb://mongodb_carbon:27017")
    return client, client[os.getenv('MONGO_DATABASE')]

def log_quality_issue(table, field, issue, value, batch_id):
    try:
        client, db = get_mongo_db()
        from datetime import datetime
        db.data_quality_logs.insert_one({
            "table": table,
            "field": field,
            "issue": issue,
            "original_value": str(value),
            "batch_id": int(batch_id),
            "timestamp": datetime.now()
        })
        client.close()
    except Exception as e:
        print(f"Quality log error: {e}")

def save_raw_to_mongodb(collection, data, batch_id):
    try:
        from datetime import datetime
        client, db = get_mongo_db()
        data['batch_id'] = int(batch_id)
        data['saved_at'] = datetime.now()
        db[collection].insert_one(data)
        client.close()
    except Exception as e:
        print(f"MongoDB save error: {e}")

def clean_numeric(value, field, min_val, max_val,
                  default=0.0, batch_id=0, table=""):
    if value is None:
        log_quality_issue(table, field, "NULL_VALUE",
                         value, batch_id)
        return default
    try:
        value = float(value)
    except (TypeError, ValueError):
        log_quality_issue(table, field, "INVALID_TYPE",
                         value, batch_id)
        return default
    if value < min_val or value > max_val:
        log_quality_issue(table, field,
                         f"OUT_OF_RANGE_{value}",
                         value, batch_id)
        return default
    return round(value, 4)

def detect_outlier_zscore(values, threshold=3.0):
    import numpy as np
    clean = [v for v in values if v is not None and v != 0.0]
    if len(clean) < 3:
        return [False] * len(values)
    mean = np.mean(clean)
    std = np.std(clean)
    if std == 0:
        return [False] * len(values)
    result = []
    for v in values:
        if v is None:
            result.append(False)
        else:
            result.append(abs((v - mean) / std) > threshold)
    return result

def forward_fill(values):
    filled = []
    last_good = None
    for v in values:
        if v is not None:
            last_good = v
            filled.append(v)
        else:
            filled.append(last_good)
    return filled

def validate_date(date_str):
    if not date_str:
        return False
    try:
        from datetime import datetime
        datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        return True
    except ValueError:
        return False

def date_exists(cursor, table, date_val):
    cursor.execute(
        f"SELECT COUNT(*) FROM {table} WHERE date = %s",
        (date_val,))
    return cursor.fetchone()[0] > 0

# ── WEATHER ─────────────────────────────────────────────────────

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
        import numpy as np
        from datetime import datetime

        for row in batch_df.collect():
            data = json.loads(row.json_str)

            # save raw to MongoDB first
            save_raw_to_mongodb('raw_weather', 
                              dict(data), batch_id)

            daily = data.get('daily', {})
            times = daily.get('time', [])
            temps = daily.get('temperature_2m_mean', [])
            precips = daily.get('precipitation_sum', [])
            winds = daily.get('windspeed_10m_max', [])
            solar = daily.get('shortwave_radiation_sum', [])

            if not times:
                print(f"Weather batch {batch_id}: empty")
                continue

            # forward fill before any processing
            temps = forward_fill(temps)
            precips = forward_fill(precips)
            winds = forward_fill(winds)
            solar = forward_fill(solar)

            # replace remaining None with 0
            temps = [t if t is not None else 0.0 
                    for t in temps]
            precips = [p if p is not None else 0.0 
                      for p in precips]
            winds = [w if w is not None else 0.0 
                    for w in winds]
            solar = [s if s is not None else 0.0 
                    for s in solar]

            # outlier detection
            temp_outliers = detect_outlier_zscore(temps)
            wind_outliers = detect_outlier_zscore(winds)
            temp_mean = round(float(np.mean(temps)), 4)
            wind_mean = round(float(np.mean(winds)), 4)

            conn = get_mysql_connection()
            cursor = conn.cursor()
            inserted = skipped = cleaned = 0

            for i in range(len(times)):
                if not validate_date(times[i]):
                    log_quality_issue("weather_data", "date",
                                    "INVALID_DATE",
                                    times[i], batch_id)
                    skipped += 1
                    continue

                if date_exists(cursor, "weather_data", times[i]):
                    skipped += 1
                    continue

                temp = clean_numeric(
                    temps[i], "temperature_mean",
                    -50, 60, 0.0, batch_id, "weather_data")
                precip = clean_numeric(
                    precips[i], "precipitation",
                    0, 500, 0.0, batch_id, "weather_data")
                wind = clean_numeric(
                    winds[i], "wind_speed",
                    0, 300, 0.0, batch_id, "weather_data")
                sol = clean_numeric(
                    solar[i], "solar_radiation",
                    0, 100, 0.0, batch_id, "weather_data")

                # replace outliers with mean
                if i < len(temp_outliers) and temp_outliers[i]:
                    temp = temp_mean
                    cleaned += 1
                if i < len(wind_outliers) and wind_outliers[i]:
                    wind = wind_mean
                    cleaned += 1

                cursor.execute("""
                    INSERT IGNORE INTO weather_data
                    (date, temperature_mean, precipitation,
                     wind_speed, solar_radiation, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (times[i], temp, precip, 
                      wind, sol, datetime.now()))
                inserted += 1

            conn.commit()
            cursor.close()
            conn.close()
            print(f"Weather batch {batch_id}: "
                  f"inserted={inserted} "
                  f"skipped={skipped} "
                  f"cleaned={cleaned}")

    weather_df.writeStream \
        .foreachBatch(save_weather) \
        .option("checkpointLocation", "/tmp/checkpoint/weather") \
        .start()

# ── AIR QUALITY ──────────────────────────────────────────────────

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
        import numpy as np
        from datetime import datetime

        for row in batch_df.collect():
            data = json.loads(row.json_str)

            # save raw to MongoDB
            save_raw_to_mongodb('raw_air_quality',
                              dict(data), batch_id)

            hourly = data.get('hourly', {})
            times = hourly.get('time', [])
            pm25 = hourly.get('pm2_5', [])
            no2 = hourly.get('nitrogen_dioxide', [])
            co = hourly.get('carbon_monoxide', [])

            if not times:
                print(f"Air quality batch {batch_id}: empty")
                continue

            # forward fill
            pm25 = forward_fill(pm25)
            no2 = forward_fill(no2)
            co = forward_fill(co)

            # replace remaining None with 0
            pm25 = [v if v is not None else 0.0 for v in pm25]
            no2 = [v if v is not None else 0.0 for v in no2]
            co = [v if v is not None else 0.0 for v in co]

            # group hourly into daily averages
            daily_data = {}
            for i in range(len(times)):
                if not validate_date(times[i]):
                    continue

                date = times[i].split('T')[0]
                if date not in daily_data:
                    daily_data[date] = {
                        'pm25': [], 'no2': [], 'co': []}

                pm25_val = clean_numeric(
                    pm25[i], "pm2_5",
                    0, 1000, None, batch_id, "air_quality_data")
                no2_val = clean_numeric(
                    no2[i], "nitrogen_dioxide",
                    0, 500, None, batch_id, "air_quality_data")
                co_val = clean_numeric(
                    co[i], "carbon_monoxide",
                    0, 10000, None, batch_id, "air_quality_data")

                if pm25_val is not None:
                    daily_data[date]['pm25'].append(pm25_val)
                if no2_val is not None:
                    daily_data[date]['no2'].append(no2_val)
                if co_val is not None:
                    daily_data[date]['co'].append(co_val)

            conn = get_mysql_connection()
            cursor = conn.cursor()
            inserted = skipped = 0

            for date, values in daily_data.items():
                if date_exists(cursor, 
                             "air_quality_data", date):
                    skipped += 1
                    continue

                # calculate daily average
                # only from valid non-zero values
                pm25_list = [v for v in values['pm25'] 
                            if v > 0]
                no2_list = [v for v in values['no2'] 
                           if v > 0]
                co_list = [v for v in values['co'] 
                          if v > 0]

                avg_pm25 = round(float(np.mean(pm25_list)), 4) \
                    if pm25_list else 0.0
                avg_no2 = round(float(np.mean(no2_list)), 4) \
                    if no2_list else 0.0
                avg_co = round(float(np.mean(co_list)), 4) \
                    if co_list else 0.0

                cursor.execute("""
                    INSERT IGNORE INTO air_quality_data
                    (date, pm2_5, nitrogen_dioxide,
                     carbon_monoxide, fetched_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (date, avg_pm25, avg_no2, 
                      avg_co, datetime.now()))
                inserted += 1

            conn.commit()
            cursor.close()
            conn.close()
            print(f"Air quality batch {batch_id}: "
                  f"inserted={inserted} "
                  f"skipped={skipped}")

    air_df.writeStream \
        .foreachBatch(save_air_quality) \
        .option("checkpointLocation", "/tmp/checkpoint/air") \
        .start()

# ── CARBON INTENSITY ─────────────────────────────────────────────

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
        from datetime import datetime

        for row in batch_df.collect():
            data = json.loads(row.json_str)

            # save raw to MongoDB
            save_raw_to_mongodb('raw_carbon_intensity',
                              dict(data), batch_id)

            intensity_data = data.get('data', [{}])[0]
            intensity = intensity_data.get('intensity', {})

            actual = intensity.get('actual')
            forecast = intensity.get('forecast')
            index = intensity.get('index')

            # clean actual
            actual = clean_numeric(
                actual, "carbon_intensity",
                0, 700, 0.0, batch_id,
                "carbon_intensity_data")

            # clean forecast
            forecast = clean_numeric(
                forecast, "fossil_fuel_percentage",
                0, 700, 0.0, batch_id,
                "carbon_intensity_data")

            # clean index
            valid_indexes = ['very low', 'low',
                           'moderate', 'high', 'very high']
            if index is None or index not in valid_indexes:
                log_quality_issue(
                    "carbon_intensity_data",
                    "index_value",
                    "NULL_OR_INVALID_INDEX",
                    index, batch_id)
                # derive index from actual value
                if actual <= 100:
                    index = 'very low'
                elif actual <= 200:
                    index = 'low'
                elif actual <= 300:
                    index = 'moderate'
                elif actual <= 400:
                    index = 'high'
                else:
                    index = 'very high'

            # skip completely empty records
            if actual == 0.0 and forecast == 0.0:
                log_quality_issue(
                    "carbon_intensity_data", "both",
                    "EMPTY_RECORD", "0,0", batch_id)
                print(f"Carbon batch {batch_id}: "
                      f"empty record skipped")
                continue

            conn = get_mysql_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO carbon_intensity_data
                (date, carbon_intensity,
                 fossil_fuel_percentage,
                 index_value, fetched_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (datetime.now().date(),
                  actual, forecast,
                  index, datetime.now()))

            conn.commit()
            cursor.close()
            conn.close()
            print(f"Carbon intensity batch {batch_id}: "
                  f"actual={actual} "
                  f"forecast={forecast} "
                  f"index={index} saved")

    carbon_df.writeStream \
        .foreachBatch(save_carbon_intensity) \
        .option("checkpointLocation", "/tmp/checkpoint/carbon") \
        .start()

# ── MAIN ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    process_weather()
    process_air_quality()
    process_carbon_intensity()
    spark.streams.awaitAnyTermination()