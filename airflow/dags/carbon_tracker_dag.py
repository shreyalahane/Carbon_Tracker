from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pymysql
import os
from dotenv import load_dotenv
from pathlib import Path
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

load_dotenv('/opt/airflow/myfile.env')

default_args = {
    'owner': 'carbon_tracker',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 8, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'carbon_tracker_nightly',
    default_args=default_args,
    description='Nightly carbon emission pipeline',
    schedule_interval='0 23 * * *',
    catchup=False,
    tags=['carbon', 'ml', 'emissions']
)

def fetch_data():
    import requests
    import json
    from kafka import KafkaProducer
    from datetime import datetime

    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS',
                                    'kafka_carbon:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0)
    )

    # fetch weather
    url = "https://api.open-meteo.com/v1/forecast?latitude=19.07&longitude=72.87&daily=temperature_2m_mean,precipitation_sum,windspeed_10m_max,shortwave_radiation_sum&timezone=Asia/Kolkata"
    data = requests.get(url).json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('weather_data', data)

    # fetch air quality
    url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=19.07&longitude=72.87&hourly=pm2_5,nitrogen_dioxide,carbon_monoxide&timezone=Asia/Kolkata"
    data = requests.get(url).json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('air_quality_data', data)

    # fetch carbon intensity
    url = "https://api.carbonintensity.org.uk/intensity"
    data = requests.get(url, headers={'Accept': 'application/json'}).json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('carbon_intensity_data', data)

    producer.flush()
    print("All data fetched and sent to Kafka")

def retrain_model():
    import sys
    sys.path.insert(0, '/opt/airflow')
    os.environ['MLFLOW_TRACKING_URI'] = os.getenv(
        'MLFLOW_TRACKING_URI', 'http://mlflow_carbon:5000')
    from ml.train_model import nightly_retrain
    nightly_retrain()

def cleanup_old_data():
    conn = pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )
    cursor = conn.cursor()

    cursor.execute("""
        DELETE FROM weather_data
        WHERE date < CURDATE() - INTERVAL 90 DAY
    """)
    cursor.execute("""
        DELETE FROM air_quality_data
        WHERE date < CURDATE() - INTERVAL 90 DAY
    """)
    cursor.execute("""
        DELETE FROM carbon_intensity_data
        WHERE date < CURDATE() - INTERVAL 90 DAY
    """)
    cursor.execute("""
        DELETE FROM emissions
        WHERE date < CURDATE() - INTERVAL 90 DAY
    """)
    cursor.execute("""
        DELETE FROM predictions
        WHERE date < CURDATE() - INTERVAL 90 DAY
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Old data cleaned successfully")

def log_pipeline_status():
    import pymongo
    from datetime import datetime
    client = pymongo.MongoClient(
        os.getenv('MONGO_URI', 'mongodb://mongodb_carbon:27017'))
    db = client['carbon_tracker']
    db.pipeline_logs.insert_one({
        "run_date": datetime.now(),
        "status": "success",
        "pipeline": "carbon_tracker_nightly"
    })
    client.close()
    print("Pipeline status logged to MongoDB")

# define tasks
task_fetch = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

# The nightly pipeline runs the Spark batch job inside the pyspark_carbon
# container. Override SPARK_RUN_COMMAND for other environments (e.g. a
# spark-submit or a different container name).
SPARK_RUN_COMMAND = os.getenv(
    "SPARK_RUN_COMMAND",
    "docker exec pyspark_carbon python /app/processing/pyspark_batch.py",
)

task_process = BashOperator(
    task_id='process_data',
    bash_command=SPARK_RUN_COMMAND,
    dag=dag
)

task_retrain = PythonOperator(
    task_id='retrain_model',
    python_callable=retrain_model,
    dag=dag
)

task_cleanup = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

task_log = PythonOperator(
    task_id='log_pipeline_status',
    python_callable=log_pipeline_status,
    dag=dag
)

# set task dependencies
task_fetch >> task_process >> task_retrain >> task_cleanup >> task_log