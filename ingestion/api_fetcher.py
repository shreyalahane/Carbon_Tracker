"""This module fetches weather, air quality, and carbon intensity data
from external APIs and publishes the responses to Kafka topics.
It also schedules automatic data collection using APScheduler."""

import requests
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / "myfile.env"

load_dotenv(ENV_FILE)

print("Loading:", ENV_FILE)
print("KAFKA:", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
print("MYSQL:", os.getenv("MYSQL_HOST"))

# Initialize the Kafka producer responsible for publishing API responses
# to the configured Kafka topics using JSON serialization.
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 5, 0)
)

def fetch_weather():
    url = f"{os.getenv('WEATHER_API_URL')}?latitude=19.07&longitude=72.87&daily=temperature_2m_mean,precipitation_sum,windspeed_10m_max,shortwave_radiation_sum&timezone=Asia/Kolkata"
    response = requests.get(url)
    data = response.json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('weather_data', data)
    print("Weather data sent to Kafka")

def fetch_air_quality():
    url = f"{os.getenv('AIR_QUALITY_API_URL')}?latitude=19.07&longitude=72.87&hourly=pm2_5,nitrogen_dioxide,carbon_monoxide&timezone=Asia/Kolkata"
    response = requests.get(url)
    data = response.json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('air_quality_data', data)
    print("Air quality data sent to Kafka")

def fetch_carbon_intensity():
    url = os.getenv('CARBON_INTENSITY_API_URL')
    response = requests.get(url, headers={'Accept': 'application/json'})
    data = response.json()
    data['fetched_at'] = datetime.now().isoformat()
    producer.send('carbon_intensity_data', data)
    print("Carbon intensity data sent to Kafka")

scheduler = BlockingScheduler()

@scheduler.scheduled_job('cron', hour=6, minute=0)
def scheduled_fetch():
    fetch_weather()
    fetch_air_quality()
    fetch_carbon_intensity()
    producer.flush()
    print("Scheduled fetch complete")

if __name__ == "__main__":
    fetch_weather()
    fetch_air_quality()
    fetch_carbon_intensity()
    producer.flush()
    print("All data sent successfully")
    scheduler.start()


