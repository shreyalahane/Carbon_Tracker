import requests
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv("myfile.env")

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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

if __name__ == "__main__":
    fetch_weather()
    fetch_air_quality()
    fetch_carbon_intensity()
    producer.flush()
    print("All data sent successfully")