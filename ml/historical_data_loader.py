import requests
import pymysql
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "myfile.env")

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )

def load_historical_weather():
    print("Loading historical weather data...")
    
    today = datetime.now() - timedelta(days=1)

    end_date = today.strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=365)).strftime('%Y-%m-%d')
    
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude=19.07&longitude=72.87"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=temperature_2m_mean,precipitation_sum,"
        f"windspeed_10m_max,shortwave_radiation_sum"
        f"&timezone=Asia/Kolkata"
    )
    
    response = requests.get(url)
    print(response.status_code)
    print(response.text[:500])
    data = response.json()
    daily = data.get("daily", {})
    print(daily.keys())
    print("Dates:", len(daily.get("time", [])))
    times = daily.get('time', [])
    temps = daily.get('temperature_2m_mean', [])
    precips = daily.get('precipitation_sum', [])
    winds = daily.get('windspeed_10m_max', [])
    solar = daily.get('shortwave_radiation_sum', [])
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    inserted = skipped = 0
    
    for i in range(len(times)):
        temp = temps[i] if temps[i] is not None else 0.0
        precip = precips[i] if precips[i] is not None else 0.0
        wind = winds[i] if winds[i] is not None else 0.0
        sol = solar[i] if solar[i] is not None else 0.0
        
        cursor.execute("""
            SELECT COUNT(*) FROM weather_data WHERE date = %s
        """, (times[i],))
        
        if cursor.fetchone()[0] > 0:
            skipped += 1
            continue
            
        cursor.execute("""
            INSERT INTO weather_data
            (date, temperature_mean, precipitation,
             wind_speed, solar_radiation, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (times[i], temp, precip, wind, sol, datetime.now()))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Weather: inserted={inserted} skipped={skipped}")

def load_historical_air_quality():
    print("Loading historical air quality data...")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude=19.07&longitude=72.87"
        f"&hourly=pm2_5,nitrogen_dioxide,carbon_monoxide"
        f"&start_date={start_date}&end_date={end_date}"
        f"&timezone=Asia/Kolkata"
    )
    
    response = requests.get(url)
    data = response.json()
    hourly = data.get('hourly', {})
    
    times = hourly.get('time', [])
    pm25 = hourly.get('pm2_5', [])
    no2 = hourly.get('nitrogen_dioxide', [])
    co = hourly.get('carbon_monoxide', [])
    
    # group by day and average
    daily_data = {}
    for i in range(len(times)):
        date = times[i].split('T')[0]
        if date not in daily_data:
            daily_data[date] = {'pm25': [], 'no2': [], 'co': []}
        
        if pm25[i] is not None:
            daily_data[date]['pm25'].append(pm25[i])
        if no2[i] is not None:
            daily_data[date]['no2'].append(no2[i])
        if co[i] is not None:
            daily_data[date]['co'].append(co[i])
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    inserted = skipped = 0
    
    import numpy as np
    for date, values in daily_data.items():
        cursor.execute("""
            SELECT COUNT(*) FROM air_quality_data WHERE date = %s
        """, (date,))
        
        if cursor.fetchone()[0] > 0:
            skipped += 1
            continue
        
        avg_pm25 = round(float(np.mean(values['pm25'])), 4) \
            if values['pm25'] else 0.0
        avg_no2 = round(float(np.mean(values['no2'])), 4) \
            if values['no2'] else 0.0
        avg_co = round(float(np.mean(values['co'])), 4) \
            if values['co'] else 0.0
        
        cursor.execute("""
            INSERT INTO air_quality_data
            (date, pm2_5, nitrogen_dioxide,
             carbon_monoxide, fetched_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (date, avg_pm25, avg_no2, avg_co, datetime.now()))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Air quality: inserted={inserted} skipped={skipped}")

def load_historical_carbon_intensity():
    print("Loading historical carbon intensity data...")
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    # generate realistic carbon intensity values
    # based on UK average patterns
    # since historical API needs paid access
    import numpy as np
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    current = start_date
    inserted = skipped = 0
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT COUNT(*) FROM carbon_intensity_data 
            WHERE date = %s
        """, (date_str,))
        
        if cursor.fetchone()[0] > 0:
            skipped += 1
            current += timedelta(days=1)
            continue
        
        # realistic carbon intensity based on:
        # season (winter higher, summer lower)
        # day of week (weekend lower)
        month = current.month
        dow = current.weekday()
        
        # base intensity (UK average ~200)
        base = 200
        
        # seasonal variation
        if month in [12, 1, 2]:
            seasonal = 50   # winter high
        elif month in [6, 7, 8]:
            seasonal = -40  # summer low (more solar)
        else:
            seasonal = 0
        
        # weekend lower (less industrial)
        weekend_effect = -30 if dow >= 5 else 0
        
        # random variation
        noise = np.random.normal(0, 20)
        
        actual = max(50, int(base + seasonal + weekend_effect + noise))
        forecast = max(50, int(actual + np.random.normal(0, 10)))
        
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
        
        cursor.execute("""
            INSERT INTO carbon_intensity_data
            (date, carbon_intensity, fossil_fuel_percentage,
             index_value, fetched_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (date_str, actual, forecast, index, datetime.now()))
        inserted += 1
        current += timedelta(days=1)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Carbon intensity: inserted={inserted} skipped={skipped}")

if __name__ == "__main__":
    print("=" * 50)
    print("Loading Historical Data")
    print("=" * 50)
    
    load_historical_weather()
    load_historical_air_quality()
    load_historical_carbon_intensity()
    
    print("\nHistorical data loading complete!")
    print("Now run: python ml/train_model.py")