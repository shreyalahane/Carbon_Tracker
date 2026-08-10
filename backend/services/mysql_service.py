"""MySQL data access layer for the FastAPI backend.

Centralizes every query the API exposes so routers stay thin.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import date, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / "myfile.env")


def get_connection():
    import pymysql
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE'),
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8mb4'
    )


def _fetch_all(sql, params=None):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        conn.close()


def _fetch_one(sql, params=None):
    rows = _fetch_all(sql, params)
    return rows[0] if rows else None


def _serialize(row):
    if not row:
        return row
    return {k: (v.isoformat() if isinstance(v, (date, timedelta)) else v)
            for k, v in row.items()}


def emissions_today():
    row = _fetch_one(
        "SELECT * FROM emissions ORDER BY date DESC, created_at DESC LIMIT 1")
    return _serialize(row)


def emissions_yesterday():
    row = _fetch_one(
        "SELECT * FROM emissions ORDER BY date DESC, created_at DESC "
        "LIMIT 1 OFFSET 1")
    return _serialize(row)


def emissions_history(days=30):
    rows = _fetch_all(
        "SELECT date, total_co2_kg, carbon_intensity, pm2_5, "
        "nitrogen_dioxide, temperature_mean "
        "FROM emissions ORDER BY date DESC LIMIT %s",
        (days,))
    return [_serialize(r) for r in reversed(rows)]


def prediction_tomorrow():
    return _serialize(_fetch_one(
        "SELECT * FROM predictions "
        "WHERE date >= CURDATE() "
        "ORDER BY date ASC LIMIT 1"))


def prediction_latest():
    return _serialize(_fetch_one(
        "SELECT * FROM predictions ORDER BY date DESC LIMIT 1"))


def predictions_weekly(days=7):
    rows = _fetch_all(
        "SELECT * FROM predictions ORDER BY date DESC LIMIT %s",
        (days,))
    return [_serialize(r) for r in reversed(rows)]


def carbon_latest():
    return _serialize(_fetch_one(
        "SELECT * FROM carbon_intensity_data "
        "ORDER BY fetched_at DESC LIMIT 1"))


def weather_latest():
    return _serialize(_fetch_one(
        "SELECT * FROM weather_data ORDER BY date DESC LIMIT 1"))
