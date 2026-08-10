"""Thin HTTP client for the Carbon Tracker FastAPI backend.

Every call returns parsed JSON or None on failure so the Streamlit pages
can degrade gracefully when the API is unreachable.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

import requests

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / "myfile.env")

BASE_URL = os.getenv("STREAMLIT_API_URL", "http://localhost:8000").rstrip("/")

TIMEOUT = 10


def _get(path, params=None):
    try:
        response = requests.get(f"{BASE_URL}{path}",
                                params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"API GET {path} failed: {e}")
        return None


def health():
    return _get("/health")


def emissions_today():
    return _get("/emissions/today")


def emissions_history(days=30):
    return _get("/emissions/history", {"days": days})


def prediction_tomorrow():
    return _get("/predictions/tomorrow")


def predictions_weekly(days=7):
    return _get("/predictions/weekly", {"days": days})


def agent_advice():
    return _get("/agents/advice")


def agent_plan():
    return _get("/agents/plan")


def agent_report():
    return _get("/agents/report")


def model_performance():
    return _get("/model/performance")


def report_pdf_url():
    return f"{BASE_URL}/agents/report/pdf"


def report_pdf_bytes():
    try:
        response = requests.get(f"{BASE_URL}/agents/report/pdf",
                                timeout=TIMEOUT)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"API GET /agents/report/pdf failed: {e}")
        return None
