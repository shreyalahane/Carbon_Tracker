"""Carbon Tracker Dashboard - entry point.

Run with:
    streamlit run frontend/app.py

The pages/ directory auto-registers the rest of the dashboard in the
sidebar (Dashboard, Predictions, Trends, Recommendations, Reports).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st

from utils import api_client

st.set_page_config(
    page_title="Carbon Tracker",
    page_icon="🌍",
    layout="wide",
)

st.title("🌍 Carbon Tracker")
st.markdown(
    "Automated carbon emission tracking, prediction, and ESG reporting "
    "powered by Kafka, PySpark, XGBoost, and Claude.")

health = api_client.health()
if health and health.get("status") == "ok":
    st.success("API connected - backend is healthy.")
else:
    st.warning("API is unreachable. Start it with "
               "`uvicorn backend.main:app --reload --port 8000`.")

st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("### 📊 Today")
    data = api_client.emissions_today()
    if data and data.get("today"):
        today = data["today"]
        st.metric("Total CO2 (kg)", today.get("total_co2_kg"))
        change = data.get("change_pct")
        st.metric("vs Yesterday", f"{change}%" if change is not None else "—")
    else:
        st.info("No data yet")
with col2:
    st.markdown("### 🔮 Prediction")
    pred = api_client.prediction_tomorrow()
    if pred:
        st.metric("Tomorrow (kg)", pred.get("predicted_co2_kg"))
        st.metric("Risk", pred.get("risk_level"))
    else:
        st.info("No prediction yet")
with col3:
    st.markdown("### ⚙️ Model")
    perf = api_client.model_performance()
    if perf:
        st.metric("Version", perf.get("version"))
        st.metric("R²", perf.get("performance", {}).get("R2"))
    else:
        st.info("No model info")
with col4:
    st.markdown("### 🧠 Agents")
    advice = api_client.agent_advice()
    if advice:
        st.metric("Recommendations", len(advice.get("recommendations", [])))
        st.metric("Source", advice.get("source"))
    else:
        st.info("Run agents")

st.divider()
st.markdown(
    "Use the **sidebar** to explore: **Dashboard**, **Predictions**, "
    "**Trends**, **Recommendations**, and **ESG Reports**.")
