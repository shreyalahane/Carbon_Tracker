"""Home Dashboard: today's intensity, risk, and anomaly alerts."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from utils import api_client, ui

st.set_page_config(page_title="Dashboard", layout="wide")

ui.page_header("Home Dashboard",
               "Today's carbon intensity, risk level, and alerts")

data = api_client.emissions_today()

if not data or not data.get("today"):
    st.warning("No emission data available yet. "
               "Run the ingestion pipeline first.")
    st.stop()

today = data["today"]
yesterday = data.get("yesterday")
change = data.get("change_pct")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total CO2 today", f"{today.get('total_co2_kg', 0):.1f} kg")
with col2:
    st.markdown("#### Change vs yesterday")
    if change is not None:
        ui.metric_card("", f"{change:+.1f}%",
                       color="#e74c3c" if abs(change) > 15 else "#2ecc71")
    else:
        st.metric("Change vs yesterday", "—")
with col3:
    ci = today.get("carbon_intensity")
    if ci is not None:
        st.metric("Carbon intensity", f"{ci:.1f} gCO2/kWh")
    else:
        st.metric("Carbon intensity", "—")

st.divider()

pred = api_client.prediction_tomorrow()
risk = (pred or {}).get("risk_level", "UNKNOWN")
color = ui.risk_color(risk)

col1, col2 = st.columns(2)
with col1:
    st.markdown("### Risk level indicator")
    st.markdown(
        f"<h1 style='color:{color};'>{risk}</h1>",
        unsafe_allow_html=True)
    if pred:
        st.caption(f"Predicted {pred.get('predicted_co2_kg')} kg "
                   f"tomorrow (confidence {pred.get('confidence')})")
with col2:
    st.markdown("### Yesterday snapshot")
    if yesterday:
        st.markdown(f"- **CO2:** {yesterday.get('total_co2_kg')} kg")
        st.markdown(f"- **Intensity:** "
                    f"{yesterday.get('carbon_intensity')} gCO2/kWh")
        st.markdown(f"- **PM2.5:** {yesterday.get('pm2_5')} µg/m³")
    else:
        st.info("No previous day data")

st.divider()

# Anomaly alerts derived from live data
st.markdown("### ⚠️ Anomaly alerts")
alerts = []
if change is not None and abs(change) > 15:
    alerts.append(f"Daily emissions changed by {change:+.1f}% vs yesterday "
                  "- investigate.")
if (pred or {}).get("risk_level", "").upper() in ("HIGH", "VERY HIGH"):
    alerts.append("Tomorrow's forecast is HIGH risk - review mitigation plans.")
pm = today.get("pm2_5")
if pm is not None and pm > 50:
    alerts.append(f"PM2.5 at {pm} µg/m³ exceeds the 50 µg/m³ guidance level.")

if alerts:
    for a in alerts:
        st.warning(a)
else:
    st.success("No anomalies detected. All metrics within normal ranges.")
