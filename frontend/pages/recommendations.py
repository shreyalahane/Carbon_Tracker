"""AI Recommendations: advisor advice and weekly reduction plan."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from utils import api_client, ui

st.set_page_config(page_title="Recommendations", layout="wide")

ui.page_header("AI Recommendations",
               "Advisor advice and weekly reduction strategy")

advice = api_client.agent_advice()

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🧠 Advisor recommendations")
    if advice:
        st.caption(advice.get("summary", ""))
        st.caption(f"Source: {advice.get('source')} • "
                   f"Generated: {str(advice.get('generated_on'))[:19]}")
        for rec in advice.get("recommendations", []):
            st.markdown(f"- {rec}")
    else:
        st.info("Run the Advisor agent (`python agents/advisor_agent.py`) "
                "to generate recommendations.")

with col2:
    st.markdown("### 📋 Weekly reduction plan")
    plan = api_client.agent_plan()
    if plan:
        st.caption(plan.get("summary", ""))
        for day in plan.get("plan", []):
            st.markdown(
                f"**Day {day.get('day')} - {day.get('focus')}**: "
                f"{day.get('action')} *(saving {day.get('target_saving')})*")
        st.divider()
        savings = [d.get("target_saving") for d in plan.get("plan", [])
                   if isinstance(d.get("target_saving"), str)
                   and d["target_saving"].replace("%", "").replace("—", "").isdigit()]
        if savings:
            st.markdown("### 💰 Potential savings")
            st.metric("Target weekly reduction",
                      sum(int(s.replace("%", "")) for s in savings))
            st.caption("Sum of day-level target percentages (%).")
    else:
        st.info("Run the Planner agent (`python agents/planner_agent.py`) "
                "to generate a plan.")

st.divider()

# Today's data context
today = api_client.emissions_today()
if today and today.get("today"):
    st.markdown("### Context")
    t = today["today"]
    cols = st.columns(4)
    with cols[0]:
        st.metric("Today CO2 (kg)", t.get("total_co2_kg"))
    with cols[1]:
        st.metric("Intensity (g/kWh)", t.get("carbon_intensity"))
    with cols[2]:
        st.metric("PM2.5 (µg/m³)", t.get("pm2_5"))
    with cols[3]:
        st.metric("Temperature (°C)", t.get("temperature_mean"))
