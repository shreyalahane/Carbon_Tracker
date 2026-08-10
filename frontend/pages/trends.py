"""Historical Trends: 30-day lines, weekly pattern, monthly comparison."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils import api_client, ui

st.set_page_config(page_title="Historical Trends", layout="wide")

ui.page_header("Historical Trends",
               "30-day trends, weekly patterns, and monthly comparison")

history = api_client.emissions_history(30)

if not history or not history.get("data"):
    st.warning("No historical data available yet.")
    st.stop()

df = pd.DataFrame(history["data"])
df["date"] = pd.to_datetime(df["date"])

# 30-day line chart
fig = go.Figure()
for col, label, color in [
    ("total_co2_kg", "Total CO2 (kg)", "#1f77b4"),
    ("carbon_intensity", "Carbon intensity (g/kWh)", "#e67e22"),
    ("pm2_5", "PM2.5 (µg/m³)", "#9b59b6"),
]:
    if col in df.columns and df[col].notna().any():
        fig.add_trace(go.Scatter(
            x=df["date"], y=df[col], mode="lines+markers",
            name=label, line=dict(width=2, color=color)))
fig.update_layout(
    title="Last 30 days",
    xaxis_title="Date",
    yaxis_title="Value",
    template="plotly_white",
    height=420,
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# Weekly pattern (average by day of week)
if df["date"].notna().any():
    df["day_of_week"] = df["date"].dt.day_name()
    order = ["Monday", "Tuesday", "Wednesday", "Thursday",
             "Friday", "Saturday", "Sunday"]
    weekly = (df.groupby("day_of_week")["total_co2_kg"].mean()
                .reindex(order).dropna())
    if not weekly.empty:
        fig = px.bar(x=weekly.index, y=weekly.values,
                     title="Average CO2 by day of week",
                     labels={"x": "", "y": "Avg CO2 (kg)"},
                     template="plotly_white")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# Monthly comparison
df["month"] = df["date"].dt.to_period("M").astype(str)
monthly = df.groupby("month")["total_co2_kg"].sum()
if len(monthly) > 1:
    fig = px.bar(x=monthly.index, y=monthly.values,
                 title="Monthly total CO2 comparison",
                 labels={"x": "Month", "y": "Total CO2 (kg)"},
                 template="plotly_white")
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Total over window: {df['total_co2_kg'].sum():.1f} kg")
else:
    st.info("Need data spanning more than one month for a monthly comparison.")
