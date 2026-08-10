"""Predictions page: tomorrow's forecast, 7-day trend, confidence."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils import api_client, ui

st.set_page_config(page_title="Predictions", layout="wide")

ui.page_header("Predictions",
               "Tomorrow's forecast and the 7-day outlook")

pred = api_client.prediction_tomorrow()
weekly = api_client.predictions_weekly(7)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### Tomorrow's forecast")
    if pred:
        ui.metric_card("", f"{pred.get('predicted_co2_kg')} kg",
                       color=ui.risk_color(pred.get("risk_level")))
        st.caption(f"For {pred.get('date')}")
    else:
        st.info("No prediction yet")
with col2:
    st.markdown("#### Confidence")
    if pred:
        conf = pred.get("confidence")
        st.progress(float(conf) if conf else 0.0)
        st.caption(f"{conf} confidence score")
    else:
        st.info("—")
with col3:
    st.markdown("#### Risk level")
    if pred:
        color = ui.risk_color(pred.get("risk_level"))
        st.markdown(f"<h2 style='color:{color};'>{pred.get('risk_level')}</h2>",
                    unsafe_allow_html=True)
    else:
        st.info("—")

st.divider()

if weekly and weekly.get("data"):
    df = pd.DataFrame(weekly["data"])
    df["date"] = pd.to_datetime(df["date"])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["predicted_co2_kg"],
        mode="lines+markers",
        name="Predicted CO2 (kg)",
        line=dict(width=3, color="#1f77b4"),
    ))
    fig.update_layout(
        title="7-day prediction trend",
        xaxis_title="Date",
        yaxis_title="Predicted CO2 (kg)",
        template="plotly_white",
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.caption("Daily predictions shown with risk colors:")
    for _, row in df.iterrows():
        color = ui.risk_color(row["risk_level"])
        st.markdown(
            f"- **{row['date'].date()}**: {row['predicted_co2_kg']} kg "
            f"<span style='color:{color};'>({row['risk_level']})</span>",
            unsafe_allow_html=True)
else:
    st.info("No weekly predictions available yet.")

st.divider()

perf = api_client.model_performance()
if perf and perf.get("top_features"):
    st.markdown("### Top features driving the prediction")
    import plotly.express as px
    features = pd.DataFrame(perf["top_features"])
    fig = px.bar(features, x="importance", y="feature",
                 orientation="h", template="plotly_white")
    fig.update_layout(height=320)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Model v{perf.get('version')} "
               f"(R²: {perf.get('performance', {}).get('R2')}, "
               f"MAE: {perf.get('performance', {}).get('MAE')})")
