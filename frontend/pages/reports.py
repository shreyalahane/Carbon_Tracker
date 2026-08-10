"""ESG Report: monthly summary, scope breakdown, PDF download."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils import api_client, ui

st.set_page_config(page_title="ESG Report", layout="wide")

ui.page_header("ESG Report",
               "Monthly compliance summary with scope breakdown")

report = api_client.agent_report()

if not report:
    st.warning("No ESG report available yet. Run "
               "`python agents/report_agent.py` to generate one.")
    st.stop()

breakdown = report.get("breakdown", {})
window = report.get("window", {})

st.markdown(
    f"**Reporting window:** {window.get('start_date')} → "
    f"{window.get('end_date')}")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total CO2 (kg)", breakdown.get("total_co2_kg"))
with col2:
    st.metric("Avg daily (kg)", breakdown.get("avg_daily_co2_kg"))
with col3:
    best = breakdown.get("best_day", {})
    st.metric("Best day", best.get("date"),
              help=f"{best.get('total_co2_kg')} kg")
with col4:
    worst = breakdown.get("worst_day", {})
    st.metric("Worst day", worst.get("date"),
              help=f"{worst.get('total_co2_kg')} kg")

st.divider()

# Scope breakdown chart
scope_data = {
    "Scope": ["Scope 1 (direct)", "Scope 2 (electricity)", "Scope 3 (value chain)"],
    "kg CO2e": [
        breakdown.get("scope1_kg", 0),
        breakdown.get("scope2_kg", 0),
        breakdown.get("scope3_kg", 0),
    ],
}
fig = go.Figure(go.Pie(
    labels=scope_data["Scope"],
    values=scope_data["kg CO2e"],
    hole=0.4,
))
fig.update_layout(title="Scope 1 / 2 / 3 breakdown (estimated)",
                  template="plotly_white", height=380)
st.plotly_chart(fig, use_container_width=True)

st.divider()

col1, col2 = st.columns([3, 2])
with col1:
    st.markdown("### Compliance summary")
    st.write(report.get("summary", ""))
    st.caption(f"Generated: {str(report.get('generated_on'))[:19]}")
with col2:
    st.markdown("### Download PDF")
    st.markdown("""
| Requirement | Status |
|---|---|
| Data coverage | 30 days |
| Scope breakdown | Estimated |
| Compliance | Needs review |
""")
    pdf = api_client.report_pdf_bytes()
    if pdf:
        st.download_button(
            "⬇️ Download ESG report PDF",
            data=pdf,
            file_name=report.get("pdf_file", "esg_report.pdf"),
            mime="application/pdf",
        )
    else:
        st.info("PDF not available on disk.")
        st.markdown(f"[Open PDF in browser]({api_client.report_pdf_url()})")
