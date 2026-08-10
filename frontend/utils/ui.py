"""Shared UI helpers for the Streamlit pages."""

import streamlit as st


def risk_color(risk_level):
    mapping = {
        "VERY LOW": "#2ecc71",
        "LOW": "#27ae60",
        "MEDIUM": "#f1c40f",
        "MODERATE": "#f1c40f",
        "HIGH": "#e67e22",
        "VERY HIGH": "#e74c3c",
    }
    return mapping.get((risk_level or "").upper(), "#95a5a6")


def page_header(title, subtitle=None):
    st.markdown(f"## {title}")
    if subtitle:
        st.caption(subtitle)
    st.divider()


def metric_card(label, value, delta=None, color=None):
    if color:
        st.markdown(
            f"<h3 style='color:{color}; margin:0;'>{value}</h3>",
            unsafe_allow_html=True)
    else:
        st.metric(label, value, delta)
