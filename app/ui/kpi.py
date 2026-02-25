import streamlit as st

def kpi_card(title, value, subtitle="", bg_color="#DEE7F1"):
    st.markdown(f"""
    <div style="
        background:{bg_color};
        padding:22px;
        border-radius:14px;
        color:white;
        border:1px solid #65676B;
        box-shadow:0 2px 8px rgba(0,0,0,0.08);
        transition: all 0.25s ease;
        transform: translateY(-3px);
        box-shadow:0 12px 20px rgba(0,0,0,0.10);
    ">
        <div style="font-size:18px;font-weight:600">{title}</div>
        <div style="font-size:32px;font-weight:700">{value}</div>
        <div style="font-size:15px">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)
