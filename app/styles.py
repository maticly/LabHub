"""CSS styling"""
import streamlit as st

def apply_custom_style():
    st.markdown("""
        <style>

        /* ================= GLOBAL APP (UNCHANGED) ================= */
        .stApp {
            background-color: #FBFBFD;
            font-family: 'Inter', -apple-system, sans-serif !important;
        }

        h1, h2, h3, h4, h5, h6, p, li, label, 
        .stMarkdown, [data-testid="stWidgetLabel"] p {
            color: #1D1D1F !important;
        }

        /* ================= SETTINGS / DEPLOY MENU ================= */
        div[role="menu"] {
            background-color: #1C1C1E !important;
            border-radius: 10px !important;
        }

        div[role="menu"] * {
            color: #FFFFFF !important;
        }

        div[role="menu"] button:hover {
            background-color: #2C2C2E !important;
        }

        /* ================= SIDEBAR (FILTER AREA) ================= */
        section[data-testid="stSidebar"] {
            background-color: #2F2F33 !important; /* dark gray box */
            padding: 10px !important;
        }

        /* Sidebar text DARK (as requested) */
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] label {
            color: #d9d7d7 !important;
            font-weight: 500 !important;
        }

        /* Sidebar multiselect container */
        section[data-testid="stSidebar"] .stMultiSelect div {
            background-color: #3A3A3F !important;
            border-radius: 8px !important;
            color: #111111 !important;
        }

        /* Sidebar multiselect selected tags */
        section[data-testid="stSidebar"] .stMultiSelect span {
            background-color: #4e79c2 !important; /* professional dark blue */
            color: #FFFFFF !important;
            border-radius: 6px !important;
            padding: 1px 8px !important;
        }

        /* ================= SIDEBAR BUTTONS ================= */
        section[data-testid="stSidebar"] .stButton>button {
            background-color: #0A3D91 !important;
            color: #FFFFFF !important;
            border: none !important;
            border-radius: 8px !important;
            height: 42px !important;
            font-weight: 600 !important;
            transition: 0.2s ease !important;
        }

        section[data-testid="stSidebar"] .stButton>button:hover {
            background-color: #072E6B !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2) !important;
        }

        /* ================= MAIN PAGE BUTTONS (UNCHANGED STYLE) ================= */
        .stButton>button {
            background-color: #E1E1E3 !important;
            color: #1D1D1F !important;
            border-radius: 8px !important;
            height: 45px !important;
        }

        .stButton>button:hover {
            background-color: #1D1D1F !important;
            color: #FFFFFF !important;
        }

        /* ================= TEXT INPUTS ================= */
        div[data-testid="stTextInput"] input {
            background-color: #E1E1E3 !important;
            color: #1D1D1F !important;
            border-radius: 8px !important;
        }

        /* ================= METRICS ================= */
        [data-testid="stMetric"] {
            background-color: #FFFFFF !important;
            border: 1px solid #CDCDCD !important;
            padding: 20px !important;
            border-radius: 15px !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1) !important;
        }

        /* ================= DIALOG / POPUP (DARK MODE) ================= */
        div[data-testid="stDialog"] {
            background-color: #1C1C1E !important;
            color: #FFFFFF !important;
            border-radius: 14px !important;
            padding: 25px !important;
            box-shadow: 0 10px 40px rgba(0,0,0,0.5) !important;
        }

        /* ALL text inside dialog white */
        div[data-testid="stDialog"] * {
            color: #FFFFFF !important;
        }

        /* Tables inside dialog */
        div[data-testid="stDialog"] table {
            background-color: #2C2C2E !important;
        }

        div[data-testid="stDialog"] table * {
            color: #FFFFFF !important;
        }

        /* Dialog buttons */
        div[data-testid="stDialog"] .stButton>button {
            background-color: #0A3D91 !important;
            color: #FFFFFF !important;
        }

        div[data-testid="stDialog"] .stButton>button:hover {
            background-color: #072E6B !important;
            color: #FFFFFF !important;
        }

        </style>
    """, unsafe_allow_html=True)