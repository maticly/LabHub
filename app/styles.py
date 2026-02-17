"""CSS styling"""
import streamlit as st

def apply_custom_style():
    st.markdown("""
        <style>
        /* 1. Global Background & Core Text */
        .stApp {
            background-color: #FBFBFD !important;
        }

        /* Force ALL text visibility (Headers, Paragraphs, Labels) */
        h1, h2, h3, h4, h5, h6, p, li, label, .stMarkdown, [data-testid="stWidgetLabel"] p {
            color: #1D1D1F !important;
            font-family: 'Inter', -apple-system, sans-serif !important;
        }

        /* 2. Inputs & Placeholders */
        div[data-testid="stTextInput"] input {
            background-color: #E1E1E3 !important;
            color: #1D1D1F !important;
            border: none !important;
            border-radius: 8px !important;
            padding: 10px !important;
        }

        /* Force Placeholder Text to be Dark Gray (visibility fix) */
        div[data-testid="stTextInput"] input::placeholder {
            color: #6E6E73 !important;
            opacity: 1 !important;
        }

        /* 3. Buttons (Standard & Download) */
        .stButton>button, .stDownloadButton>button {
            background-color: #E1E1E3 !important;
            color: #1D1D1F !important;
            border: none !important;
            border-radius: 8px !important;
            width: 100% !important;
            height: 45px !important; 
            font-weight: 500 !important;
            transition: all 0.3s ease !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
        }
  
        /* 2. KPI Rounded Containers */
        [data-testid="stMetric"] {
            background-color: #1e2130;
            padding: 15px;
            border-radius: 15px;
            border: 1px solid #31333f;
        }

        /* 3. Centered and Stylized Tabs */
        .stTabs [data-baseweb="tab-list"] {
            display: flex;
            justify-content: center;
            gap: 20px;
        }
        .stTabs [data-baseweb="tab"] {
            font-size: 20px !important;
            height: 50px;
            background-color: transparent;
            transition: background-color 0.3s ease;
            padding: 10px 20px;
            border-radius: 10px 10px 0 0;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background-color: rgba(255, 255, 255, 0.05);
        }

        /* Hover State: Dark background, WHITE text */
        .stButton>button:hover, .stDownloadButton>button:hover {
            background-color: #1D1D1F !important;
            color: #FFFFFF !important; /* Fixed: Changes to white on hover */
            box-shadow: 0 4px 12px rgba(0,0,0,0.1) !important;
        }

        /* 4. Perfect Vertical Alignment for Columned Buttons */
        [data-testid="column"] {
            display: flex !important;
            flex-direction: column !important;
            justify-content: flex-end !important;
        }
                
        /* Chart Axis Styling */
        .axis-label {
            font-weight: bold;
            color: #d3d3d3; /* Light Gray */
        }

        /* 5. Metrics Cards Visibility - Targeting the actual wrapper */
        [data-testid="stMetric"] {
            background-color: #ffffff !important; /* Professional Dark Blue/Gray */
            border: 1px solid #cdcdcd !important;
            padding: 20px !important;
            border-radius: 15px !important;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
            text-align: left !important;
        }

        /* Target the Metric Label (The Title) */
        [data-testid="stMetricLabel"] p {
            color: #131313 !important;
            font-size: 1rem !important;
            font-weight: 600 !important;
        }

        /* Target the Metric Value (The Number) */
        [data-testid="stMetricValue"] div {
            color: #131313 !important;
            font-weight: 600 !important;
            font-size: 1.6rem !important;
        }

        /* 6. Tabs Visibility & Centering */
        div[data-testid="stTabs"] [data-baseweb="tab-list"] {
            display: flex;
            justify-content: center;
            gap: 24px;
        }

        .stTabs [data-baseweb="tab"] {
            color: #86868B !important;
            font-size: 1.2rem !important;
        }

        .stTabs [aria-selected="true"] {
            color: #007AFF !important; /* Active Blue */
            font-weight: 700 !important;
            border-bottom: 2px solid #007AFF !important;
        }
        
        /* Hover effect for tabs */
        .stTabs [data-baseweb="tab"]:hover {
            color: #131313 !important;
            background-color: rgba(255, 255, 255, 0.05) !important;
        }
        </style>
    """, unsafe_allow_html=True)