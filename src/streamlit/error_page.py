import os

import streamlit as st


def page_header():
    # Header.
    st.image("src/streamlit/images/logo-sno-blue.png", width=100)
    st.subheader("Media Advertising Campaign Optimization Demo")
    st.markdown(
        """<hr style="height:2px; width:99%; border:none;color:lightgrey;background-color:lightgrey;" /> """,
        unsafe_allow_html=True)


def custom_page_styles():
    # page custom Styles
    st.markdown("""
                <style>
                    #MainMenu {visibility: hidden;} 
                    header {height:0px !important;}
                    footer {visibility: hidden;}
                    .block-container {
                        padding: 0px;
                        padding-top: 15px;
                        padding-left: 1rem;
                        max-width: 98%;
                    }
                    [data-testid="stVerticalBlock"] {
                        gap: 0;
                    }
                    [data-testid="stHorizontalBlock"] {
                        width: 99%;
                    }
                    .stApp {
                        width: 98% !important;
                    }
                    [data-testid="stMetricLabel"] {
                        font-size: 1.25rem;
                        font-weight: 700;
                    }
                    [data-testid="stMetricValue"] {
                        font-size: 1.5rem;
                        color: gray;
                    }
                    [data-testid="stCaptionContainer"] {
                        font-size: 1.35rem;
                        color: red;
                        background-color: rgba(255, 43, 43, 0.09);
                        padding-left:10px;
                    }
                    div.stButton > button:first-child {
                        background-color: #50C878;color:white; border-color: none;
                    }
                </style>""", unsafe_allow_html=True)

def build_UI(error_message, display_header=True):
    custom_page_styles()
    if display_header:
        page_header()
        st.markdown("""<hr style="height:100px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)
    c1, c2 = st.columns([0.1, 0.9])
    with c1:
        st.image('src/streamlit/images/error.png', width=100)
    with c2:
        st.caption('Ooops there was an error. Please check!')
        em = ''
        for m in error_message:
            st.error(m)