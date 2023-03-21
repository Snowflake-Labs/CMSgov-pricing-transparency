## ------------------------------------------------------------------------------------------------
# Copyright (c) 2023 Snowflake Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.You may obtain 
# a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0
    
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions andlimitations 
# under the License.
## ------------------------------------------------------------------------------------------------

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