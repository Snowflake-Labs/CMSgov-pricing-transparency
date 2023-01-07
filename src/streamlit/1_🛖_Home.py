import streamlit as st

# The home of streamlit app

st.set_page_config(
     page_title="Codespaced",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'About': "This is an *extremely* cool app demo!!"
     }
)

st.write("# Welcome to Codespaced Project! 👋")

st.sidebar.success("Choose a tab, for specific demos.")

st.markdown(
    """
    This is a sample demonstration to showcase running Streamlit as part of a codespaced environment.

    Choose the tabs on the sidebar for some capabilities.
"""
)