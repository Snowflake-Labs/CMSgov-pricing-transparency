import streamlit as st

# The home of streamlit app

st.set_page_config(
     page_title="Parsing Pricing Transperancy Files",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     
)

st.write("# Parsing Pricing Transperancy Files 👋")

st.sidebar.success("Choose a tab, for specific demos.")

st.markdown(
    """
    This solution is a demonstration of parsing and ingesting Pricing transperancy files natively in Snowflake.

    To proceed with the demo
     - Run the setups from page : '2_🛠_Setup'
     - Ingest sample data file using notebook : src/notebook/Load_segments_dag.ipynb
     - Observe the file ingestion process from page : 3_Ingested_File_Report
     
    ### Snowflake Capabilities
     - Dynamic File Access (Snowpark Python)
    
    ### Excluated privileges
     - Ability to create custom role
        - role will have abilities to create tasks

    ### Customizations
     - Update configuration file 'config.ini' before proceeding
"""
)