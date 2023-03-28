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