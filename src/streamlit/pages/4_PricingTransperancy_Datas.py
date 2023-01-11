from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
import util_fns as U

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('4_PricingTransperancy_Datas')

# Initialize a session with Snowflake
config = L.get_config(PROJECT_HOME_DIR)
sp_session = None
if "snowpark_session" not in st.session_state:
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
import pandas as pd
import numpy as np
import datetime, os
import snowflake.snowpark.functions as F

st.markdown(f"# Pricing Transperancy Data views")

para = f'''
We look at the views & table specific to the pricing transperancy dataset.
'''

def build_ui():
    st.write('## Negotiation Arrangments headers')
    U.load_sample_and_display_table(sp_session ,'negotiated_rates_segment_stats_v' ,5)
    
    st.write('## Sample rows from negotiation arrangements')
    U.load_sample_and_display_table(sp_session ,'negotiated_rates_segment_info_v' ,5)
        
    st.write('## Sample rows from negotiated prices')
    U.load_sample_and_display_table(sp_session ,'negotiated_prices_v' ,5)

# ----------------------------
if __name__ == "__main__":
    build_ui()