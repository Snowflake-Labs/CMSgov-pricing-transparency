from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
from util_fns import exec_sql_script ,exec_python_script ,exec_shell_script

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

sys.path.append('src/streamlit')
import error_page as error_page

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('exec_sql_script')

config = L.get_config(PROJECT_HOME_DIR)
sp_session = None
if "snowpark_session" not in st.session_state:
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['SNOW_CONN']['role']}''')
    sp_session.use_schema(f'''{config['SNOW_CONN']['database']}.{config['SNOW_CONN']['schema']}''')
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
# Run the Setup scripts

def page_header():
    # Header.
    st.image("src/streamlit/images/logo-sno-blue.png", width=100)
    st.subheader("Pricing Transperancy")

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
                        font-size: 1.25rem;
                    }
                    [data-testid="stMarkdownContainer"] {
                        font-size: 2rem;
                    }
                    [data-testid="metric-container"] {
                        word-wrap: break-word;
                    }
                    div.stButton > button:first-child {
                        background-color: #50C878;color:white; border-color: none;
                </style>""", unsafe_allow_html=True)

def display_connection_info():
    with st.expander("Snowflake Connection Information", expanded=True):
        c1, s1, c2, s2, c3, s3, c4, s4 = st.columns([1, 0.1, 2.5, 0.1, 2.5, 0.1, 1, 0.1])
        with s1:
            st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)

        with s2:
            st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)

        with s3:
            st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)
        with s4:
            st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)

        with c1:
            account = sp_session.get_current_account().replace('"', '')
            st.metric("Account", f"{account}")

        with c2:
            st.metric("Database", f"{config['APP_DB']['database']}.{config['APP_DB']['schema']}")

        with c3:
            st.metric("Warehouse", f"{config['SNOW_CONN']['warehouse']}")

        with c4:
            st.metric("Role", f"{config['SNOW_CONN']['role']}")

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)

#-----------------------------------------------------
import pandas as pd
import os
from datetime import datetime
from datetime import timedelta
import time
import re
from urllib import request

pd.set_option('display.max_colwidth', None)

def get_file_from_download_url(p_fl_url):
    splits = p_fl_url.split('/')
    return splits[len(splits) - 1]

def get_basename_of_datafile(p_datafile:str) -> str:
    base = os.path.basename(p_datafile)
    fl_base = os.path.splitext(base)
    return fl_base[0]

def get_cleansed_file_basename(p_datafile):
    fl_basename = get_basename_of_datafile(p_datafile)
    # Replace all non alphanumeric characters with _
    fl_name = re.sub('[^0-9a-zA-Z]+', '_', fl_basename)
    return fl_name







# import pandas as pd
# import numpy as np
# import datetime, os
# import snowflake.snowpark.functions as F
# from datetime import datetime
# from datetime import timedelta
# import time
# import re


# st.markdown(f"# Ingest sample data")

# para = f'''
# This page we will ingest the reduced sample dataset file: reduced_sample_data.json.

# If you like to perform this activity with other data samples, you try with the notebook: Load_using_dag.ipynb
# '''

# def get_basename_of_datafile(p_datafile:str) -> str:
#     base = os.path.basename(p_datafile)
#     fl_base = os.path.splitext(base)
#     return fl_base[0]

# def get_cleansed_file_basename(p_datafile):
#     fl_basename = get_basename_of_datafile(p_datafile)
#     # Replace all non alphanumeric characters with _
#     fl_name = re.sub('[^0-9a-zA-Z:=]+', '_', fl_basename)
#     return fl_name

# # Define the variables
# INPUT_DATA_STAGE = 'data_stg'
# DATA_STAGE_FOLDER = config['APP_DB']['folder_data']
# DATA_FILE = 'reduced_sample_data.json'
# DATA_FILE_BASENAME = get_basename_of_datafile(DATA_FILE)
# DATA_FILE_BASENAME_CLEANSED = get_cleansed_file_basename(DATA_FILE)
# TARGET_DATA_STAGE = config['APP_DB']['ext_stage']
# TARGET_FOLDER = config['APP_DB']['folder_parsed']
# SEGMENTS_PER_TASK = 200

# st.write(f'Input DataFile: @{INPUT_DATA_STAGE}/{DATA_STAGE_FOLDER}/{DATA_FILE}')
# st.write(f'Target: @{TARGET_DATA_STAGE}/{TARGET_FOLDER}')

# def cleanup():
#     st.write("### Cleanup block")
#     # We will cleanup specific resources and artifacts from possible previous runs.

#     stmts = [
#         f''' delete from segment_task_execution_status where data_file = '{DATA_FILE}'; '''
#         ,f''' delete from task_to_segmentids where data_file = '{DATA_FILE}'; '''
#         ,f''' delete from in_network_rates_file_header where data_file = '{DATA_FILE}'; '''
#         ,f''' delete from in_network_rates_segment_header where data_file = '{DATA_FILE}'; '''
#         ,f''' alter stage {INPUT_DATA_STAGE} refresh; '''
#     ]    
        
#     st.write(' truncating tables ...')
#     for stmt in stmts:
#         sp_session.sql(stmt).collect()

#     st.write(f''' cleaning up files in external stage under path {TARGET_FOLDER}/{DATA_FILE_BASENAME}/ ...''')
#     stmt = f''' select relative_path from directory(@{TARGET_DATA_STAGE}) where relative_path like '%{DATA_STAGE_FOLDER}/{DATA_FILE_BASENAME}/%'; '''
#     files = sp_session.sql(stmt).collect()
#     for r in files:
#         stmt = f''' remove @{TARGET_DATA_STAGE}/{r['RELATIVE_PATH']}; '''
#         sp_session.sql(stmt).collect()

# def invoke_dag_builder():
#     # we build out the DAG
#     df = sp_session.call('in_network_rates_dagbuilder' ,f'{INPUT_DATA_STAGE}/{DATA_STAGE_FOLDER}' ,DATA_FILE 
#         ,f"@{TARGET_DATA_STAGE}/{TARGET_FOLDER}" ,SEGMENTS_PER_TASK ,config['SNOW_CONN']['warehouse'])

#     sp_session.sql(f''' alter stage {TARGET_DATA_STAGE} refresh; ''').collect()
#     st.write(' Status of execution')
#     st.write(df)

# def get_defined_tasks():
#     sp_session.sql(f''' SHOW TASKS IN  DATABASE {config['APP_DB']['database']}; ''').collect()
#     sql_stmt = f'''
#         select "name" as task_name
#             ,"warehouse" as warehouse
#             ,"state" as state
#         from table(result_scan(last_query_id()))
#         where "name" like '%{DATA_FILE_BASENAME_CLEANSED.upper()}%'
#         limit 5;
#     '''
#     return sp_session.sql(sql_stmt).collect()

# def invoke_dag():
#     start_time = time.time()
#     print(f'Started at: {datetime.now().strftime("%H:%M:%S")}')

#     sql_stmts = [
#         # f''' alter warehouse {config['SNOW_CONN']['warehouse']} set max_concurrency_level = 8 '''
#         # XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE | X5LARGE | X6LARGE
#         f''' alter warehouse {config['SNOW_CONN']['warehouse']} set warehouse_size = MEDIUM; '''
#         ,f''' execute task DAG_ROOT_{DATA_FILE_BASENAME_CLEANSED}; '''
#     ]
#     for stmt in sql_stmts:
#         sp_session.sql(stmt).collect()

#     st.write('Wait for about 10 min for the DAG to finish')
#     time.sleep(10*60)

#     end_time = time.time()
#     st.write(f'Ended at: {datetime.now().strftime("%H:%M:%S")}')

#     elapsed_time = end_time - start_time
#     elapsed = str(timedelta(seconds=elapsed_time))
#     st.write(f'Elapsed: {elapsed}')

# def refresh_tables_stages():
#     sp_session.sql(f''' alter stage {config['APP_DB']['ext_stage']} refresh; ''').collect()
#     # sp_session.sql(f''' alter table ext_negotiated_arrangments_staged refresh; ''').collect()
    

def build_ui():

    custom_page_styles()
    page_header()
    st.markdown(
        """<hr style="height:2px; width:100%; border:none;color:lightgrey;background-color:lightgrey;margin-bottom: 16px; margin-top: 25px;" /> """,
        unsafe_allow_html=True)

    display_connection_info()

    DATA_FILE_URL = st.text_input('Data file URL', 'https://priorityhealthtransparencymrfs.s3.amazonaws.com/2023_03_01_priority_health_HMO_in-network-rates.zip')
    
    c1, s1, c2, s2 = st.columns([1, 0.1, 1, 0.1])
    with s1:
        st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)

    with s2:
        st.markdown(
            """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
            unsafe_allow_html=True)

    with c1:
        INPUT_DATA_STAGE = st.radio("Which stage is the data file stored at?",
            ('data_stg' ,config['APP_DB']['ext_stage']))
        
    with c2:
        # This will need to be updated based on provider
        # Priority Health ~ 500
        # CIGNA ~ 15000
        SEGMENTS_PER_TASK = st.text_input('Segments per task', 500)

    st.markdown('---')

    c3, s3, c4, s4 ,c5, s5, c6, s6 = st.columns([1, 0.1, 1, 0.1, 1, 0.1, 1, 0.1])
    with s3:
        st.markdown(
                """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
                unsafe_allow_html=True)

    with s4:
        st.markdown(
            """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
            unsafe_allow_html=True)

    with s5:
        st.markdown(
            """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
            unsafe_allow_html=True)
    with s6:
        st.markdown(
            """<div style="border: 1px solid lightgrey; height: 60px; width:2px; background-color: lightgrey;"></div> """,
            unsafe_allow_html=True)

    with c3:
        warehouses_count = int( st.text_input('No of warehouses to use for processing', 1) )

    with c4:
        create_warehouses = st.checkbox('Create warehouses ?')

    with c5:
        warehouse_size = st.selectbox(
        'Warehouse size for processing?',
        ('MEDIUM','SMALL','LARGE','XLARGE','XXLARGE','XXXLARGE','X4LARGE','X5LARGE','X6LARGE','XSMALL'))

    st.markdown('---')

    DATA_FILE = get_file_from_download_url(DATA_FILE_URL)
    DATA_FILE_BASENAME = get_basename_of_datafile(DATA_FILE)
    DATA_FILE_BASENAME_CLEANSED = get_cleansed_file_basename(DATA_FILE)

    DATA_STAGE_FOLDER = config['APP_DB']['folder_data']
    TARGET_DATA_STAGE = config['APP_DB']['ext_stage']
    TARGET_FOLDER = config['APP_DB']['folder_parsed']

    warehouses = config['SNOW_CONN']['warehouse']
    if(warehouses_count > 1):
       t_whs = [ f'INDSOL_PRICE_TRANS_TASK_{i}_WH' for i in range(warehouses_count) ]
       warehouses = ','.join(t_whs)

    

#     # Custom CSS to color the button.
#     st.markdown(""" <style>
#     div.stButton > button:first-child {
#     background-color: Grey;color:white; border-color: none;
#     } </style>""", unsafe_allow_html=True)

#     with st.expander("Step 1- Cleanup"):
#         st.button('Cleanup'
#             ,on_click=cleanup
#         )

#     with st.expander("Step 2- Invoke dag builder"):
#         st.button('Invoke dag builder'
#             ,on_click=invoke_dag_builder
#         )

#         image = Image.open(f'{PROJECT_HOME_DIR}/doc/soln_images/task_dags.png')
#         st.image(image, caption='Example of Task')

#         st.write('Defined tasks')
#         tsk_df = get_defined_tasks()
#         st.dataframe(tsk_df)

#         st.write('Task to segments mapping')
#         U.load_sample_and_display_table(sp_session ,'TASK_TO_SEGMENTIDS' ,5)
        
#     with st.expander("Step 3- Invoke dag"):
#         st.button('Invoke DAG'
#             ,on_click=invoke_dag
#         )

#     with st.expander("Step 4- Refresh stages & tables"):
#         st.button('Refresh'
#             ,on_click=refresh_tables_stages
#         )

#         U.load_sample_and_display_table(sp_session ,'ext_negotiated_arrangments_staged' ,5)
        
# ----------------------------
if __name__ == "__main__":
    build_ui()
    pass








