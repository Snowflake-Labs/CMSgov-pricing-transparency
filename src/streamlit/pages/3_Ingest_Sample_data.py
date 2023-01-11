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
logger = logging.getLogger('3_Ingest_sampledata')

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
from datetime import datetime
from datetime import timedelta
import time
import re
from PIL import Image

st.markdown(f"# Ingest sample data")

para = f'''
This page we will ingest the reduced sample dataset file: reduced_sample_data.json.

If you like to perform this activity with other data samples, you try with the notebook: Load_using_dag.ipynb
'''

def get_basename_of_datafile(p_datafile:str) -> str:
    base = os.path.basename(p_datafile)
    fl_base = os.path.splitext(base)
    return fl_base[0]

def get_cleansed_file_basename(p_datafile):
    fl_basename = get_basename_of_datafile(p_datafile)
    # Replace all non alphanumeric characters with _
    fl_name = re.sub('[^0-9a-zA-Z:=]+', '_', fl_basename)
    return fl_name

# Define the variables
INPUT_DATA_STAGE = 'data_stg'
DATA_STAGE_FOLDER = config['APP_DB']['folder_data']
DATA_FILE = 'reduced_sample_data.json'
DATA_FILE_BASENAME = get_basename_of_datafile(DATA_FILE)
DATA_FILE_BASENAME_CLEANSED = get_cleansed_file_basename(DATA_FILE)
TARGET_DATA_STAGE = config['APP_DB']['ext_stage']
TARGET_FOLDER = config['APP_DB']['folder_parsed']
SEGMENTS_PER_TASK = 200

st.write(f'Input DataFile: @{INPUT_DATA_STAGE}/{DATA_STAGE_FOLDER}/{DATA_FILE}')
st.write(f'Target: @{TARGET_DATA_STAGE}/{TARGET_FOLDER}')

def cleanup():
    st.write("### Cleanup block")
    # We will cleanup specific resources and artifacts from possible previous runs.

    stmts = [
        f''' delete from segment_task_execution_status where data_file = '{DATA_FILE}'; '''
        ,f''' delete from task_to_segmentids where data_file = '{DATA_FILE}'; '''
        ,f''' delete from in_network_rates_file_header where data_file = '{DATA_FILE}'; '''
        ,f''' delete from in_network_rates_segment_header where data_file = '{DATA_FILE}'; '''
        ,f''' alter stage {INPUT_DATA_STAGE} refresh; '''
    ]    
        
    st.write(' truncating tables ...')
    for stmt in stmts:
        sp_session.sql(stmt).collect()

    st.write(f''' cleaning up files in external stage under path {TARGET_FOLDER}/{DATA_FILE_BASENAME}/ ...''')
    stmt = f''' select relative_path from directory(@{TARGET_DATA_STAGE}) where relative_path like '%{DATA_STAGE_FOLDER}/{DATA_FILE_BASENAME}/%'; '''
    files = sp_session.sql(stmt).collect()
    for r in files:
        stmt = f''' remove @{TARGET_DATA_STAGE}/{r['RELATIVE_PATH']}; '''
        sp_session.sql(stmt).collect()

def invoke_dag_builder():
    # we build out the DAG
    df = sp_session.call('in_network_rates_dagbuilder' ,f'{INPUT_DATA_STAGE}/{DATA_STAGE_FOLDER}' ,DATA_FILE 
        ,f"@{TARGET_DATA_STAGE}/{TARGET_FOLDER}" ,SEGMENTS_PER_TASK ,config['SNOW_CONN']['warehouse'])

    sp_session.sql(f''' alter stage {TARGET_DATA_STAGE} refresh; ''').collect()
    st.write(' Status of execution')
    st.write(df)

def get_defined_tasks():
    sp_session.sql(f''' SHOW TASKS IN  DATABASE {config['APP_DB']['database']}; ''').collect()
    sql_stmt = f'''
        select "name" as task_name
            ,"warehouse" as warehouse
            ,"state" as state
        from table(result_scan(last_query_id()))
        where "name" like '%{DATA_FILE_BASENAME_CLEANSED.upper()}%'
        limit 5;
    '''
    return sp_session.sql(sql_stmt).collect()

def invoke_dag():
    start_time = time.time()
    print(f'Started at: {datetime.now().strftime("%H:%M:%S")}')

    sql_stmts = [
        # f''' alter warehouse {config['SNOW_CONN']['warehouse']} set max_concurrency_level = 8 '''
        # XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE | X5LARGE | X6LARGE
        f''' alter warehouse {config['SNOW_CONN']['warehouse']} set warehouse_size = MEDIUM; '''
        ,f''' execute task DAG_ROOT_{DATA_FILE_BASENAME_CLEANSED}; '''
    ]
    for stmt in sql_stmts:
        sp_session.sql(stmt).collect()

    st.write('Wait for about 10 min for the DAG to finish')
    time.sleep(10*60)

    end_time = time.time()
    st.write(f'Ended at: {datetime.now().strftime("%H:%M:%S")}')

    elapsed_time = end_time - start_time
    elapsed = str(timedelta(seconds=elapsed_time))
    st.write(f'Elapsed: {elapsed}')

def refresh_tables_stages():
    sp_session.sql(f''' alter stage {config['APP_DB']['ext_stage']} refresh; ''').collect()
    sp_session.sql(f''' alter table ext_negotiated_arrangments_staged refresh; ''').collect()
    

def build_ui():

    # Custom CSS to color the button.
    st.markdown(""" <style>
    div.stButton > button:first-child {
    background-color: Grey;color:white; border-color: none;
    } </style>""", unsafe_allow_html=True)

    with st.expander("Step 1- Cleanup"):
        st.button('Cleanup'
            ,on_click=cleanup
        )

    with st.expander("Step 2- Invoke dag builder"):
        st.button('Invoke dag builder'
            ,on_click=invoke_dag_builder
        )

        image = Image.open(f'{PROJECT_HOME_DIR}/doc/soln_images/task_dags.png')
        st.image(image, caption='Example of Task')

        st.write('Defined tasks')
        tsk_df = get_defined_tasks()
        st.dataframe(tsk_df)

        st.write('Task to segments mapping')
        U.load_sample_and_display_table(sp_session ,'TASK_TO_SEGMENTIDS' ,5)
        
    with st.expander("Step 3- Invoke dag"):
        st.button('Invoke DAG'
            ,on_click=invoke_dag
        )

    with st.expander("Step 4- Refresh stages & tables"):
        st.button('Refresh'
            ,on_click=refresh_tables_stages
        )
        
# ----------------------------
if __name__ == "__main__":
    build_ui()








