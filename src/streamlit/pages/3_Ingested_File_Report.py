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
logger = logging.getLogger('2_Load_sample_segment')

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

st.markdown(f"# File ingestion report")

para = f'''
This reporting page helps us to gain a quick insights/overview of the ingested file
'''

def cache_ingested_data_files():
    df = sp_session.table('in_network_rates_file_header').select('data_file','INSERTED_AT').sort(F.col('INSERTED_AT'), ascending=False).distinct()
    return [ r['DATA_FILE'] for r in df.to_local_iterator() ]

def get_fileheader_info(p_data_file):
    sql_stmt = f'''
        select
            data_file
            ,data_file_basename
            ,inserted_at
            ,header:last_updated_on::date as last_updated_on
            ,header:reporting_entity_name::varchar as reporting_entity_name
            ,header:reporting_entity_type::varchar as reporting_entity_type
            ,header:total_segments::int as total_segments_in_file
        from in_network_rates_file_header
        where data_file = '{p_data_file}'
    '''
    return sp_session.sql(sql_stmt)

data_file = ''
data_files = []
def build_ui():
    with st.sidebar:
        st.markdown('Make a selection below :point_down:\nselect data file that was ingested:')
        fl = st.selectbox("DataFile:", data_files, index=data_files.index(st.session_state['data_file']))
        data_file = fl
        st.session_state['data_file'] = data_file

    file_stats_tab, load_audits_tab, data_view_tab = st.tabs(["file_stats", "load_audits", "data_view"])

    with file_stats_tab:
        st.header("File Stats")
        
        spdf = get_fileheader_info(data_file)
        st.dataframe(spdf)


# ----------------------------
if __name__ == "__main__":
    data_files = cache_ingested_data_files()

    if 'data_files' not in st.session_state:
        st.session_state['data_files'] = data_files

    if 'data_file' not in st.session_state:
        st.session_state['data_file'] = data_files[0]

    build_ui()




# select 
#     sum(task_ret_status:stored_segment_count) as stored_segment_count_for_file
# from segment_task_execution_status
# where not (task_name like any ('DAG_ROOT_%' ,'TERM_%' ,'%_FH_%')) 
#     and data_file = 'reduced_sample_data.json' --'2022_10_01_priority_health_HMO_in-network-rates.zip'

# ;


# select 
#     task_name
#     ,timestampdiff('minutes' ,start_time ,end_time) as elapsed_minutes
#     ,task_ret_status:start_rec_num::int as start_rec_num
#     ,task_ret_status:end_rec_num::int as end_rec_num
#     ,task_ret_status:last_seg_no::int as last_seg_no
#     ,task_ret_status:stored_segment_count::int as stored_segment_count
#     ,task_ret_status:EOF_Reached::boolean as EOF_Reached
# from segment_task_execution_status
# where not (task_name like any ('DAG_ROOT_%' ,'TERM_%' ,'%_FH_%')) 
#     and data_file = 'reduced_sample_data.json' --'2022_10_01_priority_health_HMO_in-network-rates.zip'
# order by start_rec_num
# ;

# select * from negotiated_rates_segment_stats_v;
# select * from negotiated_rates_segment_info_v;

