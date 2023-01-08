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
            -- data_file
            -- ,data_file_basename
            inserted_at
            ,header:last_updated_on::date as last_updated_on
            ,header:reporting_entity_name::varchar as reporting_entity_name
            ,header:reporting_entity_type::varchar as reporting_entity_type
            ,header:total_segments::int as total_segments_in_file
        from in_network_rates_file_header
        where data_file = '{p_data_file}'
    '''
    return sp_session.sql(sql_stmt)

def get_file_ingestion_elapsed(p_data_file):
    sql_stmt = f'''
            select * exclude(data_file)
            from file_ingestion_elapsed_v as l
            where l.data_file = '{p_data_file}'
        '''
    return sp_session.sql(sql_stmt)


def get_segments_loaded_stats(p_data_file):
    sp_session.sql('alter stage ext_data_stg refresh;').collect()

    sql_stmt = f'''
        with base as (
            select
                r.data_file
                ,r.header:total_segments::int as total_segments_in_file
                -- ,split_part(d.relative_path,'/',3) as segment
                ,count( distinct split_part(d.relative_path,'/',3)) as segment_count
            from directory(@ext_data_stg) as d
                join in_network_rates_file_header as r
                    on split_part(d.relative_path,'/',2) = r.data_file_basename
            where d.relative_path like 'raw_parsed/%'
            group by data_file ,total_segments_in_file
        )
        select 
            -- l.data_file ,
            l.total_segments_in_file ,l.segment_count
            ,sum(r.task_ret_status:stored_segment_count)::int as segments_stored_by_task
        from base as l
            join segment_task_execution_status as r
                on r.data_file = l.data_file
        where r.task_ret_status:stored_segment_count is not null
            and l.data_file = '{p_data_file}'
        group by  l.data_file ,l.total_segments_in_file ,l.segment_count
    '''
    return sp_session.sql(sql_stmt)

def get_total_tasks_count(p_data_file):
    sql_stmt = f'''
        select count(*) as task_count
        from task_to_segmentids
        where data_file = '{p_data_file}'
        ;
    '''
    return sp_session.sql(sql_stmt)

def get_tasks_ingestion_stats(p_data_file):
    sql_stmt = f'''
        select 
            task_name
            ,timestampdiff('minutes' ,start_time ,end_time) as elapsed_minutes
            ,task_ret_status:start_rec_num::int as start_rec_num
            ,task_ret_status:end_rec_num::int as end_rec_num
            ,task_ret_status:last_seg_no::int as last_seg_no
            ,task_ret_status:stored_segment_count::int as stored_segment_count
            ,task_ret_status:EOF_Reached::boolean as EOF_Reached
            ,task_ret_status:segments_outof_range::boolean as segments_outof_range
            ,task_ret_status:task_ignored_parsing::boolean as task_ignored_parsing
        from segment_task_execution_status
        where not (task_name like any ('DAG_ROOT_%' ,'TERM_%' ,'%_FH_%')) 
            and data_file = '{p_data_file}'
        order by start_rec_num ,EOF_Reached 
    '''
    return sp_session.sql(sql_stmt)

def get_files_staged(p_data_file):
    sql_stmt = f'''
        select relative_path ,size
        from directory(@ext_data_stg) as l
            join in_network_rates_file_header as r
                on contains(l.relative_path ,r.data_file_basename) = True
        where r.data_file =  '{p_data_file}'
        limit 5
    '''
    return sp_session.sql(sql_stmt)

def get_segments_stats(p_data_file):
    sql_stmt = f'''
        select 
            * exclude(data_file)
        from negotiated_rates_segment_stats_v
        where data_file = '{p_data_file}'
    '''
    return sp_session.sql(sql_stmt)

def get_segments_chunks_sample(p_data_file):
    sql_stmt = f'''
        select 
            * exclude(data_file ,data_fl_basename)
        from negotiated_rates_segment_info_v
        where data_file = '{p_data_file}'
        limit 5
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


    st.write(f'## Data File: {data_file}')
    file_stats_tab, load_audits_tab, data_view_tab = st.tabs(["file_stats", "load_audits", "data_view"])

    with file_stats_tab:
        st.header("File Stats")
        
        spdf = get_fileheader_info(data_file)
        st.dataframe(spdf)

    with load_audits_tab:
        st.header("Audits")
        
        st.write('## Loaded segment stats')

        spdf = get_segments_loaded_stats(data_file)
        #progress bar for percentage of loading
        df = spdf.to_pandas()

        if len(df) > 0:
            total_segments_in_file = df['TOTAL_SEGMENTS_IN_FILE'][0]
            segment_count = df['SEGMENT_COUNT'][0]
            percentage_loaded = float( (segment_count/total_segments_in_file)*100 )
            st.text(f'Percentage of files loaded: {percentage_loaded}')
            st.progress(percentage_loaded/100)
        st.dataframe(spdf)

        st.write('## Tasks ingestion status detail')
        #progress bar for tasks still running
        
        spdf2 = get_tasks_ingestion_stats(data_file)
        executing_task_count = len( spdf2.to_pandas() )

        if executing_task_count > 0:
            task_count_df = get_total_tasks_count(data_file).to_pandas()
            total_task_count = task_count_df['TASK_COUNT'][0]
            task_execution_percentage = float( (executing_task_count/total_task_count)*100 )
            st.text(f'Percentage of tasks completed: {task_execution_percentage}')
            st.progress(task_execution_percentage/100)

        st.dataframe(spdf2 ,use_container_width=True)

        st.write('## Sample list of files staged')
        spdf3 = get_files_staged(data_file)
        st.dataframe(spdf3 ,use_container_width=True)

        st.write('## Time taken for ingestion')
        spdf = get_file_ingestion_elapsed(data_file)
        st.dataframe(spdf)

    with data_view_tab:
        st.header("Data View")
        
        st.write('## Negotiation Arrangments headers')
        spdf = get_segments_stats(data_file)
        st.dataframe(spdf)

        st.write('## Sample rows from negotiation arrangements')
        spdf32 = get_segments_chunks_sample(data_file)
        st.dataframe(spdf32 ,use_container_width=True)        


# ----------------------------
if __name__ == "__main__":
    data_files = cache_ingested_data_files()

    if 'data_files' not in st.session_state:
        st.session_state['data_files'] = data_files

    if 'data_file' not in st.session_state:
        st.session_state['data_file'] = data_files[0]

    build_ui()








