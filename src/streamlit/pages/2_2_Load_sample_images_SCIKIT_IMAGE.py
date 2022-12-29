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
logger = logging.getLogger('exec_sql_script')

st.markdown(f"# Stage sample image dataset")
st.markdown(f"**NOTE:** This will take anywhere from 15-20 min range.")

st.write("""
    Lorem ipsum...
""")

# Initialize a session with Snowflake
config = None
sp_session = None
if "snowpark_session" not in st.session_state:
    config = L.get_config(PROJECT_HOME_DIR)
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['APP_DB']['snow_opt_wh']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
import pandas as pd
import numpy as np
import datetime, os
import snowflake.snowpark.functions as F

parsed_image_tbl = 'image_parsed_raw_skimage'
img_stg = st.text_input('Stage containing the images files', 'data_stg') 

def load_and_stage_sample_data_images():
    ret = {}
    start = datetime.datetime.now()
    
    st.write(f'Starting load @{start}')

    parsing_sql_stmt = f'''
        with base as (
            select
                relative_path
                ,concat('@{img_stg}/',relative_path) as full_image_path
                ,skimage_parser_fn(full_image_path) as parsed_image_info
            from directory(@{img_stg})
            limit 10
        )
        select
            PARSED_IMAGE_INFO as p
            ,p:image_filepath::varchar as image_filepath
            ,p:parsing_status::varchar as parsing_status
            ,p:parsing_exception::varchar as parsing_exception
            ,p:image_array_shape_0::number as image_array_shape_0
            ,p:image_array_shape_1::number as image_array_shape_1
            -- ,replace(to_variant( p:image_array::varchar ) ,'"' ,'') as image_array
            -- ,replace(to_variant( p:normalized_image_array::varchar ) ,'"' ,'') as normalized_image_array
            -- ,replace(to_variant( p:resized_feature::varchar ) ,'"' ,'') as resized_feature

            ,p:image_array::variant as image_array
            ,p:normalized_image_array::variant as normalized_image_array
            ,p:resized_feature::variant as resized_feature
        from base
    '''   

    ctas_stmt = f''' create or replace transient table {parsed_image_tbl} as
    select * from (
    {parsing_sql_stmt}
    );
    '''

    st.write('Creating table: image_parsed_raw_skimage ...')
    sp_session.sql(ctas_stmt).collect()

    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '
    st.write(f'Total elapsed time: {elapsed}')

    U.load_sample_and_display_table(sp_session ,parsed_image_tbl ,10)

    st.write('Finished!!!')

    ret['status'] = True
    return ret

# ---------------
script_output = st.empty()
with script_output:
    st.button('Load sample images'
            ,on_click = load_and_stage_sample_data_images
        )

    