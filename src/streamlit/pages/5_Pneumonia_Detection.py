from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
from util_fns import exec_sql_script

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('exec_sql_script')

st.markdown(f"# Pneumonia detection")
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
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
import pandas as pd
import numpy as np
import snowflake.snowpark.functions as F

def define_inference_stored_proc():
    # As models are created only after training, we cannot pre-define this
    # stored procedure, as it needs to import the trained model file

    st.write('Defining inference function: infer_pneumonia ...')
    model_flpath = f'''@model_stg/{config['APP_DB']['model_flname']}'''
    

    # Optional todo, convert this into a snowpark like code
    sql_stmt = f'''
    create or replace function infer_pneumonia(image_array_shape_0 integer ,image_array_shape_1 integer ,resized_feature varchar)
     returns integer
     language python
     runtime_version = '3.8'
     packages = ('snowflake-snowpark-python','numpy', 'pandas', 'snowflake-snowpark-python' ,'tensorflow' ,'scikit-learn')
     imports = ('@lib_stg/scripts/pneumonia_image_inference.py' ,'{model_flpath}')
     handler = 'pneumonia_image_inference.main'
    ;
    '''
    sp_session.sql(sql_stmt).collect()

def infer_sample_rows(p_row_count: int):
    st.write('Infering pneumonia ...')

    sql_stmt = f''' 
    select 
        class_label ,class_label_num
        ,infer_pneumonia(image_array_shape_0 ,image_array_shape_1 ,resized_feature) as predicted_class_num
        ,image_filepath
    from image_parsed_raw
    limit {p_row_count}
    ;
    '''

    df = sp_session.sql(sql_stmt).to_pandas()
    return df

def run_inference(): #(p_row_count: int):
    define_inference_stored_proc()

    inferred_images_df = infer_sample_rows(10)
    st.dataframe(inferred_images_df)


txt_sample_count = st.number_input('Sample Image count' ,value=5 ,min_value=5 ,max_value=1000 )

script_output = st.empty()
with script_output:
    st.button('Run inference'
            ,on_click = run_inference
        )
