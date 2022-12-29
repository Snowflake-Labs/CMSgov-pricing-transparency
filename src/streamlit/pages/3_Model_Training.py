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

st.markdown(f"# Model Training")
st.markdown(f"**NOTE:** This will take anywhere upwards of 5 mins.")
st.write("""
    Lorem Ipsum ....
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
import time ,json

def perform_model_training(p_image_count: int ,p_epoch: int):
    st.write('Performing training ...')

    t = time.process_time()
    stmt = f''' call train_pneumonia_identification_model(
            {p_image_count} 
            ,'@model_stg' 
            ,'{config['APP_DB']['model_flname']}' 
            ,{p_epoch}); '''
    out_df = sp_session.sql(stmt).collect()
    elapsed_time = (time.process_time() - t) #/60

    st.write(f'Total execution time for training: {elapsed_time} minutes')
    
    res = out_df[0]['TRAIN_PNEUMONIA_IDENTIFICATION_MODEL']
    res_j = json.loads(res)
    st.json(res_j)
    logger.info(res_j)

    stg_df = U.list_stage(sp_session ,'model_stg')
    st.dataframe(stg_df)

    return

# =============================

txt_image_count = st.number_input('Image count' ,value=10*1000 ,min_value=1000 ,max_value=30*1000 )
txt_epoch = st.number_input('Training epoch' ,value=3 ,min_value=1 ,max_value=30)

script_output = st.empty()
with script_output:
    st.button('Train model'
            ,on_click = perform_model_training
            ,args = (txt_image_count ,txt_epoch)
        )