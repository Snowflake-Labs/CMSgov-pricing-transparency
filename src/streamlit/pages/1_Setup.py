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

st.markdown(f"# Execute SQL Script")
st.write("""
    This page is used for running a sample SQL script. These SQL scripts would typically involve
    such activities like creating database, stored procs, roles ,stage etc..
""")

config = None
sp_session = None
if "snowpark_session" not in st.session_state:
    config = L.get_config(PROJECT_HOME_DIR)
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['SNOW_CONN']['role']}''')
    sp_session.use_schema(f'''{config['SNOW_CONN']['database']}.{config['SNOW_CONN']['schema']}''')
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    config = L.get_config(PROJECT_HOME_DIR)
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
# Run the Setup scripts
import os ,datetime

uploaded_file_status = st.empty()
file_upload_progress_bar = st.progress(0)

with st.expander("Step 1- Setup database and schemas"):
    script_output = st.empty()
    btn_run_script = st.button('Setup database'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/1_setup.sql' ,script_output)
        )

with st.expander("Step 2- Create external stage" , False):
    script_output_2 = st.empty()
    with script_output_2.container():
        desc = f'''
            This steps requires manual intervention, you would need to create
            an [external stage](https://docs.snowflake.com/en/sql-reference/sql/create-external-table.html).
            You would need to create a stage with name "{config['APP_DB']['ext_stage']}". An example I had used the following command
            for AWS:

            ```sh
                use role {config['SNOW_CONN']['role']};
                use warehouse {config['SNOW_CONN']['warehouse']};
                use schema {config['APP_DB']['database']}.public;

                create or replace stage ext_data_stg
                directory = ( enable = true )
                url = 's3://sf-gsi-XYZ/'
                credentials = ( AWS_KEY_ID = 'ABCDEF' AWS_SECRET_KEY = '1234EFG' );
            ```
        '''
        st.write(desc)

with st.expander("Step 3- Define functions and procedures" , False):
    script_output_3 = st.empty()
    with script_output_3.container():
        st.button('Define functions and procedures'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/3_define_fns.sql' ,script_output_3)
        )