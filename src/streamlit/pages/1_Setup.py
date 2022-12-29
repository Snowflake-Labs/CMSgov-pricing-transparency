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
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['APP_DB']['snow_opt_wh']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
# Run the Setup scripts
import os ,datetime

uploaded_file_status = st.empty()
file_upload_progress_bar = st.progress(0)

def upload_images_to_data_stage():
    logger.info(f" Uploading images to stage: data_stg ... ")
    l_stage = 'data_stg'
    l_stage_dir = '/images'
    l_data_dir = os.path.join(PROJECT_HOME_DIR ,'data')

    # get the list of folders where images are present
    data_dirs = { path for path, subdirs, files in os.walk('./data') for name in files if '.jpeg' in name }
    total_dirs = len(data_dirs)

    for idx, img_dir in enumerate(data_dirs):
        
        # build the path to where the file will be staged
        stage_dir = img_dir.replace(l_data_dir , l_stage_dir)

        if idx == 0:
            uploaded_file_status.write(f'Uploading dir {img_dir} ...')

        print(f'    {img_dir} => @{l_stage}{stage_dir}')
        sp_session.file.put(
            local_file_name = f'{img_dir}/*.jpeg'
            ,stage_location = f'{l_stage}{stage_dir}'
            ,auto_compress=False ,overwrite=True ,parallel=20 )

        perc = (100//total_dirs)*idx
        perc_str = '{:.2f}%'.format(perc)
        file_upload_progress_bar.progress(perc)
        uploaded_file_status.write(f'Percentage : {perc_str}  uploaded dir count : {idx} total dirs : {total_dirs} : uploaded folder: {img_dir}')

    sp_session.sql(f'alter stage {l_stage} refresh; ').collect()

with st.expander("Step 1- Setup database and schemas"):
    script_output = st.empty()
    btn_run_script = st.button('Setup database'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/1_setup.sql' ,script_output)
        )

with st.expander("Step 2- Define functions and procedures" , False):
    script_output_2 = st.empty()
    with script_output_2.container():
        st.button('Define functions and procedures'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/2_define_fns.sql' ,script_output_2)
        )

with st.expander("Step 3- Upload sample images to stage" , False):
    start = datetime.datetime.now()
    script_output_2 = st.empty()
    st.write(f' **NOTE** THIS WOULD TAKE A LONG TIME AS WE HAVE TO UPLOAD INDIVIDUAL IMAGE FILES RECURSIVELY. Current time {start}')
    with script_output_2.container():
        st.button('Upload'
            ,on_click=upload_images_to_data_stage
        )