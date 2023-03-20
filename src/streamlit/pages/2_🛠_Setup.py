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
        c1, s1, c2, s2, c3, s3, c4, s4, c5 = st.columns([1, 0.1, 2.5, 0.1, 2.5, 0.1, 1, 0.1, 1])
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
            st.metric("IOTBridge staging database", f"{config['APP_DB']['database']}.{config['APP_DB']['schema']}")

        with c3:
            st.metric("Modelling database", f"{config['APP_DB']['database']}.{config['APP_DB']['schema']}")

        with c4:
            st.metric("Warehouse", f"{config['SNOW_CONN']['warehouse']}")

        with c5:
            st.metric("Role", f"{config['SNOW_CONN']['role']}")

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)

def build_UI():
    custom_page_styles()
    page_header()
    st.markdown(
        """<hr style="height:2px; width:100%; border:none;color:lightgrey;background-color:lightgrey;margin-bottom: 16px; margin-top: 25px;" /> """,
        unsafe_allow_html=True)

    display_connection_info()

    fc1, fc_line, fc2 = st.columns([0.75, 0.1, 1.25])
    with fc1:
        st.caption('Features')
        st.markdown('''
            The demo uses the following functionality:
            - Snowpark (Python)
            - DAGs & Tasks
            - Dynamic File Access (Python) - PrPr
        ''')

    with fc2:
        st.caption('Pre-requisite')
        st.markdown(f'''
            In order for running this demo, be aware of roles and actions involved:
            - We use *SYSADMIN* to create databases and transfer ownership to role {config['SNOW_CONN']['role']}
            - We use *SECURITYADMIN* to create custom role {config['APP_DB']['task_role']}
            - We use *ACCOUNTADMIN* to grant task execution privilege to role {config['APP_DB']['task_role']}
            - We will be creating tasks & building DAGS
            - Optionally we will be creating warehouses, to achieve parallelism
            - It is recommended, after execution if the database are not going to be used. Drop the database:
                -  {config['APP_DB']['database']}
                to avoid unnecessary charge/credits
            - The code has been developed specifically in Mac (bash) and might be compatible with linux. For windows, 
              certain steps/options might not work out. For ex docker usage.
        ''')
    with fc_line:
        st.markdown(
            """<div style="border: 1px solid lightgrey; height: 325px; width:2px; background-color: lightgrey;"></div> """,
            unsafe_allow_html=True)

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)

    with st.expander("Step 1 - Setup Database and Schemas"):
        script_output_1 = st.empty()
        st11, st12 = st.columns([0.3, 0.7])

        with st11:
            iot_bridge_btn = st.button(' ▶️  Setup database')
        with st12:
            if iot_bridge_btn:
                with st.spinner("Database is getting ready, please wait.."):
                    try:
                        exec_sql_script('./src/sql-script/1_setup.sql', 'script_output_1')
                    except:
                        error_page.build_UI(sys.exc_info(), False)

        st.markdown("""<hr style="height:20px; font-size:2px; width:99%; border:none;color:none;" /> """,
                    unsafe_allow_html=True)
        
        if 'script_output_1' in st.session_state:
            st.write("Script Output")
            st.json(st.session_state['script_output_1'])

    with st.expander("Step 2 - Create external stage"):
        script_output_2 = st.empty()
        with script_output_2.container():
            desc = f'''
                This steps requires manual intervention. you would need to create
                an [external stage](https://docs.snowflake.com/en/sql-reference/sql/create-external-table.html)with name 
                "{config['APP_DB']['ext_stage']}". 
                
                An example I had used the following command for AWS:

                ```sh
                    use role {config['SNOW_CONN']['role']};
                    use warehouse {config['SNOW_CONN']['warehouse']};
                    use schema {config['APP_DB']['database']}.public;

                    create or replace stage ext_data_stg
                    url = 's3://sf-gsi-XYZ/'
                    credentials = ( AWS_KEY_ID = 'ABCDEF' AWS_SECRET_KEY = '1234EFG' )
                    directory = ( enable = true refresh_on_create = false )
                    ;
                ```
            '''
            st.write(desc)
        

        # with st112:
        #     model_db_btn = st.button(' ▶️  Setup Modelling Database')
        # with st122:
        #     if model_db_btn:
        #         with st.spinner("Modelling Database is getting ready, please wait.."):
        #             try:
        #                 exec_sql_script('./src/sql-script/1_setup.sql', 'script_output')
        #             except:
        #                 error_page.build_UI(sys.exc_info(), False)

        if 'script_output' in st.session_state:
            st.write("Script Output")
            st.json(st.session_state['script_output'])

    with st.expander("Step 3 - Define Functions & Procedures", False):
        script_output_3 = st.empty()
        st21, st22 = st.columns([0.3, 0.7])

        with st21:
            fns_btn = st.button(' ▶️  Define Functions & Procedures')
        with st22:
            if fns_btn:
                with st.spinner("Required UDFs and Stored Procs are getting created, please wait.."):
                    try:
                        exec_sql_script('./src/sql-script/3_define_fns.sql', 'script_output_3')
                    except:
                        error_page.build_UI(sys.exc_info(), False)

        if 'script_output_3' in st.session_state:
            st.write("Script Output")
            st.json(st.session_state['script_output_3'])

    with st.expander("Step 4 - Define views & tables", False):
        script_output_4 = st.empty()
        st31, st32 = st.columns([0.3, 0.7])

        with st31:
            samp_ds_btn = st.button(' ▶️  Define views & tables')
        with st32:
            if samp_ds_btn:
                with st.spinner("please wait.."):
                    try:
                        exec_sql_script('./src/sql-script/5_define_external_tables.sql', 'script_output_4')
                    except:
                        error_page.build_UI(sys.exc_info(), False)

        if 'script_output_4' in st.session_state:
            st.write("Script Output")
            st.json(st.session_state['script_output_4'])

if __name__ == '__main__':
    build_UI()


