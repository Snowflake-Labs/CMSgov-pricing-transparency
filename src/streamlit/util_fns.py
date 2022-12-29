
import configparser ,json ,logging
import os ,sys ,subprocess
from snowflake.snowpark.session import Session
from pathlib import Path
import logging ,sys ,os 
import streamlit as st
import pandas as pd

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

logger = logging.getLogger('app')

def exec_sql_script(p_sqlscript: str ,p_component) -> bool:
    '''
        Executes a sql script and writes the output to a textbox.
    '''
    script_executed = False
    logger.info(f'Executing sql script: {p_sqlscript} ...')
    
    process = subprocess.Popen(
        ['./bin/exec_sqlscript.sh'
        ,p_sqlscript]
        ,stdout=subprocess.PIPE
        ,universal_newlines=True)

    while True:
        output = process.stdout.readline()
        st.write(output)
        return_code = process.poll()
        
        if return_code is not None:
            st.write(f'RETURN CODE: {return_code} \n')
            script_executed = True

            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                st.write(output)

            break

    st.write('\n --- Finished executing script ---')
    return script_executed


def load_sample_and_display_table(p_session: Session ,p_table: str ,p_sample_rowcount :int):
    '''
    Utility function to display sample records 
    '''
    st.write('sampling target table ...')
    tbl_df = (p_session
        .table(p_table)
        .sample(n=p_sample_rowcount)
        .to_pandas())

    st.dataframe(tbl_df)

def list_stage(p_session: Session ,p_stage :str):
    '''
    Utility function to display contents of a stage
    '''
    rows = p_session.sql(f''' list @{p_stage}; ''').collect()
    data = []
    for r in rows:
        data.append({
            'name': r['name']
            ,'size': r['size']
            ,'last_modified': r['last_modified']
        })

    df = pd.json_normalize(data)
    return df