
## ------------------------------------------------------------------------------------------------
# Copyright (c) 2023 Snowflake Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.You may obtain 
# a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0
    
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions andlimitations 
# under the License.
## ------------------------------------------------------------------------------------------------

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

def exec_python_script(p_pyscript: str ,p_cache_id) -> bool:
    '''
        Executes a python script and writes the output to a textbox.
    '''
    script_executed = False
    logger.info(f'Executing python script: {p_pyscript} ...')
    # Capture script output.
    script_output = []
    process = subprocess.Popen(
        ['python'
        ,p_pyscript]
        ,stdout=subprocess.PIPE
        ,universal_newlines=True)

    while True:
        output = process.stdout.readline()
        script_output.append(output)
        return_code = process.poll()
        
        if return_code is not None:
            script_output.append(f'RETURN CODE: {return_code} \n')
            script_executed = True

            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                script_output.append(output)

            break

    script_output.append('\n --- Finished executing script ---')
    st.session_state[p_cache_id] = script_output

    return script_executed
    
def exec_sql_script(p_sqlscript: str ,p_cache_id) -> bool:
    '''
        Executes a sql script and writes the output to a textbox.
    '''
    script_executed = False
    logger.info(f'Executing sql script: {p_sqlscript} ...')
    
    # Capture script output.
    script_output = []

    process = subprocess.Popen(
        ['./bin/exec_sqlscript.sh'
        ,p_sqlscript]
        ,stdout=subprocess.PIPE
        ,universal_newlines=True)

    while True:
        output = process.stdout.readline()
        # st.write(output)
        script_output.append(output)
        return_code = process.poll()
        
        if return_code is not None:
            # st.write(f'RETURN CODE: {return_code} \n')
            script_output.append(f'RETURN CODE: {return_code} \n')
            script_executed = True

            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                # st.write(output)
                script_output.append(output)

            break

    script_output.append('\n --- Finished executing script ---')
    if 'output' not in st.session_state:
        # Write the Script Output to the Session.
        st.session_state[p_cache_id] = script_output

    return script_executed

def exec_shell_script(p_shellscript: str ,p_cache_id) -> bool:
    '''
        Executes a sql script and writes the output to a textbox.
    '''
    script_executed = False
    logger.info(f'Executing shell script: {p_shellscript} ...')
    
    # Capture script output.
    script_output = []

    process = subprocess.Popen(
        ['bash'
        ,p_shellscript]
        ,stdout=subprocess.PIPE
        ,universal_newlines=True)

    while True:
        output = process.stdout.readline()
        # st.write(output)
        script_output.append(output)
        return_code = process.poll()
        
        if return_code is not None:
            # st.write(f'RETURN CODE: {return_code} \n')
            script_output.append(f'RETURN CODE: {return_code} \n')
            script_executed = True

            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                # st.write(output)
                script_output.append(output)

            break

    script_output.append('\n --- Finished executing script ---')
    if 'output' not in st.session_state:
        # Write the Script Output to the Session.
        st.session_state[p_cache_id] = script_output

    return script_executed

def load_sample_and_display_table(p_session: Session ,p_table: str ,p_sample_rowcount :int):
    '''
    Utility function to display sample records 
    '''
    st.write(f'sampling target table {p_table} ...')
    tbl_df = (p_session
        .table(p_table)
        .sample(n=p_sample_rowcount)
        .to_pandas())

    st.dataframe(tbl_df ,use_container_width=True)
    st.write(f'')

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