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

import sys ,os ,io ,json ,logging
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import hashlib
from typing import List
from sp_commons import *
import re

TASK_TO_SEGMENTIDS_TBL = 'task_to_segmentids'

# Use this matrix configuration for files that 
# are smaller < 1TB for ex from Priority health
DAG_MATRIX_SHAPE = (5,15)

# Use this matrix configuration for files that 
# are larger like 1TB sizes from CIGNA
# DAG_MATRIX_SHAPE = (10,8)

# 86400000 => 1 day
# 3600000 => 1 hour
USER_TASK_TIMEOUT = 86400000

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger("in_network_rates_dagbuilder")

def delete_taskdefinitions_for_datafile(p_session: Session ,p_datafile: str):
    logger.info(f'Cleaning up tasks that were defined for the data file [{p_datafile}] ...')

    fl_basename = get_basename_of_datafile(p_datafile)
   
    # Delete all task definitions that were previously defined for this data file
    tbl_df = p_session.table(TASK_TO_SEGMENTIDS_TBL)
    tbl_df.delete(tbl_df["DATA_FILE"] == F.lit(f'{p_datafile}'))

    # Iterate defined tasks 
    current_db = p_session.get_current_database()
    p_session.sql(f' SHOW TASKS IN  DATABASE {current_db}; ').collect()

    stmt = f'''
        select "name" as task_name
        from table(result_scan(last_query_id()))
        where "name" like '%{fl_basename.upper()}%';
    '''
    rows = p_session.sql(stmt).collect()
    for r in rows:
        p_session.sql(f'''alter task if exists {r['TASK_NAME']} suspend;'''  ).collect()
        p_session.sql(f'''drop task if exists {r['TASK_NAME']};'''  ).collect()

    return True

def append_to_table(p_session: Session ,p_df: pd.DataFrame):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas
    logger.info(f'Inserting tasks to segments mapping info to table [{TASK_TO_SEGMENTIDS_TBL}] ...')
    
    # Convert the data frame into Snowpark dataframe, needed for merge operation
    sp_df = get_snowpark_dataframe(p_session ,p_df)
    
    #Doc: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Table.merge.html#snowflake.snowpark.Table.merge
    target_table = p_session.table(TASK_TO_SEGMENTIDS_TBL)
    
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) & (target_table['ASSIGNED_TASK_NAME'] == sp_df['ASSIGNED_TASK_NAME'])
        ,[
          F.when_not_matched().insert({ 
            'BUCKET': sp_df['BUCKET']
            ,'DATA_FILE': sp_df['DATA_FILE']
            ,'ASSIGNED_TASK_NAME': sp_df['ASSIGNED_TASK_NAME']
            ,'SEGMENTS_RECORD_COUNT': sp_df['SEGMENTS_RECORD_COUNT']
            ,'FROM_IDX': sp_df['FROM_IDX']
            ,'TO_IDX': sp_df['TO_IDX']
            })
        ])

    return merged_df

def split_range_into_buckets(p_items_per_bucket, p_num_buckets):
    step = p_items_per_bucket #p_range_max / p_num_buckets
    return [(
        round(step*i)+1 if i >= 1 else 0
        ,round(step*(i+1))) for i in range(p_num_buckets)]

def get_task_name(p_fl_basename ,p_m ,p_n):
    fl_name = get_cleansed_file_basename(p_fl_basename)
    return f'''T_{fl_name}_{p_m}_{p_n}'''

def save_tasks_to_segments(p_datafile: str ,p_segments_per_task: int):
    logger.info('Saving tasks to segment')

    rows = []
    fl_basename = get_cleansed_file_basename(p_datafile)
    
    splits = split_range_into_buckets(p_segments_per_task, DAG_MATRIX_SHAPE[0] * DAG_MATRIX_SHAPE[1])
    logger.info(f'task splits : {len(splits)} ')
    #task_matrix_shape = reshape_buckets_to_matrix(splits ,parallel_rows)
    #print(task_matrix_shape)

    is_last_split = len(splits) - 1
    for idx,(m ,n) in enumerate(splits):

        # In case this is the last split, since we are splitting rather naively
        # we need to ensure the last tasks parses till end of file and not based 
        # on the split index
        to_idx = n if idx < is_last_split else 999999999

        task_name = f'''T_{fl_basename}_{m}_{to_idx}'''
        segment_count = (to_idx - m)
        
        r = {}
        r['BUCKET'] = idx
        r['DATA_FILE'] = p_datafile
        r['ASSIGNED_TASK_NAME'] = task_name
        r['SEGMENTS_RECORD_COUNT'] = segment_count
        r['FROM_IDX'] = m
        r['TO_IDX'] = to_idx

        rows.append(r)
    
    t_df = pd.DataFrame(rows)
    return t_df

def create_root_task_and_fh_loader(p_session: Session  
    ,p_stage_path: str ,p_datafile: str ,p_warehouse: str): 
    logger.info(f'Creating the root task ddl ...')

    fl_basename = get_cleansed_file_basename(p_datafile)
    root_task_name = f'''DAG_ROOT_{fl_basename}'''
    fh_task_name = f't_fh_{fl_basename}'
    segment_header_task_name = f't_segh_{fl_basename}'
    provider_reference_task_name = f't_provref_{fl_basename}'

    sql_stmts = [
        f'alter task if exists {root_task_name} suspend;' 
        ,f'''
            create or replace task {root_task_name}
                warehouse = {p_warehouse}
                schedule = 'using cron 30 2 L 6 * UTC'
                comment = 'DAG to load data for file: {p_datafile}'
                as
                begin
                    insert into segment_task_execution_status( data_file  ,task_name) 
                        values('{p_datafile}' ,'{root_task_name}');
                end;
        '''
        ,f'''
        create or replace task {fh_task_name}
            warehouse = {p_warehouse}
            comment = 'file header data ingestor for file: {p_datafile}'
            after {root_task_name} 
            as 
            call parse_file_header('{p_stage_path}','{p_datafile}');
        '''
        ,f'alter task if exists {fh_task_name} resume;'

        ,f'''
        create or replace task {provider_reference_task_name}
            warehouse = {p_warehouse}
            comment = 'provider references segment data ingestor for file: {p_datafile}'
            after {fh_task_name} 
            as 
            call provider_references('{p_stage_path}','{p_datafile}');
        '''
        ,f'alter task if exists {provider_reference_task_name} resume;'

        ,f'''
        create or replace task {segment_header_task_name}
            warehouse = {p_warehouse}
            comment = 'segment header data ingestor for file: {p_datafile}'
            after {provider_reference_task_name} 
            as 
            call negotiation_arrangements_header('{p_stage_path}','{p_datafile}');
        '''
        ,f'alter task if exists {segment_header_task_name} resume;'
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()
        

    return root_task_name ,fh_task_name ,segment_header_task_name

def reshape_tasks_to_matrix(p_task_list):
    arr = np.asarray(p_task_list ,dtype=object)
    marr = arr.reshape(DAG_MATRIX_SHAPE[1] ,-1)
    marr = marr.transpose()

    return marr

def build_tasks_dag(p_root_task ,p_task_matrix):
    task_matrix_dag = p_task_matrix.copy()
    for r_idx ,dag_r in enumerate(task_matrix_dag):
        prev_task = p_root_task
        
        for c_idx ,dag_r_c in enumerate(dag_r):
            v = (dag_r_c ,prev_task)
            prev_task = dag_r_c
            task_matrix_dag[r_idx][c_idx] = v
    return task_matrix_dag

def create_subtasks(p_session: Session ,p_root_task_name: str 
    ,p_stage_path: str  ,p_datafile: str ,p_target_stage: str
    ,p_warehouse: str ,p_task_lists:List[str]):
    logger.info(f'Creating the task ddl ...')
    line_end_tasks = []
    
    # The following works out the logic of arranging the list into a DAG parallel matrix shape
    #  - First we build out a matrix of shape as defined by DAG_MATRIX_SHAPE
    task_matrix = reshape_tasks_to_matrix(p_task_lists)
    #  - We then iterate threw the matrix to link tasks together
    task_matrix_dag = build_tasks_dag(p_root_task_name ,task_matrix)
    #  - For each of the DAG row, we find out the last elements of the row also
    line_end_tasks = task_matrix[:,-1]

    #warehouse can be one or multiple seperated by ','
    warehouses = p_warehouse.split(',') if (',' in p_warehouse) else [p_warehouse]
    warehouses_spread = warehouses * ((DAG_MATRIX_SHAPE[0] * DAG_MATRIX_SHAPE[1]) + 2)
    wh_idx = 1

    # We now iterate through the dag matrix and define the tasks
    task_matrix_dag_asarray = task_matrix_dag.flatten()
    for task_name ,preceding_task in task_matrix_dag_asarray:
        l = len(task_name.split('_'))
        range_to = task_name.split('_')[l-1]
        range_from = task_name.split('_')[l-2]

        #determine the warehouse to use from the list of warehouses
        l_warehouse = warehouses_spread[wh_idx]
        wh_idx += 1

        sql_stmts = [
            f'''
                create or replace task {task_name}
                warehouse = {l_warehouse}
                user_task_timeout_ms = {USER_TASK_TIMEOUT}
                comment = 'negotiated_arrangements segment range [{range_from} - {range_to}] data ingestor '
                after {preceding_task}
                as
                begin
                    call parse_negotiation_arrangement_segments(
                        '{p_stage_path}' ,'{p_datafile}' ,'{p_target_stage}' 
                        ,{range_from} ,{range_to} );

                    alter task if exists {task_name} suspend;
                end;
            '''
            ,f''' alter task if exists  {task_name} resume; '''
        ]
        for stmt in sql_stmts:
            p_session.sql(stmt).collect()
    
    return line_end_tasks

def create_term_tasks(p_session: Session ,p_datafile: str ,p_target_stage: str
    ,p_warehouse: str ,p_root_task_name: str ,p_line_end_task_lists:List[str] ,p_task_lists:List[str]):
    logger.info(f'Creating the task ddl ...')

    target_stg_base = p_target_stage.replace('@','').split('/')[0]
    fl_basename = get_cleansed_file_basename(p_datafile)
    term_task_name = f'TERM_tsk_{fl_basename}'
    
    after_tasks_phrase = ','.join(p_line_end_task_lists)
    task_stmts = []

    # merge two list and remove any duplicates between the list
    tasks_to_terminate = list(set(p_task_lists) | set(p_line_end_task_lists)) 
    for task_name in tasks_to_terminate:
        task_stmts.append(f''' alter task if exists  {task_name} suspend; ''')
        # task_stmts.append(f''' drop task if exists  {task_name}; ''')
    
    task_stmts_str = '\n'.join(task_stmts)
    sql_stmts = [
            f'''
                create or replace task {term_task_name}
                    warehouse = {p_warehouse}
                    after {after_tasks_phrase}
                    as
                    begin
                        {task_stmts_str}

                        -- refresh the external stage and table, this could take sometime
                        -- based on the no of files staged
                        alter stage {target_stg_base} refresh;
                        alter external table ext_negotiated_arrangments_staged refresh;
                        
                        -- drop task if exists {p_root_task_name};

                        insert into segment_task_execution_status( data_file  ,task_name) 
                            values('{p_datafile}' ,'{term_task_name}');
                    end;
            '''
            ,f''' alter task if exists {term_task_name} resume; '''
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()

    return term_task_name

def main_matrix(p_session: Session
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_segments_per_task: int ,p_warehouse: str 
    ,p_dag_rows: int ,p_dag_cols: int):
    ret = {}
    ret['data_file'] = p_datafile

    if (p_segments_per_task <= 199) or (p_segments_per_task > 20000):
        raise Exception(f'Try to keep parameter [p_segments_per_task] between 200 and 20000.')

    delete_taskdefinitions_for_datafile(p_session ,p_datafile)

    DAG_MATRIX_SHAPE = (p_dag_rows,p_dag_cols)

    # map segments to tasks. the task will parse and ingest the specified segments
    task_to_segments_df = save_tasks_to_segments(p_datafile ,p_segments_per_task)
    append_to_table(p_session ,task_to_segments_df)

    l_warehouses = p_warehouse.split(',') if (',' in p_warehouse) else [p_warehouse]

    # create root task
    root_task_name, fh_task_name ,segment_header_task_name = create_root_task_and_fh_loader(p_session  
        ,p_stage_path ,p_datafile ,l_warehouses[0])
    ret['root_task'] = root_task_name

    # # create sub tasks and add to root task
    task_to_segments_df.sort_values(by=['BUCKET'], inplace=True)
    task_list = list(task_to_segments_df.ASSIGNED_TASK_NAME.values)
    ret['task_matrix_shape'] = DAG_MATRIX_SHAPE
    
    line_end_tasks = create_subtasks(p_session ,root_task_name 
        ,p_stage_path ,p_datafile ,p_target_stage
        ,p_warehouse ,task_list)
    
    line_end_tasks = np.append(line_end_tasks ,[segment_header_task_name])
    line_end_tasks = line_end_tasks.tolist()

    task_list.append(fh_task_name)
    
    # create term task
    term_task_name = create_term_tasks(p_session ,p_datafile ,p_target_stage ,l_warehouses[0] ,root_task_name ,line_end_tasks ,task_list)
    ret['term_task'] = term_task_name

    ret['status'] = True
    return ret

def main(p_session: Session
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_segments_per_task: int ,p_warehouse: str ):

    ret = main_matrix(p_session
    ,p_stage_path ,p_datafile ,p_target_stage
    ,p_segments_per_task ,p_warehouse
    ,5 ,15)

    return ret