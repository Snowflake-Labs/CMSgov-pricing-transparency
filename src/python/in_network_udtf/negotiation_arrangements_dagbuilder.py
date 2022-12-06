import sys ,os ,io ,json ,logging
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import hashlib
from typing import List

TASK_TO_SEGMENTIDS_TBL = 'task_to_segmentids'
ASSUMED_TOTAL_NR_SEGMENTS = 1000 * 20

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger("negotiation_arrangements_dagbuilder")

def get_basename_of_datafile(p_datafile:str) -> str:
    base = os.path.basename(p_datafile)
    base = base.replace('-','_')
    fl_base = os.path.splitext(base)
    return fl_base[0]

def get_snowpark_dataframe(p_session: Session ,p_df: pd.DataFrame):
    # Convert the data frame into Snowpark dataframe, needed for merge operation
    sp_df = p_session.createDataFrame(p_df)

    # The column names gets defined in the snowpark dataframe in a case sensitive manner
    # hence rename them into a non case sensitive manner
    for c in p_df.columns:
        sp_df = sp_df.with_column_renamed(F.col(f'"{c}"'), c.upper())

    return sp_df

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch to table [{p_target_tbl}] ...')
    
    # Convert the data frame into Snowpark dataframe, needed for merge operation
    sp_df = get_snowpark_dataframe(p_session ,p_df)
    
    #Doc: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Table.merge.html#snowflake.snowpark.Table.merge
    target_table = p_session.table(p_target_tbl)
    
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) & (target_table['ASSIGNED_TASK_NAME'] == sp_df['ASSIGNED_TASK_NAME'])
        ,[
          F.when_not_matched().insert({ 
            'BUCKET': sp_df['BUCKET']
            ,'DATA_FILE': sp_df['DATA_FILE']
            ,'ASSIGNED_TASK_NAME': sp_df['ASSIGNED_TASK_NAME']
            ,'SEGMENT_IDS': sp_df['SEGMENT_IDS']
            ,'SEGMENTS_COUNT': sp_df['SEGMENTS_COUNT']
            ,'SEGMENTS_RECORD_COUNT': sp_df['SEGMENTS_RECORD_COUNT']
            })
        ])

    return merged_df

def split_range_into_buckets(p_range_max, p_num_buckets):
    step = p_range_max / p_num_buckets
    return [(
        round(step*i)+1 if i >= 1 else 0
        ,round(step*(i+1))) for i in range(p_num_buckets)]

def reshape_tasks_to_matrix(p_tasks_count: int ,p_parallel: int):
    t = p_tasks_count
    m = -1
    n = -1
    # Keep the matrix in 20 X 10 matrix
    for x in range(p_parallel):
        for y in range(p_parallel):
            if x*y == t:
                m = max(x ,y)
                n = min(x, y)
                break
        if m != -1:
            break

    #choosen_shape = f'{m} X {n} => {t}'
    task_matrix_shape = (m,n)
    return task_matrix_shape

def create_root_task_and_fh_loader(p_session: Session  
    ,p_stage_path: str ,p_datafile: str ,p_warehouse: str): 
    logger.info(f'Creating the root task ddl ...')

    fl_basename = get_basename_of_datafile(p_datafile)
    root_task_name = f'''DAG_ROOT_{fl_basename}'''
    fh_task_name = f'tsk_fh_{fl_basename}'

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
        # ,f'''
        # create or replace task fh_{fh_task_name}
        #     warehouse = {p_warehouse}
        #     comment = 'file header data ingestor for file: {p_datafile}'
        #     after {p_root_task_name} 
        #     as 
        #     call innetwork_rates_fileheader_ingestor_sp('{p_stage_path}','{p_datafile}');
        # '''
        # ,f'alter task if exists fh_{p_root_task_name} resume;'
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()

    return root_task_name ,fh_task_name

def save_tasks_to_segments(p_session: Session ,p_datafile: str ,p_parallel: int ,p_force_rerun: bool):
    logger.info('Saving tasks to segment')

    rows = []
    fl_basename = get_basename_of_datafile(p_datafile)

    splits = split_range_into_buckets(ASSUMED_TOTAL_NR_SEGMENTS,p_parallel)
    logger.info(f'task splits : {len(splits)} ')
    #task_matrix_shape = reshape_buckets_to_matrix(splits ,parallel_rows)
    #print(task_matrix_shape)

    for idx,(m ,n) in enumerate(splits):
        task_name = f'''T_{fl_basename}_{m}_{n}'''
        splits_info = f'{m} X {n}'
        segment_count = n - m
        
        r = (idx ,p_datafile ,task_name
                ,splits_info ,segment_count ,-1)
        rows.append(r)
    
    t_df = pd.DataFrame(rows
        ,columns=['BUCKET' ,'DATA_FILE' ,'ASSIGNED_TASK_NAME' ,'SEGMENT_IDS'
            ,'SEGMENTS_COUNT', 'SEGMENTS_RECORD_COUNT'])
        
    if p_force_rerun == True:
        tbl_df = p_session.table(TASK_TO_SEGMENTIDS_TBL)
        tbl_df.delete(tbl_df["DATA_FILE"] == F.lit(f'{p_datafile}'))

    append_to_table(p_session ,t_df ,TASK_TO_SEGMENTIDS_TBL)

    return t_df

def create_subtasks(p_session: Session ,p_root_task_name: str 
    ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ,p_target_stage: str
    ,p_warehouse: str ,p_task_lists:List[str] ,task_matrix_shape):
    logger.info(f'Creating the task ddl ...')
    line_end_tasks = []
    
    idx = 0
    for m in range(task_matrix_shape[0]):
        preceding_task = p_root_task_name
        end_task_name = ''

        for n in range(task_matrix_shape[1]):
            task_name = p_task_lists[idx]
            
            #taskname format: task_name = f'''T_{fl_basename}_{m}_{n}'''
            l = len(task_name.split('_'))
            range_to = task_name.split('_')[l-1]
            range_from = task_name.split('_')[l-2]

            # 86400000 => 1 day
            # 3600000 => 1 hour
            sql_stmts = [
                f'''
                    create or replace task {task_name}
                    warehouse = {p_warehouse}
                    user_task_timeout_ms = 86400000
                    comment = 'negotiated_arrangements segment range [{range_from} - {range_to}] data ingestor '
                    after {preceding_task}
                    as
                    begin
                        call innetwork_rates_segheader(
                            {p_approx_batch_size} 
                            ,'{p_stage_path}' ,'{p_datafile}' ,'{p_target_stage}' 
                            ,{range_from} ,{range_to} ,'{task_name}');
                        
                        alter task if exists {task_name} suspend;
                    end;
                '''
                ,f''' alter task if exists  {task_name} resume; '''
            ]
            for stmt in sql_stmts:
                p_session.sql(stmt).collect()

            end_task_name = task_name
            preceding_task = task_name
            idx += 1
        
        if end_task_name != '':
            line_end_tasks.append(end_task_name)
    
    return line_end_tasks

def create_term_tasks(p_session: Session ,p_datafile: str
    ,p_warehouse: str ,p_root_task_name: str  ,p_line_end_task_lists:List[str] ,p_task_lists:List[str]):
    logger.info(f'Creating the task ddl ...')

    m = get_basename_of_datafile(p_datafile)
    term_task_name = f'TERM_tsk_{m}'
    
    after_tasks_phrase = ','.join(p_line_end_task_lists)
    task_stmts = []
    for task_name in p_task_lists:
        task_stmts.append(f''' alter task if exists  {task_name} suspend; ''')
        task_stmts.append(f''' drop task if exists  {task_name}; ''')
    
    task_stmts_str = '\n'.join(task_stmts)
    sql_stmts = [
            f'''
                create or replace task {term_task_name}
                    warehouse = {p_warehouse}
                    after {after_tasks_phrase}
                    as
                    begin
                        {task_stmts_str}

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

def main(p_session: Session ,p_approx_batch_size: int 
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_parallel: int ,p_warehouse: str 
    ,p_force_rerun: bool):
    ret = {}
    ret['data_file'] = p_datafile

    # map segments to tasks. the task will parse and ingest the specified segments
    task_to_segments_df = save_tasks_to_segments(p_session ,p_datafile ,p_parallel ,p_force_rerun)

    # create root task
    root_task_name, fh_task_name = create_root_task_and_fh_loader(p_session  
        ,p_stage_path ,p_datafile ,p_warehouse)
    ret['root_task'] = root_task_name

    # create sub tasks and add to root task
    task_list = list(task_to_segments_df.ASSIGNED_TASK_NAME.values)
    task_matrix_shape = reshape_tasks_to_matrix( len(task_list) ,p_parallel)
    ret['task_matrix_shape'] = task_matrix_shape
    
    line_end_tasks = create_subtasks(p_session ,root_task_name ,p_approx_batch_size 
        ,p_stage_path ,p_datafile ,p_target_stage
        ,p_warehouse ,task_list ,task_matrix_shape)
    # line_end_tasks.append(fh_task_name)

    # create term task
    term_task = create_term_tasks(p_session ,p_datafile ,p_warehouse ,root_task_name ,line_end_tasks ,task_list)
    
    ret['status'] = True
    return ret


#-----------
