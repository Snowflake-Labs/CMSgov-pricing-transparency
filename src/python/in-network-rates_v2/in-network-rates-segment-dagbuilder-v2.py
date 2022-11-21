import sys ,os ,io ,json ,logging
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import hashlib
from typing import List

logger = logging.getLogger("innetwork_rates_segment_dagbuilder_sp")
BATCH_SIZE = 5000
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header'
TASK_TO_SEGMENTIDS_TBL = 'task_to_segmentids'

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
    # tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
    #     ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
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

def fold_and_balance(p_df: pd.DataFrame ,p_target_df_len: int):
    df = p_df
    df.sort_values('NEGOTIATED_RATES_COUNT' ,inplace=True ,ascending=False)

    for i in range((len(p_df)%2)):
        data = [['DUMMY', 0]]
        tail_df = pd.DataFrame(data, columns=['SEGMENT_IDS', 'NEGOTIATED_RATES_COUNT'])
        df = pd.concat([df ,tail_df])

    middle = int( len(df)/2 )
    df_t = df.iloc[:middle].copy(deep=True)
    df_b = df.iloc[-1*middle:].copy(deep=True)
    df_b.sort_values('NEGOTIATED_RATES_COUNT' ,inplace=True ,ascending=True)

    data = []
    for i in range(len(df_t)):
        seg_ids = ','.join([
            df_t.iloc[i,0]
            ,df_b.iloc[i,0]
        ])
        nr_count = df_t.iloc[i,1] + df_b.iloc[i,1]
        data.append( (seg_ids ,nr_count) )

    d_df = pd.DataFrame(data, columns =['SEGMENT_IDS', 'NEGOTIATED_RATES_COUNT'])

    check_len = len(d_df) > p_target_df_len
    mn = d_df.NEGOTIATED_RATES_COUNT.mean()
    print(f'check len : {check_len} : {len(d_df)} : {p_target_df_len} : {(p_target_df_len - len(d_df))} : {mn} ')
    
    if(len(d_df) > p_target_df_len):
        return fold_and_balance(d_df ,p_target_df_len)
    
    elif (p_target_df_len - len(d_df)) > 3:
        return p_df

    return d_df

def segments_count_balance(p_session: Session ,p_datafile: str ,p_parallels: int):
    logger.info(f'Mapping tasks to segments parallel: {p_parallels} datafile {p_datafile}')

    # Get the negotiated arrangements segments to negotiated_rates_count
    sql_stmt = f'''
        select segment_id, negotiated_rates_count
        from in_network_rates_segment_header_V2
    '''
    i_df = p_session.sql(sql_stmt).to_pandas()
    i_df.columns = ['SEGMENT_IDS', 'NEGOTIATED_RATES_COUNT']
    x_df = fold_and_balance(i_df ,p_parallels)

    return x_df

def save_tasks_to_segments(p_session: Session ,p_datafile: str ,p_df :pd.DataFrame ,p_force_rerun: bool):
    logger.info('Saving tasks to segment')

    rows = []
    m = get_md5of_datafile(p_datafile)
    for idx, row in p_df.iterrows():
        task_name = f'tsk_{m}_{idx}'
        seg_ids = row['SEGMENT_IDS']
        nr_c = row['NEGOTIATED_RATES_COUNT']

        # Remove DUMMY segment id
        t = [s for s in seg_ids.split(',') if s != 'DUMMY']
        segment_count = len(t)
        segment_ids = ','.join(t)

        r = (idx ,p_datafile ,task_name
                ,segment_ids ,segment_count ,nr_c)
        rows.append(r)
    
    t_df = pd.DataFrame(rows
        ,columns=['BUCKET' ,'DATA_FILE' ,'ASSIGNED_TASK_NAME' ,'SEGMENT_IDS'
            ,'SEGMENTS_COUNT', 'SEGMENTS_RECORD_COUNT'])
        
    if p_force_rerun == True:
        tbl_df = p_session.table(TASK_TO_SEGMENTIDS_TBL)
        tbl_df.delete(tbl_df["DATA_FILE"] == F.lit(f'{p_datafile}'))

    append_to_table(p_session ,t_df ,TASK_TO_SEGMENTIDS_TBL)

    return t_df

def get_md5of_datafile(p_datafile: str):
    return hashlib.md5(p_datafile.encode()).hexdigest()

def create_root_task_and_fh_loader(p_session: Session ,p_root_task_name: str ,p_datafile: str ,p_warehouse: str): 
    logger.info(f'Creating the root task {p_root_task_name} ddl ...')

    sql_stmts = [
        f'alter task if exists {p_root_task_name} suspend;' 
        ,f'''
            create or replace task {p_root_task_name}
                warehouse = {p_warehouse}
                schedule = 'using cron 30 2 L 6 * UTC'
                comment = 'DAG to load data for file: {p_datafile}'
                as
                begin
                    select current_timestamp;

                    insert into segment_task_execution_status( data_file  ,task_name) 
                        values('{p_datafile}' ,'{p_root_task_name}');
                end;
        '''
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()

def create_subtasks(p_session: Session ,p_root_task_name: str 
    ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str
    ,p_warehouse: str ,p_task_lists:List[str] ,task_matrix_shape):
    logger.info(f'Creating the task ddl ...')

    idx = 0
    line_end_tasks = []
    for m in range(task_matrix_shape[0]):
        preceding_task = p_root_task_name
        end_task_name = ''

        for n in range(task_matrix_shape[1]):
            task_name = p_task_lists[idx]

            sql_stmts = [
                f'''
                    create or replace task {task_name}
                    warehouse = {p_warehouse}
                    after {preceding_task}
                    as
                    begin
                        call innetwork_rates_segments_ingest_sp(
                            {p_approx_batch_size} ,'{p_stage_path}' ,'{p_datafile}'
                            ,'negotiated_rates'
                            ,'{task_name}');

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
    
    # for task_name in p_task_lists:
    #     sql_stmts = [
    #         f'''
    #             create or replace task {task_name}
    #             warehouse = {p_warehouse}
    #             after {p_root_task_name}
    #             as
    #             begin
    #                 call innetwork_rates_segments_ingest_sp(
    #                     {p_approx_batch_size} ,'{p_stage_path}' ,'{p_datafile}'
    #                     ,'negotiated_rates'
    #                     ,'{task_name}');

    #                 alter task if exists {task_name} suspend;
    #             end;
    #         '''
    #         ,f''' alter task if exists  {task_name} resume; '''
    #     ]
    #     for stmt in sql_stmts:
    #         p_session.sql(stmt).collect()

    return line_end_tasks

def create_term_tasks(p_session: Session ,p_datafile: str
    ,p_warehouse: str ,p_root_task_name: str  ,p_task_lists:List[str]):
    logger.info(f'Creating the task ddl ...')

    m = get_md5of_datafile(p_datafile)
    term_task_name = f'TERM_tsk_{m}'
    
    after_tasks_phrase = ','.join(p_task_lists)
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

                        drop task if exists {p_root_task_name};

                        insert into segment_task_execution_status( data_file  ,task_name) 
                            values('{p_datafile}' ,'{term_task_name}');
                    end;
            '''
            ,f''' alter task if exists  {term_task_name} resume; '''
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()

    return term_task_name

def reshape_tasks_to_matrix(p_tasks_count: int ,p_parallels: int):
    t = p_tasks_count
    m = -1
    n = -1
    for x in range(10):
        for y in range(10):
            if x*y == t:
                m = x
                n = y
                break
        if m != -1:
            break

    #choosen_shape = f'{m} X {n} => {t}'
    task_matrix_shape = (m,n)
    return task_matrix_shape

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ,p_parallels: int ,p_warehouse: str ,p_force_rerun: bool):
    ret = {}
    ret['data_file'] = p_datafile

    # map segments to tasks. the task will parse and ingest the specified segments
    seg_df = segments_count_balance(p_session ,p_datafile ,p_parallels)
    task_to_segments_df = save_tasks_to_segments(p_session ,p_datafile ,seg_df ,p_force_rerun)

    root_task_name = get_md5of_datafile(p_datafile)
    root_task_name = f'''DAG_ROOT_{root_task_name}'''

    # create root task
    create_root_task_and_fh_loader(p_session ,root_task_name ,p_datafile ,p_warehouse)

    # create sub tasks and add to root task
    task_list = list(task_to_segments_df.ASSIGNED_TASK_NAME.values)
    task_matrix_shape = reshape_tasks_to_matrix( len(task_list) ,p_parallels)
    ret['task_matrix_shape'] = task_matrix_shape
    
    line_end_tasks = create_subtasks(p_session ,root_task_name ,p_approx_batch_size ,p_stage_path ,p_datafile ,p_warehouse ,task_list ,task_matrix_shape)

    # create term task
    create_term_tasks(p_session ,p_datafile ,p_warehouse ,root_task_name ,line_end_tasks)
    
    ret['status'] = True
    return ret



# CREATE or replace task tsk_task_root
#     warehouse = dev_pctransperancy_demo_wh
#     schedule = 'using cron 30 2 L 6 * UTC'
#     AS
#     begin
#        -- truncate table dag;
#         insert into dag(task_name) values('exp_task_root');
#     end;
    
# create or replace task tsk_1
#     warehouse = dev_pctransperancy_demo_wh
#     after tsk_task_root
#     as
#     begin
#         insert into dag(task_name) values('tsk_1');
#         alter task if exists  tsk_1 suspend;
#     end;
        
# create or replace task tsk_2
#     warehouse = dev_pctransperancy_demo_wh
#     after tsk_task_root
#     as
#     begin
#         insert into dag(task_name) values('tsk_2');
#         alter task if exists  tsk_2 suspend;
#     end;

# create or replace task tsk_term
#     warehouse = dev_pctransperancy_demo_wh
#     after tsk_1 ,tsk_2
#     as
#     begin
#         alter task if exists  tsk_1 suspend;
#         alter task if exists  tsk_2 suspend;
#         alter task if exists  tsk_task_root suspend;
#         drop task if exists  tsk_1;
#         drop task if exists  tsk_2;
#         drop task if exists  tsk_task_root;
#         insert into dag(task_name) values('tsk_term');
#     end;


# alter task if exists  tsk_1 resume;
# alter task if exists  tsk_2 resume;
# alter task if exists  tsk_term resume;
# -- alter task if exists  exp_task_root resume;

# execute task tsk_task_root;