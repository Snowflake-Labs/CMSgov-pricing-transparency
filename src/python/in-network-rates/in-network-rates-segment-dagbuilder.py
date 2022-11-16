import sys ,os ,io ,json ,logging
import pandas as pd
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
import random
import string

logger = logging.getLogger("innetwork_rates_segment_dagbuilder_sp")

BUCKETS = 50
BATCH_SIZE = 5000
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header'

def create_root_task_and_fh_loader(p_session: Session ,p_root_task_name: str ,p_stage_path: str ,p_datafile: str): 
    logger.info(f'Creating the root task ddl ...')

    sql_stmts = [
        f'alter task if exists {p_root_task_name} suspend;'
        ,f'''
            create or replace task {p_root_task_name}
                schedule = 'using cron 30 2 L 6 * UTC'
                comment = 'DAG to load data for file: {p_datafile}'
                as
                select current_timestamp;
        '''
        # ,f'''
        # create or replace task fh_{p_root_task_name}
        #     user_task_managed_initial_warehouse_size = 'XSMALL'
        #     comment = 'file header data ingestor for file: {p_datafile}'
        #     after {p_root_task_name} 
        #     as 
        #     call innetwork_rates_fileheader_ingestor_sp('{p_stage_path}','{p_datafile}');
        # '''
        # ,f'alter task if exists fh_{p_root_task_name} resume;'
    ]
    for stmt in sql_stmts:
        p_session.sql(stmt).collect()


def split_range_into_buckets(p_range_max, p_num_buckets):
    step = p_range_max / p_num_buckets
    return [(round(step*i), round(step*(i+1))) for i in range(p_num_buckets)]

def iterate_define_ddl(p_session: Session ,p_root_task_name: str 
    ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,p_buckets: int): 
    logger.info(f'Creating the task ddl ...')

    # DDL task creation
    task_ddls = []
    df =(p_session.table(IN_NETWORK_RATES_SEGHDR_TBL)
            .select('REC_NUM' ,'HEADER_ID_HASH' ,'HEADER_ID' ,'DATA_FILE')
            .filter(F.col('DATA_FILE') == F.lit(p_datafile))
            .to_pandas()
        )

    # Split the segments ranges into buckets
    max_rec_num = df['REC_NUM'].max()
    l_buckets = max(BUCKETS ,p_buckets) - 4;
    ranges = split_range_into_buckets(max_rec_num ,l_buckets)
    tm = p_datafile.replace('-','_').replace('.zip','')

    for idx,(m ,n) in enumerate(ranges):
        task_name = f'''T_{tm}_{m}_{n}'''
        
        ddl = f'''
            create or replace task {task_name}
                WAREHOUSE = dev_pctransperancy_demo_wh
                comment = 'negotiated_rates rec num range [{m} - {n}] data ingestor for file: {p_datafile}'
                after {p_root_task_name}
                as
                call innetwork_rates_segments_ranges_ingestor_sp( 
                    {p_approx_batch_size} ,'{p_stage_path}','{p_datafile}'
                    ,'negotiated_rates'
                    ,{m} ,{n} ,'{task_name}');
            '''
        task_ddls.append( (task_name ,ddl) )
        
    
    return task_ddls


def define_subtasks_suspender(p_session: Session ,p_datafile: str ,p_root_task_name: str ,p_buckets: int ,p_predecessor_tasks):
    predecessor_tasks_str = ','.join(p_predecessor_tasks)
    
    letters = string.ascii_uppercase
    tm = ''.join(random.choice(letters) for i in range(10))
    # tm = p_datafile.replace('-','_').replace('.zip','')
    task_name = f'''T_{tm}_dag_suspender'''
    ddl = f'''
            create or replace task {task_name}
                WAREHOUSE = dev_pctransperancy_demo_wh
                comment = 'terminal task for sub tasks of root task: {p_root_task_name}'
                after {predecessor_tasks_str}
                as
                call innetwork_rates_dagsuspender('{p_root_task_name}' ,'{p_datafile}' ,{p_buckets});
            '''
    p_session.sql(ddl).collect()

    p_session.sql(f'alter task {task_name} resume').collect()
    return task_name


def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ,p_buckets: int):
    ret = {}
    ret['data_file'] = p_datafile

    root_task_name = f'''DAG_INNETWORK_LOAD_{p_datafile.replace('-','_').replace('.zip','')}'''
    create_root_task_and_fh_loader(p_session ,root_task_name ,p_stage_path ,p_datafile)

    task_def_errors = []
    task_ddls = iterate_define_ddl(p_session ,root_task_name ,p_approx_batch_size ,p_stage_path ,p_datafile ,p_buckets)
    idx = 0
    for task_name ,tddl in task_ddls:
        try:
            sql_stmts = [
                f'alter task if exists {task_name} suspend;'
                ,tddl
                ,f'alter task if exists {task_name} resume;'
            ]
            for stmt in sql_stmts:
                p_session.sql(stmt).collect()
            
            # sql_stmt = f'execute task {task_name};'
            # p_session.sql(sql_stmt).collect()

        except Exception as e: 
            task_def_errors.append(task_name)
            logger.info(f"Not able to define task: {task_name}")
            ret['task_ddl_exception'] = str(e)
            ret['task_ddl'] = tddl
            ret['task_ddl_idx'] = idx
            ret['failed_task'] = task_name
            break
        idx += 1
        

    predecessor_sub_tasks = [ task_name for task_name ,tddl in task_ddls ]
    suspender_task = define_subtasks_suspender(p_session ,p_datafile ,root_task_name ,p_buckets ,predecessor_sub_tasks)
    ret['suspender_task'] = suspender_task

    p_session.sql(f'alter task if exists {root_task_name} resume;').collect()
           
    # ret['task_errored'] = task_def_errors    
    ret['root_task_name'] = task_ddls

    # -- TODO
    # execute root task
    p_session.sql(f'execute task {root_task_name};').collect()

    ret['root_task_name'] = root_task_name
    ret['no_of_subtasks_created'] = len(task_ddls)
    
    ret['status'] = True
    return ret