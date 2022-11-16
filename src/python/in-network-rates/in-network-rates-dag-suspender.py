import sys ,os ,io ,json ,logging
import pandas as pd
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake

logger = logging.getLogger("innetwork_rates_segment_dagbuilder_sp")

BUCKETS = 50
BATCH_SIZE = 5000
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header'

def split_range_into_buckets(p_range_max, p_num_buckets):
    step = p_range_max / p_num_buckets
    return [(round(step*i), round(step*(i+1))) for i in range(p_num_buckets)]

def get_subtasks(p_session: Session ,p_root_task_name: str): 
    logger.info(f'Getting tasks to suspend ...')

    df = p_session.sql('select current_database() as db').to_pandas()
    current_db = df['DB'].to_list()[0]
    p_session.sql(f'show tasks in database {current_db};').collect()
    # p_session.sql(f'show tasks;').collect()

    sql_stmt = f'''
        select "name" as sub_tasks
        from TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        where ARRAY_TO_STRING("predecessors",'::') like '%{p_root_task_name}%'
        ;
    '''
    df = p_session.sql(sql_stmt).to_pandas()
    sub_tasks = df['SUB_TASKS'].to_list()
    
    return sub_tasks


def split_range_into_buckets(p_range_max, p_num_buckets):
    step = p_range_max / p_num_buckets
    return [(round(step*i), round(step*(i+1))) for i in range(p_num_buckets)]

def iterate_define_ddl(p_session: Session ,p_datafile: str ,p_buckets: int): 
    logger.info(f'Creating the task ddl ...')

    # DDL task creation
    sub_tasks = []
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
        sub_tasks.append(task_name)
        
    return sub_tasks



def main(p_session: Session ,p_root_task_name: str ,p_datafile: str ,p_buckets: int):
    ret = {}
    ret['root_task_name'] = p_root_task_name

    # sub_tasks = get_subtasks(p_session ,p_root_task_name)
    sub_tasks = iterate_define_ddl(p_session ,p_datafile ,p_buckets)
    ret['no_of_subtasks'] = len(sub_tasks)

    # p_session.sql(f'alter task if exists {p_root_task_name} suspend;').collect()

    task_def_errors = []
    
    tasks_suspended = []
    for idx, task_name in enumerate(sub_tasks):
        try:
            sql_stmts = [
                f'alter task if exists {task_name} suspend;'
                ,f'drop task if exists {task_name};'
            ]
            for stmt in sql_stmts:
                p_session.sql(stmt).collect()
            tasks_suspended.append(task_name) 
        except Exception as e: 
            task_def_errors.append(task_name)
            ret['suspension_error'] = str(e)
            logger.info(f"Not able to suspend task: {task_name} {str(e)} ")
            break

    ret['suspension_failed_tasks'] = ','.join(task_def_errors)
    ret['suspended_tasks'] = ','.join(tasks_suspended)
    ret['suspended_tasks_count'] = len(tasks_suspended)
    
    p_session.sql(f'alter task if exists {p_root_task_name} suspend;').collect()
    p_session.sql(f'drop task if exists {p_root_task_name};').collect()

    ret['status'] = True
    return ret