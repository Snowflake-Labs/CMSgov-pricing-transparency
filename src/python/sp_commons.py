'''
Script file containing functions that are common and shared across multiple stored procedures and functions
'''
import os ,logging ,sys
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import pandas as pd

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("sp_commons")

def get_basename_of_datafile(p_datafile:str) -> str:
    base = os.path.basename(p_datafile)
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

# def insert_execution_status(p_session: Session ,p_datafile: str ,p_elapsed: str ,p_status: dict):
#     ret_str = str(p_status)
#     ret_str = ret_str.replace('\'', '"')
#     sql_stmt = f'''
#         insert into segment_task_execution_status( data_file  ,task_name  ,elapsed  ,task_ret_status ) 
#         values('{p_datafile}' ,system$current_user_task_name() ,'{p_elapsed}' ,'{ret_str}');
#     '''
#     p_session.sql(sql_stmt).collect()

def report_execution_status(p_session: Session ,p_datafile: str ,p_status: dict):
    ret_str = str(p_status)
    ret_str = ret_str.replace('\'', '"')

    sql_stmt = f'''
        merge into segment_task_execution_status as t
        using (
            select 
            '{p_datafile}' as data_file
            ,system$current_user_task_name() as task_name
        ) s 
        on t.data_file = s.data_file 
            and t.task_name = s.task_name
        when not matched then insert 
            (data_file ,task_name)
            values(s.data_file ,s.task_name)

        when matched then update set 
            t.end_time = current_timestamp()
            ,t.task_ret_status = '{ret_str}'
            ;
    '''
    p_session.sql(sql_stmt).collect()
