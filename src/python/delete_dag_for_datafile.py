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
DAG_MATRIX_SHAPE = (5,15)

# 86400000 => 1 day
# 3600000 => 1 hour
USER_TASK_TIMEOUT = 86400000

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger("delete_dag_for_datafile")

def delete_taskdefinitions_for_datafile(p_session: Session ,p_datafile: str ,p_drop_tasks: bool):
    logger.info(f'Cleaning up tasks that were defined for the data file [{p_datafile}] ...')

    fl_basename = get_cleansed_file_basename(p_datafile)
   
    # Delete all task definitions that were previously defined for this data file
    tbl_df = p_session.table(TASK_TO_SEGMENTIDS_TBL)
    tbl_df.delete(tbl_df["DATA_FILE"] == F.lit(f'{p_datafile}'))

    # Iterate defined tasks 
    current_db = p_session.get_current_database()
    p_session.sql(f' SHOW TASKS IN  DATABASE {current_db}; ').collect()

    tasks_dropped = []
    stmt = f'''
        select "name" as task_name
        from table(result_scan(last_query_id()))
        where "name" like '%{fl_basename.upper()}%';
    '''
    rows = p_session.sql(stmt).collect()
    for r in rows:
        p_session.sql(f'''alter task if exists {r['TASK_NAME']} suspend;'''  ).collect()

        if (p_drop_tasks == True):
            p_session.sql(f'''drop task if exists {r['TASK_NAME']};'''  ).collect()
    tasks_dropped.append(r['TASK_NAME'])

    return tasks_dropped

def main(p_session: Session ,p_datafile: str ,p_drop_tasks: bool):
    ret = {}
    ret['data_file'] = p_datafile

    tasks_affected = delete_taskdefinitions_for_datafile(p_session ,p_datafile ,p_drop_tasks)
    ret['tasks_affected'] = tasks_affected

    ret['status'] = True
    return ret