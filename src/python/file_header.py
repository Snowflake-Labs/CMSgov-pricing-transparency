'''
    Used for parsing and staging specifically the file header.
'''
import sys ,os ,io ,json ,logging ,re
import uuid ,gzip
from zipfile import ZipFile
import pandas as pd
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
import shutil
import simplejson as sjson
import hashlib
import datetime
from sp_commons import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("file_header")

def save_header(p_session: Session ,p_fl_header):

    
    batch_records = []
    batch_records.append(p_fl_header)
    df = pd.DataFrame(batch_records)
    sp_df = get_snowpark_dataframe(p_session ,df)
    
    target_table = p_session.table('in_network_rates_file_header')
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE'])
        ,[
          F.when_not_matched().insert({
            'DATA_FILE': sp_df['DATA_FILE']
            ,'HEADER': p_fl_header
            })
        ])

    return merged_df

def parse_header_elements(p_session: Session  
        ,p_stage_path: str ,p_datafile: str
        ,f):
    logger.info('Parsing header elements ...')
    
    l_fl_header = {}
    parser = ijson.parse(f)
    # l_fl_header = { prefix:value for prefix, event, value in parser
    #             if ((event != 'string') or ('in_network' not in prefix) or ( value is not None)) and (len(prefix) > 1) }

    l_fl_header['DATA_FILE'] = p_datafile

    segments_count = -1

    for prefix, event, value in parser:
        if (event != 'string'):
            continue
        
        elif (prefix == 'in_network.item.billing_code'):
            segments_count += 1
            continue

        elif ('in_network' in prefix):
            continue
        elif( value is None):
            continue
        elif(len(prefix.strip()) < 1):
            continue
        
        l_fl_header[prefix] = value
    
    l_fl_header['total_segments'] = segments_count

    return l_fl_header

def parse_breakdown_save_wrapper(p_session: Session 
    ,p_stage_path: str ,p_datafile: str):
    logger.info('Parsing and breaking down in_network ...')
    l_fl_header = {}
    json_fl = f'@{p_stage_path}/{p_datafile}'

    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            l_fl_header = parse_header_elements(p_session 
                ,p_stage_path ,p_datafile ,f)

    elif json_fl.endswith('.gz'):
        with gzip.open(_snowflake.open(json_fl),'r') as f:
            l_fl_header = parse_header_elements(p_session 
                ,p_stage_path ,p_datafile ,f)   

    elif json_fl.endswith('.zip'):
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    l_fl_header = parse_header_elements(p_session 
                ,p_stage_path ,p_datafile ,f)
    else:
        raise Exception(f'input file is of unknown compression format {p_datafile}')
    
    return l_fl_header

def main(p_session: Session 
    ,p_stage_path: str ,p_datafile: str):
    
    ret = {}
    ret['data_file'] = p_datafile

    start = datetime.datetime.now()
    
    l_fl_header = parse_breakdown_save_wrapper(p_session 
        ,p_stage_path ,p_datafile)
    save_header(p_session ,l_fl_header)
    
    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '
    
    insert_execution_status(p_session ,p_datafile ,elapsed ,ret)
    
    ret['status'] = True
    return ret
