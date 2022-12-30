import sys ,os ,io ,json ,logging
import zipfile
import pandas as pd
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
from zipfile import ZipFile
import json


logger = logging.getLogger("ingest-file-header_sp")

IN_NETWORK_RATES_FH_TBL = 'in_network_rates_fileheader'

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch to table [{p_target_tbl}] ...')
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    return tbl_spdf

def parse_file_headers(p_stage_path: str ,p_datafile: str ,f):
    header_event_types = ['string']
    headers = {}
    
    parser = ijson.parse(f)
    for prefix, event, value in parser:
        if len(prefix) < 3:
            continue

        elif value == None:
            continue
    
        elif '.item' in prefix:
            continue
        
        if event in header_event_types:
            headers[prefix] = value
    
    return headers

def parse_file_headers_wrapper(p_stage_path: str ,p_datafile: str ):
    headers = {}
    json_fl = f'@{p_stage_path}/{p_datafile}'

    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            headers = parse_file_headers_wrapper(p_stage_path ,p_datafile ,f )
    else:
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    headers = parse_file_headers_wrapper(p_stage_path ,p_datafile ,f )
                
    return headers

def main(p_session: Session ,p_stage_path: str  ,p_datafile: str ):
    ret = {}
    ret['data_file'] = p_datafile
    
    fl_headers = parse_file_headers(p_stage_path ,p_datafile)

    curr_rec = {}
    curr_rec['file'] = p_datafile
    curr_rec['file_headers'] = str(fl_headers)
        
    batch_records = []
    batch_records.append( curr_rec )
    df = pd.DataFrame(batch_records)
    append_to_table(p_session ,df ,IN_NETWORK_RATES_FH_TBL)
    
    ret['status'] = True
    return ret



    
