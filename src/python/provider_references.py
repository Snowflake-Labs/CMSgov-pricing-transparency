'''
    Used for parsing and staging specifically the 'provider references' segments.
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
logger = logging.getLogger("in_network_rates_provider_references")

# List of childrens that form a repeatable section
REPEATABLE_CHILDREN_SECTIONS = ['negotiated_rates' ,'bundled_codes' ,'covered_services']

def save_provider_references(p_session: Session ,p_segment_headers):
    df = pd.DataFrame(p_segment_headers)
    sp_df = get_snowpark_dataframe(p_session ,df)
    
    target_table = p_session.table('in_network_rates_provider_references')
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) 
            & (target_table['SEGMENT_ID'] == sp_df['SEGMENT_ID'])
        ,[
          F.when_not_matched().insert({
            'SEQ_NO': sp_df['SEQ_NO']
            ,'DATA_FILE': sp_df['DATA_FILE']
            ,'SEGMENT_ID': sp_df['SEGMENT_ID']
            ,'PROVIDER_REFERENCE': sp_df['PROVIDER_REFERENCE']
            })
        ])
    
    return merged_df

def get_segment_id(p_rec ,p_segment_idx):
    provider_group_id = str(p_rec['provider_group_id'])
    segment_id = f'''{provider_group_id}::{p_segment_idx}'''
    segment_id = re.sub('[^0-9a-zA-Z:=]+', '_', segment_id).lower()
    return segment_id

def parse_breakdown_save(p_session: Session  
        ,p_stage_path: str ,p_datafile: str ,f):
    logger.info('Parsing and breaking down in_network ...')
    

    provider_references = []
    segment_idx = 0
    stored_segment_idx = 0
    
    for provider_ref_rec in ijson.items(f, 'provider_references.item' ,use_float=True):
        segment_idx += 1

        rec = {}
        # get a unique identifier that will form as the segment identifier
        l_segment_id = get_segment_id(provider_ref_rec ,segment_idx)
        
        rec['SEQ_NO'] = segment_idx
        rec['DATA_FILE'] = p_datafile
        rec['SEGMENT_ID'] = l_segment_id
        rec['PROVIDER_REFERENCE'] = provider_ref_rec

        provider_references.append(rec)
        stored_segment_idx += 1

    # The provider reference section is an optional element
    # dont do save if its not present; otherwise u will get an exception
    if(len(provider_references) > 0):
        save_provider_references(p_session ,provider_references)
    
    return stored_segment_idx

def parse_breakdown_save_wrapper(p_session: Session 
    ,p_stage_path: str ,p_datafile: str):
    logger.info('Parsing and breaking down in_network ...')
    iterated_segments = -1
    stored_segment_count = -1
    eof_reached = True
    parsing_error = ''
    json_fl = f'@{p_stage_path}/{p_datafile}'

    rdata = ''
    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl,is_owner_file=True) as f:
            stored_segment_count = parse_breakdown_save(p_session 
                ,p_stage_path ,p_datafile ,f)

    elif json_fl.endswith('.tar.gz'):
        raise Exception(f'input file is of unknown compression format {p_datafile}')

        #TODO have same issues related to parsing this type of file. something for the future
        # try:
        #     rec_count ,eof_reached = (1 ,True)
        #     from io import BytesIO
        #     import tarfile
        #     with tarfile.open(fileobj = BytesIO(_snowflake.open(json_fl))) as f:
        #         rec_count ,eof_reached = parse_breakdown_save(p_session 
        #             ,p_stage_path ,p_datafile ,p_target_stage 
        #             ,p_from_seg ,p_to_seg ,f)
        # except Exception as e:
        #     parsing_error = str(e)

    elif json_fl.endswith('.gz'):
        with gzip.open(_snowflake.open(json_fl,is_owner_file=True),'r') as f:
            stored_segment_count = parse_breakdown_save(p_session 
                ,p_stage_path ,p_datafile ,f)   

    elif json_fl.endswith('.zip'):
        with ZipFile(_snowflake.open(json_fl,is_owner_file=True)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    stored_segment_count = parse_breakdown_save(p_session 
                        ,p_stage_path ,p_datafile ,f)

    else:
        raise Exception(f'input file is of unknown compression format {p_datafile}')
    
    return stored_segment_count ,parsing_error
   
def main(p_session: Session 
    ,p_stage_path: str ,p_datafile: str):
    
    ret = {}
    ret['data_file'] = p_datafile
    ret['data_file_basename'] = get_basename_of_datafile(p_datafile)
    ret['cleansed_data_file_basename'] = get_cleansed_file_basename(p_datafile)

    report_execution_status(p_session ,p_datafile ,ret)
    start = datetime.datetime.now()
    
    segments_count ,parsing_error = parse_breakdown_save_wrapper(p_session 
        ,p_stage_path ,p_datafile)
    ret['segments_count'] = segments_count

    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '

    report_execution_status(p_session ,p_datafile ,ret)
    
    ret['status'] = True
    return ret