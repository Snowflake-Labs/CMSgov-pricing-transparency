'''
    Used for parsing and staging specifically the 'negotiated_arrangement' segments.
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
logger = logging.getLogger("negotiation_arrangements_header")

# List of childrens that form a repeatable section
REPEATABLE_CHILDREN_SECTIONS = ['negotiated_rates' ,'bundled_codes' ,'covered_services']

def save_header(p_session: Session ,p_innetwork_hdr ,p_rec):
    '''
     For each segment, this stores the header elements and stats information.
    '''
    negotiated_rates_count = len(p_rec['negotiated_rates']) if 'negotiated_rates' in p_rec else -1
    bundled_codes_count = len(p_rec['bundled_codes']) if 'bundled_codes' in p_rec else -1
    covered_services_count = len(p_rec['covered_services_count']) if 'covered_services_count' in p_rec else -1
    # negotiated_rates_info = str(p_innetwork_hdr)
    negotiated_rates_info = p_innetwork_hdr

    #TODO there should be a better way to perform this one record merge operations,
    #current steps is too much
    # sql_stmt = f'''
    #     merge into in_network_rates_segment_header as tgt 
    #         using in_network_rates_segment_header as src
    #             on src.data_file = tgt.data_file
    #                 and src.segment_id = tgt.segment_id
            
    #         when not matched then 
    #             insert (seq_no, segment_id ,data_file 
    #                 ,negotiated_rates_count ,bundled_codes_count ,covered_services_count
    #                 ,negotiated_rates_info) 
    #             values (
    #                 {p_innetwork_hdr['SEQ_NO']} ,'{p_innetwork_hdr['SEGMENT_ID']}' ,'{p_innetwork_hdr['DATA_FILE']}'
    #                 ,{negotiated_rates_count} ,{bundled_codes_count} ,{covered_services_count}
    #                 ,'{negotiated_rates_info}'
    #             );
    # '''
    # p_session.sql(sql_stmt).collect()
    
    curr_rec = p_innetwork_hdr.copy()
    curr_rec['NEGOTIATED_RATES_INFO'] = negotiated_rates_info
    curr_rec['NEGOTIATED_RATES_COUNT'] = negotiated_rates_count
    curr_rec['BUNDLED_CODES_COUNT'] = bundled_codes_count
    curr_rec['COVERED_SERVICES_COUNT'] = covered_services_count

    batch_records = []
    batch_records.append(curr_rec)
    df = pd.DataFrame(batch_records)
    sp_df = get_snowpark_dataframe(p_session ,df)
    
    target_table = p_session.table('in_network_rates_segment_header')
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) 
            & (target_table['SEGMENT_ID'] == sp_df['SEGMENT_ID'])
        ,[
          F.when_not_matched().insert({
            'DATA_FILE': sp_df['DATA_FILE']
            ,'SEGMENT_ID': sp_df['SEGMENT_ID']
            ,'NEGOTIATED_RATES_INFO': sp_df['NEGOTIATED_RATES_INFO']
            ,'NEGOTIATED_RATES_COUNT': sp_df['NEGOTIATED_RATES_COUNT']
            ,'BUNDLED_CODES_COUNT': sp_df['BUNDLED_CODES_COUNT']
            ,'COVERED_SERVICES_COUNT': sp_df['COVERED_SERVICES_COUNT']
            })
        ])

    return merged_df

def parse_breakdown_save(p_session: Session  
        ,p_stage_path: str ,p_datafile: str ,f):
    logger.info('Parsing and breaking down in_network ...')
    
    datafl_basename = get_basename_of_datafile(p_datafile)
    out_folder = os.path.join('/tmp', datafl_basename)
   
    eof_reached = True
    segment_idx = 0
    stored_segment_idx = 0
    for rec in ijson.items(f, 'in_network.item' ,use_float=True):
        segment_idx += 1

        # For the header elements, ignore the repeated child elements. We
        # do this by making a deep copy of the semgenet and removing the 
        # repeateable childrens.
        innetwork_hdr = rec.copy()
        entries_to_remove = REPEATABLE_CHILDREN_SECTIONS
        for k in entries_to_remove:
            innetwork_hdr.pop(k, None)

        # get a unique identifier that will form as the segment identifier
        l_segment_id = get_segment_id(rec)
        
        innetwork_hdr['SEQ_NO'] = segment_idx
        innetwork_hdr['DATA_FILE'] = p_datafile
        innetwork_hdr['SEGMENT_ID'] = l_segment_id

        #TODO investigate a better way to save the header info. May be a seperate task on its own 
        save_header(p_session ,innetwork_hdr ,rec)
        stored_segment_idx += 1

    # p_session.sql(f'alter stage {p_target_stage} refresh; ').collect()
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
        with _snowflake.open(json_fl) as f:
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
        with gzip.open(_snowflake.open(json_fl),'r') as f:
            stored_segment_count = parse_breakdown_save(p_session 
                ,p_stage_path ,p_datafile ,f)   

    elif json_fl.endswith('.zip'):
        with ZipFile(_snowflake.open(json_fl)) as zf:
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
    
    stored_segment_count ,parsing_error = parse_breakdown_save_wrapper(p_session 
        ,p_stage_path ,p_datafile)
    # ret['stored_segment_count'] = stored_segment_count

    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '

    report_execution_status(p_session ,p_datafile ,ret)
    
    ret['status'] = True
    return ret