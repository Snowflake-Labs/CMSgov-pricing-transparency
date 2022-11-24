import sys ,os ,io ,json ,logging
import hashlib
import pandas as pd
import numpy as np
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
from zipfile import ZipFile
import datetime
from datetime import datetime
from datetime import timedelta
import time

logger = logging.getLogger("ingest-in-network-rates-segment-lists_sp")

TARGET_TABLE = 'negotiated_arrangment_segments_v2'
DEFAULT_BATCH_SIZE = 1000

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
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')

    if(1==1):
        return tbl_spdf
    
    # Convert the data frame into Snowpark dataframe, needed for merge operation
    sp_df = get_snowpark_dataframe(p_session ,p_df)
    
    #Doc: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Table.merge.html#snowflake.snowpark.Table.merge
    target_table = p_session.table(p_target_tbl)
    
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) & (target_table['DATA_HASH'] == sp_df['DATA_HASH'])
        ,[
          F.when_not_matched().insert({ 
            'seq_no': sp_df['seq_no']
            ,'DATA_FILE': sp_df['DATA_FILE']
            ,'segment_id': sp_df['segment_id']
            ,'segment_type': sp_df['segment_type']
            ,'segment_data': sp_df['segment_data']
            ,'data_hash': sp_df['data_hash']
            })
        ])

    return merged_df

def save_segments(p_session: Session ,p_approx_batch_size: int ,p_datafile: str 
    ,p_segment_id: str ,p_sub_records: list ,p_segment_type: str) -> int:

    batch_records = []
    total_rec_count = len(p_sub_records)

    splits = np.array_split(p_sub_records, 50)

    for idx, split in enumerate(splits):
        sub_records_strlist = [str(r) for r in split]
        sub_records_str = '[' + ','.join(sub_records_strlist) + ']'

        #data_hash = hashlib.md5(sub_records_str.encode()).hexdigest()
        v = f'{p_segment_id}::{idx}'
        data_hash = hashlib.md5(v.encode()).hexdigest()

        curr_rec = {}
        curr_rec['seq_no'] = idx
        curr_rec['data_file'] = p_datafile
        curr_rec['segment_id'] = p_segment_id
        curr_rec['segment_type'] = p_segment_type
        curr_rec['segment_data'] = sub_records_str
        curr_rec['data_hash'] =  data_hash

        batch_records.append(curr_rec)
        
        # buffer_count = len(batch_records)

    # if buffer_count >= p_approx_batch_size:
    df = pd.DataFrame(batch_records)
    append_to_table(p_session ,df  ,TARGET_TABLE)
    batch_records.clear()

    return total_rec_count

def iterate_childobjecttypes_and_save(p_session: Session ,p_approx_batch_size: int ,p_datafile: str 
    ,p_segment_id: str ,p_sub_records: list ,p_segment_type: str ,p_segments_buffer: list) -> int:

    # batch_records = []
    total_rec_count = len(p_sub_records)

    logger.info(f'Parsing and saving child records [{p_segment_type}] len: {total_rec_count} ...')
    for idx ,r in enumerate(p_sub_records):
        rec_str = str(r)
        data_hash = f'''{p_segment_id}::{p_segment_type}::{rec_str}'''
        data_hash = hashlib.md5(data_hash.encode()).hexdigest()

        curr_rec = {}
        curr_rec['seq_no'] = idx
        curr_rec['data_file'] = p_datafile
        curr_rec['segment_id'] = p_segment_id
        curr_rec['segment_type'] = p_segment_type
        curr_rec['segment_data'] = rec_str
        curr_rec['data_hash'] =  data_hash
        p_segments_buffer.append(curr_rec)

        # if len(batch_records) >= p_approx_batch_size:
        #     df = pd.DataFrame(batch_records)
        #     append_to_table(p_session ,df  ,TARGET_TABLE)
        #     batch_records.clear()

    # append leftovers
    # if len(batch_records) > 0:
    if len(p_segments_buffer) >= p_approx_batch_size:
        df = pd.DataFrame(p_segments_buffer)
        append_to_table(p_session ,df  ,TARGET_TABLE)
        p_segments_buffer.clear()

    return total_rec_count

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int ,p_datafile: str 
    ,p_segment_type: str ,p_task_name: str ,f):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    seg_record_counts = 0
    segments_buffer = []
    segment_name_to_ids_map = build_segment_ids_map(p_session ,p_datafile  ,p_task_name)
    captured_segments = []
    
    for rec in ijson.items(f, 'in_network.item'):
        
        if rec['name'] not in segment_name_to_ids_map:
            continue

        segment_id = segment_name_to_ids_map[rec['name']]

        c_nr = iterate_childobjecttypes_and_save(p_session ,l_approx_batch_size ,p_datafile 
            ,segment_id ,rec[p_segment_type] ,p_segment_type ,segments_buffer)

        # c_nr = save_segments(p_session ,l_approx_batch_size ,p_datafile 
        #     ,segment_id ,rec[p_segment_type] ,p_segment_type)

        seg_record_counts = seg_record_counts + c_nr
        
        # We dont to scan to the end of the files, if the segments that were asked for
        # has been parsed. Hence we keep a tab of what has not been parsed
        captured_segments.append(rec['name'])

        # break loop only after all the segments were parsed
        if len(captured_segments) == len(segment_name_to_ids_map):
            break
        
    #ensure to write all records from the buffer to the table
    if len(segments_buffer) > 0:
        df = pd.DataFrame(segments_buffer)
        append_to_table(p_session ,df ,TARGET_TABLE)

    return seg_record_counts

def parse_breakdown_save_wrapper(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str 
    ,p_segment_type: str ,p_task_name: str):
    logger.info('Parsing and breaking down in_network ...')
    json_fl = f'@{p_stage_path}/{p_datafile}'
 
    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            return parse_breakdown_save(p_session ,p_approx_batch_size ,p_datafile 
        ,p_segment_type ,p_task_name ,f)
    else:
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    return parse_breakdown_save(p_session ,p_approx_batch_size ,p_datafile 
        ,p_segment_type ,p_task_name ,f)

def build_segment_ids_map(p_session: Session ,p_datafile: str  ,p_task_name: str) -> dict:
    segment_name_to_ids_map = {}

    sql_stmt = f'''
        select segment_id, negotiated_rates_name 
        from in_network_rates_segment_header_V2 as l ,task_to_segmentids as r
        where lower(r.data_file) = lower(l.data_file)
        and lower(r.data_file) = lower('{p_datafile}')
        and lower(r.assigned_task_name) = lower('{p_task_name}')
        and contains( r.segment_ids ,l.segment_id) = True
        ;
    '''
    df = p_session.sql(sql_stmt)
    for segment_id ,negotiated_rates_name in df.to_local_iterator():
        segment_name_to_ids_map[negotiated_rates_name] = segment_id
    
    return segment_name_to_ids_map

def insert_execution_status(p_session: Session ,p_datafile: str ,p_task_name: str ,p_elapsed: str ,p_status: dict):
    ret_str = str(p_status)
    ret_str = ret_str.replace('\'', '"')
    sql_stmt = f'''
        insert into segment_task_execution_status( data_file  ,task_name  ,elapsed  ,task_ret_status ) 
        values('{p_datafile}' ,'{p_task_name}' ,'{p_elapsed}' ,'{ret_str}');
    '''
    p_session.sql(sql_stmt).collect()

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ,p_negotiation_arrangement_segment_type: str
    ,p_task_name: str):
    ret = {}
    seg_record_counts = 0
    status = False

    ret['data_file'] = p_datafile
    ret['task_name'] = p_task_name
    ret['p_negotiation_arrangement_segment_type'] = p_negotiation_arrangement_segment_type
    
    if p_negotiation_arrangement_segment_type not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']:
        ret['status'] = False
        ret['EXCEPTION'] = '''allowed values for parameter 'p_negotiation_arrangement_segment_type':  'negotiated_rates' ,'bundled_codes' ,'covered_services' '''
        raise Exception(ret['EXCEPTION'])

    start_time = time.time()
    try:
        seg_record_counts = parse_breakdown_save_wrapper(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile 
            ,p_negotiation_arrangement_segment_type ,p_task_name)
        ret['ingested_record_counts'] = seg_record_counts
        status = True
    except Exception as e:
        status = False
        ex_str = str(e)
        ret['EXCEPTION'] = ex_str
        logger.error('Segment ingestion failed: '+ ex_str)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed = str(timedelta(seconds=elapsed_time))
    ret['elapsed'] =  elapsed

    insert_execution_status(p_session ,p_datafile ,p_task_name ,elapsed ,ret)

    if(status == False):
        raise Exception(ret['EXCEPTION'])
    
    ret['status'] = status
    return ret
