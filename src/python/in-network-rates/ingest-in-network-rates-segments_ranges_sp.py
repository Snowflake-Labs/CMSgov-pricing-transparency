import sys ,os ,io ,json ,logging
import zipfile
import pandas as pd
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
from zipfile import ZipFile
import datetime

logger = logging.getLogger("innetwork_rates_segment_ingestor_sp")

DEFAULT_BATCH_SIZE = 1000

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch to table [{p_target_tbl}] ...')
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    return tbl_spdf

def iterate_childobjecttypes_and_save(p_session: Session ,p_approx_batch_size: int ,p_datafile: str 
    ,p_header_id: str ,p_sub_records: list ,p_record_type: str ,p_target_table: str) -> int:

    batch_records = []
    total_rec_count = len(p_sub_records)

    logger.info(f'Parsing and saving child records [{p_record_type}] len: {total_rec_count} ...')
    for idx ,r in enumerate(p_sub_records):
        curr_rec = {}
        curr_rec['record_num'] = idx
        curr_rec['data_file'] = p_datafile
        curr_rec['header_id'] = p_header_id
        curr_rec['header_id_hash'] = hash(p_header_id)
        #curr_rec['in_network_header'] = str(p_innetwork_headers)
        curr_rec[p_record_type] = str(r)
        batch_records.append(curr_rec)

        if len(batch_records) >= p_approx_batch_size:
            df = pd.DataFrame(batch_records)
            append_to_table(p_session ,df ,p_target_table)
            batch_records.clear()

    # append leftovers
    if len(batch_records) > 0:
        df = pd.DataFrame(batch_records)
        append_to_table(p_session ,df ,p_target_table)
        batch_records.clear()

    return total_rec_count

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str 
    ,p_segment_to_parse: str ,p_start_rec_num: int ,p_end_rec_num: int):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    seg_record_counts = 0
    json_fl = f'@{p_stage_path}/{p_datafile}'

    target_table = p_segment_to_parse
    header_id = ''
    rec_count = -1
    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                
                for rec in ijson.items(f, 'in_network.item'):
                    # innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}
                    # header_id = f'''{innetwork_hdr['negotiation_arrangement']}::{innetwork_hdr['name']}'''
                    # innetwork_hdr['header_id'] = header_id.upper().replace(' ','_').replace('\t','_')
                    # innetwork_hdr['header_id_hash'] = hash(innetwork_hdr['header_id'])
                    
                    header_id = f'''{rec['negotiation_arrangement']}::{rec['name']}'''
                    header_id = header_id.upper().replace(' ','_').replace('\t','_')
                    rec_count += 1

                    if (rec_count < p_start_rec_num):
                        continue
                    elif (rec_count > p_end_rec_num):
                        break

                    c_nr = iterate_childobjecttypes_and_save(p_session ,l_approx_batch_size ,p_datafile 
                        ,header_id ,rec[p_segment_to_parse] ,p_segment_to_parse ,target_table)
                    seg_record_counts = seg_record_counts + c_nr
                    
    return seg_record_counts

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ,p_negotiation_arrangement_segment: str
    ,p_start_rec_num: int ,p_end_rec_num: int ,p_task_name: str):
    ret = {}
    ret['data_file'] = p_datafile
    ret['negotiation_arrangement_segment'] = p_negotiation_arrangement_segment
    ret['start_rec_num'] = p_start_rec_num
    ret['end_rec_num'] = p_end_rec_num

    if p_negotiation_arrangement_segment not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']:
        ret['status'] = False
        ret['ERROR_REASON'] = '''allowed values for parameter 'negotiation_arrangement_segment':  'negotiated_rates' ,'bundled_codes' ,'covered_services' '''
        return ret

    start = datetime.datetime.now()
    seg_record_counts  = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile 
        ,p_negotiation_arrangement_segment ,p_start_rec_num ,p_end_rec_num)
    end = datetime.datetime.now()

    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '
    ret['ingested_record_counts'] = seg_record_counts
    
    ret_str = str(ret)
    ret_str = ret_str.replace('\'', '"')
    sql_stmt = f'''
        insert into segment_task_execution_status(task_name ,task_ret_status) values('{p_task_name}' ,'{ret_str}');
    '''
    p_session.sql(sql_stmt).collect()
    
    ret['status'] = True
    return ret

## ---------
## The below are another version of the implementation (Original); which was better in performance
## however the biggest drawback it had was that we were not able to parse the the segment header (negotiation_arragments) details
## was not, as back traversal is not possible. I am keeping these code for safekeeping for the future, should an idea popup
##
##--------- 
# def DORMANT_parse_negotiated_rates(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,p_headers: dict):
#     logger.info('Parsing in_network/negotiated_rates ...')
#     l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
#     batch_idx = 0
#     json_fl = f'@{p_stage_path}/{p_datafile}'
#     with ZipFile(_snowflake.open(json_fl)) as zf:
#         for file in zf.namelist():
#             with zf.open(file) as f:
#                 global_idx = 0
#                 batch_records = []
#                 for nr in ijson.items(f, 'in_network.item.negotiated_rates'):
#                     global_idx = global_idx + 1
#                     in_network_header = parse_innetwork_headers(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile)

#                     for r in nr:
#                         curr_rec = {}
#                         curr_rec['file'] = p_datafile
#                         curr_rec['idx'] = global_idx
#                         curr_rec['batch_idx'] = batch_idx
#                         curr_rec['headers'] = str(p_headers)
#                         curr_rec['in_network_header'] = str(in_network_header)
#                         curr_rec['negotiated_rates'] = str(r)
#                         batch_records.append(curr_rec)

#                     if len(batch_records) >= l_approx_batch_size:
#                         df = pd.DataFrame(batch_records)
#                         append_to_table(p_session ,df ,batch_idx ,IN_NETWORK_RATES_TBL)

#                         batch_idx = batch_idx + 1
#                         batch_records.clear()
#                     #break

#                 # To append for what ever is left over
#                 if len(batch_records) > 0:
#                     df = pd.DataFrame(batch_records)    
#                     append_to_table(p_session ,df ,batch_idx ,IN_NETWORK_RATES_TBL)
#                     batch_idx = batch_idx + 1
#                     batch_records.clear()
#             #break

#     return batch_idx


# def DORMANT_parse_bundled_codes(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,p_headers: dict):
#     logger.info('Parsing in_network/bundled_codes ...')
#     l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
#     batch_idx = 0
#     json_fl = f'@{p_stage_path}/{p_datafile}'
#     with ZipFile(_snowflake.open(json_fl)) as zf:
#         for file in zf.namelist():
#             with zf.open(file) as f:
#                 global_idx = 0
#                 batch_records = []
#                 for r in ijson.items(f, 'in_network.item.bundled_codes'):
#                     global_idx = global_idx + 1


#                     curr_rec = {}
#                     curr_rec['file'] = p_datafile
#                     curr_rec['idx'] = global_idx
#                     curr_rec['batch_idx'] = batch_idx
#                     curr_rec['headers'] = str(p_headers)
#                     curr_rec['bundled_codes'] = str(r)
#                     batch_records.append(curr_rec)

#                     if len(batch_records) >= l_approx_batch_size:
#                         df = pd.DataFrame(batch_records)
#                         append_to_table(p_session ,df ,batch_idx ,BUNDLED_CODES_TBL)

#                         batch_idx = batch_idx + 1
#                         batch_records.clear()
#                     #break

#                 # To append for what ever is left over
#                 if len(batch_records) > 0:
#                     df = pd.DataFrame(batch_records)    
#                     append_to_table(p_session ,df ,batch_idx ,BUNDLED_CODES_TBL)
#                     batch_idx = batch_idx + 1
#                     batch_records.clear()
#             #break

#     return batch_idx