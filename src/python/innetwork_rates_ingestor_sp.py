import sys ,os ,io ,json ,logging
import zipfile
import pandas as pd
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
from zipfile import ZipFile
import json


logger = logging.getLogger("innetwork_rates_ingestor_sp")

IN_NETWORK_RATES_TBL = 'in_network_rates'
BUNDLED_CODES_TBL = 'bundled_codes'
COVERED_SERVICES_TBL = 'covered_services'

DEFAULT_BATCH_SIZE = 1000

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch to table [{p_target_tbl}] ...')
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    return tbl_spdf

def parse_file_headers(p_stage_path: str ,p_datafile: str ):
    header_event_types = ['string']
    headers = {}
    json_fl = f'@{p_stage_path}/{p_datafile}'
    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                parser = ijson.parse(f)
                for prefix, event, value in parser:
                    if '.item' in prefix:
                        continue
                
                    if event in header_event_types:
                        headers[prefix] = value
    
    return headers

def iterate_childobjecttypes_and_save(p_session: Session ,p_approx_batch_size: int ,p_datafile: str ,p_headers: dict 
    ,p_innetwork_headers: dict ,p_sub_records: list ,p_record_type: str ,p_target_table: str) -> int:

    batch_records = []
    total_rec_count = len(p_sub_records)

    logger.info(f'Parsing and saving child records [{p_record_type}] len: {total_rec_count} ...')

    rec_id_str = f'''{p_innetwork_headers['negotiation_arrangement']}::{p_innetwork_headers['name']}'''
    rec_id_hash = hash(rec_id_str)

    for idx ,r in enumerate(p_sub_records):
        curr_rec = {}
        curr_rec['record_num'] = idx
        curr_rec['file'] = p_datafile
        curr_rec['innetwork_rec_key'] = rec_id_str
        curr_rec['innetwork_rec_hash'] = rec_id_hash
        curr_rec['file_headers'] = str(p_headers)
        curr_rec['in_network_header'] = str(p_innetwork_headers)
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

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    fl_headers = {}
    nr_record_counts = 0
    bc_record_counts = 0
    cs_record_counts = 0
    json_fl = f'@{p_stage_path}/{p_datafile}'

    fl_headers = parse_file_headers(p_stage_path ,p_datafile)

    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                
                for rec in ijson.items(f, 'in_network.item'):
                    innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}
                    
                    if 'negotiated_rates' in rec:
                        c_nr = iterate_childobjecttypes_and_save(p_session ,l_approx_batch_size ,p_datafile ,fl_headers
                            ,innetwork_hdr ,rec['negotiated_rates'] ,'negotiated_rates' ,IN_NETWORK_RATES_TBL)
                        nr_record_counts = nr_record_counts + c_nr
                    
                    if 'bundled_codes' in rec:
                        c_bc = iterate_childobjecttypes_and_save(p_session ,l_approx_batch_size ,p_datafile ,fl_headers 
                            ,innetwork_hdr ,rec['bundled_codes'] ,'bundled_codes' ,BUNDLED_CODES_TBL)
                        bc_record_counts = bc_record_counts + c_bc

                    if 'covered_services' in rec:
                        c_cs = iterate_childobjecttypes_and_save(p_session ,l_approx_batch_size ,p_datafile ,fl_headers 
                            ,innetwork_hdr ,rec['covered_services'] ,'covered_services' ,COVERED_SERVICES_TBL)
                        cs_record_counts = cs_record_counts + c_cs
                    
    return (nr_record_counts ,bc_record_counts ,cs_record_counts)

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ):
    ret = {}
    ret['data_file'] = p_datafile
    
    nr_record_counts ,bc_record_counts ,cs_record_counts = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile)

    ret['negotiated_record_counts'] = nr_record_counts
    ret['bundled_codes_record_counts'] = bc_record_counts
    ret['covered_services_record_counts'] = cs_record_counts

    ret['status'] = True
    return ret



    
