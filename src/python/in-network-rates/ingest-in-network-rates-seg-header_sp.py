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

DEFAULT_BATCH_SIZE = 1000
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header'

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch to table [{p_target_tbl}] ...')
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    return tbl_spdf

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    seg_record_counts = 0
    json_fl = f'@{p_stage_path}/{p_datafile}'

    rec_count = 0
    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                batch_records = []
                for rec in ijson.items(f, 'in_network.item'):
                    innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}
                    header_id = f'''{innetwork_hdr['negotiation_arrangement']}::{innetwork_hdr['name']}'''
                    header_id = header_id.upper().replace(' ','_').replace('\t','_')
                    header_id_hash = hash(header_id)
                    rec_count += 1

                    curr_rec = {}
                    curr_rec['file'] = p_datafile
                    curr_rec['negotiation_arrangement_header'] = str(innetwork_hdr)
                    curr_rec['header_id'] = header_id
                    curr_rec['header_id_hash'] = header_id_hash
                    batch_records.append(curr_rec)

                    if len(batch_records) >= p_approx_batch_size:
                        df = pd.DataFrame(batch_records)
                        append_to_table(p_session ,df ,IN_NETWORK_RATES_SEGHDR_TBL)
                        batch_records.clear()

                # append leftovers
                if len(batch_records) > 0:
                    df = pd.DataFrame(batch_records)
                    append_to_table(p_session ,df ,IN_NETWORK_RATES_SEGHDR_TBL)
                    batch_records.clear()
                    
    return rec_count

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str):
    ret = {}
    ret['data_file'] = p_datafile
    
    seg_record_counts = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile)

    ret['ingested_record_counts'] = seg_record_counts
    ret['status'] = True
    return ret

