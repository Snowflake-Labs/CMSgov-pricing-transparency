import sys ,os ,io ,json ,logging ,re
import uuid ,gzip
from zipfile import ZipFile
import pandas as pd
import ijson
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake

logger = logging.getLogger("innetwork_rates_seg-header_ingestor_sp")

DEFAULT_BATCH_SIZE = 1000
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header_V2'


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
    # tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
    #     ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    # Convert the data frame into Snowpark dataframe, needed for merge operation
    sp_df = get_snowpark_dataframe(p_session ,p_df)
    
    #Doc: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Table.merge.html#snowflake.snowpark.Table.merge
    target_table = p_session.table(p_target_tbl)
    
    merged_df = target_table.merge(sp_df
        ,(target_table['DATA_FILE'] == sp_df['DATA_FILE']) & (target_table['NEGOTIATED_RATES_NAME'] == sp_df['NEGOTIATED_RATES_NAME'])
        ,[
          F.when_not_matched().insert({ 
            'SEGMENT_ID': sp_df['SEGMENT_ID']
            ,'DATA_FILE': sp_df['DATA_FILE']
            ,'NEGOTIATED_RATES_NAME': sp_df['NEGOTIATED_RATES_NAME']
            ,'NEGOTIATED_RATES_INFO': sp_df['NEGOTIATED_RATES_INFO']
            ,'NEGOTIATED_RATES_COUNT': sp_df['NEGOTIATED_RATES_COUNT']
            ,'BUNDLED_CODES_COUNT': sp_df['BUNDLED_CODES_COUNT']
            ,'COVERED_SERVICES_COUNT': sp_df['COVERED_SERVICES_COUNT']
            })
        ])

    return merged_df


def build_header_id(p_arrangement: str ,p_name: str):
    s = f'''{p_arrangement}_{p_name}'''

    #Retains only alphanumeric characters and replace any other characters with empty
    hdr_id = re.sub('[^0-9a-zA-Z]+', '', s)
    hdr_id_hash = hash(hdr_id)
    return (hdr_id ,hdr_id_hash)

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,f):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    
    seq_no = -1
    batch_records = []
    for rec in ijson.items(f, 'in_network.item'):
        innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}
        #header_id ,header_id_hash = build_header_id(innetwork_hdr['negotiation_arrangement'] ,innetwork_hdr['name'])
        seq_no += 1

        bundled_codes_count = len(rec['bundled_codes']) if 'bundled_codes' in rec else -1
        covered_services_count = len(rec['covered_services_count']) if 'covered_services_count' in rec else -1

        curr_rec = {}
        curr_rec['SEQ_NO'] = seq_no
        curr_rec['DATA_FILE'] = p_datafile
        curr_rec['SEGMENT_ID'] = str(uuid.uuid4())
        curr_rec['NEGOTIATED_RATES_NAME'] = innetwork_hdr['name']
        curr_rec['NEGOTIATED_RATES_INFO'] = str(innetwork_hdr)
        curr_rec['NEGOTIATED_RATES_COUNT'] = len(rec['negotiated_rates'])
        curr_rec['BUNDLED_CODES_COUNT'] = bundled_codes_count
        curr_rec['COVERED_SERVICES_COUNT'] = covered_services_count

        batch_records.append(curr_rec)

        if len(batch_records) >= l_approx_batch_size:
            df = pd.DataFrame(batch_records)
            append_to_table(p_session ,df ,IN_NETWORK_RATES_SEGHDR_TBL)
            batch_records.clear()

    # append leftovers
    if len(batch_records) > 0:
        df = pd.DataFrame(batch_records)
        append_to_table(p_session ,df ,IN_NETWORK_RATES_SEGHDR_TBL)
        batch_records.clear()
                    
    return seq_no

def parse_breakdown_save_wrapper(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str):
    logger.info('Parsing and breaking down in_network ...')
    rec_count = -1
    json_fl = f'@{p_stage_path}/{p_datafile}'

    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            rec_count = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile ,f)

    elif json_fl.endswith('.gz'):
        with gzip.open(_snowflake.open(json_fl),'r') as f:
            rec_count = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile ,f)      

    else:
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    rec_count = parse_breakdown_save(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile ,f)
    
    return rec_count

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str):
    ret = {}
    ret['data_file'] = p_datafile
    
    seg_record_counts = parse_breakdown_save_wrapper(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile)

    ret['ingested_record_counts'] = seg_record_counts
    ret['status'] = True
    return ret

