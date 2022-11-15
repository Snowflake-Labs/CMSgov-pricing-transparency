use role dev_pctransperancy_demo_rl;
use warehouse dev_pctransperancy_demo_wh;
use schema sflk_pricing_transperancy.public;

list @lib_stg;
list @data_stg;
select relative_path as data_filepath from directory( @data_stg );


-- ============
create or replace PROCEDURE innetwork_rates_ingestor_sp(batch_size integer , stage_path varchar ,staged_data_flname varchar)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python' ,'pandas', 'ijson')
IMPORTS = ('@lib_stg/scripts/innetwork_rates_ingestor_sp.py')
HANDLER = 'innetwork_rates_ingestor_sp.main'
as
$$
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
DEFAULT_BATCH_SIZE = 1000
LOCAL_DATA_DOWNLOAD_DIR = '/tmp/hmo_innetwork_rates_'
DOWNLOAD_EXPANDED_DIR = os.path.join(LOCAL_DATA_DOWNLOAD_DIR ,'expanded')

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_batch_idx: int ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas

    logger.info(f'Appending batch [{p_batch_idx}] ...')
    tbl_spdf = p_session.write_pandas(p_df ,table_name=p_target_tbl 
        ,quote_identifiers=False ,auto_create_table=True ,overwrite = False ,table_type='transient')
    
    return tbl_spdf

def parse_file_headers(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ):
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

def parse_negotiated_rates(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,p_headers: dict):
    logger.info('Parsing in_network/negotiated_rates ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    batch_idx = 0
    json_fl = f'@{p_stage_path}/{p_datafile}'
    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                global_idx = 0
                batch_records = []
                df = pd.DataFrame()  
                for nr in ijson.items(f, 'in_network.item.negotiated_rates'):
                    global_idx = global_idx + 1

                    for r in nr:
                        curr_rec = {}
                        curr_rec['file'] = p_datafile
                        curr_rec['idx'] = global_idx
                        curr_rec['batch_idx'] = batch_idx
                        curr_rec['headers'] = str(p_headers)
                        curr_rec['negotiated_rates'] = str(r)
                        batch_records.append(curr_rec)

                    if len(batch_records) >= l_approx_batch_size:
                        df = pd.DataFrame(batch_records)
                        append_to_table(p_session ,df ,batch_idx ,IN_NETWORK_RATES_TBL)

                        batch_idx = batch_idx + 1
                        batch_records.clear()
                    #break

                # To append for what ever is left over
                df = pd.DataFrame(batch_records)    
                append_to_table(p_session ,df ,batch_idx ,IN_NETWORK_RATES_TBL)
                batch_idx = batch_idx + 1
                batch_records.clear()
            #break

    return batch_idx


def parse_bundled_codes(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str ,p_datafile: str ,p_headers: dict):
    logger.info('Parsing in_network/bundled_codes ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    batch_idx = 0
    json_fl = f'@{p_stage_path}/{p_datafile}'
    with ZipFile(_snowflake.open(json_fl)) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                global_idx = 0
                batch_records = []
                df = pd.DataFrame()  
                for r in ijson.items(f, 'in_network.item.bundled_codes'):
                    global_idx = global_idx + 1

                    curr_rec = {}
                    curr_rec['file'] = p_datafile
                    curr_rec['idx'] = global_idx
                    curr_rec['batch_idx'] = batch_idx
                    curr_rec['headers'] = str(p_headers)
                    curr_rec['bundled_codes'] = str(r)
                    batch_records.append(curr_rec)

                    if len(batch_records) >= l_approx_batch_size:
                        df = pd.DataFrame(batch_records)
                        append_to_table(p_session ,df ,batch_idx ,BUNDLED_CODES_TBL)

                        batch_idx = batch_idx + 1
                        batch_records.clear()
                    #break

                # To append for what ever is left over
                df = pd.DataFrame(batch_records)    
                append_to_table(p_session ,df ,batch_idx ,BUNDLED_CODES_TBL)
                batch_idx = batch_idx + 1
                batch_records.clear()
            #break

    return batch_idx

def main(p_session: Session ,p_approx_batch_size: int ,p_stage_path: str  ,p_datafile: str ):
    ret = {}
    
    headers = parse_file_headers(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile)
    batch_count = parse_negotiated_rates(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile ,headers)

    parse_bundled_codes(p_session ,p_approx_batch_size ,p_stage_path ,p_datafile ,headers)

    ret['total_batches'] = batch_count
    ret['target_table'] = IN_NETWORK_RATES_TBL
    ret['status']=True
    return ret

$$;
-- ============                
                

drop table in_network_rates;

-- '@data_stg/price_transperancy/2022_10_01_priority_health_HMO_in-network-rates.zip'
-- '@data_stg/price_transperancy/cmsgov-in-network-rates-bundle-single-plan-sample.json.zip'

call innetwork_rates_ingestor_sp(25000
    ,'data_stg/price_transperancy' ,'cmsgov-in-network-rates-bundle-single-plan-sample.json.zip'); 

-- drop procedure innetwork_rates_ingestor_sp(number ,VARCHAR, VARCHAR);

select count(*)
from in_network_rates;

select *
from in_network_rates
limit 5;