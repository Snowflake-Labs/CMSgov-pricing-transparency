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

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("negotiation_arragnments")

DEFAULT_BATCH_SIZE = 2
IN_NETWORK_RATES_SEGHDR_TBL = 'in_network_rates_segment_header_V2'

def get_basename_of_datafile(p_datafile:str) -> str:
    base = os.path.basename(p_datafile)
    fl_base = os.path.splitext(base)
    return fl_base[0]

def upload_segments_file_to_stage(p_session: Session ,p_local_dir: str ,p_target_stage: str ,p_stage_dir: str):
    logger.info(f" Uploading library to stage: {p_target_stage}/{p_stage_dir} ... ")

    for path, currentDirectory, files in os.walk(p_local_dir):
        for file in files:
            # build the relative paths to the file
            local_file = os.path.join(path, file)

            # build the path to where the file will be staged
            stage_dir = path.replace(p_local_dir , p_stage_dir)

            # print(f'    {local_file} => @{p_stage}/{stage_dir}')
            p_session.file.put(
                local_file_name = local_file
                ,stage_location = f'{p_target_stage}/{stage_dir}'
                ,auto_compress=False ,overwrite=True) 
                # ,source_compression='NONE')
    
    #p_session.sql(f'alter stage {p_target_stage} refresh; ').collect()

def divide_list_to_chunks(p_list, p_chunk_size):
    #Ref: https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    for i in range(0, len(p_list), p_chunk_size):
        yield p_list[i:i + p_chunk_size]

def parse_breakdown_save(p_session: Session ,p_approx_batch_size: int 
        ,p_stage_path: str ,p_datafile: str ,p_target_stage: str 
        ,p_from_seg: int ,p_to_seg: int ,f):
    logger.info('Parsing and breaking down in_network ...')
    l_approx_batch_size = max(p_approx_batch_size ,DEFAULT_BATCH_SIZE )
    
    datafl_basename = get_basename_of_datafile(p_datafile)
    out_folder = os.path.join('/tmp', datafl_basename)
   
    rdata = ''
    files_written = -1
    seq_no = -1
    for idx ,rec in enumerate(ijson.items(f, 'in_network.item' ,use_float=True)):
        seq_no += 1

        if seq_no < p_from_seg:
            continue
        elif (seq_no > p_to_seg):
            break

        # segment_id = str(uuid.uuid4())
        #innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}
        innetwork_hdr = rec.copy()
        entries_to_remove = ['negotiated_rates' ,'bundled_codes' ,'covered_services']
        for k in entries_to_remove:
            innetwork_hdr.pop(k, None)

        negotiation_arrangement = str(innetwork_hdr['negotiation_arrangement'])
        seg_name = str(innetwork_hdr['name'])
        header_id = f'''{negotiation_arrangement}::{seg_name}'''
        segment_id = hashlib.md5(header_id.encode())

        innetwork_hdr['SEQ_NO'] = seq_no
        innetwork_hdr['DATA_FILE'] = p_datafile
        innetwork_hdr['SEGMENT_ID'] = segment_id

        segment_child_chunks = [ rec['negotiated_rates'] ]
        if len(rec['negotiated_rates']) > 5000:
            segment_child_chunks = list(divide_list_to_chunks(rec['negotiated_rates'], 5000))

        for chunk_idx ,chunk in enumerate( segment_child_chunks ):
            
            #shallow copy
            #ref: https://www.programiz.com/python-programming/methods/dictionary/copy
            curr_rec = innetwork_hdr.copy()
            curr_rec['CHUNK_NO'] = chunk_idx
            curr_rec['NEGOTIATED_RATES'] = chunk
            
            if not os.path.exists(out_folder):
                os.makedirs(out_folder)

            out_file = os.path.join(out_folder ,f'{segment_id}' , f'data_{seq_no}_{chunk_idx}.parquet.gz')
            #automatically create parent folders if it does not exists to avoid errors
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            # with open(out_file, "w") as out_fl:
            #     rec_str = sjson.dumps(curr_rec)
            #     out_fl.write(rec_str)

            # with gzip.open(out_file, 'wt') as out_fl:
            #     rec_str = sjson.dumps(curr_rec)
            #     out_fl.write(rec_str)
        
            df = pd.json_normalize(curr_rec)
            df.to_parquet(out_file,compression='gzip')  
            files_written += 1

            #if files_written >= l_approx_batch_size:
        upload_segments_file_to_stage(p_session ,out_folder ,p_target_stage ,datafl_basename)
        shutil.rmtree(out_folder)
        files_written = -1

    upload_segments_file_to_stage(p_session ,out_folder ,p_target_stage ,datafl_basename)
    # shutil.rmtree(out_folder)
    # p_session.sql(f'alter stage {p_target_stage} refresh; ').collect()
    return seq_no

def parse_breakdown_save_wrapper(p_session: Session ,p_approx_batch_size: int 
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_from_seg: int ,p_to_seg: int ):
    logger.info('Parsing and breaking down in_network ...')
    rec_count = -1
    json_fl = f'@{p_stage_path}/{p_datafile}'

    rdata = ''
    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            rec_count = parse_breakdown_save(p_session ,p_approx_batch_size 
                ,p_stage_path ,p_datafile ,p_target_stage 
                ,p_from_seg ,p_to_seg ,f)

    elif json_fl.endswith('.gz'):
        with gzip.open(_snowflake.open(json_fl),'r') as f:
            rec_count = parse_breakdown_save(p_session ,p_approx_batch_size 
                ,p_stage_path ,p_datafile ,p_target_stage 
                ,p_from_seg ,p_to_seg ,f)   

    else:
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    rec_count = parse_breakdown_save(p_session ,p_approx_batch_size 
                        ,p_stage_path ,p_datafile ,p_target_stage 
                        ,p_from_seg ,p_to_seg ,f)
    
    return rec_count

def main(p_session: Session ,p_approx_batch_size: int 
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_from_seg: int ,p_to_seg: int):
    ret = {}
    ret['data_file'] = p_datafile
    
    seg_record_counts = parse_breakdown_save_wrapper(p_session ,p_approx_batch_size 
        ,p_stage_path ,p_datafile ,p_target_stage
        ,p_from_seg ,p_to_seg)

    ret['ingested_record_counts'] = seg_record_counts
    ret['status'] = True
    return ret
