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
logger = logging.getLogger("negotiation_arragnments")

DEFAULT_BATCH_SIZE = 2

# Indicates the number of records that would be stored inside
# a chunk. Larger sizes will impact post processing as these will potentially
# cross the 16MB limit. Smaller sizes will result in too many smaller files and
# could impact performamce.
# We came up with this number, based on our own experimentations that kinda
# satisfied the loading strategy
MAX_RECORDS_PER_SEGMENT_CHUNK = 5000

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

def upload_segments_file_to_stage(p_session: Session ,p_local_dir: str ,p_target_stage: str ,p_stage_dir: str):
    logger.info(f" Uploading data to stage: {p_target_stage}/{p_stage_dir} ... ")

    # for path, currentDirectory, files in os.walk(p_local_dir):
    #     for file in files:
    #         # build the relative paths to the file
    #         local_file = os.path.join(path, file)

    #         # build the path to where the file will be staged
    #         stage_dir = path.replace(p_local_dir , p_stage_dir)

    #         # print(f'    {local_file} => @{p_stage}/{stage_dir}')
    #         p_session.file.put(
    #             local_file_name = local_file
    #             ,stage_location = f'{p_target_stage}/{stage_dir}'
    #             ,auto_compress=False ,overwrite=True) 
    #             # ,source_compression='NONE')
    
    # #p_session.sql(f'alter stage {p_target_stage} refresh; ').collect()

    # get the list of folders where parquet files are present
    data_dirs = { path for path, subdirs, files in os.walk(p_local_dir) for name in files if '.parquet.gz' in name }
    
    for idx, parquet_dir in enumerate(data_dirs):
        
        # build the path to where the file will be staged
        stage_dir = parquet_dir.replace(p_local_dir , p_stage_dir)

        # print(f'    {p_local_dir} => @{p_target_stage}{stage_dir}')
        p_session.file.put(
            local_file_name = f'{parquet_dir}/*.parquet.gz'
            ,stage_location = f'{p_target_stage}/{stage_dir}/'
            ,auto_compress=False ,overwrite=True ,parallel=20 )
        
    return

def divide_list_to_chunks(p_list, p_chunk_size):
    #Ref: https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    for i in range(0, len(p_list), p_chunk_size):
        yield p_list[i:i + p_chunk_size]

def get_segment_id(p_rec):
    negotiation_arrangement = str(p_rec['negotiation_arrangement'])
    billing_code = str(p_rec.get('billing_code' ,'-'))
    billing_code_type = str(p_rec.get('billing_code_type' ,'-'))
    billing_code_type_version = str(p_rec.get('billing_code_type_version' ,'-'))
    header_id = f'''{negotiation_arrangement}::{billing_code_type}::{billing_code}::{billing_code_type_version}'''
    header_id = header_id.replace(' ','_').lower()
    segment_id = header_id

    return segment_id

def parse_save_segment_children(p_seq_no: int ,p_segment_id: str ,p_out_folder: str 
        ,p_children_type ,p_innetwork_hdr ,p_rec ):
    logger.info(f'Breaking down children type {p_children_type} ...')
    
    segment_child_chunks = [ p_rec[p_children_type] ]

    #under certain cases,for a very large repeatable childrens, we would need to
    #chunk them into smaller sizes. If we dont do this, we will not be able to parse them
    #as currently Snowflake puts a restriction on the max col data size of 16MB.
    if len(p_rec[p_children_type]) > MAX_RECORDS_PER_SEGMENT_CHUNK:
        segment_child_chunks = list(divide_list_to_chunks(p_rec[p_children_type], MAX_RECORDS_PER_SEGMENT_CHUNK))

    # store these segment chunks
    for chunk_idx ,chunk in enumerate( segment_child_chunks ):
        
        #shallow copy
        #ref: https://www.programiz.com/python-programming/methods/dictionary/copy
        curr_rec = p_innetwork_hdr.copy()
        curr_rec['CHUNK_NO'] = chunk_idx
        curr_rec[p_children_type.upper()] = chunk
        
        if not os.path.exists(p_out_folder):
            os.makedirs(p_out_folder)

        out_file = os.path.join(p_out_folder ,p_segment_id ,p_children_type , f'data_{p_seq_no}_{chunk_idx}.parquet.gz')
        #automatically create parent folders if it does not exists to avoid errors
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        
        # TODO delete if not better approach
        # store file as json
        # with open(out_file, "w") as out_fl:
        #     rec_str = sjson.dumps(curr_rec)
        #     out_fl.write(rec_str)

        # with gzip.open(out_file, 'wt') as out_fl:
        #     rec_str = sjson.dumps(curr_rec)
        #     out_fl.write(rec_str)
    
        # store as parquet for better read performance than JSON
        df = pd.json_normalize(curr_rec)
        df.to_parquet(out_file,compression='gzip')  

def parse_breakdown_save(p_session: Session  
        ,p_stage_path: str ,p_datafile: str ,p_target_stage: str 
        ,p_from_seg: int ,p_to_seg: int ,f):
    logger.info('Parsing and breaking down in_network ...')
    
    datafl_basename = get_basename_of_datafile(p_datafile)
    out_folder = os.path.join('/tmp', datafl_basename)
   
    eof_reached = True
    seq_no = -1
    for rec in ijson.items(f, 'in_network.item' ,use_float=True):
        seq_no += 1

        # This ensures that we are parsing specifically a range of segments
        # that are within the range. Ignoring any segments that are not part
        # of the range band.
        if seq_no < p_from_seg:
            continue
        elif (seq_no > p_to_seg + 2):
            eof_reached = False
            break

        # For the header elements, ignore the repeated child elements. We
        # do this by making a deep copy of the semgenet and removing the 
        # repeateable childrens.
        innetwork_hdr = rec.copy()
        entries_to_remove = REPEATABLE_CHILDREN_SECTIONS
        for k in entries_to_remove:
            innetwork_hdr.pop(k, None)

        # get a unique identifier that will form as the segment identifier
        l_segment_id = get_segment_id(rec)
        
        innetwork_hdr['SEQ_NO'] = seq_no
        innetwork_hdr['DATA_FILE'] = p_datafile
        innetwork_hdr['SEGMENT_ID'] = l_segment_id

        for children_type in REPEATABLE_CHILDREN_SECTIONS:
            if children_type not in rec:
                # As these children types are optional element, we
                # ignore if they are not present in the input
                continue

            parse_save_segment_children(seq_no ,l_segment_id ,out_folder 
                ,children_type ,innetwork_hdr ,rec)

        upload_segments_file_to_stage(p_session ,out_folder ,p_target_stage ,datafl_basename)
        shutil.rmtree(out_folder)

        save_header(p_session ,innetwork_hdr ,rec)

    #upload any residual
    upload_segments_file_to_stage(p_session ,out_folder ,p_target_stage ,datafl_basename)
    # if Path(out_folder).exists() and Path(out_folder).is_dir():
    shutil.rmtree(out_folder, ignore_errors=True)

    # p_session.sql(f'alter stage {p_target_stage} refresh; ').collect()
    return (seq_no ,eof_reached)

def parse_breakdown_save_wrapper(p_session: Session 
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_from_seg: int ,p_to_seg: int ):
    logger.info('Parsing and breaking down in_network ...')
    rec_count = -1
    eof_reached = True
    parsing_error = ''
    json_fl = f'@{p_stage_path}/{p_datafile}'

    rdata = ''
    if json_fl.endswith('.json'):
        with _snowflake.open(json_fl) as f:
            rec_count ,eof_reached = parse_breakdown_save(p_session 
                ,p_stage_path ,p_datafile ,p_target_stage 
                ,p_from_seg ,p_to_seg ,f)

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
            rec_count ,eof_reached = parse_breakdown_save(p_session 
                ,p_stage_path ,p_datafile ,p_target_stage 
                ,p_from_seg ,p_to_seg ,f)   

    elif json_fl.endswith('.zip'):
        with ZipFile(_snowflake.open(json_fl)) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    rec_count ,eof_reached = parse_breakdown_save(p_session 
                        ,p_stage_path ,p_datafile ,p_target_stage 
                        ,p_from_seg ,p_to_seg ,f)

    else:
        raise Exception(f'input file is of unknown compression format {p_datafile}')
    
    return rec_count ,eof_reached ,parsing_error

def should_proceed_with_parsing(p_session: Session ,p_datafile: str ,p_from_seg: int ,p_to_seg: int):

    if p_from_seg == 0:
        return (-1 ,True)

    # For a given data file, check if there are any records present in the view: segments_counts_for_datafile_v
    # see if the total_no_segments, which indicates the total number of segments in the file, is greater
    # than the current segment range 
    df = (p_session.table('segments_counts_for_datafile_v')
            .filter(F.col('data_file') == F.lit(f'{p_datafile}'))
            .with_column('current_seg_range_end', F.lit(p_to_seg))
    )
    df = df.with_column("seq_range_greater_than_total_segments", (df['current_seg_range_end'] >= df["TOTAL_NO_OF_SEGMENTS"]))
    df = df.to_pandas()

    #if the file contains more segments than the current segment range, then proceed. 
    #if the file contains lesser segment do not proceed
    #if no record is present in the view, then proceed
    row_count = len(df)
    should_proceed_processing = (row_count < 1)
    # total_no_of_segments = df['TOTAL_NO_OF_SEGMENTS'][0] if row_count >= 1  else -1
    total_no_of_segments = -2 if row_count >= 1  else -1

    return (total_no_of_segments ,should_proceed_processing)
    

def main(p_session: Session 
    ,p_stage_path: str ,p_datafile: str ,p_target_stage: str
    ,p_from_seg: int ,p_to_seg: int):
    
    ret = {}
    ret['data_file'] = p_datafile
    ret['start_rec_num'] = p_from_seg
    ret['end_rec_num'] = p_to_seg

    report_execution_status(p_session ,p_datafile ,ret)
    start = datetime.datetime.now()
    
    #TODO verify if previously the EOF_Reached flag has been set (table: segment_task_execution_status)
    #from other parallel task instances. If it has been set; then proceed only
    #if the current from_seg and to_seg is within the last_seg_no (of that record)
    #otherwise exit out
    
    total_no_of_segments ,should_proceed_processing = should_proceed_with_parsing(p_session ,p_datafile ,p_from_seg ,p_to_seg)
    
    if(should_proceed_processing == True):
        last_seg_no ,eof_reached ,parsing_error = parse_breakdown_save_wrapper(p_session 
            ,p_stage_path ,p_datafile ,p_target_stage
            ,p_from_seg ,p_to_seg)
        ret['Parsing_error'] = parsing_error
        ret['last_seg_no'] = last_seg_no
        ret['EOF_Reached'] = eof_reached
        ret['task_ignored_parsing'] = False
    else:
        ret['task_ignored_parsing'] = True
        ret['task_parsing_ignore_message'] = f'The segment range is greater than the total segments {total_no_of_segments} in the file, hence ignore further parsing'
    
    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '

    report_execution_status(p_session ,p_datafile ,ret)
    
    ret['status'] = True
    return ret
