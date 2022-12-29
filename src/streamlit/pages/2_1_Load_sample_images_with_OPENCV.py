from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
from util_fns import exec_sql_script

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('exec_sql_script')

st.markdown(f"# Stage sample image dataset")
st.markdown(f"**NOTE:** This will take anywhere from 15-20 min range.")

st.write("""
    Lorem ipsum...
""")

# Initialize a session with Snowflake
config = None
sp_session = None
if "snowpark_session" not in st.session_state:
    config = L.get_config(PROJECT_HOME_DIR)
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['APP_DB']['snow_opt_wh']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
import pandas as pd
import numpy as np
import datetime, os
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import IntegerType ,StringType ,BooleanType ,VariantType ,StructField ,StructType
import cv2

IMG_SIZE = 150

file_parsing_bar = st.progress(0)

def append_to_table(p_session: Session ,p_df: pd.DataFrame ,p_target_tbl: str):
    #ref: https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.html#snowflake.snowpark.Session.write_pandas
    st.write(f'Appending batch to table [{p_target_tbl}] ...')
    
    p_df.columns = map(lambda x: str(x).upper(), p_df.columns)

    tbl_schema = StructType([
        StructField('SEQ_NO', IntegerType())
        ,StructField('IMAGE_FILEPATH', StringType())
        ,StructField('PARSING_STATUS', BooleanType())
        ,StructField('PARSING_EXCEPTION', StringType())
        ,StructField('IMAGE_ARRAY_SHAPE_0', IntegerType())
        ,StructField('IMAGE_ARRAY_SHAPE_1', IntegerType())
        ,StructField('IMAGE_ARRAY', VariantType())
        ,StructField('NORMALIZED_IMAGE_ARRAY', VariantType())
        ,StructField('RESIZED_FEATURE', VariantType())
    ])

    tbl_spdf = sp_session.create_dataframe(p_df ,schema=tbl_schema)
    
    target_table = p_session.table(p_target_tbl)
    target_table.merge(tbl_spdf
        ,(target_table['IMAGE_FILEPATH'] == tbl_spdf['IMAGE_FILEPATH'])
        ,[
          F.when_not_matched().insert({ 
            'SEQ_NO': tbl_spdf['SEQ_NO']
            ,'IMAGE_FILEPATH': tbl_spdf['IMAGE_FILEPATH']
            ,'PARSING_STATUS': tbl_spdf['PARSING_STATUS']
            ,'PARSING_EXCEPTION': tbl_spdf['PARSING_EXCEPTION']
            ,'CLASS_LABEL': tbl_spdf['CLASS_LABEL']
            ,'CLASS_LABEL_NUM': tbl_spdf['CLASS_LABEL_NUM']
            ,'IMAGE_ARRAY_SHAPE_0': tbl_spdf['IMAGE_ARRAY_SHAPE_0']
            ,'IMAGE_ARRAY_SHAPE_1': tbl_spdf['IMAGE_ARRAY_SHAPE_1']
            ,'IMAGE_ARRAY': tbl_spdf['IMAGE_ARRAY']
            ,'NORMALIZED_IMAGE_ARRAY': tbl_spdf['NORMALIZED_IMAGE_ARRAY']
            ,'RESIZED_FEATURE': tbl_spdf['RESIZED_FEATURE']
            })
        ])

def parse_img_fl_to_array(p_img_fl):
    logger.info(f' : {p_img_fl}')

    ex = ''
    status = False
    resized_arr = []
    try:
        # convert into an array
        img_arr = cv2.imread(p_img_fl, cv2.IMREAD_GRAYSCALE)

        # Reshaping images to preferred size
        resized_arr = cv2.resize(img_arr, (IMG_SIZE, IMG_SIZE)) 
        status = True
    except Exception as e:
        ex = str(e)

    return (status ,ex ,resized_arr)

def parse_compressed_imgfl(p_session: Session):
    logger.info(f'Parsing sample image file ...')
    l_images_parsed_list = []
    l_image_files = []
    
    # ========
    # iterate the data directory and for each image; we convert to matrice
    # and then reshape the matrices
    data_dirpath = f'{PROJECT_HOME_DIR}/data'
    for subdir, dirs, files in os.walk(data_dirpath):
        for file in files:
            l_image_files.append(os.path.join(subdir, file))

    noof_files = len(l_image_files)
    for idx ,img_flpath in enumerate(l_image_files):
        if img_flpath.endswith('jpeg') == False:
            continue
        
        elif '/.' in img_flpath:
            continue

        # img_flpath = os.path.join(data_dirpath, img_flpath)
        l_class = 'PNEUMONIA' if '/PNEUMONIA/' in img_flpath else 'NORMAL'
        l_class_num = 1 if '/PNEUMONIA/' in img_flpath else 0
        
        status ,ex ,image_arr = parse_img_fl_to_array(img_flpath)
        arr_shape = np.shape(image_arr)
        normalized_arr = np.array(image_arr) / 255
        resized_feature = normalized_arr.reshape(-1, IMG_SIZE, IMG_SIZE, 1)
        l_images_parsed_list.append( (
            idx ,img_flpath ,status ,ex
            ,l_class ,l_class_num 
            ,arr_shape[0] ,arr_shape[1]
            ,image_arr.flatten() 
            ,normalized_arr.flatten() 
            ,resized_feature.flatten()) )

        perc = (idx/noof_files)
        file_parsing_bar.progress(perc)
        
    images_parsed_pddf = pd.DataFrame(l_images_parsed_list
    , columns =['seq_no' ,'image_filepath' 
        ,'parsing_status' ,'parsing_exception'
        ,'class_label' ,'class_label_num'
        ,'image_array_shape_0' ,'image_array_shape_1' 
        ,'image_array' ,'normalized_image_array' ,'resized_feature'])

    return images_parsed_pddf

def load_and_display_target_tbl(p_target_tbl):
    st.write('sampling target table ...')
    tbl_df = (sp_session
        .table(p_target_tbl)
        .sample(n=5)
        .to_pandas())

    st.dataframe(tbl_df)

def load_and_stage_sample_data_images():
    ret = {}
    start = datetime.datetime.now()
    
    st.write(f'Starting load @{start}')
    parsed_images_df = parse_compressed_imgfl(sp_session)
    
    parsed_img_table = f'''{config['APP_DB']['database']}.public.image_parsed_raw'''
    append_to_table(sp_session ,parsed_images_df ,parsed_img_table)
    end = datetime.datetime.now()
    elapsed = (end - start)
    ret['elapsed'] =  f'=> {elapsed} '
    st.write(f'Total elapsed time: {elapsed}')
    
    load_and_display_target_tbl(parsed_img_table)
    st.write('Finished!!!')

    ret['status'] = True
    return ret

# ---------------
script_output = st.empty()
with script_output:
    st.button('Load sample images'
            ,on_click = load_and_stage_sample_data_images
        )

    