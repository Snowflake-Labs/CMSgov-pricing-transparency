
import logging
import io ,sys ,os ,ast
import joblib
import numpy as np
import pandas as pd
from _snowflake import vectorized

logger = logging.getLogger("pneumonia_inferencer")

# --------------------------------------------------------------------------------
#
# Donwload model from stage and install locally
#

# The snowflake directory, which would contain the various artifacts that we
# had defined as imports
IMPORT_DIR = sys._xoptions["snowflake_import_directory"]
MODEL_FLNAME = 'pneumonia_model.joblib'

def load_model(p_staged_model_flname: str):
    # Load the model and initialize the predictor
    model_fl_path = os.path.join(IMPORT_DIR, p_staged_model_flname)
    predictor = joblib.load(model_fl_path)
    return predictor

predictor = load_model(MODEL_FLNAME)

# --------------------------------------------------------------------------------
#
# Reshape the data to model expected input
#

# The feature columns that were used during model training 
# and that will be used during prediction
FEATURE_COLS = [
            'IMAGE_ARRAY_SHAPE_0' 
            ,'IMAGE_ARRAY_SHAPE_1' 
            ,'RESIZED_FEATURE']

def reshape_feature(p_df):
    def reshape_flattened_array(x):
        image_size = (-1 ,x['IMAGE_ARRAY_SHAPE_0'] ,x['IMAGE_ARRAY_SHAPE_1'] ,1)
        
        # The array is stored in Snowflake as an array, hence we need to convert it back
        # to array
        image_arr_str = x['RESIZED_FEATURE']
        image_arr_str = image_arr_str.replace('\n', ' ')
        arr = np.array(ast.literal_eval(image_arr_str))
        B = np.reshape(arr, image_size)
        return B

    df = p_df.copy(deep=True)
    df['IMAGE_FEATURE'] = df.apply(lambda x: reshape_flattened_array(x) ,axis=1)
    
    df.drop(['IMAGE_ARRAY_SHAPE_0' ,'IMAGE_ARRAY_SHAPE_1' ,'RESIZED_FEATURE'] ,axis=1 ,inplace=True) #,'IMAGE_ARRAY'  ,'IMAGE_ARRAY','NORMALIZED_IMAGE_ARRAY','RESIZED_FEATURE'
    return df

def restructure_data(p_X):
    logger.info('#restructure_data ')
    try:
        x = np.array( [ x[0] for x in p_X['IMAGE_FEATURE'] ] )
        
        return x
    except Exception as e: 
        # logger.error(str(e))
        pass

    return None

# --------------------------------------------------------------------------------
# The predict(main) function, which will be invoked as part of the UDF. The
# set of columns would be passed into the function as dataframe. Also the
# model that need to be used for predicts, would be passed in via the 
# dataframe
@vectorized(input=pd.DataFrame)
def main(p_df: pd.DataFrame) -> int:

    # Snowpark currently does not set the column name in the input dataframe
    # The default col names are like 0,1,2,... Hence we need to reset the column
    # names to the features that we initially used for training. 
    p_df.columns = FEATURE_COLS
    
    df_reshaped = reshape_feature(p_df)
    df_restructured = restructure_data(df_reshaped)

    logger.info('Perform prediction ...')
    # Perform prediction
    y_pred = predictor.predict(df_restructured)

    return y_pred
