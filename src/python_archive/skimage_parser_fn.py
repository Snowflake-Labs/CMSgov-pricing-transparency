import sys ,os ,io ,json ,logging 
from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import _snowflake
import pandas as pd
import numpy as np
import datetime
from snowflake.snowpark.types import IntegerType ,StringType ,BooleanType ,VariantType ,StructField ,StructType

from skimage.color import rgb2gray
from skimage import data
from skimage.filters import gaussian
from skimage.segmentation import active_contour
import skimage.io
from skimage.transform import resize, rescale

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("skimage_parser_fn")

IMG_SIZE = 150

def parse_img_fl_to_array(p_img_fl):
    logger.info(f' : {p_img_fl}')

    ex = ''
    status = False
    resized_arr = []
    try:
        # convert into an array
        img_arr = skimage.io.imread( _snowflake.open(p_img_fl), as_gray=True, plugin=None)

        # Reshaping images to preferred size
        resized_arr = resize(img_arr,(IMG_SIZE,IMG_SIZE),preserve_range=True, anti_aliasing=True,order=0)

        status = True
    except Exception as e:
        ex = str(e)

    return (status ,ex ,resized_arr)
    
def main(p_image_fl: str):
    l_images_parsed = {}
    
    try:
        status ,ex ,image_arr = parse_img_fl_to_array(p_image_fl)
        arr_shape = np.shape(image_arr)
        normalized_arr = np.array(image_arr) / 255
        resized_feature = normalized_arr.reshape(-1, IMG_SIZE, IMG_SIZE, 1)

    except Exception as e:
        ex = str(e)

    l_images_parsed['image_filepath'] = p_image_fl
    l_images_parsed['parsing_status'] = status
    l_images_parsed['parsing_exception'] = ex
    l_images_parsed['image_array_shape_0'] = arr_shape[0]
    l_images_parsed['image_array_shape_1'] = arr_shape[1]
    l_images_parsed['image_array'] = json.dumps(image_arr.flatten().tolist())
    l_images_parsed['normalized_image_array'] = json.dumps(normalized_arr.flatten().tolist()) #normalized_arr.flatten()
    l_images_parsed['resized_feature'] = json.dumps(resized_feature.flatten().tolist()) #resized_feature.flatten()

    return l_images_parsed
