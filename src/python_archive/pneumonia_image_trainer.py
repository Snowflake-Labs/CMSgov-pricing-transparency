

import logging
import numpy as np
import pandas as pd
import ast  
import tensorflow
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Conv2D , MaxPool2D , Flatten , Dropout , BatchNormalization
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report,confusion_matrix
from tensorflow.keras.callbacks import ReduceLROnPlateau
from joblib import dump, load
import joblib ,io ,sys ,os
# import logging

#logger = logging.getLogger("mtrainer")

os.environ['NUMEXPR_MAX_THREADS'] = '28'

# Read table into dataframe
def read_table_to_dataframe(p_session ,p_table ,p_row_limit):
    columns =['CLASS_LABEL_NUM' ,'IMAGE_ARRAY_SHAPE_0' ,'IMAGE_ARRAY_SHAPE_1' ,'RESIZED_FEATURE' ] #,'IMAGE_ARRAY','NORMALIZED_IMAGE_ARRAY','RESIZED_FEATURE'
    spdf = p_session.table(p_table).limit(p_row_limit)
    projected_spdf = spdf.select(columns)
    pddf = projected_spdf.to_pandas()
    return pddf

def train_split(p_df ,p_class_col):
    target_col = p_class_col
    split_X = p_df.drop([target_col], axis=1)
    split_y = p_df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(split_X, split_y, test_size=0.7, random_state=123)

    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.5, random_state=123)

    return (X_train, X_test, y_train, y_test ,X_val ,y_val)

def reshape_feature(p_df):
    def reshape_flattened_array(x):
        image_size = (-1 ,x['IMAGE_ARRAY_SHAPE_0'] ,x['IMAGE_ARRAY_SHAPE_1'] ,1)
        # image_size = (x['IMAGE_ARRAY_SHAPE_0'] ,x['IMAGE_ARRAY_SHAPE_1'])

        # The array is stored in Snowflake as an array, hence we need to convert it back
        # to array
        # image_arr_str = x['NORMALIZED_IMAGE_ARRAY']
        image_arr_str = x['RESIZED_FEATURE']
        image_arr_str = image_arr_str.replace('\n', ' ')
        arr = np.array(ast.literal_eval(image_arr_str))
        B = np.reshape(arr, image_size)
        return B

    df = p_df.copy(deep=True)
    df['IMAGE_FEATURE'] = df.apply(lambda x: reshape_flattened_array(x) ,axis=1)
    # df['IMAGE_FEATURE_SHAPE'] = df.apply(lambda x: np.shape(x['IMAGE_FEATURE']) ,axis=1)

    df.drop(['IMAGE_ARRAY_SHAPE_0' ,'IMAGE_ARRAY_SHAPE_1' ,'RESIZED_FEATURE'] ,axis=1 ,inplace=True) #,'IMAGE_ARRAY'  ,'IMAGE_ARRAY','NORMALIZED_IMAGE_ARRAY','RESIZED_FEATURE'
    return df

def restructure_data(p_X ,p_Y):
    #logger.info('#restructure_data ')
    try:
        # convert the dataframe into an numpy array
        #   step-1: convert df to array. This method is done for the reason
        #           to_numpy screws up and does not return the shape as expected
        # x = np.array( [ x for x in p_X['IMAGE_FEATURE'] ] ).reshape(-1, 150, 150, 1)

        x = np.array( [ x[0] for x in p_X['IMAGE_FEATURE'] ] )
        
        y = [ x for x in p_Y ]
        return (x , y)
    except Exception as e: 
        # logger.error(str(e))
        pass

    return (None , None)

# Data augementation and fit
def datagenerate_fit(p_X_train):
    # # convert the dataframe into an numpy array
    # #   step-1: convert df to array. This method is done for the reason
    # #           to_numpy screws up and does not return the shape as expected
    # arr = [ x for x in p_X_train ]

    # np_arr = np.array(arr)

    # reshape the array
    # np_arr_reshaped = np_arr.reshape(-1, 150, 150, 1)
    np_arr_reshaped = p_X_train

    # With data augmentation to prevent overfitting and handling the imbalance in dataset
    datagen = ImageDataGenerator(
        featurewise_center=False,  # set input mean to 0 over the dataset
        samplewise_center=False,  # set each sample mean to 0
        featurewise_std_normalization=False,  # divide inputs by std of the dataset
        samplewise_std_normalization=False,  # divide each input by its std
        zca_whitening=False,  # apply ZCA whitening
        rotation_range = 30,  # randomly rotate images in the range (degrees, 0 to 180)
        zoom_range = 0.2, # Randomly zoom image 
        width_shift_range=0.1,  # randomly shift images horizontally (fraction of total width)
        height_shift_range=0.1,  # randomly shift images vertically (fraction of total height)
        horizontal_flip = True,  # randomly flip images
        vertical_flip=False)  # randomly flip images
    
    datagen.fit(np_arr_reshaped)

    return datagen

def define_model_pipeline():
    model = Sequential()
    model.add(Conv2D(32 , (3,3) , strides = 1 , padding = 'same' , activation = 'relu' , input_shape = (150,150,1)))
    model.add(BatchNormalization())
    model.add(MaxPool2D((2,2) , strides = 2 , padding = 'same'))
    model.add(Conv2D(64 , (3,3) , strides = 1 , padding = 'same' , activation = 'relu'))
    model.add(Dropout(0.1))
    model.add(BatchNormalization())
    model.add(MaxPool2D((2,2) , strides = 2 , padding = 'same'))
    model.add(Flatten())
    model.add(Dense(units = 128 , activation = 'relu'))
    model.add(Dropout(0.2))
    model.add(Dense(units = 1 , activation = 'sigmoid')) # Tanh
    model.compile(optimizer = "rmsprop" , loss = 'binary_crossentropy' , metrics = ['accuracy'])
    
    return model

def model_fit(p_model ,p_datagen ,p_x_train ,p_y_train ,p_X_val ,p_y_val ,p_batch_size ,p_epochs):
    learning_rate_reduction = ReduceLROnPlateau(monitor='val_accuracy', patience = 2, verbose=1,factor=0.3, min_lr=0.000001)

    history = p_model.fit(
        p_datagen.flow(p_x_train,p_y_train, batch_size = p_batch_size) 
        ,epochs = p_epochs 
        ,validation_data = p_datagen.flow(p_X_val, p_y_val) 
        ,callbacks = [learning_rate_reduction])

    return history

# Save model
def save_file(session, model, path, dest_filename):
  input_stream = io.BytesIO()
  joblib.dump(model, input_stream)
  session._conn.upload_stream(input_stream, path, dest_filename)
  return "successfully created file: " + path

def main(p_session ,p_row_limit ,p_stage_path ,p_staged_model_flname ,p_epochs):
    logs = []
    # logger.info('-----------------------')
    # #logger.info(f' {p_row_limit} :: {p_stage_path} :: {p_staged_model_flname} ')
    # logs.append(f' {p_row_limit} :: {p_stage_path} :: {p_staged_model_flname} ')
    try:
        #logger.info('load data ...')
        logs.append('load data ...')
        pddf = read_table_to_dataframe(p_session ,'image_parsed_raw' ,p_row_limit)

        #logger.info('train and split ...')
        logs.append('train and split ...')
        X_train, X_test, Y_train, Y_test ,X_val ,Y_val = train_split(pddf ,'CLASS_LABEL_NUM')
        pddf = None

        logs.append(f' LEN train test val : {len(X_train)} / {len(X_test)} / {len(X_val)}')

        #logger.info('reshape data ...')
        logs.append('reshape data ...')
        X_train_reshaped = reshape_feature(X_train)

        # logs.append('reshape data 2 ...')
        # # X_test_reshaped = reshape_feature(X_test)

        logs.append('reshape data 3 ...')
        X_val_reshaped = reshape_feature(X_val)
        
        logs.append(f' Shapeof Train / Val : {np.shape(X_train_reshaped)} / {np.shape(X_val_reshaped)}')
        #logger.info(f' Shapeof Train / Val : {np.shape(X_train_reshaped)} / {np.shape(X_val_reshaped)}')

        # logs.append(f'reshaping completed ')
        # for c in X_train_reshaped.columns:
        #     logs.append(f' col: col {c} ')

        #logger.info('reshape data 2.1 ...')
        logs.append('reshape data 2.1 ...')
        x_train ,y_train = restructure_data(X_train_reshaped ,Y_train)
        X_train_reshaped = None
        Y_train = None
        logs.append(f' Shapeof Train / Val : {np.shape(x_train)} / {np.shape(y_train)}')

        # logs.append('reshape data 2.2 ...')
        # # x_test ,y_test = restructure_data(X_test_reshaped ,Y_test)

        logs.append('reshape data 2.3 ...')
        x_val ,y_val = restructure_data(X_val_reshaped ,Y_val)
        X_val_reshaped = None
        Y_val = None
        logs.append(f' Shapeof Train / Val : {np.shape(x_val)} / {np.shape(y_val)}')

        #logger.info('instantiate data generator ...')
        logs.append('instantiate data generator ...')
        datagen = datagenerate_fit(x_train)

        #logger.info('define model pipeline ...')
        logs.append('define model pipeline ...')
        model = define_model_pipeline()

        #logger.info('model fit ...')
        logs.append('model fit ...')
        history = model_fit(model ,datagen ,x_train ,y_train ,x_val ,y_val ,10 ,p_epochs)
        # history = None
        # datagen = None
        # x_train = None
        # y_train = None
        # x_val = None
        # y_val = None

        #logger.info('save model ...')
        logs.append('save model ...')
        # # model.save('model/pneumonia_cnn_macos.h5')
        save_file(p_session, model, p_stage_path ,p_staged_model_flname)

        #logger.info('Finished')
        logs.append('Finished')
    except Exception as e: 
        logs.append(str(e))
        #logger.error(str(e))

    ret_val = {}
    ret_val['logs'] = logs

    return ret_val