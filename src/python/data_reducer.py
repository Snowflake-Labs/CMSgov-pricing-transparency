
'''
    This script is used to create a smaller sample of the pricing transperancy file. By 
creating a smaller sample set, it helps in understanding and demonstrating a quicker demo.

To run this script:
 - conda activate pysnowpark
 - python ./src/python/data_reducer.py
'''
## ------------------------------------------------------------------------------------------------
# Copyright (c) 2023 Snowflake Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.You may obtain 
# a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0
    
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions andlimitations 
# under the License.
## ------------------------------------------------------------------------------------------------

import ijson 
import json 
from zipfile import ZipFile
from decimal import *
import simplejson as sjson

PROJECT_HOME_DIR = '.'
ip_data_fl = f'{PROJECT_HOME_DIR}/data/2022_10_01_priority_health_HMO_in-network-rates.zip'
op_reduced_sample_data_fl = f'{PROJECT_HOME_DIR}/data/reduced_sample_data.json'

sampled_data = {}
MAX_IN_NETWORK_SAMPLE_COUNT = 10

print(f'Input data file: {ip_data_fl}')
print('Copying non in_network elements')
with ZipFile(ip_data_fl) as zf:
    for file in zf.namelist():
        with zf.open(file) as f:
            parser = ijson.parse(f)
            seg_count = -1
            for prefix, event, value in parser:
                if event != 'string':
                    continue
                elif 'in_network' in prefix:
                    continue
                else:
                    sampled_data[prefix] = value
                

            print('Copying in_network elements')
            f.seek(0)

# with ZipFile(json_filename) as zf:
#     for file in zf.namelist():
#         with zf.open(file) as f:
            seq_no = -1
            innetwork_records = []
            for rec in ijson.items(f, 'in_network.item' ,use_float=True):
                seq_no += 1

                
                innetwork_rec = rec.copy()
                innetwork_records.append(innetwork_rec)

                if seq_no >= MAX_IN_NETWORK_SAMPLE_COUNT:
                    break
                
            sampled_data['in_network'] = innetwork_records
            

print(f'Storing sample data in file:{op_reduced_sample_data_fl} ...')
with open(op_reduced_sample_data_fl, "w") as out_fl:
    rec_str = sjson.dumps(sampled_data , indent=4, sort_keys=True)
    out_fl.write(rec_str)

print('Finished!!')