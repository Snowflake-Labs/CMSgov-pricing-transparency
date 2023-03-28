#!/bin/bash

#
# This script gets executed at the end of the codespace creation. This is
# a place to run any configurations or command line utilities.
#
# NOTE: If you add lot of utilities here, it might take a longer time to initialize the final 
# environment. 
#
#

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

# Used by execution script to verify if we are running in a codespace docker environment 
echo "running in codespace" >> .is_codespace_env.txt 

# The following will install additional packages in the 'pyspark' conda enviroment
# enable this if you want them to be added during the startup
#
# WARN: This would increase the startup time, test it out and see if you would be ok
#       on the timeline
#
#conda env update -f ./environment.yml

#This needs to be run to ensure the local utility scripts are add to path
# python ./bin/setup.py install