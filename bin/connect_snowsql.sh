#!/bin/bash
#
# Purpose: 
#   Utility script to run the snowsql, the benefit of using this wrapper script
# is that it, pre-configures the snowsql with connection information provided
# it also creates the local config file, which contains the various variables that could
# be substituted in the sql scripts. 
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

# Retreive the connection secrets
SNOWSQL_ACCOUNT="$( python ./bin/parse_connection_secrets.py account )"
SNOWSQL_USER="$( python ./bin/parse_connection_secrets.py user )"
export SNOWSQL_PWD="$( python ./bin/parse_connection_secrets.py password )"

#start snowsql session
snowsql --config .app_store/snowsql_config.ini -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER