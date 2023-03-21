#!/bin/bash

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
echo "Running SQL script : $1 ..."
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$1"