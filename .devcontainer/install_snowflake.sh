
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

export snowsql_version=1.2.23
export bootstrap_version=`echo ${snowsql_version}|cut -c -3`
export snowsql_file=${snowsql_file:-snowsql-${snowsql_version}-linux_x86_64.bash}
cd /
touch ~/.profile

# Install snowsql
echo "Download SnowSQL client version" ${snowsql_version} "..."
curl --create-dirs -O --output-dir /tmp/downloads https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${bootstrap_version}/linux_x86_64/${snowsql_file}

SNOWSQL_DEST=/usr/bin SNOWSQL_LOGIN_SHELL=~/.profile bash /tmp/downloads/${snowsql_file}
export PATH=$PATH:/usr/bin

# Cleanup
rm /tmp/downloads/${snowsql_file}