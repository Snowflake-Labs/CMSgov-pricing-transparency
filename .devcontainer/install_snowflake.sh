
#!/bin/bash

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