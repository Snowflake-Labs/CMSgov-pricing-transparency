
-- ## ------------------------------------------------------------------------------------------------
-- # Copyright (c) 2023 Snowflake Inc.
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.You may obtain 
-- # a copy of the License at

-- #     http://www.apache.org/licenses/LICENSE-2.0
    
-- # Unless required by applicable law or agreed to in writing, software
-- # distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

-- # See the License for the specific language governing permissions andlimitations 
-- # under the License.
-- ## ------------------------------------------------------------------------------------------------

-- The following resources are assumed and pre-existing
use role &APP_DB_role;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- =========================
PUT file://./src/python/sp_commons.py @lib_stg/scripts overwrite = true;

-- =========================
PUT file://./src/python/file_header.py @lib_stg/scripts overwrite = true;

create or replace procedure parse_file_header(
        stage_path varchar ,staged_data_flname varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/file_header.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'file_header.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

-- call parse_file_header( 'data_stg/data','reduced_sample_data.json');
-- call parse_file_header( 'data_stg/data','2022_10_01_priority_health_HMO_in-network-rates.zip');

-- =========================
PUT file://./src/python/negotiation_arrangements.py @lib_stg/scripts overwrite = true;

create or replace procedure parse_negotiation_arrangement_segments(
        stage_path varchar ,staged_data_flname varchar ,target_stage_and_path varchar
        ,from_idx integer ,to_idx integer)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/negotiation_arrangements.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'negotiation_arrangements.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

-- call parse_negotiation_arrangement_segments( 'data_stg/data','reduced_sample_data.json','@ext_data_stg/raw_parsed' ,0 ,10);
-- call parse_negotiation_arrangement_segments( 'data_stg/data','2022_10_01_priority_health_HMO_in-network-rates.zip','@ext_data_stg/raw_parsed' ,0 ,200)

-- =========================
PUT file://./src/python/negotiation_arrangements_header.py @lib_stg/scripts overwrite = true;

create or replace procedure negotiation_arrangements_header(
        stage_path varchar ,staged_data_flname varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/negotiation_arrangements_header.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'negotiation_arrangements_header.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

-- =========================
PUT file://./src/python/provider_references.py @lib_stg/scripts overwrite = true;

create or replace procedure provider_references(
        stage_path varchar ,staged_data_flname varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/provider_references.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'provider_references.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

-- =========================
PUT file://./src/python/in_network_rates_dagbuilder.py @lib_stg/scripts overwrite = true;

create or replace procedure in_network_rates_dagbuilder(
        stage_path varchar ,staged_data_flname varchar ,target_stage_and_path varchar
        ,segments_per_task integer ,warehouse_to_be_used varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/in_network_rates_dagbuilder.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'in_network_rates_dagbuilder.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

create or replace procedure in_network_rates_dagbuilder_matrix(
        stage_path varchar ,staged_data_flname varchar ,target_stage_and_path varchar
        ,segments_per_task integer ,warehouse_to_be_used varchar ,dag_rows integer ,dag_cols integer)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
    imports = ('@lib_stg/scripts/in_network_rates_dagbuilder.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'in_network_rates_dagbuilder.main_matrix'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;
-- call in_network_rates_dagbuilder('data_stg/data','reduced_sample_data.json','@ext_data_stg/raw_parsed', 200 ,'DEMO_BUILD_WH');

-- =========================
PUT file://./src/python/delete_dag_for_datafile.py @lib_stg/scripts overwrite = true;

create or replace procedure delete_dag_for_datafile(staged_data_flname varchar ,drop_task boolean)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas')
    imports = ('@lib_stg/scripts/delete_dag_for_datafile.py' 
        ,'@lib_stg/scripts/sp_commons.py')
    handler = 'delete_dag_for_datafile.main'
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
;

-- call delete_dag_for_datafile('2022_10_01_priority_health_HMO_in-network-rates.zip' ,true);
-- call delete_dag_for_datafile('reduced_sample_data.json' ,true);
-- =========================
