
-- The following resources are assumed and pre-existing
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- =========================
PUT file://./src/python/sp_commons.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

-- =========================
PUT file://./src/python/file_header.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

create or replace procedure parse_file_header(
            stage_path varchar ,staged_data_flname varchar)
        returns variant
        language python
        runtime_version = '3.8'
        packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
        imports = ('@lib_stg/scripts/file_header.py' 
            ,'@lib_stg/scripts/sp_commons.py')
        handler = 'file_header.main';

-- call parse_file_header( 'data_stg/data','reduced_sample_data.json');
-- call parse_file_header( 'data_stg/data','2022_10_01_priority_health_HMO_in-network-rates.zip');


-- =========================
PUT file://./src/python/negotiation_arrangements.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

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

-- call parse_negotiation_arrangement_segments( 'data_stg/data','reduced_sample_data.json','@ext_data_stg/raw_parsed' ,0 ,10);
-- call parse_negotiation_arrangement_segments( 'data_stg/data','2022_10_01_priority_health_HMO_in-network-rates.zip','@ext_data_stg/raw_parsed' ,0 ,200)