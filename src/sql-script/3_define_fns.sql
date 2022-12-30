
-- The following resources are assumed and pre-existing
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- =========================
PUT file://./src/python/sp_commons.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

-- =========================
PUT file://./src/python/negotiation_arrangements.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

create or replace procedure parse_negotiation_arrangement_segments(
            batch_size integer ,stage_path varchar ,staged_data_flname varchar ,target_stage_for_segment_files varchar
            ,from_idx integer ,to_idx integer)
        returns variant
        language python
        runtime_version = '3.8'
        packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
        imports = ('@lib_stg/scripts/negotiation_arrangements.py' 
            ,'@lib_stg/scripts/sp_commons.py')
        handler = 'negotiation_arrangements.main'

-- call parse_negotiation_arrangement_segments(1000 ,)