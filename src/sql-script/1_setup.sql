
-- The following resources are assumed and pre-existing
use warehouse &SNOW_CONN_warehouse;

-- =========================
-- This script is used to configure the base resources that will be used by
-- the demo
-- =========================
use role sysadmin;

create or replace database &APP_DB_database
    comment = 'used for pricing transperancy demo';

-- Transfer ownership
grant ownership on database &APP_DB_database
    to role public;

grant ownership  on schema &APP_DB_database.public
    to role public;

grant all privileges  on database &APP_DB_database
    to role public;

grant all privileges  on schema &APP_DB_database.public
    to role public;
    
-- =========================
-- Define stages
-- =========================
use role public;
use schema &APP_DB_database.public;

create or replace stage lib_stg
    comment = 'used for holding libraries and other core artifacts.';

create or replace stage data_stg
    directory = ( enable = true )
    comment = 'used for holding data.';

create or replace stage model_stg
    comment = 'used for holding ml models.';


-- =========================
-- Define tables
-- =========================
create or replace transient table segment_task_execution_status (
    data_file varchar
    ,task_name varchar
    ,elapsed varchar
    ,task_ret_status varchar
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Execution status for the various sub-tasks that gets spawned'
;

create or replace transient table task_to_segmentids (
    bucket varchar
    ,data_file varchar
    ,assigned_task_name varchar
    ,segment_ids varchar
    ,segments_count number
    ,segments_record_count number
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Maps the task that would be spunned to parse the data file. Indicates the segments that should be parsed by these task instances'
;

create or replace transient table in_network_rates_segment_header_V2 (
    seq_no number
    ,data_file varchar
    ,segment_id varchar
    ,negotiated_rates_name varchar
    ,negotiated_rates_info varchar
    ,negotiated_rates_count number
    ,bundled_codes_count number
    ,covered_services_count number
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Used for storing header portion of the negotiated arragments'
;

create or replace transient table negotiated_arrangment_segments_v2 (
    seq_no number
    ,data_file varchar
    ,segment_id varchar
    ,segment_type varchar
    ,segment_data varchar
    ,data_hash varchar
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Used for storing parsed segments of negotiated arragments of type negotiated_rates or bundled_codes or covered_services'
;
