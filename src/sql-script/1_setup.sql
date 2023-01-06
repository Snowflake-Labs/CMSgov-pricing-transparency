
-- The following resources are assumed and pre-existing
use warehouse &SNOW_CONN_warehouse;

-- ============================
-- Task role definition
-- This role is needed for building dags and tasks as part of the demo
-- ============================
use role securityadmin;
create or replace role &APP_DB_task_role 
    comment = ' task admin created for pricing transperancy solution';

-- set the active role to accountadmin before granting the account-level privileges to the new role
use role accountadmin;
grant execute task, execute managed task on account to role &APP_DB_task_role;

-- set the active role to securityadmin to show that this role can grant a role to another role
use role securityadmin;
grant role &APP_DB_task_role to role &APP_DB_role;

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

-- create or replace stage model_stg
--     comment = 'used for holding ml models.';


-- =========================
-- Define tables
-- =========================
create or replace transient table segment_task_execution_status (
    data_file varchar
    ,task_name varchar
    ,start_time timestamp default CURRENT_TIMESTAMP()
    ,end_time timestamp
    ,task_ret_status varchar
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Execution status for the various sub-tasks that gets spawned'
;

create or replace transient table task_to_segmentids (
    bucket varchar
    ,data_file varchar
    ,assigned_task_name varchar
    ,from_idx number
    ,to_idx number
    ,segments_record_count number
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Maps the task that would be spunned to parse the data file. Indicates the segments that should be parsed by these task instances'
;

create or replace transient table in_network_rates_segment_header (
    data_file varchar
    ,segment_id varchar
    ,negotiated_rates_info variant
    ,negotiated_rates_count number
    ,bundled_codes_count number
    ,covered_services_count number
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Used for storing header portion of the negotiated arragments'
;

create or replace transient table in_network_rates_file_header (
    data_file varchar
    ,header variant
    ,inserted_at timestamp default current_timestamp()
)
comment = 'Used for storing header portion of the pricing transperancy files'
;

create or replace view segments_counts_for_datafile_v 
comment = 'view to indicate if all the segments were parsed out'
as
select 
    distinct data_file ,task_name
    ,JSON_EXTRACT_PATH_TEXT(task_ret_status ,'start_rec_num')::int as start_rec_num
    ,JSON_EXTRACT_PATH_TEXT(task_ret_status ,'end_rec_num')::int as end_rec_num
    ,JSON_EXTRACT_PATH_TEXT(task_ret_status ,'last_seg_no')::int as total_no_of_segments
    ,timestampdiff('minute' ,start_time ,end_time) as elapsed
    ,JSON_EXTRACT_PATH_TEXT(task_ret_status ,'Parsing_error') as Parsing_error
    ,JSON_EXTRACT_PATH_TEXT(task_ret_status ,'EOF_Reached') as EOF_Reached
from segment_task_execution_status
where EOF_Reached = True
order by start_rec_num
;

create or replace view current_segment_parsing_tasks_v
comment = 'list of running tasks that are parsing a data file'
as
select 
    l.* 
    ,r.* exclude (data_file ,inserted_at)
from segment_task_execution_status as l
    join task_to_segmentids as r
        on r.data_file = l.data_file
            and contains( lower(l.task_name) ,lower(r.assigned_task_name)) = True
where end_time is null;