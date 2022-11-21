
-- =========================
-- This script is used to configure the base resources that will be used by
-- the demo
-- =========================

create or replace database sflk_pricing_transperancy
    comment = 'used for pricing transperancy demo';

create or replace stage sflk_pricing_transperancy.public.lib_stg
    comment = 'used for holding libraries and other core artifacts.';

create or replace stage sflk_pricing_transperancy.public.data_stg
    comment = 'used for holding data.';

-- ============================
-- Task role definition (OPTIONAL)
-- ============================
-- Task admin role
-- USE ROLE securityadmin;
-- CREATE or replace ROLE pt_taskadmin comment = ' task admin created for pricing transperancy solution';

-- -- set the active role to ACCOUNTADMIN before granting the account-level privileges to the new role
-- USE ROLE accountadmin;
-- GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE pt_taskadmin;

-- -- set the active role to SECURITYADMIN to show that this role can grant a role to another role
-- USE ROLE securityadmin;
-- GRANT ROLE pt_taskadmin TO ROLE dev_pctransperancy_demo_rl;


-- ============================
-- Table definitions
-- ============================

-- create or replace transient table negotiated_rates ( 
--     record_num number
--     ,data_file varchar
--     ,header_id varchar
--     ,header_id_hash number
--     ,negotiated_rates varchar
-- );

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
