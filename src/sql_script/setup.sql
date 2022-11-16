
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

create or replace transient table segment_task_execution_status (
    task_name varchar
    ,task_ret_status varchar
    ,elapsed varchar
    ,inserted_at timestamp default current_timestamp()
);



create or replace transient table negotiated_rates ( 
    record_num number
    ,data_file varchar
    ,header_id varchar
    ,header_id_hash number
    ,negotiated_rates varchar
);