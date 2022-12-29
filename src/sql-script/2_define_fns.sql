
-- The following resources are assumed and pre-existing
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;


-- =========================
-- PUT file://./src/python/skimage_parser_fn.py @lib_stg/scripts 
--     auto_compress = false
--     overwrite = true;

-- create or replace function skimage_parser_fn(image_fl varchar)
--      returns variant
--      language python
--      runtime_version = '3.8'
--      packages = ('snowflake-snowpark-python','numpy', 'pandas', 'scikit-learn' ,'scikit-image')
--      imports = ('@lib_stg/scripts/skimage_parser_fn.py')
--      handler = 'skimage_parser_fn.main'
--      ;

-- =========================
-- PUT file://./src/python/pneumonia_image_trainer.py @lib_stg/scripts 
--     auto_compress = false
--     overwrite = true;

        create or replace procedure innetwork_rates_segheader(
            batch_size integer ,stage_path varchar ,staged_data_flname varchar ,target_stage_for_segment_files varchar
            ,from_idx integer ,to_idx integer ,self_task_name varchar)
        returns variant
        language python
        runtime_version = '3.8'
        packages = ('snowflake-snowpark-python' ,'pandas', 'ijson' ,'simplejson')
        imports = ('@sflk_pricing_transperancy.public.lib_stg/scripts/negotiation_arrangements.py')
        handler = 'negotiation_arrangements.main'
    ;

-- =========================

-- upload_locallibraries_to_p_stage(sp_session ,'../python/in_network_udtf' ,config['APP']['database'] ,'public' ,lib_stage ,'scripts')

-- stmts = [
--     f''' 
--     create or replace procedure innetwork_rates_nr_dagbuilder(
--             batch_size integer ,stage_path varchar ,staged_data_flname varchar ,target_stage_for_segment_files varchar
--             ,parallels number ,execution_warehouse varchar ,rerun boolean)
--         returns variant
--         language python
--         runtime_version = '3.8'
--         packages = ('snowflake-snowpark-python' ,'pandas', 'ijson')
--         imports = ('@sflk_pricing_transperancy.public.lib_stg/scripts/negotiation_arrangements_dagbuilder.py')
--         handler = 'negotiation_arrangements_dagbuilder.main'
--         ;
--     '''

--        f''' create or replace stage {config['APP']['database']}.public.{config['APP']['data_parsed_stg']}
--             directory = (enable=true)
--             comment = 'used for holding parsed record.'; '''

--     # ,f''' alter warehouse dev_pctransperancy_demo_wh set max_concurrency_level = 8 '''
--     ,f''' alter warehouse dev_pctransperancy_demo_wh set warehouse_size = XSMALL; '''
--     ,f'''truncate table {config['APP']['database']}.public.segment_task_execution_status; '''
--     ,f''' call {config['APP']['database']}.public.innetwork_rates_segheader(
--             5 ,'{stage_path}' ,'{data_file}' ,'ext_data_stg/data_pricing_parsed' 
--             ,-1 ,50 ,'-');'''