
-- =========================
-- This script is used to configure the stored procedures and functions that will be used by
-- the demo
-- =========================

create or replace procedure sflk_pricing_transperancy.public.innetwork_rates_ingestor_sp(
    batch_size integer , stage_path varchar ,staged_data_flname varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson')
    imports = ('@sflk_pricing_transperancy.public.lib_stg/scripts/innetwork_rates_ingestor_sp.py')
    handler = 'innetwork_rates_ingestor_sp.main'
    ;

--
create or replace procedure sflk_pricing_transperancy.public.innetwork_rates_segments_ingestor_sp(
    batch_size integer , stage_path varchar ,staged_data_flname varchar ,negotiation_arrangement_segment varchar)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python' ,'pandas', 'ijson')
    imports = ('@sflk_pricing_transperancy.public.lib_stg/scripts/ingest-in-network-rates-segments_sp.py')
    handler = 'ingest-in-network-rates-segments_sp.main'
    ;
