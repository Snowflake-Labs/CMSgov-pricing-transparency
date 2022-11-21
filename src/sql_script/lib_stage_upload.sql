
-- =========================
-- This script is used to upload the python script
-- files to the library stage
-- =========================


-- PUT file://./src/python/innetwork_rates_ingestor_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-file-header_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-seg-header_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-segments_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-segments_ranges_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/in-network-rates-segment-dagbuilder.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/in-network-rates-dag-suspender.py @sflk_pricing_transperancy.public.lib_stg/scripts
--     overwrite = true;


-- ==============================================
PUT file://./src/python/in-network-rates_v2/ingest-in-network-rates-seg-header_sp_v2.py @sflk_pricing_transperancy.public.lib_stg/scripts
    overwrite = true;

PUT file://./src/python/in-network-rates_v2/ingest-in-network-rates-segment-lists_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
    overwrite = true;

PUT file://./src/python/in-network-rates_v2/in-network-rates-segment-dagbuilder-v2.py @sflk_pricing_transperancy.public.lib_stg/scripts
    overwrite = true;

list @sflk_pricing_transperancy.public.lib_stg/scripts;