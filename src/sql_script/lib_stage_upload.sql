
-- =========================
-- This script is used to upload the python script
-- files to the library stage
-- =========================


-- PUT file://./src/python/innetwork_rates_ingestor_sp.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-file-header_sp.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-seg-header_sp.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-segments_sp.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/ingest-in-network-rates-segments_ranges_sp.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/in-network-rates-segment-dagbuilder.py @lib_stg/scripts
--     overwrite = true;

-- PUT file://./src/python/in-network-rates/in-network-rates-dag-suspender.py @lib_stg/scripts
--     overwrite = true;


-- ==============================================
PUT file://./src/python/in-network-rates_v2/ingest-in-network-rates-seg-header_sp_v2.py @lib_stg/scripts
    overwrite = true;

PUT file://./src/python/in-network-rates_v2/ingest-in-network-rates-segment-lists_sp.py @lib_stg/scripts
    overwrite = true;

PUT file://./src/python/in-network-rates_v2/in-network-rates-segment-dagbuilder-v2.py @lib_stg/scripts
    overwrite = true;

list @lib_stg/scripts;