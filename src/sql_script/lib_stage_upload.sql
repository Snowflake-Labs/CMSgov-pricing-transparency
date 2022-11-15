
-- =========================
-- This script is used to upload the python script
-- files to the library stage
-- =========================


PUT file://./src/python/innetwork_rates_ingestor_sp.py @sflk_pricing_transperancy.public.lib_stg/scripts
    overwrite = true;

list @sflk_pricing_transperancy.public.lib_stg/scripts;