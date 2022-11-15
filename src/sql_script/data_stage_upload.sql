
-- =========================
-- This script is used to upload the data files
-- files to the data stage
-- =========================


PUT file://./data/2022_10_01_priority_health_HMO_in-network-rates.zip @sflk_pricing_transperancy.public.data_stg/price_transperancy
    parallel = 4
    auto_compress = false
    overwrite = true;

list @sflk_pricing_transperancy.public.data_stg/price_transperancy;

