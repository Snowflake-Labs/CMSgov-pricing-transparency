
-- The following resources are assumed and pre-existing
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- =========================
PUT file://./data/reduced_sample_data.json @data_stg/raw_data
    auto_compress = false
    overwrite = true;

-- =========================