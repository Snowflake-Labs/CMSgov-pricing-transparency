
-- The following resources are assumed and pre-existing
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- -- =========================
create or replace external table ext_negotiated_arrangments_staged(
    p_data_fl varchar as ( split_part(metadata$filename, '/', 2) )
    ,p_segment_id varchar as ( split_part(metadata$filename, '/', 3) )
    ,p_negotiation_arrangement varchar as ( split_part( split_part(metadata$filename, '/', 3), '::', 1) )
    ,p_billing_code_type varchar as ( split_part(split_part(metadata$filename, '/', 3), '::', 2) )
    ,p_billing_code varchar as ( split_part(split_part(metadata$filename, '/', 3), '::', 3) )
    ,p_billing_code_type_version varchar as ( split_part(split_part(metadata$filename, '/', 3), '::', 4) )
    ,p_segment_type varchar as ( split_part(metadata$filename, '/', 4) )
)
partition by (p_data_fl ,p_segment_id ,p_negotiation_arrangement ,p_billing_code_type ,p_billing_code ,p_billing_code_type_version ,p_segment_type)
location = @ext_data_stg/raw_parsed/reduced_sample_data
file_format = ( type = parquet )
;
