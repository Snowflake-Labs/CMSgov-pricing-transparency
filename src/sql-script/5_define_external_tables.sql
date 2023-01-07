
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

create or replace view negotiated_rates_segments_v
comment = 'a view of negotiated rates segment in stage'
as
select 
    r.data_file as data_file
    ,p_data_fl as data_fl_basename
    ,value:SEQ_NO::int as segment_idx
    ,p_negotiation_arrangement as negotiation_arrangement
    ,p_billing_code_type as billing_code_type 
    ,p_billing_code as billing_code 
    ,p_billing_code_type_version as billing_code_type_version 
    ,value:name::varchar as name
    ,value:description::varchar as description
    ,value as negotiated_rates_segment_chunks
from ext_negotiated_arrangments_staged as l
    join in_network_rates_file_header as r
        on l.p_data_fl = r.data_file_basename
where p_segment_type = 'negotiated_rates'
;


create or replace view negotiated_rates_segment_stats_v
comment = 'a grouped view of various negotiated rates segment in a file and thier sub-ordinate record count'
as
select 
    data_file
    ,segment_idx
    ,negotiation_arrangement
    ,billing_code_type 
    ,billing_code 
    ,billing_code_type_version 
    ,name
    ,sum(ARRAY_SIZE(negotiated_rates_segment_chunks:NEGOTIATED_RATES)) as segement_record_count
from negotiated_rates_segments_v
group by 
    data_file
    ,segment_idx
    ,negotiation_arrangement
    ,billing_code_type 
    ,billing_code 
    ,billing_code_type_version 
    ,name
order by segment_idx
;

create or replace view negotiated_rates_segment_info_v
comment = 'a flattened view of various negotiated rates segment in a file and thier sub-ordinate record count'
as
select 
    t.* exclude(negotiated_rates_segment_chunks)
    ,nr.value:negotiated_prices as negotiated_prices
    ,nr.value:provider_groups as provider_groups
from negotiated_rates_segments_v as t
    , lateral flatten (input => t.negotiated_rates_segment_chunks:NEGOTIATED_RATES) as nr
;
