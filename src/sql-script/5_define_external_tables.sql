

-- ## ------------------------------------------------------------------------------------------------
-- # Copyright (c) 2023 Snowflake Inc.
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.You may obtain 
-- # a copy of the License at

-- #     http://www.apache.org/licenses/LICENSE-2.0
    
-- # Unless required by applicable law or agreed to in writing, software
-- # distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

-- # See the License for the specific language governing permissions andlimitations 
-- # under the License.
-- ## ------------------------------------------------------------------------------------------------

-- The following resources are assumed and pre-existing
use role &APP_DB_role;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.&APP_DB_schema;

-- -- =========================
create or replace external table ext_negotiated_arrangments_staged(
        p_data_fl varchar as ( split_part(metadata$filename, '/', 2) )
        ,p_billing_code varchar as ( split_part(metadata$filename, '/', 3) )
        ,p_billing_code_type_and_version varchar as ( split_part(metadata$filename, '/', 4) )
        ,p_negotiation_arrangement varchar as ( split_part(metadata$filename, '/', 5) )
        ,p_segment_type varchar as ( split_part(metadata$filename, '/', 6) )
    )
    partition by (p_data_fl ,p_negotiation_arrangement ,p_billing_code ,p_billing_code_type_and_version ,p_segment_type)
    location = @&APP_DB_ext_stage/&APP_DB_folder_parsed/
    refresh_on_create = false 
    auto_refresh = false
    file_format = ( type = parquet )
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
    ;

/* a view of negotiated rates segment in stage */
create or replace view negotiated_rates_segments_v
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
    as
    select 
        r.data_file as data_file
        ,p_data_fl as data_fl_basename
        ,value:SEQ_NO::int as segment_idx
        ,p_negotiation_arrangement as negotiation_arrangement
        ,p_billing_code as billing_code 
        ,p_billing_code_type_and_version as billing_code_type_version_and_version 
        ,value:name::varchar as name
        ,value:description::varchar as description
        ,value:CHUNK_NO::int as chunk_no
        ,array_size(value:NEGOTIATED_RATES) as chunk_size
        ,value:NEGOTIATED_RATES as negotiated_rates
        ,value as segment_chunk_raw
    from ext_negotiated_arrangments_staged as l
        join in_network_rates_file_header as r
            on l.p_data_fl = r.data_file_basename
    where p_segment_type = 'negotiated_rates'
    ;

/* a grouped view of various negotiated rates segment in a file and their sub-ordinate record count */
create or replace view negotiated_rates_segment_stats_v
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
    as
    select 
        data_file
        ,segment_idx
        ,negotiation_arrangement
        ,billing_code 
        ,billing_code_type_version_and_version 
        ,name
        ,sum(chunk_size) as segment_record_count
    from negotiated_rates_segments_v
    group by 
        data_file
        ,segment_idx
        ,negotiation_arrangement
        ,billing_code 
        ,billing_code_type_version_and_version 
        ,name
    order by segment_idx
    ;

/* a flattened view of various negotiated rates segment in a file and their sub-ordinate record count */
create or replace view negotiated_rates_segment_info_v
    comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
    as
    select 
        t.* exclude(negotiated_rates)
        ,nr.index as negotiated_rates_record_index
        ,nr.value:negotiated_prices as negotiated_prices
        ,nr.value:provider_groups as provider_groups
    from negotiated_rates_segments_v as t
        , lateral flatten (input => t.negotiated_rates) as nr
    ;

/* a flattened view of negotiated prices */
create or replace view negotiated_prices_v
comment = '{"origin":"sf_sit","name":"pricing_transparency","version":{"major":1, "minor":0},"attributes":{"component":"pricing_transparency"}}'
as
select 
    b.* exclude(negotiated_prices)
    ,p.index as negotiated_prices_record_index
    ,p.value:billing_class::varchar as billing_class
    ,p.value:expiration_date::date as expiration_date
    ,p.value:negotiated_rate::double as negotiated_rate
    ,p.value:negotiated_type::varchar as negotiated_type
    ,p.value:service_code as service_code
from negotiated_rates_segment_info_v as b
    ,lateral flatten(input => b.negotiated_prices) as p
;