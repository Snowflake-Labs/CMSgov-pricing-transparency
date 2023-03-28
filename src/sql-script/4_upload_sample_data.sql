

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
use role public;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.public;

-- =========================
PUT file://./data/* @data_stg/data
    auto_compress = false
    overwrite = true
    parallel=5;

-- =========================
