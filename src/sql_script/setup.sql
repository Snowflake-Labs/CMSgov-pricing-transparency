
-- =========================
-- This script is used to configure the base resources that will be used by
-- the demo
-- =========================

create or replace database sflk_pricing_transperancy
    comment = 'used for pricing transperancy demo';

create or replace stage sflk_pricing_transperancy.public.lib_stg
    comment = 'used for holding libraries and other core artifacts.';

create or replace stage sflk_pricing_transperancy.public.data_stg
    comment = 'used for holding data.';
