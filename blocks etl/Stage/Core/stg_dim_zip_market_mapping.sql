{{ config(meta = {
    'critical': true
}) }}

select
    lpad(zip, 5, '0') as zip,
    map_flag,
    lpad(map_zip, 5, '0') as map_zip,
    city,
    city_state,
    cbsa,
    lpad(state_county_fips_code, 5, '0') as county_fips_code,
    dominant_county,
    dominant_county_state,
    state,
    state_abbrev as state_code,
    chop_market,
    region_category,
    koph_service_area_tier,
    upd_dt as update_date
from
    {{ source('manual_ods', 'zip_market_mapping') }}
