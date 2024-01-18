select
    zip,
    map_flag,
    map_zip,
    city,
    city_state,
    cbsa,
    county_fips_code,
    dominant_county,
    dominant_county_state,
    state,
    state_code,
    chop_market,
    region_category,
    update_date
from
    {{ ref('stg_dim_zip_market_mapping') }}
