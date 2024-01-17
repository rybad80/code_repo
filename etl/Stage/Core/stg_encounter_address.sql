{{ config(
    materialized='table', 
    dist='visit_key',
    meta = {
        'critical': true
    } 
) }}

select
    pat_addr_chng_hx.pat_id,
    stg_visit_dates.visit_key,
    zc_county.name as county,
    zc_state.name as state,
    pat_addr_chng_hx.line as seq_num,
    pat_addr_chng_hx.zip_hx as zip,
    strleft(pat_addr_chng_hx.zip_hx, 5) as zip_5_digit,
    case when lower(zc_state.name) in ('international', 'other') then 1 else 0 end as intl_other_ind,
    row_number() over (
    partition by
        stg_visit_dates.visit_key
    order by
        stg_visit_dates.encounter_date - pat_addr_chng_hx.eff_start_date
    ) as line_most_recent_address
from
    {{ref('stg_visit_dates')}} as stg_visit_dates
    inner join  {{source('clarity_ods','pat_addr_chng_hx')}} as pat_addr_chng_hx
        on pat_addr_chng_hx.pat_id = stg_visit_dates.pat_id
        and stg_visit_dates.encounter_date
        between
        pat_addr_chng_hx.eff_start_date
            and coalesce(pat_addr_chng_hx.eff_end_date - 1, current_date + 5 * 365)
    left join {{source('clarity_ods','zc_state')}} as zc_state
        on zc_state.internal_id = pat_addr_chng_hx.state_hx_c
    left join {{source('clarity_ods','zc_county')}} as zc_county
        on zc_county.internal_id = pat_addr_chng_hx.county_hx_c
