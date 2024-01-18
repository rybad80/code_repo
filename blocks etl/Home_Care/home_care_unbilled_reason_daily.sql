{{
  config(
    materialized = 'incremental',
    unique_key = 'unbilled_reason_unique_key',
        meta = {
        'critical': false
    }
  )
}}

with incremental as (
select
    hdms_unbilled_reason,
    current_date as unbilled_date,
    sum(hdms_net_charge) as total_daily_unbilled_reason_revenue,
    {{ dbt_utils.surrogate_key([
        'unbilled_date',            
        'hdms_unbilled_reason'
        ]) }} as unbilled_reason_unique_key
from
    {{ ref('home_care_claim_details') }}
where hdms_unbilled_reason != ''
group by unbilled_date, hdms_unbilled_reason
)

select
    *

from
    incremental

where
    1 = 1
    {%- if is_incremental() %}
        and unbilled_reason_unique_key not in
        (
            select unbilled_reason_unique_key
            from
                {{ this }} -- TDL dim table
            where unbilled_reason_unique_key = incremental.unbilled_reason_unique_key
        )
    {%- endif %}
    