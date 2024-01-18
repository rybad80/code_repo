select
    {{
        dbt_utils.surrogate_key([
            'qliksense_app_id',
            'application_title',
            'user_name'
        ])
    }} as qliksense_app_user_key,
    application_title,
    user_name,
    sum(case when usage_date >= current_timestamp - 1 then 1 else 0 end) as usage_1_day,
    sum(case when usage_date >= current_timestamp - 7 then 1 else 0 end) as usage_7_day,
    sum(case when usage_date >= current_timestamp - 90 then 1 else 0 end) as usage_90_day,
    min(usage_date) as first_usage_date,
    max(usage_date) as last_usage_date,
    qliksense_app_id,
    asset_inventory_key
from
    {{ ref('stg_usage_qliksense_sessions') }}
group by
    qliksense_app_id,
    application_title,
    user_name,
    asset_inventory_key
