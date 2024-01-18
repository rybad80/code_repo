select
    rsc_content_user_key,
    content_name,
    user_name,
    sum(case when usage_date >= current_timestamp - 1 then 1 else 0 end) as usage_1_day,
    sum(case when usage_date >= current_timestamp - 7 then 1 else 0 end) as usage_7_day,
    sum(case when usage_date >= current_timestamp - 90 then 1 else 0 end) as usage_90_day,
    sum(case when usage_date >= current_timestamp - 180 then 1 else 0 end) as usage_180_day,
    sum(case when usage_date >= current_timestamp - 365 then 1 else 0 end) as usage_365_day,
    min(usage_date) as first_usage_date,
    max(usage_date) as last_usage_date,
    content_guid,
    content_id,
    asset_inventory_key
from
    {{ ref('stg_usage_rstudio_connect_content_sessions') }}
group by
    rsc_content_user_key,
    content_name,
    user_name,
    content_guid,
    content_id,
    asset_inventory_key
