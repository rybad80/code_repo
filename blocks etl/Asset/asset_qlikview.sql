select
    qlikview_app_key,
    application_title,
    min(first_usage_date)::date as first_usage_date,
    max(last_usage_date)::date as last_usage_date,
    count(distinct case when usage_90_day > 0 then user_name end) as n_users_90_day,
    sum(usage_90_day) as usage_90_day,
    ceil(sum(usage_7_day) / 7.0) as usage_1_day,
    lower(group_concat(case when usage_90_day > 0 then user_name end)) as users_90_day,
    asset_inventory_key
from
    {{ ref('usage_qlikview_users') }}
group by
    qlikview_app_key,
    application_title,
    asset_inventory_key
