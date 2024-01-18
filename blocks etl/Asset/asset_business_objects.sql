select
    bo_content_key,
    object_name,
    object_type,
    top_folder_name,
    full_folder_path,
    min(first_usage_date)::date as first_usage_date,
    max(last_usage_date)::date as last_usage_date,
    sum(usage_90_day) as n_user_runs_90_day,
    count(distinct case when usage_90_day > 0 then user_name end) as n_users_90_day,
    group_concat(case when usage_90_day > 0 then user_name end) as users_90_day,
    asset_inventory_key
from
    {{ ref('usage_business_objects_users') }}
group by
    bo_content_key,
    object_name,
    object_type,
    top_folder_name,
    full_folder_path,
    asset_inventory_key
