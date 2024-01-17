select
    {{
        dbt_utils.surrogate_key([
            'bo_content_key',
            'user_name'
        ])
    }} as bo_content_user_key,
    lower(object_name) as object_name,
    object_type,
    top_folder_name,
    full_folder_path,
    user_name,
    count(distinct case when usage_date >= current_date - 1 then session_id end) as usage_1_day,
    count(distinct case when usage_date >= current_date - 7 then session_id end) as usage_7_day,
    count(distinct case when usage_date >= current_date - 90 then session_id end) as usage_90_day,
    count(distinct case when usage_date >= current_date - 180 then session_id end) as usage_180_day,
    count(distinct case when usage_date >= current_date - 365 then session_id end) as usage_365_day,
    min(usage_date) as first_usage_date,
    max(usage_date) as last_usage_date,
    bo_content_key,
    asset_inventory_key
from
    {{ ref('stg_usage_business_objects_sessions') }}
group by
    bo_content_key,
    object_name,
    object_type,
    top_folder_name,
    full_folder_path,
    user_name,
    asset_inventory_key
