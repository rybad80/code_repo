select
    {{
        dbt_utils.surrogate_key([
            'cost_center_code',
            'cost_center_site_id',
            'post_date_month'
        ])
    }} as primary_key, 
    time_class,
    post_date_fy,
    post_date_month,
    cost_center_code,
    cost_center_name,
    cost_center_site_id,
    cost_center_site_name,
    revenue_location,
    revenue_location_name,
    metric_name,
    metric_actual_value

from
    {{source('manual_ods', 'care_network_pc_finance_visit_actual')}}
