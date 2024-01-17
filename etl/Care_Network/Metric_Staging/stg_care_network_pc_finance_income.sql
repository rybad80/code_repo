select
    {{
        dbt_utils.surrogate_key([
            'company',
            'cost_center_code',
            'cost_center_site_id',
            'post_date_month'
        ])
    }} as primary_key, 
    time_class,
    fy_yyyy,
    post_date_month,
    cost_center_code,
    cost_center_name,
    cost_center_site_id,
    cost_center_site_name,
    operating_income,
    operating_revenue

from
    {{source('manual_ods', 'care_network_pc_finance_operate_income')}}
