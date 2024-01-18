select distinct
    {{
        dbt_utils.surrogate_key([
            'cost_center_code',
            'cost_center_site_id',
            'post_date_month'
        ])
    }} as primary_key, 
    post_date_month,
    cost_center_code,
    cost_center_name,
    cost_center_site_id,
    cost_center_site_name,
    metric_budget_value,
    month_day_cnt

from
    {{ref('stg_finance_month_cost_center_budget')}} as stg_finance_month_cost_center_budget
    --care network cost center codes
    inner join {{ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
        on
            stg_finance_month_cost_center_budget.cost_center_code
            = lookup_cost_center_service_line.cost_center_gl_id
        and lower(lookup_cost_center_service_line.service_line) = 'care network'
    
where
    statistic_code = '22' --metric_name: Physician Visits
