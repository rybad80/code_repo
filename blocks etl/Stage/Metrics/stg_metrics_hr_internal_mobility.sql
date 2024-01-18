{{ config(meta = {
    'critical': true
}) }}

select
    'operational' as domain,
    'hr' as subdomain,
    'Internal Mobility Rate - Rolling 12 Month Average' as metric_name,
    month_year as metric_date,
    regexp_replace(sldb_cost_centers, '^\d+\s+', '') as drill_down_one,
    number_moves as num,
    average_headcount as denom,
    'sum' as num_calculation,
    'sum' as denom_calculation,
    'percentage' as metric_type,
    'month_lag' as metric_lag,
    'down' as desired_direction,
    lower(sldb_categories) as sldb_categories_lower,
    {{
    dbt_utils.surrogate_key([
        'metric_date',
        'drill_down_one'
        ])
    }} as primary_key
from
    {{ source('ods','cr_service_line_dashboard_internal_mobility') }}
