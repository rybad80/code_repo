{{ config(meta = {
    'critical': true
}) }}

select
    'Turnover Rate - Rolling 12 Month Average' as turnover_metric_name,
    'Voluntary Turnover Rate - Rolling 12 Month Average' as voluntary_turnover_metric_name,
    month_year as metric_date,
    regexp_replace(cost_center, '^\d+\s+', '') as drill_down_one,
    sum(rolling_terminations) as turnover_numerator,
    sum(rolling_voluntary_terminations) as voluntary_turnover_numerator,
    'sum' as num_calculation,
    sum(rolling_average_headcount) as denom,
    'sum' as denom_calculation,
    lower(sldb_categories) as sldb_categories_lower,
    {{
    dbt_utils.surrogate_key([
        'metric_date',
        'drill_down_one'
        ])
    }} as primary_key
from
    {{ source('ods', 'cr_service_line_db_rolling_turnover_all_areas') }}
group by
    month_year,
    cost_center,
    sldb_categories
