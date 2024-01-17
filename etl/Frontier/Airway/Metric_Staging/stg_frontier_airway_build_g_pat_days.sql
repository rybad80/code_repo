select
    'Airway: Patient Days' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'hsp_acct_id'
        ])
    }} as primary_key,          
    department_name as drill_down_one,
    post_date as metric_date,
    'sum' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_airway_pat_days' as metric_id,
    sum(statistic_measure) as num
from
    {{ ref('stg_frontier_airway_finance')}}
where
    lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
group by
    primary_key,
    department_name,
    post_date
