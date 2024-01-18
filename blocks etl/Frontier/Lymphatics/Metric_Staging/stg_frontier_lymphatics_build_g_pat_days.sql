select
    'Lymphatics: Patient Days' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'hsp_acct_id'
        ])
    }} as primary_key,
    admission_department as drill_down,
    post_date as metric_date,
    'sum' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_lymph_pat_days' as metric_id,
    sum(statistic_measure) as num
from
    {{ ref('stg_frontier_lymphatics_finance') }}
where
    lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
group by
    primary_key,
    admission_department,
    post_date
