select
    'Motility: Discharges' as metric_name,
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
    'fp_motility_discharges' as metric_id,
    sum(statistic_measure) as num
from
    {{ ref('stg_frontier_motility_finance')}}
where
    lower(statistic_name) in ('discharges')
group by
    primary_key,
    department_name,
    post_date
