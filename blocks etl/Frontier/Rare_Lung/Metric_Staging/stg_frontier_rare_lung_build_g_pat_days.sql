select
    'Rare Lung: Patient Days' as metric_name,
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
    'fp_rare_lung_pat_days' as metric_id,
    sum(statistic_measure) as num
from
    {{ ref('stg_frontier_rare_lung_finance')}}
where
    lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
    and ip_by_note_only_ind = '0'
group by
    primary_key,
    department_name,
    post_date
