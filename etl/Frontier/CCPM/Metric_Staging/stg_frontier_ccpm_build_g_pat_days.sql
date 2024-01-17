select
    'CCPM: Patient Days' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'hsp_acct_id'
        ])
    }} as primary_key,
    patient_sub_cohort as drill_down_one,
    admission_department as drill_down_two,
    post_date as metric_date,
    'sum' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_ccpm_pat_days' as metric_id,
    sum(statistic_measure) as num
from
    {{ ref('stg_frontier_ccpm_finance') }}
where
    lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
group by
    primary_key,
    patient_sub_cohort,
    admission_department,
    post_date
