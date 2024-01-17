select
    sub_cohort as metric_name,
    primary_key,
    metric_name as drill_down_one,
    metric_level as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_drof_demo_nnss' as metric_id,
    mrn as num
from
    {{ ref('frontier_all_load_demo')}}
where
    lower(program_name) = 'drof'
        and lower(sub_cohort) = 'neonatal non-surgical service'
        and metric_level_fy is not null
