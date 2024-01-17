select
    sub_cohort as metric_name,
    primary_key,
    metric_name as drill_down_one,
    metric_level as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_ccpm_demo_mg' as metric_id,
    mrn as num
from
    {{ ref('frontier_all_load_demo')}}
where
    lower(program_name) = 'ccpm'
        and lower(sub_cohort) = 'ccpm: targeted therapies patient group'
        and metric_level_fy is not null
