select
    sub_cohort as metric_name,
    primary_key,
    metric_name as drill_down_one,
    metric_level as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_n_o_t_bleeding_demo_rbd' as metric_id,
    mrn as num
from
    {{ ref('frontier_all_load_demo')}}
where
    lower(program_name) = 'not bleeding'
        and lower(sub_cohort) = 'rbd patients'
        and metric_level_fy is not null
