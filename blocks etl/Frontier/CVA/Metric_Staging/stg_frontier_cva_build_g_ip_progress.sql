select
    'CVA: IP/OP - Progress Notes' as metric_name,
    frontier_cva_encounter_cohort.visit_key as primary_key,
    frontier_cva_encounter_cohort.department_name as drill_down_one,
    frontier_cva_note_data.version_author_name as drill_down_two,
    frontier_cva_note_data.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_cva_ip_progress_notes' as metric_id,
    frontier_cva_encounter_cohort.visit_key as num
from
    {{ ref('frontier_cva_encounter_cohort') }} as frontier_cva_encounter_cohort
    left join {{ ref('frontier_cva_note_data') }} as frontier_cva_note_data
        on frontier_cva_encounter_cohort.visit_key = frontier_cva_note_data.visit_key
where
    frontier_cva_encounter_cohort.cva_ip_progress_ind = 1
    and (lower(frontier_cva_note_data.version_author_name) in (
            'adams, denise m',
            'cohen-cutler, sally',
            'fox, michael d',
            'snyder, kristen',
            'borst, alexandra'
            )
        --or (lower(frontier_cva_note_data.version_author_name) in ('borst, alexandra')
            --and frontier_cva_encounter_cohort.department_id = '101012176' --bgr cvap multi d pgm
            --)
        )
