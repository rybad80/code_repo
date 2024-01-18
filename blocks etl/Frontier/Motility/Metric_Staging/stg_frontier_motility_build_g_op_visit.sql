select
    'Motility: Outpatient Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_motility_op_visits' as metric_id,
    visit_key as num
from
    {{ ref('frontier_motility_encounter_cohort')}} as frontier_motility_encounter_cohort
    inner join {{ ref('lookup_frontier_program_visit') }} as lookup_frontier_program_visit
        on cast(lookup_frontier_program_visit.id as nvarchar(20)
                ) = frontier_motility_encounter_cohort.visit_type_id
            and lookup_frontier_program_visit.program = 'motility'
where
    motility_inpatient_ind = 0
        and encounter_type_id in (--region
            50,     --'appointment'
            1057,   --'dxa encounter'
            3,      --'hospital encounter'
            101,    --'office visit'
            1058    --'procedure only'
            --end region
            )
