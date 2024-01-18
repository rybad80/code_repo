select
    'Rare Lung: Outpatient Visits' as metric_name,
    frontier_rare_lung_encounter_cohort.visit_key as primary_key,
    frontier_rare_lung_encounter_cohort.visit_type as drill_down_one,
    frontier_rare_lung_encounter_cohort.provider_name as drill_down_two,
    frontier_rare_lung_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_rare_lung_op_visits' as metric_id,
    frontier_rare_lung_encounter_cohort.visit_key as num
from
    {{ ref('frontier_rare_lung_encounter_cohort')}} as frontier_rare_lung_encounter_cohort
    inner join {{ ref('lookup_frontier_program_visit')}} as lookup_frontier_program_visit
        on frontier_rare_lung_encounter_cohort.visit_type_id = cast(
            lookup_frontier_program_visit.id as nvarchar(20))
            and lookup_frontier_program_visit.program = 'rare-lung'
where
    frontier_rare_lung_encounter_cohort.rare_lung_ip_ind = 0
    and frontier_rare_lung_encounter_cohort.visit_type_id != '0'
    and lower(frontier_rare_lung_encounter_cohort.appointment_status) in (
        'completed',
        'arrived'
    )
