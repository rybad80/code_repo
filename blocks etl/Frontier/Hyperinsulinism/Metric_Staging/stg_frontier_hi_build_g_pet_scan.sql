select
    'Program-Specific: PET Scans' as metric_name,
    frontier_hi_encounter_cohort.visit_key as primary_key,
    frontier_hi_encounter_cohort.department_name as drill_down_one,
    frontier_hi_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_hi_pet' as metric_id,
    frontier_hi_encounter_cohort.visit_key as num
from
    {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
    inner join {{ ref('procedure_order_all')}} as procedure_order_all
        on frontier_hi_encounter_cohort.visit_key = procedure_order_all.visit_key
            and procedure_order_all.cpt_code = '74160'
            and lower(procedure_order_all.procedure_group_name) = 'imaging - idxrad'
