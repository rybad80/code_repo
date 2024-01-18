select
    'Lymphatics: Inpatients (Unique)' as metric_name,
    frontier_lymphatics_encounter_cohort.visit_key as primary_key,
    frontier_lymphatics_encounter_cohort.admission_department as drill_down,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_lymph_ip_unique' as metric_id,
    frontier_lymphatics_encounter_cohort.pat_key as num
from
    {{ ref('frontier_lymphatics_encounter_cohort') }} as frontier_lymphatics_encounter_cohort
    left join {{ ref('cardiac_unit_encounter') }} as cardiac_unit_encounter
        on frontier_lymphatics_encounter_cohort.visit_key = cardiac_unit_encounter.visit_key
where
    inpatient_ind = 1
