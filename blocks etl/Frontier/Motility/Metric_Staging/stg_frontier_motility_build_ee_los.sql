select
    'Motility: Average Inpatient Length of Stay' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    encounter_date as metric_date,
    inpatient_los_days as num,
    visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'rate' as metric_type,
    'down' as direction,
    'fp_motility_los' as metric_id
from
    {{ ref('frontier_motility_encounter_cohort')}}
where
    motility_inpatient_ind = 1
    and (lower(admission_service) in ('gi/nutrition', 'gi/motility', 'general surgery')
        or (motility_cpt_groups like '% blue%' and lower(admission_service) in ('general pediatrics'))
        or (motility_cpt_groups is null and lower(admission_service) in ('general pediatrics'))
        or lower(discharge_service) in ('gi/nutrition', 'gi/motility', 'general surgery')
        )
