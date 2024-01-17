select
    'Motility: Inpatients (Unique)' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_motility_ip_unique' as metric_id,
    pat_key as num
from
    {{ ref('frontier_motility_encounter_cohort')}}
where
    motility_inpatient_ind = 1
    and (lower(admission_service) in ('gi/nutrition', 'gi/motility', 'general surgery')
        or (motility_cpt_groups like '% blue%' and lower(admission_service) in ('general pediatrics'))
        or (motility_cpt_groups is null and lower(admission_service) in ('general pediatrics'))
        or lower(discharge_service) in ('gi/nutrition', 'gi/motility', 'general surgery')
        )
