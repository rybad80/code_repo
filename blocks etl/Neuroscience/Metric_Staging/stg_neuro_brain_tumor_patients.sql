select
    'Brain Tumor Patients' as metric_name,
    encounter_date,
    pat_key as num,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction,
    pat_key as primary_key
from
    {{ ref('neuro_encounter')}}
where
    brain_tumor_patient_ind = 1
group by
    encounter_date,
    primary_key
