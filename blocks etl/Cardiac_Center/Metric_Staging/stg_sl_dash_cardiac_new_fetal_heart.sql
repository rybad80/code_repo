select
    study_id,
    baby_mrn,
    mother_mrn,
    initial_fhp_date,
    study_id as primary_key,
    'cardiac_fhp_pat_chd' as metric_id
from
    {{ ref('fetal_heart_program')}}
