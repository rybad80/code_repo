select
    ids_no_patient.visit_key as no_patient_visit_key,
    ids_with_patient.visit_key as with_patient_visit_key,
    ids_no_patient.encounter_id,
    ids_with_patient.patient_id,
    ids_no_patient.source_name
from {{ref('stg_visit_key_clarity_no_patient')}} as ids_no_patient
inner join {{ref('stg_visit_key_clarity_no_patient')}} as ids_with_patient
    on ids_no_patient.encounter_id = ids_with_patient.encounter_id
    and ids_no_patient.visit_key != ids_with_patient.visit_key
where
    ids_no_patient.patient_id = ''
    and ids_with_patient.patient_id != ''