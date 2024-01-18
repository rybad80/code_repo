select
    stg_note_visit_info.note_visit_key,
    stg_encounter.pat_key,
    stg_encounter.visit_key,
    stg_note_info.note_key
from
    {{ref('stg_note_visit_info')}} as stg_note_visit_info
    inner join {{source('cdw', 'dim_hospital_patient_service')}} as dim_hospital_patient_service
        on dim_hospital_patient_service.hosp_pat_svc_id = stg_note_visit_info.author_service_id
    inner join {{ref('stg_note_info')}} as stg_note_info
        on stg_note_info.note_id = stg_note_visit_info.note_id
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = stg_note_info.pat_enc_csn_id
where
   dim_hospital_patient_service.hosp_pat_svc_id = 27
