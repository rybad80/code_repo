select
    stg_note_visit_info.note_visit_key,
    stg_encounter.pat_key,
    stg_encounter.visit_key,
    stg_note_info.note_key
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = stg_encounter.dept_key
    inner join {{ref('stg_note_info')}} as stg_note_info
        on stg_note_info.pat_enc_csn_id = stg_encounter.csn
    inner join {{ref('stg_note_visit_info')}} as stg_note_visit_info
        on stg_note_visit_info.note_id  = stg_note_info.note_id
where
    lower(department.specialty) = 'orthopedics'
