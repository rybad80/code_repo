select
    stg_encounter.visit_key,
    note_visit_info.note_key as action_key,
    row_number() over(partition by note_info.note_key, note_visit_info.note_visit_key
        order by entry_local_dt desc) as action_seq_num,
    employee.prov_key,
    employee.emp_key,
    employee.full_nm as employee_name,
    entry_local_dt as event_date,
    'note edit' as event_description,
    dim_hospital_patient_service.hosp_pat_svc_nm as event_location,
    'note_key' as action_key_field
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'note_info')}} as note_info
        on note_info.visit_key = stg_encounter.visit_key
    inner join {{source('cdw', 'note_visit_info')}} as note_visit_info
        on note_visit_info.note_key = note_info.note_key
    inner join {{source('cdw', 'dim_note_status')}} as dim_note_status
        on dim_note_status.dim_note_stat_key = note_visit_info.dim_note_stat_key
    inner join {{source('cdw', 'dim_hospital_patient_service')}} as dim_hospital_patient_service
        on dim_hospital_patient_service.dim_hosp_pat_svc_key = note_visit_info.dim_hosp_pat_svc_key
    inner join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = note_visit_info.auth_emp_key
where
    dim_note_status.note_stat_id not in(
        '-1', --invalid
        '4' --deleted
    )
