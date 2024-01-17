select
  {{
    dbt_utils.surrogate_key([
      'stg_patient_ods.patient_key',
      'start_event_info.event_time'
    ])
  }} as sdu_resus_key,
  stg_patient_ods.patient_name,
  stg_patient_ods.mrn,
  stg_patient_ods.dob,
  stg_encounter.csn,
  stg_encounter.encounter_date,
  start_event_info.event_time as resus_start,
  end_event_info.event_time as resus_end,
  stg_patient_ods.patient_key,
  stg_encounter.encounter_key
from
-- need to get rid of dupes from when there are two code_ends. see if event_sign_off_id is an option
  {{source('clarity_ods', 'ed_iev_pat_info')}} as ed_iev_pat_info
  inner join {{source('clarity_ods', 'ed_iev_event_info')}} as start_event_info
    on start_event_info.event_id = ed_iev_pat_info.event_id
  inner join {{source('clarity_ods', 'ed_iev_event_info')}} as end_event_info
    on end_event_info.event_sign_off_id = start_event_info.event_sign_off_id
  inner join {{ref('stg_patient_ods')}} as stg_patient_ods
    on stg_patient_ods.pat_id = ed_iev_pat_info.pat_id
  inner join {{ref('stg_encounter')}} as stg_encounter
    on stg_encounter.csn = ed_iev_pat_info.pat_enc_csn_id
where
  start_event_info.event_type = '34380'
  and start_event_info.event_dept_id = '22'
  and start_event_info.event_status_c is null
  and end_event_info.event_type = '34381'
  and end_event_info.event_status_c is null