select
    stg_pcoti_report_code_events_patient.episode_key,
    stg_pcoti_report_code_events_patient.episode_event_key,
    stg_pcoti_report_code_events_patient.event_type_name as event_type,
    stg_pcoti_report_code_events_patient.event_start_date,
    stg_pcoti_report_code_events_patient.pat_key,
    stg_pcoti_report_code_events_patient.visit_key,
    stg_pcoti_report_code_events_patient.mrn,
    stg_pcoti_report_code_events_patient.csn,
    stg_pcoti_report_code_events_patient.patient_name,
    stg_pcoti_report_code_events_patient.patient_dob,
    stg_pcoti_report_code_events_patient.ip_service_name,
    stg_pcoti_report_code_events_patient.department_name,
    stg_pcoti_report_code_events_patient.department_group_name,
    stg_pcoti_report_code_events_patient.campus_name,
    stg_pcoti_report_code_events_patient.icu_enter_date,
    stg_pcoti_report_code_events_patient.immediate_disposition,
    null as init_note_episode_event_key
from
    {{ ref('stg_pcoti_report_code_events_patient') }} as stg_pcoti_report_code_events_patient
union all
select
    stg_pcoti_report_cat_calls.episode_key,
    stg_pcoti_report_cat_calls.index_episode_event_key as episode_event_key,
    'CAT Call' as event_type,
    stg_pcoti_report_cat_calls.index_event_date as event_start_date,
    stg_pcoti_report_cat_calls.pat_key,
    stg_pcoti_report_cat_calls.visit_key,
    stg_pcoti_report_cat_calls.mrn,
    stg_pcoti_report_cat_calls.csn,
    stg_pcoti_report_cat_calls.patient_name,
    stg_pcoti_report_cat_calls.patient_dob,
    stg_pcoti_report_cat_calls.ip_service_name,
    stg_pcoti_report_cat_calls.department_name,
    stg_pcoti_report_cat_calls.department_group_name,
    stg_pcoti_report_cat_calls.campus_name,
    stg_pcoti_report_cat_calls.icu_enter_date,
    stg_pcoti_report_cat_calls.immediate_disposition,
    stg_pcoti_report_cat_calls.init_note_episode_event_key
from
    {{ ref('stg_pcoti_report_cat_calls') }} as stg_pcoti_report_cat_calls
