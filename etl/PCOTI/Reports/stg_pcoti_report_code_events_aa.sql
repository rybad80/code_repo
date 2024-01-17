select
    stg_pcoti_report_code_events.episode_key,
    stg_pcoti_report_code_events.episode_event_key,
    stg_pcoti_report_code_events.pat_key,
    stg_pcoti_report_code_events.visit_key,
    stg_pcoti_report_code_events.code_category,
    stg_pcoti_report_code_events.event_type_name,
    stg_pcoti_report_code_events.event_start_date,
    stg_pcoti_report_code_events.mrn,
    stg_pcoti_report_code_events.csn,
    stg_pcoti_report_code_events.patient_name,
    stg_pcoti_report_code_events.patient_dob,
    stg_pcoti_report_code_events.non_patient_category,
    stg_pcoti_report_code_events.department_name,
    stg_pcoti_report_code_events.department_group_name,
    stg_pcoti_report_code_events.bed_care_group,
    stg_pcoti_report_code_events.campus_name
from
    {{ ref('stg_pcoti_report_code_events') }} as stg_pcoti_report_code_events
where
    stg_pcoti_report_code_events.event_type_abbrev = 'REDCAP_CODE_AA'
