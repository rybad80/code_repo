select
    pcoti_episode_events.episode_event_key,
    pcoti_episode_events.episode_key,
    pcoti_episode_events.pat_key,
    pcoti_episode_events.visit_key,
    pcoti_cat_code_details_1.code_category,
    stg_patient.mrn,
    encounter_inpatient.csn,
    coalesce(
        stg_patient.patient_name,
        initcap(pcoti_cat_code_details_1.last_name || ', ' || pcoti_cat_code_details_1.first_name)
    ) as patient_name,
    coalesce(stg_patient.dob, pcoti_cat_code_details_1.dob) as patient_dob,
    pcoti_episode_events.event_type_name,
    pcoti_episode_events.event_type_abbrev,
    pcoti_episode_events.event_start_date,
    case
        when pcoti_cat_code_details_1.cha_code_ind = 1 then 'Yes'
        else 'No'
    end as cha_code_ind,
    pcoti_episode_events.ip_service_name,
    pcoti_episode_events.dept_key,
    pcoti_episode_events.department_name,
    pcoti_episode_events.department_group_name,
    pcoti_episode_events.bed_care_group,
    pcoti_episode_events.campus_name
from
    {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    inner join {{ ref('pcoti_cat_code_details_1') }} as pcoti_cat_code_details_1
        on pcoti_episode_events.episode_event_key = pcoti_cat_code_details_1.episode_event_key
    left join {{ ref('stg_patient') }} as stg_patient
        on pcoti_episode_events.pat_key = stg_patient.pat_key
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on pcoti_episode_events.visit_key = encounter_inpatient.visit_key
where
    pcoti_cat_code_details_1.cha_code_ind = 1
