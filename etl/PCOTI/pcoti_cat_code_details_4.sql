with cat_code_episode_keys as (
    select
        pcoti_episode_events.episode_key,
        pcoti_episode_events.episode_event_key,
        pcoti_episode_events.pat_key,
        pcoti_episode_events.visit_key,
        pcoti_episode_events.redcap_record_id,
        pcoti_episode_events.event_start_date
    from
        {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    where
        regexp_like(
            pcoti_episode_events.event_type_abbrev,
            '^REDCAP'
        )
)

select
    cat_code_episode_keys.episode_event_key,
    stg_pcoti_redcap_all.record as redcap_record_id,
    cat_code_episode_keys.episode_key,
    stg_pcoti_redcap_all.neuro_interventions_alt_mental_status_ind,
    stg_pcoti_redcap_all.neuro_interventions_concern_icp_shunt_malfunc_ind,
    stg_pcoti_redcap_all.neuro_interventions_increase_seizure_freq_ind,
    stg_pcoti_redcap_all.neuro_interventions_other_ind,
    stg_pcoti_redcap_all.non_icu_inpatient_ind,
    stg_pcoti_redcap_all.non_patient_category,
    stg_pcoti_redcap_all.non_pt_escalation_ind,
    stg_pcoti_redcap_all.np_return_to_ed_time,
    stg_pcoti_redcap_all.reason_cardiovascular_change_ind,
    stg_pcoti_redcap_all.reason_family_concern_ind,
    stg_pcoti_redcap_all.reason_mpews_ind,
    stg_pcoti_redcap_all.reason_neurologic_change_ind,
    stg_pcoti_redcap_all.reason_other_ind,
    stg_pcoti_redcap_all.reason_respiratory_change_ind,
    stg_pcoti_redcap_all.reason_staff_concern_gut_feeling_ind,
    stg_pcoti_redcap_all.reason_vascular_access_ind,
    stg_pcoti_redcap_all.signature_complete_ind,
    stg_pcoti_redcap_all.survival_status,
    stg_pcoti_redcap_all.telecat_length,
    stg_pcoti_redcap_all.telecat_use_ind,
    stg_pcoti_redcap_all.video_review_performed_ind
from
    cat_code_episode_keys
    inner join {{ ref('stg_pcoti_redcap_all') }} as stg_pcoti_redcap_all
        on cat_code_episode_keys.redcap_record_id = stg_pcoti_redcap_all.record
