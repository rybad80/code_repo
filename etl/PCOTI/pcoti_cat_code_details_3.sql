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
    stg_pcoti_redcap_all.dcp_ind,
    stg_pcoti_redcap_all.dnr_status_at_event,
    stg_pcoti_redcap_all.documentation_issues_ind,
    stg_pcoti_redcap_all.dx_at_event,
    stg_pcoti_redcap_all.edu_remediation_complete_ind,
    stg_pcoti_redcap_all.escalated_to_code,
    stg_pcoti_redcap_all.follow_up_leave_time,
    stg_pcoti_redcap_all.follow_up_pending_ind,
    stg_pcoti_redcap_all.follow_up_return_time,
    stg_pcoti_redcap_all.icu_12hrs_cpa_ind,
    stg_pcoti_redcap_all.icu_cpa_dt_tm,
    stg_pcoti_redcap_all.immediate_disposition,
    stg_pcoti_redcap_all.keypoint_summary_complete_ind,
    stg_pcoti_redcap_all.meds_anti_convulsants_ind,
    stg_pcoti_redcap_all.meds_atropine_iv_ind,
    stg_pcoti_redcap_all.meds_blood_products_ind,
    stg_pcoti_redcap_all.meds_blood,
    stg_pcoti_redcap_all.meds_epinephrine_im_ind,
    stg_pcoti_redcap_all.meds_epinephrine_iv_ind,
    stg_pcoti_redcap_all.meds_fluid_bolus_ind,
    stg_pcoti_redcap_all.meds_nmb_vecuronium_ind,
    stg_pcoti_redcap_all.meds_other_ind,
    stg_pcoti_redcap_all.meds_sedative_narcotic_ind,
    stg_pcoti_redcap_all.meds_sodium_bicarbonate_iv_ind
from
    cat_code_episode_keys
    inner join {{ ref('stg_pcoti_redcap_all') }} as stg_pcoti_redcap_all
        on cat_code_episode_keys.redcap_record_id = stg_pcoti_redcap_all.record
