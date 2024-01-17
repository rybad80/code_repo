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
    stg_pcoti_redcap_all.airway_support_bag_valve_artificial_airway_ind,
    stg_pcoti_redcap_all.airway_support_bag_valve_mask_ind,
    stg_pcoti_redcap_all.airway_support_bipap_ind,
    stg_pcoti_redcap_all.airway_support_cpap_ind,
    stg_pcoti_redcap_all.airway_support_intubation_ind,
    stg_pcoti_redcap_all.code_int_d_stick_ind,
    stg_pcoti_redcap_all.code_int_echo_ind,
    stg_pcoti_redcap_all.code_int_labs_ind,
    stg_pcoti_redcap_all.code_int_needle_decompr_ind,
    stg_pcoti_redcap_all.code_int_other_ind,
    stg_pcoti_redcap_all.code_leader_signature_complete_ind,
    stg_pcoti_redcap_all.code_narrator_ind,
    stg_pcoti_redcap_all.code_return_to_picu_time,
    stg_pcoti_redcap_all.codenarrator_barrier_computer_access_ind,
    stg_pcoti_redcap_all.codenarrator_barrier_epic_issue_ind,
    stg_pcoti_redcap_all.codenarrator_barrier_not_impl_in_unit_ind,
    stg_pcoti_redcap_all.codenarrator_barrier_other_ind,
    stg_pcoti_redcap_all.codenarrator_barrier_staff_comfort_ind,
    stg_pcoti_redcap_all.codenarrator_barrier_tech_issue_ind,
    stg_pcoti_redcap_all.cqi_cat_als_protocol_ind,
    stg_pcoti_redcap_all.cqi_cat_ascom_ind,
    stg_pcoti_redcap_all.cqi_cat_cat_recommendations_not_followed_ind,
    stg_pcoti_redcap_all.cqi_cat_ecg_rhythm_analysis_ind,
    stg_pcoti_redcap_all.cqi_cat_equipment_ind,
    stg_pcoti_redcap_all.cqi_cat_family_support_ind,
    stg_pcoti_redcap_all.cqi_cat_leadership_ind,
    stg_pcoti_redcap_all.cqi_cat_md_unaware_ind,
    stg_pcoti_redcap_all.cqi_cat_medications_ind,
    stg_pcoti_redcap_all.cqi_cat_notification_ind,
    stg_pcoti_redcap_all.cqi_cat_rn_unaware_ind,
    stg_pcoti_redcap_all.cqi_cat_roles_ind,
    stg_pcoti_redcap_all.cqi_cat_universal_precautions_ind,
    stg_pcoti_redcap_all.cqi_cat_unplanned_extubation_ind,
    stg_pcoti_redcap_all.cqi_form_ind,
    stg_pcoti_redcap_all.cqi_issues_ind
from
    cat_code_episode_keys
    inner join {{ ref('stg_pcoti_redcap_all') }} as stg_pcoti_redcap_all
        on cat_code_episode_keys.redcap_record_id = stg_pcoti_redcap_all.record
