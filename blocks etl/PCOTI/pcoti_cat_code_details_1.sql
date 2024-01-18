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
    stg_pcoti_redcap_all.mrn,
    stg_pcoti_redcap_all.csn,
    stg_pcoti_redcap_all.dob,
    stg_pcoti_redcap_all.last_name,
    stg_pcoti_redcap_all.first_name,
    stg_pcoti_redcap_all.admit_source,
    stg_pcoti_redcap_all.event_dt_tm as event_date,
    stg_pcoti_redcap_all.event_location_details,
    stg_pcoti_redcap_all.event_location,
    stg_pcoti_redcap_all.event_type,
    stg_pcoti_redcap_all.attending_saw_cat_ind,
    stg_pcoti_redcap_all.caller_role,
    stg_pcoti_redcap_all.cat_called_in_prior_24hrs_ind,
    stg_pcoti_redcap_all.cat_nippv_escalated_ind,
    stg_pcoti_redcap_all.cat_none_reason,
    stg_pcoti_redcap_all.cat_follow_up,
    stg_pcoti_redcap_all.cat_grp_debrief_ind,
    stg_pcoti_redcap_all.cat_left_time,
    stg_pcoti_redcap_all.cat_provider_type,
    stg_pcoti_redcap_all.cat_return_time,
    stg_pcoti_redcap_all.cat_rn_follow_up_ind,
    stg_pcoti_redcap_all.cc_done_ind,
    stg_pcoti_redcap_all.cc_duration_category,
    stg_pcoti_redcap_all.cc_duration_mins,
    stg_pcoti_redcap_all.cha_code_ind,
    stg_pcoti_redcap_all.code_category,
    stg_pcoti_redcap_all.code_cpa_arc
from
    cat_code_episode_keys
    inner join {{ ref('stg_pcoti_redcap_all') }} as stg_pcoti_redcap_all
        on cat_code_episode_keys.redcap_record_id = stg_pcoti_redcap_all.record
