select
    episode.episode_id,
    episode_link.line as episode_line,
    episode.pat_link_id as pat_id,
    episode_link.pat_enc_csn_id as csn,
    stg_encounter.encounter_date as encounter_date,
    stg_encounter.visit_key as visit_key,
    episode.name,
    episode.start_date,
    episode.end_date,
    episode_def.episode_def_name,
    episode.comments,
    zc_epi_status.name as status,
    poc_info.poc_created_in_dttm,
    poc_info.poc_completed_dttm,
    poc_info.poc_start_dt,
    poc_info.poc_end_dt,
    poc_info.display_nm as poc_name,
    poc_version_info.spoc_eff_from_date as poc_eff_from_date,
    poc_version_info.spoc_eff_to_date as poc_eff_to_date,
    zc_stat.name as poc_status,
    zc_spoc_type.name as poc_type,
    zc_care_plan_info.name as care_plan_info_name
from
    {{source('clarity_ods', 'episode')}} as episode
    inner join  {{source('clarity_ods', 'episode_def')}} as episode_def
        on episode.sum_blk_type_id = episode_def.episode_def_id and episode_def.episode_type_c = 27
    inner join {{ref('stg_patient')}} as stg_patient
        on episode.pat_link_id = stg_patient.pat_id
    left join  {{source('clarity_ods', 'episode_link')}} as episode_link
        on episode.episode_id = episode_link.episode_id
    left join  {{source('clarity_ods', 'poc_info')}} as poc_info
        on episode.episode_id = poc_info.episode_id
    left join  {{source('clarity_ods', 'zc_stat')}}  as zc_stat
        on poc_info.stat_c = zc_stat.stat_c
    left join  {{source('clarity_ods', 'zc_spoc_type')}} as zc_spoc_type
        on poc_info.spoc_type_c = zc_spoc_type.spoc_type_c
    left join  {{source('clarity_ods', 'zc_care_plan_info')}} as zc_care_plan_info
        on poc_info.care_plan_info_c = zc_care_plan_info.care_plan_info_c
    left join  {{source('clarity_ods', 'poc_version_info')}} as poc_version_info
        on poc_info.record_id = poc_version_info.poc_id
    left join  {{source('clarity_ods', 'zc_epi_status')}} as zc_epi_status
        on episode.status_c = zc_epi_status.epi_status_c
    left join {{ref('stg_encounter')}} as stg_encounter
        on episode_link.pat_enc_csn_id = stg_encounter.csn
