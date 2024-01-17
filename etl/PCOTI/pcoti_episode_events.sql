select
    stg_pcoti_event_all.episode_event_key,
    stg_pcoti_event_all.episode_key,
    stg_pcoti_event_all.pat_key,
    stg_pcoti_event_all.visit_key,
    stg_pcoti_event_all.redcap_record_id,
    stg_pcoti_event_all.event_type_name,
    stg_pcoti_event_all.event_type_abbrev,
    stg_pcoti_event_all.ip_service_name,
    stg_pcoti_event_all.dept_key,
    stg_pcoti_event_all.department_name,
    stg_pcoti_event_all.department_group_name,
    stg_pcoti_event_all.bed_care_group,
    stg_pcoti_event_all.campus_name,
    stg_pcoti_event_all.event_start_date,
    stg_pcoti_event_all.event_end_date
from
    {{ ref('stg_pcoti_event_all') }} as stg_pcoti_event_all
    inner join {{ ref('pcoti_episodes') }} as pcoti_episodes
        on stg_pcoti_event_all.episode_key = pcoti_episodes.episode_key
