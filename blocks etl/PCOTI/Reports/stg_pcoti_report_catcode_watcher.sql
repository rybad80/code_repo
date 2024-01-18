with watchers as (
    select
        pcoti_episode_events.episode_key,
        pcoti_episode_events.episode_event_key,
        pcoti_episode_events.event_start_date
    from
        {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    where
        pcoti_episode_events.event_type_abbrev = 'WATCHER'
),

joined as (
    select
        stg_pcoti_report_catcode_all.episode_key,
        stg_pcoti_report_catcode_all.episode_event_key as catcode_event_key,
        stg_pcoti_report_catcode_all.event_start_date as catcode_date,
        stg_pcoti_report_catcode_all.event_type,
        stg_pcoti_report_catcode_all.pat_key,
        stg_pcoti_report_catcode_all.visit_key,
        stg_pcoti_report_catcode_all.mrn,
        stg_pcoti_report_catcode_all.csn,
        stg_pcoti_report_catcode_all.patient_name,
        stg_pcoti_report_catcode_all.patient_dob,
        stg_pcoti_report_catcode_all.ip_service_name,
        stg_pcoti_report_catcode_all.department_name,
        stg_pcoti_report_catcode_all.department_group_name,
        stg_pcoti_report_catcode_all.campus_name,
        watchers.episode_event_key as watcher_event_key,
        watchers.event_start_date as watcher_date,
        row_number() over (
            partition by
                stg_pcoti_report_catcode_all.episode_key,
                stg_pcoti_report_catcode_all.episode_event_key
            order by
                watchers.event_start_date desc
        ) as watcher_order
    from
        {{ ref('stg_pcoti_report_catcode_all') }} as stg_pcoti_report_catcode_all
        left join watchers
            on stg_pcoti_report_catcode_all.episode_key = watchers.episode_key
            and watchers.event_start_date <= stg_pcoti_report_catcode_all.event_start_date
            and watchers.event_start_date >= stg_pcoti_report_catcode_all.event_start_date - interval '48 hours'
)

select
    *,
    extract(epoch from catcode_date - watcher_date) / 3600::float as watcher_hours_to_cat,
    case when watcher_hours_to_cat <= 2 then 1 else 0 end as watcher_prior_2hrs,
    case when watcher_hours_to_cat <= 48 then 1 else 0 end as watcher_prior_48hrs
from
    joined
where
    watcher_order = 1
    or watcher_order is null
