with
enterprise_stream as (
    -- enterprise streams all have a parallel '_Deployment' stream
    select
        regexp_replace(qliksense_qsr_streams.name, '_Deployment$', '') as stream_name
    from
        {{ source('qliksense_ods', 'qliksense_qsr_streams') }} as qliksense_qsr_streams
    where
        qliksense_qsr_streams.name like '%_Deployment'
),

reload_schedule as (
    select
        qliksense_qsr_reload_tasks.app_id
    from
        {{ source('qliksense_ods', 'qliksense_qsr_reload_tasks') }} as qliksense_qsr_reload_tasks
    where
        qliksense_qsr_reload_tasks.is_manually_triggered = false
        and qliksense_qsr_reload_tasks.enabled = true
    group by
        qliksense_qsr_reload_tasks.app_id
),

app_tags as (
    select
        qliksense_qsr_tag_apps.app_id,
        max(
            case when qliksense_qsr_tags.id = '419bd898-5c83-47b4-aa63-eb3db998ba71' then 1 else 0 end
        ) as phi_ind,
        max(
            case when qliksense_qsr_tags.id = '9661686d-eccf-48e1-a557-74cd189e7206' then 1 else 0 end
        ) as chqa_governed_ind,
        group_concat(qliksense_qsr_tags.name) as all_tags
    from
        {{ source('qliksense_ods', 'qliksense_qsr_tag_apps') }} as qliksense_qsr_tag_apps
        inner join {{ source('qliksense_ods', 'qliksense_qsr_tags') }} as qliksense_qsr_tags
            on qliksense_qsr_tags.id = qliksense_qsr_tag_apps.tag_id
    group by
        qliksense_qsr_tag_apps.app_id
),

stream_tags as (
    select
        qliksense_qsr_stream_tags.stream_id,
        max(
            case when qliksense_qsr_tags.id = '419bd898-5c83-47b4-aa63-eb3db998ba71' then 1 else 0 end
        ) as phi_ind,
        max(
            case when qliksense_qsr_tags.id = '9661686d-eccf-48e1-a557-74cd189e7206' then 1 else 0 end
        ) as chqa_governed_ind,
        group_concat(qliksense_qsr_tags.name) as all_tags
    from
        {{ source('qliksense_ods', 'qliksense_qsr_stream_tags') }} as qliksense_qsr_stream_tags
        inner join {{ source('qliksense_ods', 'qliksense_qsr_tags') }} as qliksense_qsr_tags
            on qliksense_qsr_tags.id = qliksense_qsr_stream_tags.tag_id
    group by
        qliksense_qsr_stream_tags.stream_id
),

usage_stats as (
    select
        usage_qliksense_users.qliksense_app_id,
        min(usage_qliksense_users.first_usage_date) as first_usage_date,
        max(usage_qliksense_users.last_usage_date) as last_usage_date,
        sum(usage_qliksense_users.usage_90_day) as n_accessed_90_day,
        count(distinct usage_qliksense_users.user_name) as n_users_90_day,
        group_concat(
            case when usage_qliksense_users.usage_90_day > 0 then usage_qliksense_users.user_name end
        ) as user_names_90_day
    from
        {{ ref('usage_qliksense_users') }} as usage_qliksense_users
    group by
        usage_qliksense_users.qliksense_app_id
)

select
    qliksense_qsr_apps.id as qliksense_app_id,
    qliksense_qsr_apps.name as application_title,
    qliksense_qsr_users.user_id as owner_user_name,
    qliksense_qsr_streams.name as stream_name,
    nvl2(enterprise_stream.stream_name, 'Enterprise', 'Local') as stream_type,
    datetime(timezone(qliksense_qsr_apps.created_date, 'UTC', 'America/New_York'))::date as created_date,
    datetime(timezone(qliksense_qsr_apps.last_reload_time, 'UTC', 'America/New_York'))::date as last_reload_date,
    case when qliksense_qsr_apps.published = true then 1 else 0 end as published_ind,
    case
        when qliksense_qsr_apps.published = true
        then datetime(timezone(qliksense_qsr_apps.publish_time, 'UTC', 'America/New_York'))::date
    end as published_date,
    usage_stats.first_usage_date,
    usage_stats.last_usage_date,
    usage_stats.n_accessed_90_day,
    usage_stats.n_users_90_day,
    usage_stats.user_names_90_day,
    coalesce(app_tags.phi_ind, stream_tags.phi_ind, 0) as phi_ind,
    coalesce(app_tags.chqa_governed_ind, stream_tags.chqa_governed_ind, 0) as chqa_governed_ind,
    case
        when
            qliksense_qsr_apps.published != true
            or qliksense_qsr_streams.name in ('Storage Bin', 'Recycle Bin')
            or regexp_like(qliksense_qsr_streams.name, '(?i)consulting|_dev|_deployment|[_ ]staging')
        then 1
        else 0
        end as temp_stream_ind,
    nvl2(reload_schedule.app_id, 1, 0) as reload_ind,
    stream_tags.stream_id,
    '['
      || '{app:[' || app_tags.all_tags || ']}'
      || ', {stream:[' || stream_tags.all_tags || ']}'
      || ']' as all_tags,
    'https://qliksense.chop.edu/sense/app/' || qliksense_qsr_apps.id || '/overview' as url,
    qliksense_qsr_users.id as owner_user_id,
    {{ dbt_utils.surrogate_key([ "'qlik sense'", 'qliksense_qsr_apps.id']) }} as asset_inventory_key
from
    {{ source('qliksense_ods', 'qliksense_qsr_apps') }} as qliksense_qsr_apps
    inner join {{ source('qliksense_ods', 'qliksense_qsr_users') }} as qliksense_qsr_users
        on qliksense_qsr_users.id = qliksense_qsr_apps.owner_id
    left join {{ source('qliksense_ods', 'qliksense_qsr_streams') }} as qliksense_qsr_streams
        on qliksense_qsr_streams.id = qliksense_qsr_apps.stream_id
    left join app_tags
        on app_tags.app_id = qliksense_qsr_apps.id
    left join stream_tags
        on stream_tags.stream_id  = qliksense_qsr_apps.stream_id
    left join usage_stats
        on usage_stats.qliksense_app_id = qliksense_qsr_apps.id
    left join enterprise_stream
        on enterprise_stream.stream_name = qliksense_qsr_streams.name
    left join reload_schedule
        on reload_schedule.app_id = qliksense_qsr_apps.id
