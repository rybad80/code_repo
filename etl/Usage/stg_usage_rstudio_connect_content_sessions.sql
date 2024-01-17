select
    {{
        dbt_utils.surrogate_key([
            'rstudio_connect_apps.guid',
            'rstudio_connect_users.username',
            'coalesce(rstudio_connect_shiny_app_usage.id, rstudio_connect_rmd_and_static_hits.id)'
        ])
    }} as rsc_content_usage_key,
    lower(coalesce(rstudio_connect_apps.title, rstudio_connect_apps.name)) as content_name,
    rstudio_connect_users.username as user_name,
    coalesce(
        rstudio_connect_shiny_app_usage.started,
        rstudio_connect_rmd_and_static_hits.hit_time
    )::date as usage_date,
    coalesce(
        rstudio_connect_shiny_app_usage.started,
        rstudio_connect_rmd_and_static_hits.hit_time
    )::timestamp as session_start,
    rstudio_connect_shiny_app_usage.ended::timestamp as session_end,
    extract(epoch from session_end - session_start) as session_duration_secs,
    case
        when rstudio_connect_shiny_app_usage.id is not null then 'rstudio_connect_shiny_app_usage'
        else 'rstudio_connect_rmd_and_static_hits'
    end as usage_source,
    coalesce(rstudio_connect_shiny_app_usage.id, rstudio_connect_rmd_and_static_hits.id) as usage_id,
    rstudio_connect_apps.guid as content_guid,
    rstudio_connect_apps.id as content_id,
    {{
        dbt_utils.surrogate_key([
            'rstudio_connect_apps.guid',
            'rstudio_connect_users.username'
        ])
    }} as rsc_content_user_key,
    {{ dbt_utils.surrogate_key([ "'posit connect'", 'rstudio_connect_apps.guid']) }} as asset_inventory_key
from
    {{ source('rstudio_connect_ods', 'rstudio_connect_apps') }} as rstudio_connect_apps
    left join
        {{ source('rstudio_connect_ods', 'rstudio_connect_shiny_app_usage') }} as rstudio_connect_shiny_app_usage
        on rstudio_connect_shiny_app_usage.app_guid = rstudio_connect_apps.guid
    left join
        {{ source('rstudio_connect_ods', 'rstudio_connect_rmd_and_static_hits') }}
        as rstudio_connect_rmd_and_static_hits
        on rstudio_connect_rmd_and_static_hits.app_guid = rstudio_connect_apps.guid
    left join {{ source('rstudio_connect_ods', 'rstudio_connect_users') }} as rstudio_connect_users
        on rstudio_connect_users.guid = coalesce(
            rstudio_connect_shiny_app_usage.user_guid, rstudio_connect_rmd_and_static_hits.user_guid
        )
where
    rstudio_connect_users.username is not null -- APIs pins, & unpublished content don't have usage data
