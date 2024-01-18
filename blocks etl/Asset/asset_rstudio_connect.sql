with
usage_stats as (
    select
        content_guid,
        min(first_usage_date)::date as first_usage_date,
        max(last_usage_date)::date as last_usage_date,
        sum(usage_90_day) as n_accessed_90_day,
        count(distinct case when usage_90_day > 0 then user_name end) as n_users_90_day,
        group_concat(case when usage_90_day > 0 then user_name end) as users_90_day,
        sum(usage_365_day) as n_accessed_365_day,
        count(distinct case when usage_365_day > 0 then user_name end) as n_users_365_day,
        group_concat(case when usage_365_day > 0 then user_name end) as users_365_day
    from
        {{ ref('usage_rstudio_connect_content_users') }}
    group by
        content_guid
),

rsc_vars as (
    select
        app_id,
        group_concat(name) as env_vars
    from
        {{ source('rstudio_connect_ods', 'rstudio_connect_app_environment_variables') }}
    group by
        app_id
),

emails as (
    select
        app_id
    from
        {{ source('rstudio_connect_ods', 'rstudio_connect_schedule') }}
    where
        email = true
    group by
        app_id
),

collaborators as (
    select
        rstudio_connect_permissions.app_id,
        group_concat(rstudio_connect_users.username) as active_dna_collaborator
    from
        {{ source('rstudio_connect_ods', 'rstudio_connect_permissions') }} as rstudio_connect_permissions
        inner join {{ source('rstudio_connect_ods', 'rstudio_connect_users') }} as rstudio_connect_users
            on rstudio_connect_users.principal_id = rstudio_connect_permissions.principal_id
        left join {{ ref('stg_asset_dna_analyst') }} as stg_asset_dna_analyst
            on stg_asset_dna_analyst.ad_login = rstudio_connect_users.username
    where
        rstudio_connect_permissions.app_role = 'owner'
        and stg_asset_dna_analyst.active_ind = 1
        and stg_asset_dna_analyst.employee_ind = 1
        and stg_asset_dna_analyst.expat_ind = 0
    group by
        rstudio_connect_permissions.app_id
)

select
    rstudio_connect_apps.guid as content_guid,
    case
        when emails.app_id is not null then 'email'
        else
        case rstudio_connect_apps.app_mode --noqa: L058
            when 0 then 'unpublished'
            when 1 then 'app'
            when 2 then 'dashboard'
            when 3 then 'report'
            when 4 then 'pin'
            when 5 then 'api'
            else 'other'
            end
       end as content_type,
    coalesce(
        rstudio_connect_apps.title,
        rstudio_connect_apps.name
    ) as content_name,
    rstudio_connect_users.username as owner_user_name,
    rstudio_connect_apps.bundle_id as current_bundle_id,
    substring(rstudio_connect_apps.created_time, 1, 10)::date as created_date,
    substring(rstudio_connect_apps.last_deployed_time, 1, 10)::date as last_deployed,
    usage_stats.first_usage_date,
    usage_stats.last_usage_date,
    coalesce(usage_stats.n_accessed_90_day, 0) as n_accessed_90_day,
    usage_stats.n_users_90_day,
    usage_stats.users_90_day,
    coalesce(usage_stats.n_accessed_365_day, 0) as n_accessed_365_day,
    usage_stats.n_users_365_day,
    usage_stats.users_365_day,
    case when rstudio_connect_apps.has_parameters = true then 1 else 0 end as has_parameters_ind,
    case when rsc_vars.app_id is not null then 1 else 0 end as env_vars_ind,
    rsc_vars.env_vars,
    case when rstudio_connect_git.app_id is not null then 1 else 0 end as git_backed_ind,
    regexp_extract(rstudio_connect_git.repository_url, '(?<=chop\.edu/)[^\/]+') as github_org,
    regexp_extract(rstudio_connect_git.repository_url, '[^\/]+(?=\/)?$') as github_repository,
    'https://rstudio-connect.chop.edu/connect/#/apps/' || rstudio_connect_apps.guid as app_url,
    'https://rstudio-connect.chop.edu/connect' || rstudio_connect_vanities.path_prefix as vanity_url,
    worker.active_ind as employee_active_ind,
    case when collaborators.app_id is not null then 1 else 0 end as active_dna_collaborator_ind,
    collaborators.active_dna_collaborator,
    worker.manager_name,
    worker.worker_id,
    worker.manager_id,
    rstudio_connect_apps.id as content_id,
    {{ dbt_utils.surrogate_key([ "'posit connect'", 'rstudio_connect_apps.guid']) }} as asset_inventory_key
from
    {{ source('rstudio_connect_ods', 'rstudio_connect_apps') }} as rstudio_connect_apps
    inner join {{ source('rstudio_connect_ods', 'rstudio_connect_users') }} as rstudio_connect_users
        on rstudio_connect_users.principal_id = rstudio_connect_apps.principal_id
    left join {{ ref('worker') }} as worker
        on worker.ad_login = rstudio_connect_users.username
    left join {{ source('rstudio_connect_ods', 'rstudio_connect_git') }} as rstudio_connect_git
        on rstudio_connect_git.app_id = rstudio_connect_apps.id
    left join {{ source('rstudio_connect_ods', 'rstudio_connect_vanities')}} as rstudio_connect_vanities
        on rstudio_connect_vanities.app_id = rstudio_connect_apps.id
    left join rsc_vars
        on rsc_vars.app_id = rstudio_connect_apps.id
    left join collaborators
        on collaborators.app_id = rstudio_connect_apps.id
    left join usage_stats
        on usage_stats.content_guid = rstudio_connect_apps.guid
    left join emails
        on emails.app_id = rstudio_connect_apps.id
