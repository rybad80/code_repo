with
redcap_details as (
    select
        record::int as redcap_id,
        max(case when field_name = 'gh_repo_name' then value::varchar(100) end) as repo_name,
        case max(case when field_name = 'refresh_frequency' then value::int end)
            when 1 then 'daily'
            when 2 then 'weekly'
            when 3 then 'monthly'
            end as refresh_frequency,
        max(case when field_name = 'username' then lower(value)::varchar(50) end) as owner_user_name
    from
        {{ ref('stg_redcap_all')}} as redcap_data
    where
        project_id = 545
    group by
        record
),

usage_stats as (
     select
        table_name, -- noqa
        min(first_usage_date)::date as first_usage_date,
        max(last_usage_date)::date as last_usage_date,
        sum(case when employee_ind = 1 then usage_90_day else 0 end) as n_user_runs_90_day,
        sum(case when usage_90_day > 0 then employee_ind else 0 end) as n_users_90_day,
        group_concat(case when usage_90_day > 0 and employee_ind = 1 then user_name end) as users_90_day,
        group_concat(
            case when usage_90_day > 0 and employee_ind = 0 then user_name end
        ) as service_accounts_90_day,
        sum(usage_90_day) as n_accessed_90_day
    from
        {{ ref('usage_dw_table_users') }}
    where
        db_name like 'ocqi_%'
    group by
        table_name -- noqa
)

select
    {{
        dbt_utils.surrogate_key([
            'readyornot_automart_tables_today.table_name'
        ])
    }} as automart_table_key,
    redcap_details.redcap_id,
    redcap_details.repo_name,
    redcap_details.refresh_frequency,
    redcap_details.owner_user_name,
    worker.manager_name,
    lower(readyornot_automart_tables_today.table_name) as table_name, -- noqa
    usage_stats.last_usage_date,
    usage_stats.n_user_runs_90_day,
    usage_stats.n_users_90_day,
    usage_stats.users_90_day,
    usage_stats.service_accounts_90_day,
    usage_stats.n_accessed_90_day,
    case when usage_stats.n_accessed_90_day = 0 then 1 else 0 end as no_runs_ind,
    case when usage_stats.n_accessed_90_day < 30 then 1 else 0 end as low_runs_ind,
    {{
      dbt_utils.surrogate_key([
        "'automart'",
        'lower(readyornot_automart_tables_today.table_name)'
      ])
    }} as asset_inventory_key
from
    {% if target.name == 'prod' %}
        {{ source('readyornot_ods', 'readyornot_automart_tables_today') }} as readyornot_automart_tables_today
    {% else %}
        {{ source('readyornot_ods', 'readyornot_uat_automart_tables_today') }} as readyornot_automart_tables_today
    {% endif %}
    left join redcap_details
        on redcap_details.repo_name = readyornot_automart_tables_today.repo_name
    left join {{ ref('worker') }} as worker
        on worker.ad_login = redcap_details.owner_user_name
    left join usage_stats
        on usage_stats.table_name = lower(readyornot_automart_tables_today.table_name)
