with
daily_query_totals as (
    select
        table_name, -- noqa
        query_date,
        user_name,
        db_name,
        count(*) as n_query_required,
        sum(query_impact_score) as query_impact_score,
        sum(n_rows) as n_rows,
        sum(n_plans) as n_plans,
        sum(n_snippets) as n_snippets,
        sum(cost_log10) as cost_log10,
        sum(query_runtime_secs) as query_runtime_secs,
        sum(query_text_length) as query_text_length,
        row_number() over(partition by table_name, db_name order by query_date desc) as day_seq_num
    from
        {{ ref('cdw_scheduled_query') }}
    group by
        table_name, -- noqa
        query_date,
        user_name,
        db_name
),

query_stats as (
    select
        table_name, -- noqa
        db_name,
        user_name,
        -- most recent stats
        floor(max(case when day_seq_num = 1 then query_impact_score end)) as query_impact_score,
        floor(max(case when day_seq_num = 1 then n_query_required end)) as n_query_required,
        floor(max(case when day_seq_num = 1 then n_plans end)) as n_plans,
        floor(max(case when day_seq_num = 1 then n_snippets end)) as n_snippets,
        round(max(case when day_seq_num = 1 then cost_log10 end), 2) as cost_log10,
        floor(max(case when day_seq_num = 1 then n_rows end)) as n_rows,
        floor(max(case when day_seq_num = 1 then query_text_length end)) as query_text_length,
        -- avg last five days
        round(avg(case when day_seq_num <= 5 then query_runtime_secs end), 2) as mean_runtime_secs,
        floor(stddev(case when day_seq_num <= 5 then query_runtime_secs end)) as sd_runtime_secs
    from
        daily_query_totals
    group by
        table_name, -- noqa
        db_name,
        user_name
),

usage_stats as (
    select
        table_key,
        lower(table_name) as table_name,  -- noqa
        db_name,
        min(first_usage_date)::date as first_usage_date,
        max(last_usage_date)::date as last_usage_date,
        sum(case when usage_90_day > 0 then employee_ind else 0 end) as n_users_90_day,
        group_concat(
            case
                when usage_90_day > 0 and employee_ind = 1 then user_name
                when user_name like 'svc_ocqi%' then 'rstudio_connect'
                when account_type like 'qlik%' then 'qlik'
                end
            ) as users_90_day,
        sum(case when employee_ind = 1 then usage_90_day else 0 end) as n_user_runs_90_day,
        round(
            -- safer when job fails and not addressed quickly enough
            sum(case when account_type = 'automarts' then usage_90_day else 0 end) / 90.0
            ) as avg_automart_1_day,
        max(
            case when account_type = 'jenkins' and usage_90_day > 0 then 1 else 0 end
        )::byteint as jenkins_90_day_ind,
        max(
            case when account_type = 'qlikview' and usage_90_day > 0 then 1 else 0 end
        )::byteint as qlik_90_day_ind,
        sum(
            case when user_name  like 'svc_ocqi%' and usage_90_day > 0 then usage_90_day else 0 end
        ) as n_rsc_90_day,
        (n_user_runs_90_day
            + avg_automart_1_day
            + jenkins_90_day_ind
            + qlik_90_day_ind
            + n_rsc_90_day
        ) as n_accessed_90_day
    from
        {{ ref('usage_dw_table_users') }}
    where
        user_name != 'admin'
    group by
        table_key,
        table_name,
        db_name,
        schema_name
)

select
    query_stats.table_name, -- noqa
    query_stats.db_name,
    lower(coalesce(
        blocks_objects.owner,
        ods_objects.owner,
        cdw_objects.owner,
        query_stats.db_name
    )) as db_table_owner,
    query_stats.user_name,
    case
        when lower(db_table_owner) like 'blocks%' then 'block'
        when lower(db_table_owner) like 'stack%' then 'stack'
        when usage_stats.table_name like 'fact_%' then 'fact'
        when
            usage_stats.table_name like 's\_%'
            or usage_stats.table_name like 'stg\_%' or usage_stats.table_name like 'stage\_%'
            then 'stage'
        else 'other'
        end as table_type,
    query_stats.n_query_required,
    query_stats.query_impact_score,
    query_stats.n_plans,
    query_stats.n_snippets,
    query_stats.cost_log10,
    query_stats.n_rows,
    query_stats.mean_runtime_secs,
    query_stats.sd_runtime_secs,
    query_stats.query_text_length,
    usage_stats.first_usage_date,
    usage_stats.last_usage_date,
    usage_stats.n_users_90_day,
    usage_stats.users_90_day,
    usage_stats.n_user_runs_90_day,
    usage_stats.avg_automart_1_day,
    usage_stats.jenkins_90_day_ind,
    usage_stats.qlik_90_day_ind,
    usage_stats.n_rsc_90_day,
    usage_stats.n_accessed_90_day,
    usage_stats.table_key
from
    query_stats
    left join usage_stats
        on usage_stats.table_name = query_stats.table_name
        and usage_stats.db_name = query_stats.db_name
    left join {{ source('manual', '_v_objects') }} as blocks_objects
        on lower(blocks_objects.objname) = usage_stats.table_name
    left join {{ source('ods', '_v_objects') }} as ods_objects
        on lower(ods_objects.objname) = usage_stats.table_name
    left join {{ source('cdw', '_v_objects') }} as cdw_objects
        on lower(cdw_objects.objname) = usage_stats.table_name
