select
    {{
        dbt_utils.surrogate_key([
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.schemaname)',
            'lower(hist_column_access.tablename)',
            'lower(hist_column_access.columnname)',
            'lower(hist_query_prolog.username)'
        ])
    }} as column_user_key,
    lower(hist_query_prolog.username) as user_name,
    lower(hist_column_access.dbname) as db_name,
    lower(hist_column_access.schemaname) as schema_name, -- noqa
    lower(hist_column_access.tablename) as table_name, -- noqa
    lower(hist_column_access.columnname) as column_name, -- noqa
    sum(case when hist_query_prolog.submittime >= current_timestamp - 1 then 1 else 0 end) as usage_1_day,
    sum(case when hist_query_prolog.submittime >= current_timestamp - 7 then 1 else 0 end) as usage_7_day,
    sum(case when hist_query_prolog.submittime >= current_timestamp - 90 then 1 else 0 end) as usage_90_day,
    sum(case when hist_query_prolog.submittime >= current_timestamp - 180 then 1 else 0 end) as usage_180_day,
    sum(case when hist_query_prolog.submittime >= current_timestamp - 365 then 1 else 0 end) as usage_365_day,
    min(hist_query_prolog.submittime) as first_usage_date,
    max(hist_query_prolog.submittime) as last_usage_date,
    max(case when worker.ad_login is not null then 1 else 0 end) as employee_ind,
    coalesce(lower(lookup_user_account.account_subgroup), lower(lookup_user_account.account_group), 'user')
        as account_type,
    {{
        dbt_utils.surrogate_key([
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.schemaname)',
            'lower(hist_column_access.tablename)',
            'lower(hist_column_access.columnname)'
        ])
    }} as column_key
from
    {{source('histdb', 'hist_column_access')}} as hist_column_access
    inner join {{source('histdb', 'hist_query_prolog')}} as hist_query_prolog
        on hist_query_prolog.npsid = hist_column_access.npsid
        and hist_query_prolog.npsinstanceid = hist_column_access.npsinstanceid
        and hist_query_prolog.opid = hist_column_access.opid
    left join  {{ ref('worker') }} as worker
        on worker.ad_login = lower(hist_query_prolog.username)
    left join {{ref('lookup_user_account')}} as lookup_user_account
        on lookup_user_account.user_name = hist_query_prolog.username
where
    /* Ignore Informatica temp tables */
    lower(hist_column_access.tablename) not like 'pcn%'
    and lower(hist_column_access.schemaname) not in ('definition_schema')
    and {{ limit_dates_for_dev(ref_date = 'hist_query_prolog.submittime') }}
group by
    lower(hist_query_prolog.username),
    lower(hist_column_access.dbname),
    lower(hist_column_access.schemaname),
    lower(hist_column_access.tablename),
    lower(hist_column_access.columnname),
    lower(lookup_user_account.account_subgroup), 
    lower(lookup_user_account.account_group)
