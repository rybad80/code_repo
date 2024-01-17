with employees as (
    select distinct
        ad_login as username
    from
        {{source('cdw', 'employee')}}
)
select
    {{
        dbt_utils.surrogate_key([
            'lower(hist_table_access.dbname)',
            'date_trunc(\'month\', hist_query_prolog.submittime)'
        ])
    }} as database_month_key,
    date_trunc('month', hist_query_prolog.submittime) as usage_month,
    lower(hist_table_access.dbname) as db_name,
    count(distinct hist_query_prolog.userid) as monthly_active_total,
    count(distinct employees.username) as monthly_active_employee,
    monthly_active_total - monthly_active_employee as monthly_active_service_account,
    {{ dbt_utils.surrogate_key(['lower(hist_table_access.dbname)']) }} as database_key
from
    {{source('histdb', 'hist_table_access')}} as hist_table_access
    inner join {{source('histdb', 'hist_query_prolog')}} as hist_query_prolog
        on hist_query_prolog.npsid = hist_table_access.npsid
        and hist_query_prolog.npsinstanceid = hist_table_access.npsinstanceid
        and hist_query_prolog.opid = hist_table_access.opid
    left join employees
        on lower(employees.username) = lower(hist_query_prolog.username)
where
    /* Ignore Informatica temp tables */
    lower(hist_table_access.tablename) not like 'pcn%'
    and lower(hist_table_access.schemaname) not in ('definition_schema')
group by
    date_trunc('month', hist_query_prolog.submittime),
    lower(hist_table_access.dbname)
