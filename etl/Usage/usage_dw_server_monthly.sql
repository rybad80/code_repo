{{
    config(
        materialized = 'incremental',
        unique_key = 'usage_month'
    )
}}

with employees as (
    select distinct
        ad_login as username
    from
        {{source('cdw', 'employee')}}
)

select
    date_trunc('month', hist_query_prolog.submittime) as usage_month,
    count(distinct hist_query_prolog.userid) as monthly_active_total,
    count(distinct employees.username) as monthly_active_employee,
    monthly_active_total - monthly_active_employee as monthly_active_service_account,
    count(*) as monthly_query
from
    {{source('histdb', 'hist_query_prolog')}} as hist_query_prolog
    left join employees
        on lower(employees.username) = lower(hist_query_prolog.username)
{% if is_incremental() %}
where
    submittime > (select max(usage_month) from {{ this }})
{% endif %}
group by
    date_trunc('month', submittime)
