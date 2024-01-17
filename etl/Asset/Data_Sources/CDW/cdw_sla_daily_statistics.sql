with
    runtimes as (
        select 
            query_date,
            user_name,
            query_start_time,
            query_end_time,
            end_before_7am_ind
        from 
            {{ ref('cdw_scheduled_query') }}
    )
    
    , daily_stats as (
        select 
            query_date,
            user_name,
            sum(case when hour(query_end_time) < 7 then 1 else 0 end) as n_completed_by_7am,
            avg(end_before_7am_ind) as pct_before_7am,
            min(query_start_time) as first_run
        from
            runtimes 
        group by
            query_date,
            user_name
    )

    , daily_percentiles as (
        select
            user_name,
            ceil(avg(n_completed_by_7am)) as avg_n_completed_by_7am,
            percentile_cont(0.25) within group (order by pct_before_7am) as lower_25_pct,
            percentile_cont(0.75) within group (order by pct_before_7am) as upper_75_pct
        from
            daily_stats
        group by
    user_name
    )

select
    {{
        dbt_utils.surrogate_key([
            'daily_stats.query_date',
            'daily_stats.user_name'
        ])
    }} as query_date_user_key,
    daily_stats.query_date,
    daily_stats.user_name,
    lookup_user_account.account_group,
    lookup_user_account.account_subgroup,
    daily_stats.first_run,
    daily_stats.n_completed_by_7am,
    daily_stats.pct_before_7am,
    case
        when daily_stats.pct_before_7am = 1.0 then 'excellent'
        when daily_stats.pct_before_7am > daily_percentiles.upper_75_pct then 'better'
        when daily_stats.pct_before_7am < daily_percentiles.lower_25_pct then 'worse'
        else 'average'
        end as day_type,
    lower_25_pct,
    upper_75_pct
from
    daily_stats
    inner join daily_percentiles using (user_name)
    inner join {{ ref('lookup_user_account') }} as lookup_user_account
        on lower(lookup_user_account.user_name) = daily_percentiles.user_name
