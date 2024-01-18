with 
days_in_month as (
    select
        full_dt,
        day_of_mm,
        to_char(full_dt, 'mm/01/yyyy') as visual_month
    from
        {{ source('cdw', 'master_date') }}
    where
        last_day_month_ind = 1 and full_dt <= current_date
        or (full_dt = current_date - 1)
)
select
    'CTIS: Average Daily Census (CTIS surgery only)' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'post_date',
            'hsp_acct_id'
        ])
    }} as primary_key,         
    stg_frontier_ctis_finance.department_name as drill_down_one,
    stg_frontier_ctis_finance.post_date as metric_date,
    'sum' as num_calculation,
    'rate' as metric_type,
    'up' as direction,
    'frontier_ctis_avg_dc' as metric_id,
    sum(stg_frontier_ctis_finance.statistic_measure) as num,
    (cast(days_in_month.day_of_mm as decimal(6, 4))
    / count(distinct primary_key) over (partition by date_trunc('month', stg_frontier_ctis_finance.post_date)))
    as average_daily_census_denom
from
    {{ ref('stg_frontier_ctis_finance')}} as stg_frontier_ctis_finance
    inner join days_in_month
        on stg_frontier_ctis_finance.post_date_month_year = days_in_month.visual_month
where
    lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
group by
    primary_key,
    department_name,
    post_date,
    day_of_mm
