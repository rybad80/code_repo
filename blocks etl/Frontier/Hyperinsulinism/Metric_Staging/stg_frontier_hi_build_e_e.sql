with
average_daily_census as (--region
    select
        'HI: Average Daily Census' as metric_name,
        {{
            dbt_utils.surrogate_key([
                'post_date',
                'hsp_acct_id'
            ])
        }} as primary_key,         
        stg_frontier_hi_finance.department_name as drill_down_one,
        stg_frontier_hi_finance.post_date as metric_date,
        null as denom,
        'sum' as num_calculation,
        'null' as denom_calculation,
        'count' as metric_type,
        'up' as direction,
        'frontier_hi_avg_dc' as metric_id,
        sum(stg_frontier_hi_finance.statistic_measure) / stg_frontier_hi_calendar.cum_sum_max as num

    from
        {{ ref('stg_frontier_hi_finance')}} as stg_frontier_hi_finance
        left join {{ ref('stg_frontier_hi_calendar')}} as stg_frontier_hi_calendar
            on stg_frontier_hi_finance.post_date_month_year = stg_frontier_hi_calendar.post_date_month_year
    where
        lower(statistic_name) in ('observation patient day equivalents', 'ip patient days')
    group by
        primary_key,
        department_name,
        post_date,
        cum_sum_max
    having num is not null
    --end region
),
length_of_stay as (--region
    select
        'HI: Average Inpatient Length of Stay' as metric_name,
        frontier_hi_encounter_cohort.visit_key as primary_key,
        null as drill_down_one,
        frontier_hi_encounter_cohort.encounter_date as metric_date,
        frontier_hi_encounter_cohort.csn as denom,
        'sum' as num_calculation,
        'count' as denom_calculation,
        'rate' as metric_type,
        'down' as direction,
        'frontier_hi_los' as metric_id,
        encounter_inpatient.inpatient_los_days as num
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('encounter_inpatient')}} as encounter_inpatient
            on frontier_hi_encounter_cohort.visit_key = encounter_inpatient.visit_key
    --end region
),
final_union as (--region
        select * from average_daily_census
        union all
        select * from length_of_stay
    --end region
)
select * from final_union
--;
