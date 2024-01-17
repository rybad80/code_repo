{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p1_time_type
group budget to variable and fixed job group rollups
to support flex metrics
Variable ones "flex" with the patient volumes.  Both pull from monthly budgets.
Includes: job group granularity for the Flex Component display in Qlik

Special case note: Fixed Vacancy Job Code budget (annualized) will be handled as a "fixed"
contribution to the Productive Flex target roles (a subtraction) up through FY23,
then dropped out of this metric starting in FY24
*/
with
rollup_job_group as (
    select
        stg_nursing_budget_period_monthly.cost_center_id,
        stg_nursing_budget_period_monthly.metric_dt_key,
        sum(row_metric_calculation) as productive_total,
	case
        when metric_abbreviation = 'JgBdgtPrdctvVar'
            then 'variable'
            else 'fixed'
        end as job_time_type,
        nursing_pay_period.fiscal_year,
        stg_nursing_budget_period_monthly.job_group_id
    from
       {{ ref('stg_nursing_budget_period_monthly') }} as stg_nursing_budget_period_monthly
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on stg_nursing_budget_period_monthly.metric_dt_key
            = nursing_pay_period.pp_end_dt_key
    where
        metric_abbreviation in (  /* thus excluding PPL and Other time off */
            'JgBdgtPrdctvVar',
            'JgBdgtPrdctvFixed'
        )
group by
    stg_nursing_budget_period_monthly.cost_center_id,
    stg_nursing_budget_period_monthly.metric_dt_key,
    job_time_type,
    nursing_pay_period.fiscal_year,
    stg_nursing_budget_period_monthly.job_group_id
)

select
    metric_dt_key,
    cost_center_id,
    fiscal_year,
    sum(productive_total) as productive_budget_for_time_type,
    job_time_type,
    1 as cc_granularity_ind,
    null as job_group_id
from
    rollup_job_group
group by
    metric_dt_key,
    cost_center_id,
    fiscal_year,
    job_time_type

union all

select
    metric_dt_key,
    cost_center_id,
    fiscal_year,
    productive_total as productive_budget_for_time_type,
    job_time_type,
    0 as cc_granularity_ind,
    job_group_id
from
    rollup_job_group
