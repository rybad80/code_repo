{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w7_productive_budget
capture the target detail at the job group roll-up into the nursing metric
structure so that for recent pay periods users can see the target calculation
source (budgets broken by fixed & variable) in app visualizations
*/
with
latest_three_pp_plus as (
    select
        fiscal_year,
        pp_end_dt_key as recent_pp_dt_key
    from
        {{ ref('nursing_pay_period') }}
    where
        latest_complete_3_pay_periods_ind  = 1
        /* or earlier Jul/Dec snapshots in the fiscal year */
        or (pay_period_number in (2, 3, 12, 13)
            and current_fiscal_year_ind = 1)
),

budget_fte_three_pp as (
    select
        stg_nursing_flex_p1_time_type_budget.metric_dt_key,
        stg_nursing_flex_p1_time_type_budget.cost_center_id,
        stg_nursing_flex_p1_time_type_budget.fiscal_year,
        stg_nursing_flex_p1_time_type_budget.productive_budget_for_time_type,
        stg_nursing_flex_p1_time_type_budget.job_time_type,
        stg_nursing_flex_p1_time_type_budget.job_group_id
    from
        {{ ref('stg_nursing_flex_p1_time_type_budget') }} as stg_nursing_flex_p1_time_type_budget
        inner join latest_three_pp_plus
            on stg_nursing_flex_p1_time_type_budget.metric_dt_key
            = latest_three_pp_plus.recent_pp_dt_key
    where
        stg_nursing_flex_p1_time_type_budget.cc_granularity_ind = 0
),

metric_row_data as (
    select
        case
            when budget_fte_three_pp.job_time_type = 'fixed'
            then 'FixedJgFlexBdgt'
            else 'VrblJgFlexBdgt'
        end as metric_abbreviation,
        budget_fte_three_pp.metric_dt_key,
        budget_fte_three_pp.fiscal_year,
        budget_fte_three_pp.cost_center_id,
        budget_fte_three_pp.job_group_id,
        budget_fte_three_pp.productive_budget_for_time_type as numerator
    from
        budget_fte_three_pp
        inner join latest_three_pp_plus
            on budget_fte_three_pp.metric_dt_key = latest_three_pp_plus.recent_pp_dt_key
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    numerator,
    null::numeric,
    numerator as row_metric_calculation
from
    metric_row_data
