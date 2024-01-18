{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p2_fixed_budget
create component of the productive flex target that is
made of the fixed job roles, which will not need to
have the patient days actuals applied
*/

with
fixed_budget_fte as (
    select
        metric_dt_key,
        cost_center_id,
        fiscal_year,
        productive_budget_for_time_type as productive_budget_fixed,
        job_time_type
    from
        {{ ref('stg_nursing_flex_p1_time_type_budget') }}
    where job_time_type = 'fixed'
        and cc_granularity_ind = 1
)

select
    'FlexBdgtFixed' as metric_abbreviation,
    fixed_budget_fte.metric_dt_key,
    fixed_budget_fte.cost_center_id,
    'CC Role Total' as metric_grouper,
    fixed_budget_fte.productive_budget_fixed as numerator
from
    fixed_budget_fte
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on nursing_pay_period.prior_pay_period_ind = 1
        and fixed_budget_fte.metric_dt_key = nursing_pay_period.pp_end_dt_key
