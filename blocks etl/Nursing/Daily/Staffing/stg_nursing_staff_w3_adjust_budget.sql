/* stg_nursing_staff_w3_adjust_budget
When the budget is for psych techs, which began to have the job title of
behavioral health clinician II in early 2023,
shift that budget to the centralized behavioral health cost center,
10910 Centralized Behavioral Health Staff,
which is the home cost center for the people in these jobs
even though the budget is where they expect to be working
and the Kronos actual hours will show up where budgeted.
This provides appropriate alignment for vacancy determination
to not create false vacancy alarms.
*/

with
job_group_lvl_4_adj_budget_safetyobserver as (
    select
        orig_budget.metric_dt_key,
        orig_budget.job_group_id,
        orig_budget.cost_center_id,
        - sum(orig_budget.numerator) as fte_shifted_budget
    from
        {{ ref('stg_nursing_staff_w2_budget') }} as orig_budget
    where
        orig_budget.metric_abbreviation = 'currFTEBdgtLvl4'
        and orig_budget.job_group_id = 'SafetyObserver'
        and orig_budget.cost_center_id != '10910'
    group by
        orig_budget.metric_dt_key,
        orig_budget.cost_center_id,
        orig_budget.job_group_id
),

job_group_lvl_4_adj_budget_apply_to_10910 as (
    select
        '10910' as cost_center_id,
        metric_dt_key,
        job_group_id,
        - sum(fte_shifted_budget) as budget_move_to_10190
    from
        job_group_lvl_4_adj_budget_safetyobserver
    group by
        metric_dt_key,
        job_group_id
),

hiring_target_adjustment as (
    select
        cost_center_id,
        job_group_id,
        hiring_target_adjustment,
        lookup_nursing_budget_adjustment.fiscal_year,
        pp_end_dt_key as metric_dt_key
    from {{ ref('lookup_nursing_budget_adjustment') }} as lookup_nursing_budget_adjustment
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on lookup_nursing_budget_adjustment.fiscal_year = nursing_pay_period.fiscal_year
        and nursing_pay_period.current_working_pay_period_ind = 1
),

adjusted_budget_amount as (
    select
        'FTEadjBdgtAmt' as metric_abbreviation,
        coalesce(job_group_lvl_4_adj_budget_safetyobserver.metric_dt_key,
            job_group_lvl_4_adj_budget_apply_to_10910.metric_dt_key,
            hiring_target_adjustment.metric_dt_key) as metric_dt_key,
        coalesce(job_group_lvl_4_adj_budget_safetyobserver.job_group_id,
            job_group_lvl_4_adj_budget_apply_to_10910.job_group_id,
            hiring_target_adjustment.job_group_id) as job_group_id,
        coalesce(job_group_lvl_4_adj_budget_safetyobserver.cost_center_id,
            job_group_lvl_4_adj_budget_apply_to_10910.cost_center_id,
            hiring_target_adjustment.cost_center_id) as cost_center_id,
        coalesce(job_group_lvl_4_adj_budget_safetyobserver.fte_shifted_budget,
            job_group_lvl_4_adj_budget_apply_to_10910.budget_move_to_10190,
            hiring_target_adjustment.hiring_target_adjustment) as fte_adj_bdgt_amt
    from job_group_lvl_4_adj_budget_safetyobserver
    full outer join job_group_lvl_4_adj_budget_apply_to_10910
        on job_group_lvl_4_adj_budget_safetyobserver.metric_dt_key
            = job_group_lvl_4_adj_budget_apply_to_10910.metric_dt_key
        and job_group_lvl_4_adj_budget_safetyobserver.job_group_id
            = job_group_lvl_4_adj_budget_apply_to_10910.job_group_id
        and job_group_lvl_4_adj_budget_safetyobserver.cost_center_id
            = job_group_lvl_4_adj_budget_apply_to_10910.cost_center_id
    full outer join hiring_target_adjustment
        on job_group_lvl_4_adj_budget_safetyobserver.metric_dt_key
            = hiring_target_adjustment.metric_dt_key
        and job_group_lvl_4_adj_budget_safetyobserver.job_group_id
            = hiring_target_adjustment.job_group_id
        and job_group_lvl_4_adj_budget_safetyobserver.cost_center_id
            = hiring_target_adjustment.cost_center_id
),

job_group_lvl_4_cc_budget_as_adjusted as (
    select
        'currFTEBdgtLvl4Adj' as metric_abbreviation,
        orig_budget.metric_dt_key,
        orig_budget.cost_center_id,
        orig_budget.job_group_id,
        orig_budget.numerator as numerator,
        adjusted_budget_amount.fte_adj_bdgt_amt as denominator,
        orig_budget.numerator
        + coalesce(adjusted_budget_amount.fte_adj_bdgt_amt, 0) /* apply adjustment if any */
        as row_metric_calculation
    from
        {{ ref('stg_nursing_staff_w2_budget') }} as orig_budget
    left join adjusted_budget_amount
        on adjusted_budget_amount.cost_center_id = orig_budget.cost_center_id
        and adjusted_budget_amount.metric_dt_key = orig_budget.metric_dt_key
        and adjusted_budget_amount.job_group_id = orig_budget.job_group_id
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
    denominator,
    row_metric_calculation
from
    job_group_lvl_4_cc_budget_as_adjusted

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    fte_adj_bdgt_amt as numerator,
    null::numeric as denominator,
    fte_adj_bdgt_amt as row_metric_calculation
from
    adjusted_budget_amount
