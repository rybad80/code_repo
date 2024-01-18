{{ config(meta = {
    'critical': true
}) }}

/*  stg_nursing_flex_p3_whuos_planned
calculate the variable_component_target (pre actual adjustment)
by using the budgeted patient days

convert the FTE budget to hours a * 80 (for a two-week pay period)
then divide by the budgeted patient days to get the planned for
WHUOS = workerd hours per unit of service for the variable roles

P4 step will then reverse this math using actual patient days to get the FTE
that should have been used for the volumnes experienced
*/
with
variable_jg_budget_fte as (
    select
        metric_dt_key,
        cost_center_id,
        fiscal_year,
        productive_budget_for_time_type  as productive_budget_var_roles,
        job_time_type,
        job_group_id
    from
        {{ ref('stg_nursing_flex_p1_time_type_budget') }}
    where
        job_time_type = 'variable'
        and cc_granularity_ind = 0
),

variable_cc_budget_fte as (
    select
        metric_dt_key,
        cost_center_id,
        fiscal_year,
        productive_budget_for_time_type  as productive_budget_var_roles,
        job_time_type
    from
        {{ ref('stg_nursing_flex_p1_time_type_budget') }}
    where
        job_time_type = 'variable'
        and cc_granularity_ind = 1
),

earliest_budget_year as (
    select
        min(fiscal_year) as min_budget_fiscal_year
    from
        variable_cc_budget_fte
),

keep_pp as (
    select
        nursing_pay_period.pp_end_dt_key as budget_dt_key,
        nursing_pay_period.fiscal_year
    from
        earliest_budget_year
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on earliest_budget_year.min_budget_fiscal_year <= nursing_pay_period.fiscal_year
            and nursing_pay_period.prior_pay_period_ind = 1
),

volume_budget_total_by_pay_period as (
    select
        pat_days.metric_dt_key,
        pat_days.cost_center_id,
        pat_days.metric_grouper,
        sum(pat_days.row_metric_calculation) as patient_days_budget,
        keep_pp.fiscal_year as patient_days_year
    from
        {{ ref('stg_nursing_unit_w1_patient_days') }} as pat_days
        inner join keep_pp on pat_days.metric_dt_key = keep_pp.budget_dt_key
    where
        pat_days.metric_abbreviation = 'PatDaysPPbdgtTot'
        and pat_days.metric_grouper = 'all'
    group by
        pat_days.metric_dt_key,
        pat_days.cost_center_id,
        pat_days.metric_grouper,
        keep_pp.fiscal_year
),

volume_budget as (
    select
        metric_dt_key,
        cost_center_id,
        metric_grouper,
        patient_days_budget,
        patient_days_year
    from
        volume_budget_total_by_pay_period
    where
        patient_days_budget > 0
),

variable_component_target as (
    select
        'FlexCcVwhuosPlnd' as metric_abbreviation,
        volume_budget.metric_dt_key,
        variable_cc_budget_fte.cost_center_id,
        volume_budget.patient_days_budget,
        variable_cc_budget_fte.productive_budget_var_roles,
        (variable_cc_budget_fte.productive_budget_var_roles * 80 /* convert FTE to hours */
        / volume_budget.patient_days_budget) as flex_target_variable_component_calc,
        round(flex_target_variable_component_calc, 2) as variable_target_productive,
        null as job_group_id,
        'CC Role Total' as metric_grouper
    from
        variable_cc_budget_fte
        inner join volume_budget
            on variable_cc_budget_fte.cost_center_id = volume_budget.cost_center_id
            and variable_cc_budget_fte.fiscal_year = volume_budget.patient_days_year
            and variable_cc_budget_fte.metric_dt_key = volume_budget.metric_dt_key

    union all

    select
        'FlexJgVwhuosPlnd' as metric_abbreviation, /* job group granularity */
        volume_budget.metric_dt_key,
        variable_jg_budget_fte.cost_center_id,
        volume_budget.patient_days_budget,
        variable_jg_budget_fte.productive_budget_var_roles,
        (variable_jg_budget_fte.productive_budget_var_roles * 80 /* convert FTE to hours */
        / volume_budget.patient_days_budget) as flex_target_variable_component_calc,
        round(flex_target_variable_component_calc, 2) as variable_target_productive,
        variable_jg_budget_fte.job_group_id,
        null as metric_grouper
    from
        variable_jg_budget_fte
    inner join volume_budget
            on variable_jg_budget_fte.cost_center_id = volume_budget.cost_center_id
            and variable_jg_budget_fte.fiscal_year = volume_budget.patient_days_year
            and variable_jg_budget_fte.metric_dt_key = volume_budget.metric_dt_key
)

select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    variable_target_productive as numerator
from
    variable_component_target
