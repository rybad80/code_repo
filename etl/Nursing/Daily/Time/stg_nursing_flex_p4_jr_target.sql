{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p4_jr_target
in order to build the final flex target using the variable role component
convert the WHUOS planned (FlexBdgtVar & FlexBdgtJgVar from p3) adjusting for actual patient days
for the variable roles:
convert the planned WHUOS = workerd hours per unit of service
back to a target variable FTE
 -> multiplying by ACTUAL patient days to get the hours
 -> dividing by 80 (for a two-week pay period)
to convert back to FTE for the variable roles

this SQL handles the CC calculation as well as the Job Rollup level

for the Fixed roles, it is already in FTE units so just roll them up

Note:  similar to staffing, it does not make sense to compare to actuals at the type of StaffNurse
or UAP but at the level 4 hieararchy when applicable so rolling up to nursing_job_rollup first
*/
with
variable_whuos_rolled_up as (
    select
        vrbl_whuos_planned.metric_abbreviation,
        vrbl_whuos_planned.metric_dt_key,
        vrbl_whuos_planned.cost_center_id,
        vrbl_whuos_planned.metric_grouper,
        case
            when vrbl_whuos_planned.job_group_id is null
            then null
            else get_rollup.nursing_job_rollup
            end as job_group_id,
        sum(vrbl_whuos_planned.numerator) as jr_total
    from
        {{ ref('stg_nursing_flex_p3_whuos_planned') }} as vrbl_whuos_planned
    left join {{ ref('job_group_levels_nursing') }} as get_rollup
        on vrbl_whuos_planned.job_group_id = get_rollup.job_group_id
    group by
        vrbl_whuos_planned.metric_abbreviation,
        vrbl_whuos_planned.metric_dt_key,
        vrbl_whuos_planned.cost_center_id,
        vrbl_whuos_planned.metric_grouper,
        vrbl_whuos_planned.job_group_id,
        get_rollup.nursing_job_rollup

),

adjust_variable_component_by_actual_patient_days as (
    /* for variable roles the WHUOS is converted based on actual patient days for the flex target */
    select
        case variable_whuos_rolled_up.metric_abbreviation
            when 'FlexCcVwhuosPlnd' then 'FlexTrgtCcVrbl'
            when 'FlexJgVwhuosPlnd' then 'FlexTrgtJr'
            end as metric_abbreviation,
        variable_whuos_rolled_up.metric_dt_key,
        variable_whuos_rolled_up.cost_center_id,
        variable_whuos_rolled_up.metric_grouper,
        variable_whuos_rolled_up.job_group_id,
        sum(variable_whuos_rolled_up.jr_total)
        * stg_nursing_unit_w1_patient_days.numerator
        / 80 as target_fte
    from
        variable_whuos_rolled_up
	inner join {{ ref('stg_nursing_unit_w1_patient_days') }} as stg_nursing_unit_w1_patient_days
        on variable_whuos_rolled_up.cost_center_id = stg_nursing_unit_w1_patient_days.cost_center_id
        and variable_whuos_rolled_up.metric_dt_key = stg_nursing_unit_w1_patient_days.metric_dt_key
        and stg_nursing_unit_w1_patient_days.metric_abbreviation = 'PatDaysPPactualTot'
    group by
        variable_whuos_rolled_up.metric_abbreviation,
        variable_whuos_rolled_up.metric_dt_key,
        variable_whuos_rolled_up.cost_center_id,
        variable_whuos_rolled_up.metric_grouper,
        variable_whuos_rolled_up.job_group_id,
        stg_nursing_unit_w1_patient_days.numerator
), --select * from adjust_variable_component_by_actual_patient_days;

fixed_component_jr as ( /* for fixed roles, the budget is the flex target */
    select
        stg_nursing_flex_p1_time_type_budget.metric_dt_key,
        stg_nursing_flex_p1_time_type_budget.cost_center_id,
        get_rollup.nursing_job_rollup,
	stg_nursing_flex_p1_time_type_budget.job_group_id as job_group_id,
        coalesce(get_rollup.nursing_job_rollup,
        stg_nursing_flex_p1_time_type_budget.job_group_id) as jr_job_group_id,
	stg_nursing_flex_p1_time_type_budget.productive_budget_for_time_type
    from
        {{ ref('stg_nursing_flex_p1_time_type_budget') }} as stg_nursing_flex_p1_time_type_budget
        left join {{ ref('job_group_levels_nursing') }} as get_rollup
            on stg_nursing_flex_p1_time_type_budget.job_group_id = get_rollup.job_group_id
    where
        stg_nursing_flex_p1_time_type_budget.job_time_type  = 'fixed'
        and stg_nursing_flex_p1_time_type_budget.cc_granularity_ind = 0
),

fixed_component_rolled_up as ( /* to nursing_job_rollup */
    select
        'FlexTrgtJr' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        null as metric_grouper,
        jr_job_group_id as job_group_id,
        sum(productive_budget_for_time_type) as target_fte
    from
        fixed_component_jr
    group by
        metric_dt_key,
        cost_center_id,
        jr_job_group_id

)
select /* for Variable roles at job rollup & CC Total */
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    target_fte
from
    adjust_variable_component_by_actual_patient_days

union all

select /* for Fixed roles at job rollup */
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    target_fte
from
    fixed_component_rolled_up
