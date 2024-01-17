{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p5_component
components and the final flex budget
which is the sum of
#1 fixed FTE (FlexBdgtFixed from p2)
#2 FlexTrgtCcVrbl (= the variable target FTE/pat-days-vol (FlexBdgtVar
from p3) adjusted for actual patient days)
so a final flex budget (FlexTargetAdj) can be an input to stg_nursing_TIME_w6_flex_gap

#2 calculation:
convert the budgeted WHUOS = workerd hours per unit of service
back to a target variable FTE
 -> * ACTUAL patient days to get the hours
 -> / 80 (for a two-week pay period)
to convert back to FTE for the variable roles
*/

/* cost center fixed role total
from p2:  FlexBdgtFixed
*/
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    null as job_group_id,
    numerator
from
    {{ ref('stg_nursing_flex_p2_fixed_budget') }}
union all

/* cost center worked hours per unit of service variable total and broken out by job rollup
from P3:  FlexCcVwhuosPlnd & FlexJgVwhuosPlnd
*/
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    numerator
from
    {{ ref('stg_nursing_flex_p3_whuos_planned') }}
union all

/* cost center variable role target and broken out by job rollup
from P4:  FlexTrgtCcVrbl & FlexTrgtJr
*/
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    target_fte
from
    {{ ref('stg_nursing_flex_p4_jr_target') }}
union all

/* cost center complete flex target when fixed + variable roles need to be added together
from P4:  FlexTrgtCcVrbl
from p2:  FlexBdgtFixed
*/
select
    'FlexTrgtCc' as metric_abbreviation,
    stg_nursing_flex_p4_jr_target.metric_dt_key,
    stg_nursing_flex_p4_jr_target.cost_center_id,
    stg_nursing_flex_p4_jr_target.metric_grouper,
    stg_nursing_flex_p4_jr_target.job_group_id,
    stg_nursing_flex_p4_jr_target.target_fte
    + stg_nursing_flex_p2_fixed_budget.numerator
    as total_budget_fte_adj_by_pat_days
from
    {{ ref('stg_nursing_flex_p4_jr_target') }} as stg_nursing_flex_p4_jr_target
    inner join {{ ref('stg_nursing_flex_p2_fixed_budget') }} as stg_nursing_flex_p2_fixed_budget
        on stg_nursing_flex_p4_jr_target.cost_center_id
        = stg_nursing_flex_p2_fixed_budget.cost_center_id
        and stg_nursing_flex_p4_jr_target.metric_dt_key
        = stg_nursing_flex_p2_fixed_budget.metric_dt_key
        and stg_nursing_flex_p4_jr_target.metric_grouper
        = stg_nursing_flex_p2_fixed_budget.metric_grouper

union all

/* cost center complete flex target when only fixed roles
from p2:  FlexBdgtFixed
*/
select
    'FlexTrgtCc' as metric_abbreviation,
    stg_nursing_flex_p2_fixed_budget.metric_dt_key,
    stg_nursing_flex_p2_fixed_budget.cost_center_id,
    stg_nursing_flex_p2_fixed_budget.metric_grouper,
    null as job_group_id,
    stg_nursing_flex_p2_fixed_budget.numerator
    as total_budget_fte_adj_by_pat_days
from
    {{ ref('nursing_cost_center_attributes') }} as cc_fixed_only
    inner join {{ ref('stg_nursing_flex_p2_fixed_budget') }} as stg_nursing_flex_p2_fixed_budget
        on cc_fixed_only.cost_center_id
        = stg_nursing_flex_p2_fixed_budget.cost_center_id
        and cc_fixed_only.nursing_flex_ind = 1
        and cc_fixed_only.flex_variable_ind = 0
