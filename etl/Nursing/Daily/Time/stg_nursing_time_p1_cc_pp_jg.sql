{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_p1_cc_pp_jg
for TIME variance at the job group granulairty get the cost center / pay period / job group
combinations that applied to support either budget or actual or both scenarios.
Then drop off any where we have actual FTE but have no budget at all for the cc .
Note: in general outside of productive flex, this is using the annualized
budget for each job group
*/
with job_group_budget as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        coalesce(job_group_id, 'nullJG') as job_group_id,
        sum(numerator) as job_group_fte_budget
    from
        {{ ref('stg_nursing_budget_period_workforce') }}
    where
        metric_abbreviation like '%JgBdgt'
        or metric_abbreviation = 'SafetyObsBdgt'
    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        coalesce(job_group_id, 'nullJG')
),

cc_has_budget as (
    select
        metric_dt_key,
        cost_center_id
    from
        job_group_budget
    group by
        metric_dt_key,
        cost_center_id
),

adjusted_w3_fte_rows as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        coalesce(job_group_id, 'nullJG') as job_group_id,
        sum(numerator) as job_group_fte_actual
    from
        {{ ref('stg_nursing_time_w3_fte') }}

    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        coalesce(job_group_id, 'nullJG')
),

cc_has_actuals as (
    select
        metric_dt_key,
        cost_center_id
    from
        adjusted_w3_fte_rows
    group by
        metric_dt_key,
        cost_center_id
),

match_bdgt_fte_metrics as (
    select
        metric_focus,
        budget_metric_abbreviation,
        fte_metric_abbreviation,
        gap_metric_abbreviation
    from
        {{ ref('nursing_metric_mapping_time') }}
    where
        gap_metric_abbreviation is not null
        and gap_metric_abbreviation != 'FlexGap'
    group by
        metric_focus,
        budget_metric_abbreviation,
        fte_metric_abbreviation,
        gap_metric_abbreviation
),

cc_pp_set as ( /* in order to not use a full outer join
    get the combos where wither or both are present */
    select
        focus_match.metric_focus,
        has_budget.metric_dt_key,
        has_budget.cost_center_id,
        has_budget.job_group_id
    from
        job_group_budget as has_budget
        inner join match_bdgt_fte_metrics as focus_match
            on has_budget.metric_abbreviation = focus_match.budget_metric_abbreviation
    group by
        focus_match.metric_focus,
        has_budget.metric_dt_key,
        has_budget.cost_center_id,
        has_budget.job_group_id
    union /* need distinct cc/pp combos for the focus & job group */
    select
        focus_match.metric_focus,
        has_actual_fte.metric_dt_key,
        has_actual_fte.cost_center_id,
        has_actual_fte.job_group_id
    from
        adjusted_w3_fte_rows as has_actual_fte
        inner join match_bdgt_fte_metrics as focus_match
            on has_actual_fte.metric_abbreviation = focus_match.fte_metric_abbreviation
    group by
        focus_match.metric_focus,
        has_actual_fte.metric_dt_key,
        has_actual_fte.cost_center_id,
        has_actual_fte.job_group_id
)

select
    cc_pp_set.metric_focus,
    cc_pp_set.metric_dt_key,
    cc_pp_set.cost_center_id,
    cc_pp_set.job_group_id
from
    cc_pp_set
    /* now drop off any where we have actual FTE but have no budget at all for the cc */
    inner join cc_has_budget
        on cc_pp_set.cost_center_id = cc_has_budget.cost_center_id
        and cc_pp_set.metric_dt_key = cc_has_budget.metric_dt_key
    /*and data we we have not actauls at all for the cc/pp */
    inner join cc_has_actuals
        on cc_pp_set.cost_center_id = cc_has_actuals.cost_center_id
        and cc_pp_set.metric_dt_key = cc_has_actuals.metric_dt_key
