{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w5_variance
for TIME FTEs where a budget is present for the cost center, subtract the actual
from the budget to see the FTE variance experieinced for that pay period
Note: in general outside of productive flex, this is using the annualized
budget for each job group.  And unlike Productive Flex or HPPD, the target to which
the actual is compared is NOT adjusted for actual patient volumes.
*/
with job_group_budget as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        coalesce(job_group_id, 'nullJG') as adj_budget_job_group_id,
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
        coalesce(job_group_id, 'nullJG')
),

adjusted_w3_fte_rows as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        coalesce(job_group_id, 'nullJG') as adj_actual_job_group_id,
        sum(numerator) as job_group_fte_actual
    from
        {{ ref('stg_nursing_time_w3_fte') }}

    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        coalesce(job_group_id, 'nullJG')
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

cc_pp_set as ( /* in order to not use a full outer join */
    select
        metric_focus,
        metric_dt_key,
        cost_center_id,
        job_group_id as adj_job_group_id
     from
        {{ ref('stg_nursing_time_p1_cc_pp_jg') }}
)

select
    match_bdgt_fte_metrics.gap_metric_abbreviation as metric_abbreviation,
    cc_pp_set.metric_dt_key,
    null as worker_id,
    cc_pp_set.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    case when cc_pp_set.adj_job_group_id = 'nullJG'
        then null else  cc_pp_set.adj_job_group_id
    end as job_group_id,
    job_group_id as metric_grouper,
    coalesce(job_group_budget.job_group_fte_budget, 0)
    - coalesce(actual_fte.job_group_fte_actual, 0) as numerator,
    null::numeric as denominator,
    coalesce(job_group_budget.job_group_fte_budget, 0)
        -  coalesce(actual_fte.job_group_fte_actual, 0) as row_metric_calculation
from cc_pp_set
    inner join match_bdgt_fte_metrics
        on cc_pp_set.metric_focus = match_bdgt_fte_metrics.metric_focus
    left join job_group_budget
        on cc_pp_set.metric_dt_key = job_group_budget.metric_dt_key
        and cc_pp_set.cost_center_id = job_group_budget.cost_center_id
        and cc_pp_set.adj_job_group_id  = job_group_budget.adj_budget_job_group_id
        and match_bdgt_fte_metrics.budget_metric_abbreviation = job_group_budget.metric_abbreviation
    left join adjusted_w3_fte_rows as actual_fte
        on cc_pp_set.metric_dt_key = actual_fte.metric_dt_key
        and cc_pp_set.cost_center_id = actual_fte.cost_center_id
        and cc_pp_set.adj_job_group_id  = actual_fte.adj_actual_job_group_id
        and match_bdgt_fte_metrics.fte_metric_abbreviation = actual_fte.metric_abbreviation
