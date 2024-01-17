{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_sum_partial
takes care of other SUMs that will be final metrics themselves as well as sums that
then become part of an even higher sum rollup in stg_nursing_time_sum_final
and/or get used in the w3_fte, w4_gap, w5_percent
*/

with
metrics_to_combine as (
    select
        addend_metric,
        summed_metric,
        operation
    from
        {{ ref('lookup_nursing_metric_sum_mapping') }}
    where
        sum_metric_type = 'Kronos hours'
        and sum_set = 1
),

potential_source_hours as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        numerator as hours_subset
    from
        {{ ref('stg_nursing_time_w1a_hrs_direct') }}

    union all

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        numerator as hours_subset
    from
        {{ ref('stg_nursing_time_w2_hrs_non_reg_non_ot') }}
),

apply_plus_minus as (
    select
        metrics_to_combine.summed_metric as sum_metric_abbreviation,
        potential_source_hours.metric_dt_key,
        potential_source_hours.cost_center_id,
        job_group_id,
        potential_source_hours.metric_grouper,
        case metrics_to_combine.operation
            when 'add'
            then potential_source_hours.hours_subset
            when 'subtract'
            then - potential_source_hours.hours_subset
        end as hours_subtotal
    from
        metrics_to_combine
        inner join potential_source_hours
            on metrics_to_combine.addend_metric = potential_source_hours.metric_abbreviation
),

sum_the_hours_parts as (
    select
        sum_metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        sum(hours_subtotal) as sum_result
    from
        apply_plus_minus
    group by
        sum_metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper
)

select
    sum_metric_abbreviation as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    sum_result as numerator,
    null::numeric as denominator,
    sum_result as row_metric_calculation
from
    sum_the_hours_parts
