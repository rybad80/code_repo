{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_sum_final
has the rows from stg_nursing_time_sum_partial  (sum set 1)
and processes the (sum set 2) totals
to even higher sum rollups from the stg_nursing_time_sum_partial totals.
These set 2 ones also may get used in the w3_fte, w4_gap, w5_percent
*/
with
metrics_to_combine_set_2 as (
    select
        addend_metric,
        summed_metric,
        operation
    from
        {{ ref('lookup_nursing_metric_sum_mapping') }}
    where
        sum_metric_type = 'Kronos hours'
        and sum_set = 2
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

	union all

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        metric_grouper,
        numerator as hours_subset
    from
        {{ ref('stg_nursing_time_sum_partial') }}
),

apply_plus_minus as (
    select
        metrics_to_combine_set_2.summed_metric as sum_metric_abbreviation,
        potential_source_hours.metric_dt_key,
        potential_source_hours.cost_center_id,
        potential_source_hours.job_group_id,
        potential_source_hours.metric_grouper,
        case metrics_to_combine_set_2.operation
            when 'add'
            then potential_source_hours.hours_subset
            when 'subtract'
            then - potential_source_hours.hours_subset
        end as hours_subtotal
    from
        metrics_to_combine_set_2
        inner join potential_source_hours
            on metrics_to_combine_set_2.addend_metric = potential_source_hours.metric_abbreviation
),

sum_the_hours_parts_set_2 as (
    select
        subset_hours_row.sum_metric_abbreviation,
        subset_hours_row.metric_dt_key,
        subset_hours_row.cost_center_id,
        subset_hours_row.job_group_id,
        subset_hours_row.metric_grouper,
        sum(subset_hours_row.hours_subtotal) as sum_result
    from
        apply_plus_minus as subset_hours_row
    group by
        subset_hours_row.sum_metric_abbreviation,
        subset_hours_row.metric_dt_key,
        subset_hours_row.cost_center_id,
        subset_hours_row.job_group_id,
        subset_hours_row.metric_grouper
)

select
    set_2.sum_metric_abbreviation as metric_abbreviation,
    set_2.metric_dt_key,
    null as worker_id,
    set_2.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    set_2.job_group_id,
    set_2.metric_grouper,
    set_2.sum_result as numerator,
    null::numeric as denominator,
    set_2.sum_result as row_metric_calculation
from
    sum_the_hours_parts_set_2 as set_2

union all

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    {{ ref('stg_nursing_time_sum_partial') }}
