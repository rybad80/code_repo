{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w4_percent
based on the nursing_metric_mapping_time metric matches
assign the appropriate hours value to the numerator and the
corresponding denominator value so they can be divided but also
able to SUM & re-calc for rollups in a visualization
*/
with
match_metrics as (
    select
        hours_metric_abbreviation,
        denominator_metric_abbreviation,
        percent_metric_abbreviation
    from
        {{ ref('nursing_metric_mapping_time') }}
    where
        percent_metric_abbreviation is not null
    group by
        hours_metric_abbreviation,
        denominator_metric_abbreviation,
        percent_metric_abbreviation
),

pct_metric_src as (
    /* numerator and denominator both pull from data in earlier waves */
    select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        cost_center_id,
        cost_center_site_id,
        job_code,
        metric_grouper as job_group_id,
        metric_grouper,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_time_w1a_hrs_direct') }}

    union all

    select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        cost_center_id,
        cost_center_site_id,
        job_code,
        metric_grouper as job_group_id,
        metric_grouper,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_time_w2_hrs_non_reg_non_ot') }}


    union all

    select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        cost_center_id,
        cost_center_site_id,
        job_code,
        metric_grouper as job_group_id,
        metric_grouper,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_time_sum_final') }}

),

nursing_time_subset as (
    select
        match_metrics.percent_metric_abbreviation,
        numerator_row.metric_dt_key,
        numerator_row.cost_center_id,
        numerator_row.job_code,
        numerator_row.metric_grouper as job_group_id,
        numerator_row.metric_grouper,
        sum(numerator_row.numerator) as numerator,
        match_metrics.denominator_metric_abbreviation
    from
        pct_metric_src as numerator_row
        inner join match_metrics on
            numerator_row.metric_abbreviation = match_metrics.hours_metric_abbreviation
    group by
        match_metrics.percent_metric_abbreviation,
        numerator_row.metric_dt_key,
        numerator_row.cost_center_id,
        numerator_row.job_code,
        -- job_group_id,
        numerator_row.metric_grouper,
        numerator_row.numerator,
        match_metrics.denominator_metric_abbreviation
),

use_denominator as (
    select
        match_metrics.denominator_metric_abbreviation,
        denominator_row.metric_dt_key,
        denominator_row.cost_center_id,
        denominator_row.metric_grouper as job_group_id,
        denominator_row.metric_grouper,
        denominator_row.numerator as pct_denominator,
        match_metrics.percent_metric_abbreviation
    from
        pct_metric_src as denominator_row
        inner join match_metrics
            on denominator_row.metric_abbreviation
            = match_metrics.denominator_metric_abbreviation
    where
        denominator_row.numerator != 0
    group by
        match_metrics.denominator_metric_abbreviation,
        denominator_row.metric_dt_key,
        denominator_row.cost_center_id,
        -- job_group_id,
        denominator_row.metric_grouper,
        denominator_row.numerator,
        match_metrics.percent_metric_abbreviation
)

select
    use_denominator.percent_metric_abbreviation as metric_abbreviation,
    use_denominator.metric_dt_key,
    null as worker_id,
    use_denominator.cost_center_id,
    null as cost_center_site_id,
    use_denominator.job_group_id,
    null as job_code,
    use_denominator.metric_grouper,
    coalesce(nursing_time_subset.numerator, 0) as numerator,
    use_denominator.pct_denominator as denominator,
    round(nursing_time_subset.numerator / use_denominator.pct_denominator, 4) as row_metric_calculation
from
    use_denominator
    left join nursing_time_subset
        on use_denominator.metric_dt_key = nursing_time_subset.metric_dt_key
        and use_denominator.cost_center_id = nursing_time_subset.cost_center_id
        and use_denominator.job_group_id  = nursing_time_subset.job_group_id
        and use_denominator.metric_grouper  = nursing_time_subset.metric_grouper
        and use_denominator.denominator_metric_abbreviation
        = nursing_time_subset.denominator_metric_abbreviation
        and use_denominator.percent_metric_abbreviation
        = nursing_time_subset.percent_metric_abbreviation
