{{ config(meta = {
    'critical': true
}) }}

with match_hours_fte_metrics as (
    select
        hours_metric_abbreviation,
        fte_metric_abbreviation
    from
        {{ ref('nursing_metric_mapping_time') }}
    where
        fte_metric_abbreviation is not null
),

stg_nursing_time_w1_w2_and_sum as (
    select *
    from
        {{ ref('stg_nursing_time_w1a_hrs_direct') }}

    union all

    select *
    from
        {{ ref('stg_nursing_time_w1b_extrapay') }}

    union all

    select *
    from
        {{ ref('stg_nursing_time_w2_hrs_non_reg_non_ot') }}

    union all

    select *
    from
        {{ ref('stg_nursing_time_sum_final') }}
),

metric_component_row as (
    select
        match_hours_fte_metrics.fte_metric_abbreviation as metric_abbreviation,
        stg_nursing_time_w1_w2.metric_dt_key,
        stg_nursing_time_w1_w2.worker_id,
        stg_nursing_time_w1_w2.cost_center_id,
        stg_nursing_time_w1_w2.cost_center_site_id,
        stg_nursing_time_w1_w2.job_code,
        stg_nursing_time_w1_w2.metric_grouper as job_group_id,
        stg_nursing_time_w1_w2.metric_grouper,
        round(stg_nursing_time_w1_w2.numerator / 80, 5) as fte_numerator,
        stg_nursing_time_w1_w2.denominator
    from
        stg_nursing_time_w1_w2_and_sum as stg_nursing_time_w1_w2
        inner join match_hours_fte_metrics as match_hours_fte_metrics on
            stg_nursing_time_w1_w2.metric_abbreviation = match_hours_fte_metrics.hours_metric_abbreviation
)

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    fte_numerator as numerator,
    denominator,
    fte_numerator as row_metric_calculation
from
    metric_component_row
