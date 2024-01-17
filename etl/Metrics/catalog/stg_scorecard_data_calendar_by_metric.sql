with date_by_metric as (
    select
        metric_id,
        min(visual_month) as min_month,
        case
            when metric_lag is null and day(current_date) != 1
                then date_trunc('month', current_date)
            when metric_lag is null and day(current_date) = 1
                then date_trunc('month', current_date) - 1
            else max(visual_month)
        end as max_month
    from
        {{ ref_for_env('stg_scorecard_data') }}
    group by
        metric_id,
        metric_lag
),

scorecard_metadata as (
    select distinct
        stg_scorecard_data.metric_name,
        date_by_metric.max_month,
        date_by_metric.min_month,
        stg_scorecard_data.drill_down_one,
        stg_scorecard_data.drill_down_two,
        stg_scorecard_data.domain,
        stg_scorecard_data.subdomain,
        stg_scorecard_data.metric_type,
        stg_scorecard_data.desired_direction, 
        stg_scorecard_data.metric_lag,
        stg_scorecard_data.metric_id
    from
        date_by_metric
    inner join
        {{ ref_for_env('stg_scorecard_data') }} as stg_scorecard_data
            on date_by_metric.metric_id = stg_scorecard_data.metric_id
)
select
    scorecard_metadata.metric_name,
    scorecard_metadata.max_month,
    scorecard_metadata.min_month,
    scorecard_metadata.drill_down_one,
    scorecard_metadata.drill_down_two,
    scorecard_metadata.domain,
    scorecard_metadata.subdomain,
    scorecard_metadata.metric_type,
    scorecard_metadata.desired_direction,
    {{ standard_date_fields(alias='calendar') }},
    calendar.full_dt,
    calendar.f_day,
    calendar.c_day,
    {{- calculate_as_of_date('metric_lag') -}},
    scorecard_metadata.metric_id
from
    scorecard_metadata
inner join 
    {{ ref_for_env('stg_scorecard_calendar') }} as calendar
        on calendar.visual_month <= scorecard_metadata.max_month
        and calendar.visual_month >= scorecard_metadata.min_month
group by
    scorecard_metadata.metric_name,
    scorecard_metadata.max_month,
    scorecard_metadata.min_month,
    scorecard_metadata.drill_down_one,
    scorecard_metadata.drill_down_two,
    scorecard_metadata.domain,
    scorecard_metadata.subdomain,
    scorecard_metadata.metric_type,
    scorecard_metadata.desired_direction,
    {{ standard_date_fields(alias='calendar') }},
    calendar.full_dt,
    calendar.f_day,
    calendar.c_day,
    as_of_date,
    scorecard_metadata.metric_id
