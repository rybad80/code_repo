{{ config(meta = {
    'critical': true
}) }}

with nursing_metric as (
    select
        metric_abbreviation,
        metric_focus,
        metric_type,
        percent_denominator_metric_abbreviation
    from {{ref('lookup_nursing_metric')}}
),

time_map_base as (
    select
        metric_abbreviation as hours_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'Hours'
),

fte_metric as (
    select
        metric_abbreviation as fte_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'FTE'
),

budget_metric as (
    select
        metric_abbreviation as budget_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'Budget'
),

percent_metric as (
    select
        percent_denominator_metric_abbreviation,
        metric_abbreviation as percent_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'Percent'
),

gap_metric as (
    select
        metric_abbreviation as gap_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'Gap'
),

headcount_metric as (
    select
        metric_abbreviation as headcount_metric_abbreviation,
        metric_focus
    from nursing_metric
    where metric_type = 'Headcount'
)

select
    time_map_base.metric_focus,
    headcount_metric_abbreviation as headcount_metric_abbreviation,
    time_map_base.hours_metric_abbreviation,
    fte_metric.fte_metric_abbreviation,
    budget_metric.budget_metric_abbreviation,
    percent_metric.percent_denominator_metric_abbreviation as denominator_metric_abbreviation,
    percent_metric.percent_metric_abbreviation,
    gap_metric.gap_metric_abbreviation,
    'recorded time' as metric_mapping_focus
from time_map_base
left join headcount_metric on time_map_base.metric_focus = headcount_metric.metric_focus
left join fte_metric on time_map_base.metric_focus = fte_metric.metric_focus
left join budget_metric on time_map_base.metric_focus = budget_metric.metric_focus
left join percent_metric on time_map_base.metric_focus = percent_metric.metric_focus
left join gap_metric on time_map_base.metric_focus = gap_metric.metric_focus

union all

select
    map_base.metric_focus,
    map_base.headcount_metric_abbreviation,
    null as hours_metric_abbreviation,
    fte_metric.fte_metric_abbreviation,
    budget_metric.budget_metric_abbreviation,
    percent_metric.percent_denominator_metric_abbreviation as denominator_metric_abbreviation,
    percent_metric.percent_metric_abbreviation,
    gap_metric.gap_metric_abbreviation,
    'staffing' as metric_mapping_focus
from headcount_metric as map_base
left join fte_metric on map_base.metric_focus = fte_metric.metric_focus
left join budget_metric on map_base.metric_focus = budget_metric.metric_focus
left join percent_metric on map_base.metric_focus = percent_metric.metric_focus
left join gap_metric on map_base.metric_focus = gap_metric.metric_focus
