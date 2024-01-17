/* nursing_metric_mapping_engage
via the metric lookup associate metrics by metric focus
and grouper_type, where applicable, for use in matching and generating
metrics in the nursing engagement survey data processing from
question aggregates for nursing & associated benchmarks
to rollups where needed
to determining result category
and lastly summarizing results by category year over year (YoY)
*/
with
nursing_metric as (
    select
        metric_abbreviation,
        metric_focus,
        metric_type,
        grouper_type
    from
        {{ ref('lookup_nursing_metric') }}
),

aggregate_map_base as (
    select
        metric_abbreviation as aggregate_metric_abbreviation,
        metric_focus,
        grouper_type
    from
        nursing_metric
    where
        metric_type in ('score', 'favorable', 'topbox')
),

benchmark_metric as (
    select
        metric_abbreviation as benchmark_metric_abbreviation,
        metric_focus,
        grouper_type,
        case
            when grouper_type like '%favorable%'
            then 1 else 0
        end as favorable_grouper_ind
    from
        nursing_metric
    where
        metric_type = 'benchmark'
),

belowmark_metric as (
    select
        metric_abbreviation as belowmark_metric_abbreviation,
        metric_focus,
        grouper_type,
        case
            when grouper_type like '%favorable%'
            then 1 else 0
        end as favorable_grouper_ind
    from
        nursing_metric
    where
        metric_type = 'belowmark'
),

rollup_metric as (
    select
        metric_abbreviation as rollup_metric_abbreviation,
        metric_focus,
        grouper_type
    from
        nursing_metric
    where
        metric_type = 'rollup'
        or metric_type like 'rollup%'
),

category_metric as (
    select
        metric_abbreviation as category_metric_abbreviation,
        metric_focus,
        grouper_type
    from
        nursing_metric
    where
        metric_type = 'category'
        or metric_type like 'catg%'
),

yoy_metric as (
    select
        metric_abbreviation as yoy_metric_abbreviation,
        metric_focus,
        grouper_type
    from
        nursing_metric
    where
        lower(metric_type) = 'yoy'
        or lower(metric_type) like 'yoy%'
)

select
    aggregate_map_base.metric_focus,
    rollup_metric.grouper_type,
    aggregate_map_base.aggregate_metric_abbreviation,
    benchmark_metric.benchmark_metric_abbreviation,
    belowmark_metric.belowmark_metric_abbreviation,
    rollup_metric.rollup_metric_abbreviation,
    category_metric.category_metric_abbreviation,
    yoy_metric.yoy_metric_abbreviation
from
    aggregate_map_base
    left join benchmark_metric
        on aggregate_map_base.metric_focus = benchmark_metric.metric_focus
        and ((coalesce(aggregate_map_base.grouper_type, 'null')
        = coalesce(benchmark_metric.grouper_type, 'null'))
            or benchmark_metric.favorable_grouper_ind = 1)
    left join belowmark_metric
        on aggregate_map_base.metric_focus = belowmark_metric.metric_focus
        and ((coalesce(aggregate_map_base.grouper_type, 'null')
        = coalesce(belowmark_metric.grouper_type, 'null'))
            or belowmark_metric.favorable_grouper_ind = 1)
    left join rollup_metric
        on aggregate_map_base.metric_focus = rollup_metric.metric_focus
        and ((coalesce(benchmark_metric.grouper_type, 'null')
        = coalesce(rollup_metric.grouper_type, 'null'))
            or benchmark_metric.favorable_grouper_ind = 1)
    left join category_metric
        on aggregate_map_base.metric_focus = category_metric.metric_focus
        and coalesce(rollup_metric.grouper_type, 'null')
        = coalesce(category_metric.grouper_type, 'null')
    left join yoy_metric
        on aggregate_map_base.metric_focus = yoy_metric.metric_focus
        and coalesce(rollup_metric.grouper_type, 'null')
        = coalesce(yoy_metric.grouper_type, 'null')
