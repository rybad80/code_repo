with metric_records as (
    select
        stg_pcoti_report_catcode_watcher.catcode_date as event_year_month,
        stg_pcoti_report_catcode_watcher.campus_name,
        stg_pcoti_report_catcode_watcher.department_group_name,
        stg_pcoti_report_catcode_watcher.watcher_prior_2hrs,
        stg_pcoti_report_catcode_watcher.watcher_prior_48hrs
    from
        {{ ref('stg_pcoti_report_catcode_watcher') }} as stg_pcoti_report_catcode_watcher
)

select
    stg_pcoti_metrics_date_spine.event_year_month,
    metric_records.campus_name,
    metric_records.department_group_name,
    sum(1) as denom,
    sum(
        coalesce(metric_records.watcher_prior_2hrs, 0)
    ) as watcher_prior_2hrs,
    sum(
        coalesce(metric_records.watcher_prior_48hrs, 0)
    ) as watcher_prior_48hrs
from
    {{ ref('stg_pcoti_metrics_date_spine') }} as stg_pcoti_metrics_date_spine
    left join metric_records
        on stg_pcoti_metrics_date_spine.event_year_month = metric_records.event_year_month
group by
    stg_pcoti_metrics_date_spine.event_year_month,
    metric_records.campus_name,
    metric_records.department_group_name
