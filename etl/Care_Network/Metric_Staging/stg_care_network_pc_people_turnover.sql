select
    stg_metrics_turnover.*,
    'pc_people_turnover' as turnover_metric_id,
    'pc_people_voluntary_turnover' as voluntary_turnover_metric_id

from
    {{ref('stg_metrics_turnover')}} as stg_metrics_turnover
where
    stg_metrics_turnover.sldb_categories_lower = 'care network'
