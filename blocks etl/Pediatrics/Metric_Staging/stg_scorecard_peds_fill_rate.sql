select
    stg_metrics_fill_rate.primary_key,
    stg_metrics_fill_rate.metric_date,
    stg_metrics_fill_rate.num,
    stg_metrics_fill_rate.denom
from
	{{ref('stg_metrics_fill_rate')}} as stg_metrics_fill_rate
where
	lower(stg_metrics_fill_rate.revenue_location_group) = 'chca'
	and stg_metrics_fill_rate.metric_date >= '01/01/2019'
