select
    *,
    'neo_turnover' as turnover_metric_id,
    'neo_vol_turnover' as voluntary_turnover_metric_id
from
    {{ ref('stg_metrics_turnover') }}
where
    sldb_categories_lower = 'neonatology'
