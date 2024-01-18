select
    *,
    'neuro_turnover' as turnover_metric_id,
    'neuro_vol_turnover' as voluntary_turnover_metric_id
from
    {{ ref('stg_metrics_turnover') }}
where
    sldb_categories_lower = 'neuroscience'
