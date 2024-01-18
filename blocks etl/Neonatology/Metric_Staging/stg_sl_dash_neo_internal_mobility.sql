select
    *,
    'neo_internal_mobility' as metric_id
from
    {{ ref('stg_metrics_hr_internal_mobility') }}
where
    sldb_categories_lower = 'neonatology'
