select
    'Program-Specific: CTIS Surgical Procedures' as metric_name,
    primary_key as primary_key,
    surgery_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_ctis_surgery' as metric_id,
    primary_key as num
from
    {{ ref('ctis_surgery_timeline')}}
where
    metric_date < current_date
