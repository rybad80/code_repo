select
    subdiv_type,
    subdiv_code,
    sum(1) as count_pats,
    sum(1) / 1000 as count_thousand_pats
from
    {{ ref('stg_equity_geos_pivot') }}
group by
    subdiv_type,
    subdiv_code
