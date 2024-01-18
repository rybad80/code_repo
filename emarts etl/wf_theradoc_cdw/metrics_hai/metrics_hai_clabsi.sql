select
    *
from
    {{ref('stg_metrics_hai_clabsi_theradoc')}}

union all

select
    *
from
    {{ref('stg_metrics_hai_clabsi_bugsy')}}
