select
    *
from
    {{ref('stg_metrics_hai_vap_theradoc')}}

union all

select
    *
from
    {{ref('stg_metrics_hai_vap_bugsy')}}
