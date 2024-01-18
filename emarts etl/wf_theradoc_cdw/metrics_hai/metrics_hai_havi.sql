select
    *
from
    {{ref('stg_metrics_hai_havi_theradoc')}}

union all

select
    *
from
    {{ref('stg_metrics_hai_havi_bugsy')}}
