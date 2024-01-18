select
    *
from
    {{ref('stg_metrics_hai_cauti_theradoc')}}

union all

select
    *
from
    {{ref('stg_metrics_hai_cauti_bugsy')}}
