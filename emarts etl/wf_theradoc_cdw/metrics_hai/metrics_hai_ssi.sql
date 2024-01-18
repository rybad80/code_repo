select
    *
from
    {{ref('stg_metrics_hai_ssi_theradoc')}}

union all

select
    *
from
    {{ref('stg_metrics_hai_ssi_bugsy')}}
