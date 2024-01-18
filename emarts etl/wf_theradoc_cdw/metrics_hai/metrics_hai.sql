select
    *
from
    {{ref('metrics_hai_cauti')}}

union all

select
    *
from
    {{ref('metrics_hai_clabsi')}}

union all

select
    *
from
    {{ref('metrics_hai_havi')}}

union all

select
    *
from
    {{ref('metrics_hai_ssi')}}

union all

select
    *
from
    {{ref('metrics_hai_vap')}}
