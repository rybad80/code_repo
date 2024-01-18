select
    *
from
    {{ref('stg_neuro_surgical_cases')}}
where
    epilepsy_surgical_dx_ind = 1
