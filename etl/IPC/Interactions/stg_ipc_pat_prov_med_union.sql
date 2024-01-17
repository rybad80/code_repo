select
    *
from
    {{ref('stg_ipc_pat_prov_med_ordered')}} as stg_ipc_pat_prov_med_ordered --noqa: L025
union all
select
    *
from
    {{ref('stg_ipc_pat_prov_med_authorized')}} as stg_ipc_pat_prov_med_authorized --noqa: L025 
union all
select
    *
from
    {{ref('stg_ipc_pat_prov_med_prescribed')}} as stg_ipc_pat_prov_med_prescribed --noqa: L025
union all
select
    *
from
    {{ref('stg_ipc_pat_prov_med_administered')}} as stg_ipc_pat_prov_med_administered --noqa: L025
