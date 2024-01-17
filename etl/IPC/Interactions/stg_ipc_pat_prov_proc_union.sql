select
    stg_ipc_pat_prov_proc_ordered.*
from
    {{ref('stg_ipc_pat_prov_proc_ordered')}} as stg_ipc_pat_prov_proc_ordered --noqa: L025
union all
select
    stg_ipc_pat_prov_proc_authorized.*
from
    {{ref('stg_ipc_pat_prov_proc_authorized')}} as stg_ipc_pat_prov_proc_authorized --noqa: L025
