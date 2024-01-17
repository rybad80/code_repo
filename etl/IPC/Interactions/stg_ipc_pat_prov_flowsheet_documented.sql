select
    flowsheet_all.fs_rec_key,
    min(flowsheet_all.seq_num) as seq_num,
    min(flowsheet_all.visit_key) as visit_key,
    flowsheet_all.recorded_date as event_date,
    flowsheet_all.taken_by_employee_key,
    stg_ipc_pat_prov_flowsheet_lookup.flowsheet_interaction_type || ' documented' as event_description
from
    {{ref('flowsheet_all')}} as flowsheet_all
    inner join {{ref('stg_ipc_pat_prov_flowsheet_lookup')}} as stg_ipc_pat_prov_flowsheet_lookup
        on stg_ipc_pat_prov_flowsheet_lookup.fs_key = flowsheet_all.fs_key
where
    lower(flowsheet_all.taken_by_employee) not like 'intfusr%'
    and flowsheet_all.documented_by_employee_key != flowsheet_all.taken_by_employee_key
group by
    flowsheet_all.fs_rec_key,
    flowsheet_all.recorded_date,
    flowsheet_all.taken_by_employee_key,
    stg_ipc_pat_prov_flowsheet_lookup.flowsheet_interaction_type
