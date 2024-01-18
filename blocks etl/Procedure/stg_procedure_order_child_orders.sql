select
    proc_child.proc_ord_key,
    proc_parent.proc_ord_key as proc_ord_parent_key,
    proc_parent.placed_dt as parent_placed_dt,
    dept_ord_parent.dept_key as parent_dept_key,
    dept_ord_parent.dept_nm as parent_department_name,
    coalesce(provider.prov_key, 0) as parent_ord_prov_key,
    provider.full_nm as parent_ord_provider_name,
    protocol.ptcl_nm as orderset_name
from
    {{source('cdw', 'procedure_order')}} as proc_child
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = proc_child.visit_key
    inner join {{source('cdw', 'procedure_order')}} as proc_parent
        on proc_parent.proc_ord_key = proc_child.proc_ord_parent_key
    inner join {{source('cdw', 'protocol')}} as protocol
        on protocol.ptcl_key = proc_parent.ptcl_key
    inner join {{source('cdw', 'department')}} as dept_ord_parent
        on dept_ord_parent.dept_key = proc_parent.pat_loc_dept_key
    left join {{source('cdw', 'hospital_procedure_order')}} as hosp_proc_parent
        on hosp_proc_parent.proc_ord_key = proc_child.proc_ord_parent_key
    left join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = proc_parent.ordering_prov_key
where
    proc_child.proc_ord_rec_type = 'C'
