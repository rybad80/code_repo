with proc_ord as (
    select
        procedure_order_clinical.visit_key,
        procedure_order.auth_prov_key as prov_key,
        procedure_order_clinical.placed_date as event_date,
        case
            when lower(procedure_order_clinical.department_name) = 'unknown'
            then procedure_order_clinical.parent_department_name
            else procedure_order_clinical.department_name
        end as event_location,
        min(procedure_order_clinical.proc_ord_key) as action_key
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{source('cdw', 'procedure_order')}} as procedure_order
            on procedure_order_clinical.proc_ord_key = procedure_order.proc_ord_key
    where
        procedure_order.auth_prov_key != 0
        and procedure_order.auth_prov_key != procedure_order.ordering_prov_key
    group by
        procedure_order_clinical.visit_key,
        procedure_order.auth_prov_key,
        procedure_order_clinical.placed_date,
        case
            when lower(procedure_order_clinical.department_name) = 'unknown'
            then procedure_order_clinical.parent_department_name else procedure_order_clinical.department_name
        end
)

select
    proc_ord.visit_key,
    proc_ord.action_key,
    1 as action_seq_num,
    proc_ord.prov_key,
    stg_ipc_pat_prov_employee_lookup.emp_key,
    stg_ipc_pat_prov_employee_lookup.employee_name,
    proc_ord.event_date,
    'procedure authorized' as event_description,
    proc_ord.event_location,
    'proc_ord_key' as action_key_field
from
    proc_ord
    inner join {{ref('stg_ipc_pat_prov_employee_lookup')}} as stg_ipc_pat_prov_employee_lookup
        on stg_ipc_pat_prov_employee_lookup.prov_key = proc_ord.prov_key
