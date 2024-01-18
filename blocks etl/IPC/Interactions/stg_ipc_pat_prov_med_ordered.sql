with med_orders as (
    select
        medication_order.visit_key,
        medication_order.med_ord_prov_key as prov_key,
        medication_order.med_ord_create_dt as event_date,
        medication_order.pat_loc_dept_key,
        min(medication_order.med_ord_key) as action_key
    from
        {{source('cdw', 'medication_order')}} as medication_order
    where
        medication_order.med_ord_prov_key != 0
    group by
        medication_order.visit_key,
        medication_order.med_ord_prov_key,
        medication_order.med_ord_create_dt,
        medication_order.pat_loc_dept_key
)

select
    med_orders.visit_key,
    med_orders.action_key,
    1 as action_seq_num,
    med_orders.prov_key,
    stg_ipc_pat_prov_employee_lookup.emp_key,
    stg_ipc_pat_prov_employee_lookup.employee_name,
    med_orders.event_date,
    'medication ordered' as event_description,
    department.dept_nm as event_location,
    'med_ord_key' as action_key_field
from
    med_orders
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = med_orders.pat_loc_dept_key
    inner join {{ref('stg_ipc_pat_prov_employee_lookup')}} as stg_ipc_pat_prov_employee_lookup
        on stg_ipc_pat_prov_employee_lookup.prov_key = med_orders.prov_key
