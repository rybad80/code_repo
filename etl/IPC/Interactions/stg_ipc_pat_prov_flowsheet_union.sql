with interactions as (
    select
        stg_ipc_pat_prov_flowsheet_taken.fs_rec_key as action_key,
        stg_ipc_pat_prov_flowsheet_taken.seq_num as action_seq_num,
        stg_ipc_pat_prov_flowsheet_taken.event_date,
        stg_ipc_pat_prov_flowsheet_taken.visit_key,
        stg_ipc_pat_prov_flowsheet_taken.taken_by_employee_key as emp_key,
        stg_ipc_pat_prov_flowsheet_taken.event_description
    from
        {{ref('stg_ipc_pat_prov_flowsheet_taken')}} as stg_ipc_pat_prov_flowsheet_taken
    union all
    select
        stg_ipc_pat_prov_flowsheet_documented.fs_rec_key as action_key,
        stg_ipc_pat_prov_flowsheet_documented.seq_num as action_seq_num,
        stg_ipc_pat_prov_flowsheet_documented.event_date,
        stg_ipc_pat_prov_flowsheet_documented.visit_key,
        stg_ipc_pat_prov_flowsheet_documented.taken_by_employee_key as emp_key,
        stg_ipc_pat_prov_flowsheet_documented.event_description
    from
        {{ref('stg_ipc_pat_prov_flowsheet_documented')}} as stg_ipc_pat_prov_flowsheet_documented
)

select
    interactions.action_key,
    interactions.action_seq_num,
    interactions.emp_key,
    coalesce(employee.prov_key, 0) as prov_key,
    employee.full_nm as employee_name,
    interactions.event_date,
    interactions.event_description,
    stg_ipc_pat_prov_location_lookup.department_name as event_location,
    interactions.visit_key,
    'fs_rec_key' as action_key_field
from
    interactions
    inner join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = interactions.emp_key
    left join {{ref('stg_ipc_pat_prov_location_lookup')}} as stg_ipc_pat_prov_location_lookup
        on stg_ipc_pat_prov_location_lookup.visit_key = interactions.visit_key
        and interactions.event_date
            between stg_ipc_pat_prov_location_lookup.start_date and stg_ipc_pat_prov_location_lookup.end_date
