select
    stg_encounter.visit_key,
    visit_ed_event.visit_ed_event_key as action_key,
    seq_num as action_seq_num,
    provider.prov_key,
    employee.emp_key,
    employee.full_nm as employee_name,
    visit_ed_event.event_dt as event_date,
    coalesce(master_event_type.event_disp_nm, master_event_type.event_nm) as event_description,
    stg_ipc_pat_prov_location_lookup.department_name as event_location,
    'visit_ed_event_key' as action_key_field
from
    {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on master_event_type.event_type_key = visit_ed_event.event_type_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit_ed_event.visit_key
    inner join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = visit_ed_event.event_init_emp_key
    left join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = employee.prov_key and provider.prov_key != 0
    left join {{ref('stg_ipc_pat_prov_location_lookup')}}
        as stg_ipc_pat_prov_location_lookup
            on stg_ipc_pat_prov_location_lookup.visit_key = visit_ed_event.visit_key
            and visit_ed_event.event_dt
                between stg_ipc_pat_prov_location_lookup.start_date and stg_ipc_pat_prov_location_lookup.end_date
where
    event_init_emp_key not in (0, -1)
