select
    stg_encounter.visit_key,
    visit_treatment.prov_key as action_key,
    visit_treatment.seq_num as action_seq_num,
    visit_treatment.prov_key,
    employee.emp_key,
    provider.full_nm as employee_name,
    visit_treatment.prov_start_dt as event_date,
    'treatment team assign' as event_description,
    stg_ipc_pat_prov_location_lookup.department_name as event_location,
    'prov_key' as action_key_field
from
    {{source('cdw', 'visit_treatment')}} as visit_treatment
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit_treatment.visit_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = visit_treatment.prov_key
    inner join {{source('cdw', 'employee')}} as employee
        on employee.prov_key = visit_treatment.prov_key
    left join {{ref('stg_ipc_pat_prov_location_lookup')}} as stg_ipc_pat_prov_location_lookup
            on stg_ipc_pat_prov_location_lookup.visit_key = visit_treatment.visit_key
            and visit_treatment.prov_start_dt between stg_ipc_pat_prov_location_lookup.start_date
            and stg_ipc_pat_prov_location_lookup.end_date
where
    provider.prov_type != 'Resource'
    and visit_treatment.prov_key != 0
