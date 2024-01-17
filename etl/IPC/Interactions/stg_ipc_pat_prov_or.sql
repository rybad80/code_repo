with or_staff as (
        select distinct
            or_log_staff.log_key,
            stg_ipc_pat_prov_employee_lookup.employee_name,
            stg_ipc_pat_prov_employee_lookup.emp_key,
            stg_ipc_pat_prov_employee_lookup.prov_key,
            'OR Staff' as staff_group
        from
            {{source('cdw','or_log_staff')}} as or_log_staff
            inner join {{ref('stg_ipc_pat_prov_employee_lookup')}} as stg_ipc_pat_prov_employee_lookup
                on stg_ipc_pat_prov_employee_lookup.prov_key = or_log_staff.surg_prov_key
),

anes_staff as (
    select distinct
        or_log_anes_staff.log_key,
        stg_ipc_pat_prov_employee_lookup.employee_name,
        stg_ipc_pat_prov_employee_lookup.emp_key,
        stg_ipc_pat_prov_employee_lookup.prov_key,
        'Anesthesia' as staff_group
    from
        {{source('cdw','or_log_anes_staff')}} as or_log_anes_staff
        inner join {{ref('stg_ipc_pat_prov_employee_lookup')}} as stg_ipc_pat_prov_employee_lookup
            on stg_ipc_pat_prov_employee_lookup.prov_key = or_log_anes_staff.anes_prov_key
),

surg_staff as (
    select distinct
        or_log_surgeons.log_key,
        stg_ipc_pat_prov_employee_lookup.employee_name,
        stg_ipc_pat_prov_employee_lookup.emp_key,
        stg_ipc_pat_prov_employee_lookup.prov_key,
        'Surgeon' as staff_group
    from {{source('cdw','or_log_surgeons')}} as or_log_surgeons
        inner join {{ref('stg_ipc_pat_prov_employee_lookup')}} as stg_ipc_pat_prov_employee_lookup
            on stg_ipc_pat_prov_employee_lookup.prov_key = or_log_surgeons.surg_prov_key
),

staff as (
    select * from or_staff
    union all
    select * from anes_staff
    union all
    select * from surg_staff
)

select
    surgery_encounter.visit_key,
    surgery_encounter.or_key as action_key,
    1 as action_seq_num,
    staff.emp_key,
    staff.prov_key,
    staff.employee_name,
    surgery_encounter.surgery_date as event_date,
    'OR' as event_description,
    surgery_encounter.location as event_location,
    'or_key' as action_key_field

from
    {{ref('surgery_encounter')}} as surgery_encounter
    inner join staff on staff.log_key = surgery_encounter.or_key
