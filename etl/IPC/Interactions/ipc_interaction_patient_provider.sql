with employee_raw as (
    select distinct
        stg_ipc_pat_prov_union_all.emp_key,
        stg_ipc_pat_prov_union_all.prov_key,
        stg_ipc_pat_prov_union_all.employee_name
    from
        {{ref('stg_ipc_pat_prov_union_all')}} as stg_ipc_pat_prov_union_all
),

employee_no_keys as (
    select
        *,
        row_number()  over (order by employee_raw.employee_name) + 99000000000 as dummy_prov_key
    from
        employee_raw
),

employee_unique as (
    select
        employee_no_keys.emp_key,
        coalesce(employee_no_keys.prov_key, employee_no_keys.dummy_prov_key) as prov_key,
        employee_no_keys.employee_name,
        row_number() over (
            partition by coalesce(employee_no_keys.prov_key, employee_no_keys.dummy_prov_key
        ) order by employee_no_keys.employee_name) as employee_rank
    from employee_no_keys
)

select
    stg_ipc_pat_prov_union_all.visit_key,
    stg_ipc_pat_prov_union_all.action_key,
    stg_ipc_pat_prov_union_all.action_seq_num,
    stg_ipc_pat_prov_final_columns.patient_name,
    stg_ipc_pat_prov_final_columns.mrn,
    stg_ipc_pat_prov_final_columns.dob,
    stg_ipc_pat_prov_final_columns.csn,
    stg_ipc_pat_prov_final_columns.sex,
    stg_ipc_pat_prov_final_columns.age_years,
    stg_ipc_pat_prov_final_columns.primary_care_provider,
    stg_patient.mailing_address_line1,
    stg_patient.mailing_address_line2,
    stg_patient.mailing_city,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.county,
    patient.home_ph as home_phone_number,
    stg_ipc_pat_prov_final_columns.appointment_made_date,
    stg_ipc_pat_prov_final_columns.appointment_status,
    stg_ipc_pat_prov_final_columns.check_in_date,
    stg_ipc_pat_prov_final_columns.time_in_room,
    stg_ipc_pat_prov_final_columns.reason_name,
    coalesce(prov.employee_name, emp.employee_name) as employee_name,
    stg_ipc_pat_prov_union_all.event_date,
    stg_ipc_pat_prov_union_all.event_description,
    stg_ipc_pat_prov_union_all.event_location,
    stg_ipc_pat_prov_union_all.action_key_field,
    stg_ipc_pat_prov_union_all.event_type,
    stg_ipc_pat_prov_final_columns.pat_key,
    stg_ipc_pat_prov_union_all.prov_key,
    stg_ipc_pat_prov_union_all.emp_key,
    employee.ad_login
from
    {{ref('stg_ipc_pat_prov_union_all')}} as stg_ipc_pat_prov_union_all
    inner join {{ref('stg_ipc_pat_prov_final_columns')}} as stg_ipc_pat_prov_final_columns
        on stg_ipc_pat_prov_final_columns.visit_key = stg_ipc_pat_prov_union_all.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stg_ipc_pat_prov_final_columns.pat_key
    inner join {{source('cdw', 'patient')}} as patient
        on stg_patient.pat_key = patient.pat_key
    left join employee_unique as emp
        on stg_ipc_pat_prov_union_all.emp_key = emp.emp_key
        and emp.employee_rank = 1
    left join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = stg_ipc_pat_prov_union_all.emp_key
    left join employee_unique as prov
        on stg_ipc_pat_prov_union_all.prov_key = prov.prov_key
        and prov.employee_rank = 1
where
    {{ limit_dates_for_dev(ref_date = 'stg_ipc_pat_prov_union_all.event_date') }}
