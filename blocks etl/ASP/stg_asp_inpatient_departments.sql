select
    visit_key,
    visit_event_key,
    hospital_admit_date,
    hospital_discharge_date,
    department_group_name,
    department_center_abbr,
    dept_enter_date as enter_date,
    dept_exit_date as exit_date
from
    {{ref('stg_adt_all')}}
where
    considered_ip_unit = 1
    and department_group_ind = 1
    and (
        date(hospital_discharge_date) >= '2013-07-01'
        or currently_admitted_ind = 1
    )
