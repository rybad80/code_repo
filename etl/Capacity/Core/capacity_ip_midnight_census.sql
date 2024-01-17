select
    visit_key,
    encounter_key,
    visit_event_key,
    pat_key,
    patient_key,
    census_dept_key,
    department_id,
    department_group_key,
    census_date as midnight_date,
    service,
    department_name,
    department_group_name,
    location_group_name,
    bed_care_group,
    department_center_abbr
from
    {{ref('capacity_ip_hourly_census')}}
where
    census_hour = 0
