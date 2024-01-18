select
    visit_key,
    hospital_discharge_date,
    admission_department_name,
    admission_department_group_name,
    admission_department_center_abbr,
    discharge_department_name,
    discharge_department_group_name,
    discharge_department_center_abbr,
    hospital_los_days,
    expected_hospital_los_days,
    los_elos_ratio,
    discharge_before_noon_ind,
    discharge_order_to_discharge_mins,
    COALESCE(discharge_service, 'Other') as discharge_service
from
    {{ref('capacity_ip_census_cohort')}}
where
    DATE_TRUNC('month', hospital_discharge_date) is not null
