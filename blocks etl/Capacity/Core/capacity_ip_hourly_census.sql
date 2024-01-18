with current_service as (
    select
        adt_service.encounter_key,
        adt_service.service,
        adt_service.service_start_datetime,
        coalesce(adt_service.service_end_datetime, current_date) as service_end_datetime_or_current_datetime
    from
        {{ref('adt_service')}} as adt_service
    where
        coalesce(adt_service.hospital_discharge_date, current_date) >= '2014-01-01'
        and adt_service.hospital_admit_date > '2012-01-01'
)

select
    stg_capacity_census_department.visit_key,
    stg_capacity_census_department.encounter_key,
    stg_capacity_census_department.visit_event_key,
    stg_capacity_census_department.pat_key,
    stg_capacity_census_department.patient_key,
    stg_capacity_census_department.census_dept_key,
    stg_capacity_census_department.department_id,
    stg_capacity_census_department.department_group_key,
    dim_date.full_date as census_date,
    master_time.time_key as census_hour,
    current_service.service,
    stg_capacity_census_department.department_name,
    stg_capacity_census_department.department_group_name,
    stg_capacity_census_department.location_group_name,
    stg_capacity_census_department.bed_care_group,
    stg_capacity_census_department.department_center_abbr
from
    {{ref('stg_capacity_census_department')}} as stg_capacity_census_department
    inner join {{ref('dim_date')}} as dim_date
        on dim_date.full_date >= date(stg_capacity_census_department.enter_date)
        and dim_date.full_date <= date(stg_capacity_census_department.exit_date_or_current_date)
    inner join {{source('cdw','master_time')}} as master_time
        on dim_date.full_date + master_time.full_time_24 >= stg_capacity_census_department.enter_date
        and dim_date.full_date + master_time.full_time_24
            < stg_capacity_census_department.exit_date_or_current_date
    left join current_service
        on current_service.encounter_key = stg_capacity_census_department.encounter_key
        and dim_date.full_date + master_time.full_time_24 >= current_service.service_start_datetime
        and dim_date.full_date + master_time.full_time_24
            < current_service.service_end_datetime_or_current_datetime
