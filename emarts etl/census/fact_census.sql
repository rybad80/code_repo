{{
    config(materialized = 'view')
}}

select
    dim_date.date_key as dt_key,
    capacity_ip_hourly_census.census_hour as time_key,
    capacity_ip_hourly_census.visit_event_key,
    adt_department.dept_key as seg_dept_key,
    capacity_ip_hourly_census.visit_key,
    adt_department.department_name as adt_dept,
    capacity_ip_hourly_census.department_group_key as census_dept_grp_key,
    capacity_ip_hourly_census.census_dept_key,
    capacity_ip_hourly_census.department_name as census_dept,
    capacity_ip_hourly_census.census_date as census_dt,
    capacity_ip_hourly_census.census_hour as census_hr,
    adt_department.enter_date as eff_event_dt,
    adt_department.exit_date - cast('1 minute' as interval) as end_event_dt,
    'ENTERPRISE_MARTS' as create_by,
    now() as create_dt,
    'ENTERPRISE_MARTS' as upd_by,
    now() as upd_dt
from
    {{source('chop_analytics','capacity_ip_hourly_census')}} as capacity_ip_hourly_census
    inner join {{source('chop_analytics','dim_date')}} as dim_date
        on dim_date.full_date = capacity_ip_hourly_census.census_date
    inner join {{source('chop_analytics','adt_department')}} as adt_department
        on adt_department.visit_event_key = capacity_ip_hourly_census.visit_event_key
