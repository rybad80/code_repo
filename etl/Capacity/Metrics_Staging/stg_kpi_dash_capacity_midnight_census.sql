select 
    {{
        dbt_utils.surrogate_key([
            'lower(visit_key)',
             'lower(date_key)'
            ])
    }} as surrogate_key,
    visit_key,
    visit_event_key,
    pat_key,
    census_dept_key,
    department_group_key,
    midnight_date,
    date_key as dt_key,
    service,
    department_name,
    department_group_name,
    location_group_name,
    bed_care_group,
    department_center_abbr
from
    {{ref('capacity_ip_midnight_census')}} as capacity_ip_midnight_census
        inner join {{ref('dim_date')}} as dim_date
    on dim_date.full_date = capacity_ip_midnight_census.midnight_date
where
    midnight_date >= '2017-07-01'
