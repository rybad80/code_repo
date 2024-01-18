with department_group_census as (
    select
        census_hour,
        census_date,
        census_dept_key,
        department_group_key,
        department_group_name,
        department_center_abbr,
        count(*) as census
    from
        {{ref('capacity_ip_hourly_census')}}
    group by
        census_hour,
        census_date,
        census_dept_key,
        department_group_key,
        department_group_name,
        department_center_abbr
),

department_group_beds as (
    select
        bed_count_date,
        dept_key,
        sum(
            case
              when department_id != 22
              then licensed_bed_count
            end
        ) as beds_available
    from
        {{ref('capacity_licensed_bed_department')}}
    group by
        bed_count_date,
        dept_key

),

department_group_occupancy as (
    select
        department_group_census.census_hour,
        department_group_census.census_date,
        department_group_census.department_group_key,
        department_group_census.department_group_name,
        department_group_census.department_center_abbr,
        sum(department_group_census.census) as census,
        sum(department_group_beds.beds_available) as beds_available,
        case
          when sum(department_group_beds.beds_available) > 0
          then round(sum(department_group_census.census) / sum(department_group_beds.beds_available), 3)
          end as occupancy
    from
        department_group_census
        left join department_group_beds
            on department_group_beds.dept_key = department_group_census.census_dept_key
            and department_group_beds.bed_count_date = department_group_census.census_date
    group by
        department_group_census.census_hour,
        department_group_census.census_date,
        department_group_census.department_group_key,
        department_group_census.department_group_name,
        department_group_census.department_center_abbr
)

select
    department_group_occupancy.census_hour,
    department_group_occupancy.census_date,
    department_group_occupancy.department_group_key,
    department_group_occupancy.department_group_name,
    department_group_occupancy.department_center_abbr,
    department_group_occupancy.census,
    department_group_occupancy.beds_available,
    department_group_occupancy.occupancy,
    capacity_occupancy_thresholds.threshold_color
from
    department_group_occupancy
    left join {{ref('capacity_occupancy_thresholds')}} as capacity_occupancy_thresholds
        on capacity_occupancy_thresholds.start_threshold <= department_group_occupancy.occupancy
        and capacity_occupancy_thresholds.next_threshold > department_group_occupancy.occupancy
