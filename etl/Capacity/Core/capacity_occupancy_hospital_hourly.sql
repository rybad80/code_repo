with hospital_census as (
    select
        census_hour,
        census_date,
        department_center_abbr,
        -- remove SDU from count
        sum(case when department_id != 22 then 1 end) as census
    from
      {{ref('capacity_ip_hourly_census')}}
    group by
        census_hour,
        census_date,
        department_center_abbr
),

hospital_beds as (
    select
        capacity_licensed_bed_department.bed_count_date,
        sum(capacity_licensed_bed_department.licensed_bed_count) as beds_available,
        fact_department_rollup.department_center_abbr
    from
        {{ref('capacity_licensed_bed_department')}} as capacity_licensed_bed_department
        inner join {{source('cdw_analytics','fact_department_rollup')}} as fact_department_rollup
            on fact_department_rollup.dept_key = capacity_licensed_bed_department.dept_key
            and fact_department_rollup.dept_align_dt = capacity_licensed_bed_department.bed_count_date
    where
        department_id != 22 --SDU
    group by
        capacity_licensed_bed_department.bed_count_date,
        fact_department_rollup.department_center_abbr
),

hospital_occupancy as (
    select
        hospital_census.census_hour,
        hospital_census.census_date,
        hospital_census.census,
        hospital_beds.beds_available,
        case
          when hospital_beds.beds_available > 0
          then round(hospital_census.census / hospital_beds.beds_available, 3)
        end as occupancy,
        hospital_census.department_center_abbr
    from
        hospital_census
        left join hospital_beds
            on hospital_beds.bed_count_date = hospital_census.census_date
            and hospital_beds.department_center_abbr = hospital_census.department_center_abbr
)

select
    hospital_occupancy.census_hour,
    hospital_occupancy.census_date,
    hospital_occupancy.census,
    hospital_occupancy.beds_available,
    hospital_occupancy.occupancy,
    hospital_occupancy.department_center_abbr,
    capacity_occupancy_thresholds.threshold_color
from
    hospital_occupancy
    left join {{ref('capacity_occupancy_thresholds')}} as capacity_occupancy_thresholds
        on capacity_occupancy_thresholds.start_threshold <= hospital_occupancy.occupancy
        and capacity_occupancy_thresholds.next_threshold > hospital_occupancy.occupancy
