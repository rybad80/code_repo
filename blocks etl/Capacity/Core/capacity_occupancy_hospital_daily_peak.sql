with daily_peak_census as (
    select
        capacity_occupancy_hospital_hourly.census_date,
        capacity_occupancy_hospital_hourly.department_center_abbr,
        max(capacity_occupancy_hospital_hourly.beds_available) as beds_available,
        max(capacity_occupancy_hospital_hourly.census) as peak_census
    from
        {{ref('capacity_occupancy_hospital_hourly')}} as capacity_occupancy_hospital_hourly
    group by
        capacity_occupancy_hospital_hourly.census_date,
        capacity_occupancy_hospital_hourly.department_center_abbr
),

daily_peak_occupancy as (
    select
        census_date,
        peak_census,
        beds_available,
        case
          when beds_available > 0
          then round(peak_census / beds_available, 3)
        end as peak_occupancy,
        --round(peak_census / beds_available, 3) as peak_occupancy,
        department_center_abbr
    from
        daily_peak_census
)

select
    daily_peak_occupancy.census_date,
    daily_peak_occupancy.peak_census,
    daily_peak_occupancy.beds_available,
    daily_peak_occupancy.peak_occupancy,
    daily_peak_occupancy.department_center_abbr,
    capacity_occupancy_thresholds.threshold_color
from
    daily_peak_occupancy
    left join {{ref('capacity_occupancy_thresholds')}} as capacity_occupancy_thresholds
        on capacity_occupancy_thresholds.start_threshold <= daily_peak_occupancy.peak_occupancy
        and capacity_occupancy_thresholds.next_threshold > daily_peak_occupancy.peak_occupancy
