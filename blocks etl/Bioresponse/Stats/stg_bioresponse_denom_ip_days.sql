with census_days as ( -- noqa: PRS
    select
        capacity_ip_midnight_census.midnight_date as stat_date,
        case
            when capacity_ip_midnight_census.department_center_abbr like 'KOPH%'
            then 'KOP' else 'PHL'
        end as campus
    from
        {{ ref('capacity_ip_midnight_census') }} as capacity_ip_midnight_census
    where
        capacity_ip_midnight_census.midnight_date >= {{ var('start_data_date') }}
)

select
    census_days.stat_date,
    census_days.campus,
    sum(1) as stat_denominator_val
from
    census_days
group by
    census_days.stat_date,
    census_days.campus
