select
   date_key as dt_key,
    census_date,
    peak_occupancy,
    case when lower(threshold_color) = 'red' then 1 else 0 end as threshold_red_ind
 from
    {{ref('capacity_occupancy_enterprise_daily_peak')}} as capacity_occupancy_enterprise_daily_peak
    inner join {{ref('dim_date')}} as dim_date
    on dim_date.full_date = capacity_occupancy_enterprise_daily_peak.census_date
