select
    *
from
    {{ref('stg_kpi_dash_capacity_discharges')}}
where
    expected_hospital_los_days is not null
