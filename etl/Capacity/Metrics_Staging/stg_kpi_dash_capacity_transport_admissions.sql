select
  *
from
  {{ref("stg_kpi_dash_capacity_admissions")}}
where
  transport_ind = 1
