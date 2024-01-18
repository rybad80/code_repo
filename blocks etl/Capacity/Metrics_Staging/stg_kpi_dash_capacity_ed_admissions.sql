select
  *
from
  {{ref("stg_kpi_dash_capacity_admissions")}}
where
  ed_ind = 1
