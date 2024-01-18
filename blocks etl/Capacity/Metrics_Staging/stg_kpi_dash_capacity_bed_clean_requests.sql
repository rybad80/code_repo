select
  visit_key,
  visit_event_key,
  bed_key,
  department_name,
  clean_request_date,
  department_group_name,
  clean_in_progess_to_clean_mins,
  clean_request_to_clean_target_ind,
  department_center_abbr
from
  {{ref('capacity_bed_clean_request_intervals')}}
where
  clean_request_to_clean_mins is not null
