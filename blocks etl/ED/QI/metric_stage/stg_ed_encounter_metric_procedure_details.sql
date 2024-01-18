select
  stg_ed_events_procedure_details.*
from
  {{ref('stg_ed_events_procedure_details')}} as stg_ed_events_procedure_details
where
    stg_ed_events_procedure_details.event_category = 'procedure_details'
    and stg_ed_events_procedure_details.event_repeat_number = 1
