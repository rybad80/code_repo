select *
from {{ref('ed_events')}}
where
    event_category = 'medication_details'
    and event_repeat_number = 1
