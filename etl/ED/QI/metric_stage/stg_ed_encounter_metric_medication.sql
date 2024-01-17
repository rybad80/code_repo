select *
from {{ref('ed_events')}}
where
    event_category = 'medication'
    and event_repeat_number = 1
