select *
from {{ref('ed_events')}}
where
    event_category = 'pathway_ordered'
    and event_repeat_number = 1
